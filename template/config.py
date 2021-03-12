import os
import threading
# Global Setting for the Database
# PageSize, StartRID, etc..

# Element(byte, byte, byte, byte, byte, byte, byte, '\x00')                     # 8 "bytes" in one "element" Note that only 7 of the bytes can be written to!
# PhysicalPage(Element, Element, Element, ...)                                  # 512 "element"s in one "PhysicalPage"
# BasePage(PhysicalPage, PhysicalPage, PhysicalPage, ...)                       # 9 (4 are meta filled, 5 are data filled) "PhysicalPage"s in one "BasePage"
# PageRange(BasePage, BasePage, BasePage, ...)                                  # 16 "BasePage"s in one "PageRange"
def init():
    pass
BytesPerElement = 8
PhysicalPageBytes = 4096
# aka records per base page
ElementsPerPhysicalPage = int(PhysicalPageBytes /  BytesPerElement)
MetaElements = 4
# When we get 10 filled up tail pages, merge
MergePolicy = 25
PagesPerPageRange = 16
# records per base page * number of base pages per range = records per page range
RecordsPerPageRange = int(PagesPerPageRange * ElementsPerPhysicalPage)

BufferpoolSize = 16

INVALID = 72057594037927935 #(max int for 7 byes, Hexadecimal: 0xFFFFFFFFFFFFFF)

threads = []

# global must be defined after class definition (its just under it)
class Bufferpool():
    def __init__(self):
        self.bufferpool = [None]*BufferpoolSize
        self.latch = threading.Lock()
        pass

    def BufferpoolIsFull(self):
        return not any(spot is None for spot in self.bufferpool)

    # this way passes in an index
    def refresh(self, index):
        #Find the Page that has this path in the bufferpool, then refresh its age, and increment the other pages in bufferpool age
        for spot in self.bufferpool:
            if not spot is None:
                if spot.path == self.bufferpool[index].path:
                    spot.age = 1
                    spot.pinned += 1
                else:
                    spot.age += 1
        return index

    def add(self, page):
        if (self.BufferpoolIsFull()):
            self.kick()
        for index, spot in enumerate(self.bufferpool):
            if spot is None:
                self.bufferpool[index] = page
                self.refresh(index) #on the way in, we set the age to 1 and update the other ages
                return index
        raise Exception("Bufferpool Error: couldn't find empty spot after kicking.")

    def kick(self):
        oldest = 0 # note that the minimum age is 1
        for index, spot in enumerate(self.bufferpool):
            if not spot is None:
                if (spot.pinned == 0):
                    if(self.bufferpool[index].age > oldest):
                        oldest = self.bufferpool[index].age
                        index_oldest = index
        if oldest == 0:
            raise Exception("Bufferpool Error: all pages in the bufferpool are pinned.")
        kicked = self.bufferpool[index_oldest]
        kicked.age = 1 #on the way out, we set the age to 1
        self.bufferpool[index_oldest] = None
        if (kicked.dirty):
            if not os.path.exists(kicked.path):
                os.mkdir(kicked.path)
            kicked.writePageToDisk(kicked.path)

    def kickAll(self):
        empty = [None]*BufferpoolSize
        while self.bufferpool != empty:
            self.kick()

    def pathInBP(self, path):
        for index, spot in enumerate(self.bufferpool):
            if not spot is None:
                if (spot.path == path):
                    return index
        return None

global BP
BP = Bufferpool()

# whole record locked from base record
class RecordLock():
    def __init__(self):
        self.sLocks = 0
        self.xLocks =  0
        self.isShrinking = False
        self.inUseBy = []

# lock then unlock all functions since they are critical sections in a shared data structure
class LockManager():
    def __init__(self):
        # hash table mapping RIDs to list of S lock, X lock, bool isShrinkingPhase
        # - if we see there's already an exclusive lock on that RID, we abort
        # - otherwise, increment number of shared locks
        self.latch = threading.Lock()
        self.KeytoLocks = {}
        self.transactionID = -1  # number of transactions holding a lock

    def getTransactionID(self):
        self.transactionID += 1
        return self.transactionID

    # return false if X lock already present or we're in shrinking phase
    # - once one shared lock is given up, all of them have to be given up before more can be given out
    #       i. This is so Xlocks can be given out at some point
    def obtainSLock(self, Key, transactionID):
        giveLock = False
        if Key not in self.KeytoLocks:
            self.KeytoLocks[Key] = RecordLock()
            self.KeytoLocks[Key].sLocks += 1
            self.KeytoLocks[Key].inUseBy.append(transactionID)
            # already has lock
            return True

        if self.KeytoLocks[Key].isShrinking:
            # cannot give lock when lock is shrinking
            return False

        # if there is not an xLock
        if self.KeytoLocks[Key].xLocks == 0:
            if transactionID not in self.KeytoLocks[Key].inUseBy:
                self.KeytoLocks[Key].inUseBy.append(transactionID)
                self.KeytoLocks[Key].sLocks += 1
            giveLock = True

        # if there is an xLock
        elif self.KeytoLocks[Key].xLocks == 1:
            if transactionID in self.KeytoLocks[Key].inUseBy:
                self.KeytoLocks[Key].sLocks += 1
                giveLock = True

        return giveLock

    # return false if X or S lock already present
    def obtainXLock(self, Key, transactionID):
        giveLock = False

        if Key not in self.KeytoLocks:
            self.KeytoLocks[Key] = RecordLock()
            self.KeytoLocks[Key].xLocks = 1
            self.KeytoLocks[Key].inUseBy.append(transactionID)
            return True

        if self.KeytoLocks[Key].isShrinking:
            # cannot give lock when lock is shrinking
            return False

        #if there are no X locks
        if self.KeytoLocks[Key].xLocks == 0:
            # and no S locks, give out loc
            if self.KeytoLocks[Key].sLocks == 0:
                self.KeytoLocks[Key].xLocks = 1
                self.KeytoLocks[Key].inUseBy.append(transactionID)
                giveLock = True
            # and there is an s Lock, then check what's using lock
            elif self.KeytoLocks[Key].sLocks == 1:
                if transactionID in self.KeytoLocks[Key].inUseBy:
                    self.KeytoLocks[Key].xLocks = 1
                    self.KeytoLocks[Key].inUseBy.append(transactionID)
                    giveLock = True

        # if there is an x lock already then any s locks are from the same transaction
        elif self.KeytoLocks[Key].xLocks == 1:
            if transactionID in self.KeytoLocks[Key].inUseBy:
                giveLock = True

        return giveLock

    # Initiate shrinking phase
    # If num S locks == 0, set shrinkingPhase to false
    def giveUpSLock(self, Key, transactionID):
        removeLock = False

        if Key not in self.KeytoLocks:
            return True
        if (self.KeytoLocks[Key].sLocks > 0):
            self.KeytoLocks[Key].isShrinking = True
            if transactionID in self.KeytoLocks[Key].inUseBy:
                self.KeytoLocks[Key].inUseBy.remove(transactionID)
            self.KeytoLocks[Key].sLocks = self.KeytoLocks[Key].sLocks - 1
            if (self.KeytoLocks[Key].sLocks == 0):
                self.KeytoLocks[Key].isShrinking = False
            removeLock = True
        return removeLock


    def giveUpXLock(self, Key, transactionID):
        removeLock = False
        if Key not in self.KeytoLocks:
            return True
        if (self.KeytoLocks[Key].xLocks == 1):
            if transactionID in self.KeytoLocks[Key].inUseBy:
                self.KeytoLocks[Key].inUseBy.remove(transactionID)
            self.KeytoLocks[Key].xLocks = 0
            removeLock = True

        return removeLock


global LM
LM = LockManager()
