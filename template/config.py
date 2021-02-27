import os
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
MergePolicy = 10
PagesPerPageRange = 16
# records per base page * number of base pages per range = records per page range
RecordsPerPageRange = int(PagesPerPageRange * ElementsPerPhysicalPage)

INVALID = 72057594037927935 #(max int for 7 byes, Hexadecimal: 0xFFFFFFFFFFFFFF)

BufferpoolSize = 16

'''
our bufferpool will act as the intermediary between the physical table and our operations
so when we insert: we create a page in memeory and perform operations on it
when that page is kicked out (kick()) of the bufferpool, we write it onto the disk
    pages will created in order in the bufferpool, and then written to physical memory in order 
    (must be pinned to be kicked, must be dirty to be written)

updates will have to pull the physical base page and then tail pages into memory/create a tail page in memory, and then update the record

deletions will function similarly to updates
'''

# global must be defined after class definition (its just under it)
class Bufferpool():
    def __init__(self):
        self.bufferpool = [None]*BufferpoolSize
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
                    # self.refresh(index) #on the way in, we set the age to 1 and update the other ages
                    return index
        return None


global BP
BP = Bufferpool()
