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

BufferpoolSize = 16

# global must be defined after class definition (its just under it)
class Bufferpool():
    def __init__(self):
        self.bufferpool = []
        pass

    def BufferpoolIsFull(self):
        return len(self.bufferpool) >= BufferpoolSize

    def refresh(self, index):
        page = self.bufferpool.pop(index)
        page.pinned += 1
        self.bufferpool.append(page)
        return len(self.bufferpool) - 1

    def add(self, page):
        if (self.BufferpoolIsFull()):
            self.kick()
        self.bufferpool.append(page)
        page.pinned += 1
        return len(self.bufferpool) - 1

    # 1. Update page meta file with meta information
    def kick(self):
        # called when we need to kick a page
        for index in range(0, len(self.bufferpool)):
            if (self.bufferpool[index].pinned == 0):
                kicked = self.bufferpool.pop(0)
                if (kicked.dirty):
                    if not os.path.exists(kicked.path):
                        os.mkdir(kicked.path)
                    kicked.writePageToDisk(kicked.path)
                return
        raise Exception("Bufferpool: all pages in the bufferpool are pinned.")

    def kickAll(self):
        while len(self.bufferpool) > 0:
            self.kick()

    def pathInBP(self, path):
        index = len(self.bufferpool) - 1
        while(index >= 0):
            if self.bufferpool[index].path == path:
                return index
            index -= 1
        return None

    '''
    our bufferpool will act as the intermediary between the physical table and our operations
    so when we insert: we create a page in memeory and perform operations on it
    when that page is kicked out (kick()) of the bufferpool, we write it onto the disk
        pages will created in order in the bufferpool, and then written to physical memory in order (unless pinned)

    updates will have to pull the physical base page and then tail pages into memory/create a tail page in memory, and then update the record

    deletions will function similarly to updates

    base functionality
        rewrite all function to hook into bufferpool
    merge/dirty functionality
    pinning pages, locking from getting kicked
    '''
global BP
BP = Bufferpool()












#going to attempt to remake this so that ages, and pinned work much better
#getting rid of the queue aspect

#1. look into better data structure than queue, can hold Nones in some spots and pages in other spots, but indexing doesn't change
# [BPage, TPage, None, None, TPage, None, BPage, ... etc] max length is "BufferpoolSize"
# (decided on just using a list, but doing it smart. fill list with "None"s to start)
# (then when we add a page, replace the "None")
# (then when we remove a page, replace it with a "None")
#DONE

#2. implement data stucture
#DONE

#3. redo Bufferpool_Not_a_Queue Class functions
#DONE

#4. add self.age to the Page, PasePage(Page), and TailPage(Page) classes (minimum age = 1)

#5. do unpinning better in table funcitons

class Bufferpool_Not_a_Queue():
    def __init__(self):
        self.bufferpool = [None]*BufferpoolSize
        pass

    def BufferpoolIsFull(self):
        return not any(spot is None for spot in self.bufferpool)

    def refresh(self, path):
        #Find the Page that has this path in the bufferpool, then refresh its age, and increment the other pages in bufferpool age
        found_index = self.pathInBP(path)
        if found_index == None:
            raise Exception("Bufferpool Error: can't refresh because page is not in bufferpool.")
        for spot in self.bufferpool:
            if not spot is None:
                if spot.path == path:
                    spot.age = 1
                    spot.pinned += 1
                else:
                    spot.age += 1
        return found_index

    def add(self, page):
        if (self.BufferpoolIsFull()):
            self.kick()
        for index, spot in enumerate(self.bufferpool):
            if spot is None:
                self.bufferpool[index] = page
                self.refresh(page.path) #on the way in, we set the age to 1
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
        self.bufferpool[index_oldest] == None
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
                    self.refresh(spot.path) #when we check what the page's path is, we set the age to 0
                    return index
        return None


