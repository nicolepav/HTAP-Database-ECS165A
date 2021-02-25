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
        for index in range(len(self.bufferpool)):
            if (self.bufferpool[index].pinned == 0):
                kicked = self.bufferpool.pop(0)
                if (kicked.dirty):
                    if not os.path.exists(kicked.path):
                        os.mkdir(kicked.path)
                    kicked.writePageToDisk(kicked.path)
                    return
        raise Exception("Buffer pool: all pages in the bufferpool are pinned.")

    def kickAll(self):
        for page in self.bufferpool:
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