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
ElementsPerPhysicalPage = PhysicalPageBytes /  BytesPerElement
MetaElements = 4
# When we get 10 filled up tail pages, merge
MergePolicy = 10
PagesPerPageRange = 16
# records per base page * number of base pages per range = records per page range
RecordsPerPageRange = PagesPerPageRange * ElementsPerPhysicalPage

BufferpoolSize = 16

# global must be defined after class definition (its just under it)
# access the global Bufferpool by using "BP"
# Example: print(len(BP.bufferpool))

class Bufferpool():
    
    def __init__(self):
        # global bufferpool
        self.bufferpool = []
        #initialize queue
        #bufferpool.pop(0)
        #bufferpool.append(<page>)

        ##add a struct that has bufferpool page index mapped to the page's path???

        self.LatestBasePagePath = ""      # wont work when more than one table is using
        self.LatestBasePageNumRecords = 0 # wont work when more than one table is using
        pass

    def BufferpoolIsFull(self):
        return len(self.bufferpool) >= BufferpoolSize
        
    def handleReplacement(self, page):  ## needs to be changed to path passed?

        # gets the path of a page
        # path look like "./ECS165/table_<table.name>/pageRange_<pageRange index>/(base/tail)Page_<index>" 


        # also need a book keeping mechanism for the bufferpool:
        # -RID is a page number and a slot within a page!!!!!
        # -this will allow us to ask the bufferpool if it has a given page, when we just give it a RID
        # -so, give this structure a RID, it will know what page id belongs to this RID, then it will check its bufferpool to see if it has that page id, (if not, go get it), then return that page



        #check if page if page is already in bufferpool
        if page in self.bufferpool:     ###THIS NEEDS TO BE CHANGED
            #move the page to the back of the q
            self.bufferpool.remove(page)
            self.bufferpool.append(page)

            return self.bufferpool.index(page)

        if (self.BufferpoolIsFull()):
            self.kick()
            
        #add the new page here
        self.bufferpool.append(page)
        page.pinned += 1 # the requester of the page must unpin it and indicate whether the page has been modified!!!!!!!!!!

        #return the index of the added page ie. the back
        return len(self.bufferpool) - 1


    #need a way to perform operation on that page in the bufferpool
    # 1. Update page meta file with meta information
    def kick(self):
        # called when we need to kick a page
        kicked = self.bufferpool.pop(0)
        
        if (kicked.pinned > 0):
            # throw it to the back of the bufferpool so next object can be kicked
            self.bufferpool.append(kicked)
            kicked = self.bufferpool.pop(0)
            return

        if (kicked.dirty):
            # write the dirty page to disk
            # get the correct path of where we need to write to
            # path should look like: "./ECS165/table_<table.name>/pageRange_<pageRange index>/(base/tail)Page_<basePage or tailPage index>"
            kicked.writeToDisk(kicked.path)

    def pathExists(self, path):
        return True

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


    Hooke bufferpool into:
        insert

        update

        delete

        how does base indirection id function for tail pages? important for delete and update/merge

    '''

global BP
BP = Bufferpool()
