from template.page import *
from template.index import Index
from time import time
from math import floor

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class PageRange:
    def __init__(self):
        # creates 1 base page and 1 tail page to start
            # should be an array so we can add more base and tail pages as needed
        self.basePages = []
        self.currentBasePage = 0

        self.tailPages = []
        self.currentTailPage = 0

        # holds range of RIDS
        containsRIDS = (0, 0) #tuple of RID range


        
        self.rangeFull = False          # boolean: full or not
                   # current page
        
        # insert operation into base page, if base page full, create new base page
        # update operation into tail page, if tail page full, create new tail page
        pass

    def insert(self, baseRID, record):

        if not self.basePages:
            self.basePages.append(Page(record))
            return True
        

        currentPage = self.basePages[self.currentBasePage]
        
        # before inserting check if page is full
        if currentPage.isFull():
            if len(self.basePages) < 17: # !!TODO <- define base page Global?
                self.addPage(record)
            else:
                return False #TODO indicates to caller that another page range needs to be created
        else:
            # inserting a record means copying each of its data elements to the correct physical page associated with that column
            currentPage.insert(record) #TODO <- tune insert so everthing added will be added

        return True


    def update(self, tailRID):
        if not self.tailPages:
            self.tailPages.append(Page(tailRID, record))
            return True
        pass

    def addPage(self, record):
        self.basePages.append(Page(record))
        self.currentBasePage += 1

class Table:

    #insert to table -> insert to page range object -> page range object creates new base page and inserts
    #if check if page range is full. If full then create new page range.

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    :variable keyToRID          #
    :variable baseRID           #The current RID used to store a new base record
    :variable tailRID           #The current RID used for updating a base record, used for tail record
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = [PageRange()]
            #operations:
                #: find record from RID, from a set of page ranges
                #: store Page ranges
        self.keyToRID = {}
        self.baseRID = -1
        self.tailRID = -1
        self.index = Index(self)

        #add page range to page directory

        #page_directory = {"PageRanges": [pr1, pr2]}
        
        #whichPageRange = floor(RID / PageRangeSize)
        #OffsetInPageRange = RID % PageRangeSize

        # floor(3000 / 65535) = 0 <- index in list?
        # page range

        #Andrew: if we using floor to calculate the page range to pull from why do we need a dict? I just changed it to list for now.
        
 
        #page_directory = {:[pr1, pr2, ...]}
        pass

    def __merge(self):
        pass

    def insert(self, record):
        #note: record is a tuple
        self.baseRID += 1
        key = record[0]
        self.keyToRID[key] = self.baseRID
        
        # do math on RID to get pageRange from page_directory
        # - whichPageRange = floor(RID / PageRangeSize)
        # - OffsetInPageRange = RID % PageRangeSize

        selectedRange  = floor(self.baseRID, 16)                     # should change 16 to a global
        # TODO: determine PageRangeSize global
        
        # then call pageRange.insert(record)
        calculatedPagerange = 0
        self.page_directory[whichPageRange].insert(record)
        


        # NOT THIS
        # Example:
                                    # 65536*0         to 65536*1 - 1         : PageRange1
                                    # 65536*1         to 65536*2 - 1         : PageRange2
        # self.page_directory.update({PageRangeSize*0 to PageRangeSize*1 - 1 : PageRange1})
        # self.page_directory.update({PageRangeSize*1 to PageRangeSize*2 - 1 : PageRange2})

