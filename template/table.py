from template.page import *
from template.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3



class PageRange:
    def __init__(self):
        # creates 1 base page and 1 tail page to start
        # holds range of RIDS
        # boolean: full or not
        # current page
            # page should contain current record
            # before inserting check if page is full
         
        # int: holds current avaliable index

        # insert operation into base page, if base page full, create new base page
        # update operation into tail page, if tail page full, create new tail page
        pass



        

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

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
        self.page_directory = {}
            #operations:
                #: find record from RID, from a set of page ranges
                #: store Page ranges
        self.keyToRID = {}
        self.baseRID = -1
        self.tailRID = -1
        self.index = Index(self)

        initialPageRange = PageRange()
        #add page range to page directory

        #page_directory = {"PageRanges": [pr1, pr2]}
        
        #whichPageRange = floor(RID / PageRangeSize)
        #OffsetInPageRange = RID % PageRangeSize

        # floor(3000 / 65535) = 0
        # page range
        
 
        #page_directory = {:[pr1, pr2, ...]}
        pass

    def __merge(self):
        pass

    def insertIntoTable(self, record):
        self.baseRID += 1
        key = record[0]
        self.keyToRID[key] = self.baseRID
        # do math on RID to get pageRange from page_directory
        # then call pageRange.insert(record)
        
        


        # NOT THIS
        # Example:
                                    # 65536*0         to 65536*1 - 1         : PageRange1
                                    # 65536*1         to 65536*2 - 1         : PageRange2
        # self.page_directory.update({PageRangeSize*0 to PageRangeSize*1 - 1 : PageRange1})
        # self.page_directory.update({PageRangeSize*1 to PageRangeSize*2 - 1 : PageRange2})

        
        

        
        
        

        