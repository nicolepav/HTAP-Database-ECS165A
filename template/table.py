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
        self.basePages = []
        self.tailPages = []
        pass

    # insert operation into base or tail page:
    # 1. create 1st page if needed or if page list is full, create new page
    # 2. else insert into page list
    def baseInsert(self, RID, recordData):
        # 1.
        if not self.basePages or self.basePages[-1].isFull():
            newPage = Page(len(recordData))
            newPage.insert(RID, recordData)
            self.basePages.append(newPage)
        # 2.
        else:
            self.basePages[-1].insert(RID, recordData)

    # Similar to baseInsert but takes in record with meta data and data
    # and does a full insert of meta data and data
    def tailInsert(self, RID, fullRecord):
        if not self.tailPages or self.tailPages[-1].isFull():
            # only want len(dataColumns) for Page Instantiation
            newPage = Page(len(fullRecord) - 4)
            newPage.fullInsert(RID, fullRecord)
            self.tailPages.append(newPage)
        else:
            self.tailPages[-1].fullInsert(RID, fullRecord)

    # single tail page cumulative update
    # 1. Get base page index, offset, and record (metadata and data)
    # 2. Create updated record: If schemaBit set, need to get latest tail record to splice records (for cumulative),
    # else splice baseRecord and update schema bit
    # 3. append updated record to tail page
    # 4. update indirection column of base record and newly appended record
    def update(self, baseRID, tailRID, record):
        # 1.
        basePageIndex = self.calculatePageIndex(baseRID)
        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = self.basePages[basePageIndex].getRecord(basePageOffset)
        baseIndirectionRID = baseRecord[INDIRECTION_COLUMN]
        schemaBit = baseRecord[SCHEMA_ENCODING_COLUMN]
        # 2.
        if schemaBit == 1:
            previousTailPageIndex = self.calculatePageIndex(baseIndirectionRID)
            previousTailPageOffset = self.calculatePageOffset(baseIndirectionRID)
            previouslyUpdatedRecord = self.tailPages[previousTailPageIndex].getRecord(previousTailPageOffset)
            baseRecord = self.spliceRecord(previouslyUpdatedRecord, record)
            self.tailPages[previousTailPageIndex].setSchemaOn(previousTailPageOffset)
        else:
            baseRecord = self.spliceRecord(baseRecord, record)
            self.basePages[basePageIndex].setSchemaOn(basePageOffset)
        # 3.
        self.tailInsert(tailRID, baseRecord)
        # 4.
        self.basePages[basePageIndex].setIndirectionValue(tailRID, basePageOffset)
        tailPageIndex = self.calculatePageIndex(tailRID)
        tailPageOffset = self.calculatePageOffset(tailRID)
        # TODO debug update when tail page becomes full
        if tailPageIndex == len(self.tailPages):
            print(tailRID)
            # tail page records aren't being kept track of correctly for some reason
            print(self.tailPages[-1].dataColumns[0].num_records)
        self.tailPages[tailPageIndex].setIndirectionValue(baseIndirectionRID, tailPageOffset)

    # Returns record objects
    # 1. Same as self.update step 1.
    # 2. Setup record object
    # TODO: set up getting specific record columns based on the column data (0 or 1) provided in query.select()
    def select(self, key, baseRID):
        basePageIndex = self.calculatePageIndex(baseRID)
        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = self.basePages[basePageIndex].getRecord(basePageOffset)
        baseIndirectionRID = baseRecord[INDIRECTION_COLUMN]
        schemaBit = baseRecord[SCHEMA_ENCODING_COLUMN]
        if schemaBit == 1:
            tailPageIndex = self.calculatePageIndex(baseIndirectionRID)
            tailPageOffset = self.calculatePageOffset(baseIndirectionRID)
            tailRecord = self.tailPages[tailPageIndex].getRecord(tailPageOffset)
            record = Record(tailRecord[RID_COLUMN], key, tailRecord[4:])
            return record
        else:
            record = Record(baseRecord[RID_COLUMN], key, baseRecord[4:])
            return record

    # Example: RID 6000, page range 5000 records, get remainder 1000, divide it by how much elements are in each page say 100, then the rid is located in base page 10 of the range
    def calculatePageIndex(self, RID):
        return floor((RID % (PagesPerPageRange * ElementsPerPhysicalPage)) / ElementsPerPhysicalPage)

    # translate RID to actual pageOffset
    def calculatePageOffset(self, RID):
        offset = RID
        while offset >= RecordsPerPageRange:
            offset -= RecordsPerPageRange
        while offset >= ElementsPerPhysicalPage:
            offset -= ElementsPerPhysicalPage
        return offset

    def addPage(self, record):
        self.basePages.appendBase(Page(record))
        self.currentBasePage += 1

    def spliceRecord(self, oldRecord, newRecord):
        createdRecord = []
        for metaIndex in range(0, 4):
            createdRecord.append(oldRecord[metaIndex])
        for columnIndex in range(0, len(newRecord)):
            #use data from the oldRecord
            if newRecord[columnIndex] == None:
                createdRecord.append(oldRecord[columnIndex + 4])
            else:
                createdRecord.append(newRecord[columnIndex])

        return createdRecord

class Table:
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
        # add page range to page directory
        self.page_directory = [PageRange()]
        # map key to RID for query operations
        self.keyToRID = {}
        self.baseRID = -1
        self.tailRID = -1
        self.index = Index(self)
        pass

    def __merge(self):
        pass

    # Calls insert on the correct page range
    # 1. updates baseRID and maps to key
    # 2. calculate range from baseRID, creating new range if necessary
    # 3. calls insert for selected pageRange
    def insert(self, record):
        # 1.
        self.baseRID += 1
        key = record[0]
        self.keyToRID[key] = self.baseRID
        # 2.
        selectedPageRange = floor(self.baseRID / RecordsPerPageRange)
        if selectedPageRange >= len(self.page_directory):
            self.page_directory.append(PageRange())
            print("new page range")
        # 3.
        self.page_directory[selectedPageRange].baseInsert(self.baseRID, record)

    # Similar to insert steps but calls update at end and doesn't check page range (TODO can a page range contain any number of tail pages?)
    def update(self, key, record):
        if key not in self.keyToRID:
            print("No RID found for this key")
            return False
        baseRID = self.keyToRID[key]
        self.tailRID += 1
        selectedPageRange  = floor(baseRID / RecordsPerPageRange)
        self.page_directory[selectedPageRange].update(baseRID, self.tailRID, record)
        return True

    # m1_tester expects a list of records for some reason
    def select(self, key, column, query_columns):
        if key not in self.keyToRID:
            print("No RID found for this key")
            return False
        baseRID = self.keyToRID[key]
        selectedPageRange  = floor(baseRID / RecordsPerPageRange)
        record = self.page_directory[selectedPageRange].select(key, baseRID)
        return [record]