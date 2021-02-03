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
        self.tailRID = -1
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
            newPage = Page(len(fullRecord) - MetaElements)
            newPage.fullInsert(RID, fullRecord)
            self.tailPages.append(newPage)
        else:
            self.tailPages[-1].fullInsert(RID, fullRecord)

    # single tail page cumulative update
    # 1. Get the base record's page index, the record's offset, and record values
    # 2. Create the cumulative record based off a. previous tail record or b. base record
    #       a. Get previous tail record, update new record's indirection column, splice in previous tail record values
    #       b. Splice base record into updatedRecord and update indirection column
    # 3. Increment and insert the new cumulative record. Update base record meta data
    def update(self, baseRID, updatedRecord):
        # 1.
        basePageIndex = self.calculatePageIndex(baseRID)
        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = self.basePages[basePageIndex].getRecord(basePageOffset)
        # 2.
        if baseRecord[SCHEMA_ENCODING_COLUMN] == 1:
            previousTailRecord = self.getPreviousTailRecord(baseRecord[INDIRECTION_COLUMN])
            cumulativeRecord = self.spliceRecord(previousTailRecord, updatedRecord)
            cumulativeRecord[INDIRECTION_COLUMN] = previousTailRecord[INDIRECTION_COLUMN]
            cumulativeRecord[SCHEMA_ENCODING_COLUMN] = 1
        else:
            cumulativeRecord = self.spliceRecord(baseRecord, updatedRecord)
            cumulativeRecord[INDIRECTION_COLUMN] = baseRecord[INDIRECTION_COLUMN]
        # 3.
        self.tailRID += 1
        self.tailInsert(self.tailRID, cumulativeRecord)
        self.basePages[basePageIndex].newRecordAppended(self.tailRID, basePageOffset)

    def getPreviousTailRecord(self, baseIndirectionRID):
        previousTailPageIndex = self.calculatePageIndex(baseIndirectionRID)
        previousTailPageOffset = self.calculatePageOffset(baseIndirectionRID)
        previouslyUpdatedRecord = self.tailPages[previousTailPageIndex].getRecord(previousTailPageOffset)
        return previouslyUpdatedRecord

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
            record = Record(tailRecord[RID_COLUMN], key, tailRecord[MetaElements:])
            return record
        else:
            record = Record(baseRecord[RID_COLUMN], key, baseRecord[MetaElements:])
            return record

    # 1. Invalidate base record
    # 2. Recurse through tail record indirections, invalidating each tail record until invalidated base record reached
    def delete(self, key, baseRID):
        # 1.
        basePageIndex = self.calculatePageIndex(baseRID)
        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = self.basePages[basePageIndex].getRecord(basePageOffset)
        self.basePages[basePageIndex].invalidateRecord(basePageOffset)
        baseIndirectionRID = baseRecord[INDIRECTION_COLUMN]
        # 2.
        if baseRecord[SCHEMA_ENCODING_COLUMN] == 1:
            self.invalidateTailRecords(baseIndirectionRID, baseIndirectionRID)

    def invalidateTailRecords(self, indirectionRID, baseIndirectionRID):
        if indirectionRID == baseIndirectionRID:
            return
        else:
            pageIndex = self.calculatePageIndex(indirectionRID)
            pageOffset = self.calculatePageOffset(indirectionRID)
            nextRID = self.tailPages[pageIndex].invalidateRecord(pageOffset)
            self.invalidateTailRecords(nextRID, baseIndirectionRID)

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

    def spliceRecord(self, oldRecord, updatedRecord):
        createdRecord = []
        for metaIndex in range(0, MetaElements):
            createdRecord.append(oldRecord[metaIndex])
        for columnIndex in range(0, len(updatedRecord)):
            #use data from the oldRecord
            if updatedRecord[columnIndex] == None:
                createdRecord.append(oldRecord[columnIndex + 4])
            else:
                createdRecord.append(updatedRecord[columnIndex])

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
        selectedPageRange = self.getPageRange(self.baseRID)
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
        selectedPageRange  = self.getPageRange(baseRID)
        self.page_directory[selectedPageRange].update(baseRID, record)
        return True

    # m1_tester expects a list of record objects, but we should only be passing back certain columns
    def select(self, key, column, query_columns):
        if key not in self.keyToRID:
            print("No RID found for this key")
            return False
        baseRID = self.keyToRID[key]
        selectedPageRange  = self.getPageRange(baseRID)
        record = self.page_directory[selectedPageRange].select(key, baseRID)
        # Here is one way to pass back only certain columns, but it will be faster if implemented in the query.select() function
        # returned_record_columns = []
        # for query_column in range(len(query_columns)):
        #     if (query_columns[query_column] == 1):
        #         returned_record_columns.append(record.columns[query_column])
        # return [Record(record.rid, record.key, returned_record_columns)]
        return [record]

    def delete(self, key):
        if key not in self.keyToRID:
            print("No RID found for this key")
            return False
        baseRID = self.keyToRID[key]
        selectedPageRange = self.getPageRange(baseRID)
        self.page_directory[selectedPageRange].delete(key, baseRID)

    def sum(self, start_range, end_range, aggregate_column_index):
        summation = 0
        none_in_range = True
        for key in range(start_range, end_range + 1):
            if key not in self.keyToRID:
                record = False
            else:
                baseRID = self.keyToRID[key]
                selectedPageRange  = self.getPageRange(baseRID)
                record = self.page_directory[selectedPageRange].select(key, baseRID)
                none_in_range = False
                summation += record.columns[aggregate_column_index]
        if (none_in_range):
            return False
        else:
            return summation

    def getPageRange(self, baseRID):
        return floor(baseRID / RecordsPerPageRange)
