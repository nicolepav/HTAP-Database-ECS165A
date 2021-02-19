from template.page import *
from template.index import Index
import time
import copy
from math import floor
import threading
import concurrent.futures
import os
import json

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
        self.numMerges = 0
        pass

    # insert operation into base or tail page:
    # 1. create 1st page if needed or if page list is full, create new page
    # 2. else insert into page list
    def baseInsert(self, RID, recordData):
        # 1.
        if not self.basePages or self.basePages[-1].isFull():
            newPage = BasePage(len(recordData))
            newPage.insert(RID, recordData)
            self.basePages.append(newPage)
        # 2.
        else:
            self.basePages[-1].insert(RID, recordData)

    # Similar to baseInsert but takes in record with meta data and data
    # and does a full insert of meta data and data
    # Accounts for merge if needed
    def tailInsert(self, fullRecord):
        if not self.tailPages or self.tailPages[-1].isFull():
            if len(self.tailPages) == MergePolicy:
                self.initiateMerge()
            # only want len(dataColumns) for Page Instantiation
            newPage = TailPage(len(fullRecord) - MetaElements)
            newPage.insert(fullRecord)
            self.tailPages.append(newPage)
        else:
            self.tailPages[-1].insert(fullRecord)

    # 1. Copy base pages
    # 2. When backgroundThread returns, only copy mergedPages data into our current basePages
    # 3. Remove merged tail pages
    def initiateMerge(self):
        copiedBasePages = copy.deepcopy(self.basePages)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            backgroundThread = executor.submit(self.performMerge, copiedBasePages, self.tailPages)
            mergedPages = backgroundThread.result()
            for index, mergedBasePage in enumerate(mergedPages):
                self.basePages[index].TPS = mergedBasePage.TPS
                self.basePages[index].dataColumns = mergedBasePage.dataColumns
            self.numMerges += 1
            self.tailPages = self.tailPages[MergePolicy:]

    # 1. For each base page
        # a. Iterate through all base records and if updated, get tail page record
        # b. Merge tail record into base record in BasePage object
    # 2. return updated basePages
    def performMerge(self, basePages, tailPages):
        for basePage in basePages:
            allBasePageRecords = basePage.getAllRecords()
            for baseRecord in allBasePageRecords:
                if baseRecord[SCHEMA_ENCODING_COLUMN] == 1:
                    # updated record outside of page's we're currently merging
                    if self.calculateTailPageIndex(baseRecord[INDIRECTION_COLUMN]) >= len(self.tailPages):
                        continue
                    tailRecord = self.getPreviousTailRecord(baseRecord[INDIRECTION_COLUMN])
                    basePageOffset = self.calculatePageOffset(baseRecord[RID_COLUMN])
                    basePage.mergeTailRecord(basePageOffset, tailRecord[RID_COLUMN], tailRecord[MetaElements:])
                    mergedRecord = basePage.getRecord(basePageOffset)
        return basePages

    # single tail page cumulative update
    # 1. Get the base record's page index, the record's offset, and record values
    # 2. Create the cumulative record based off a. previous tail record or b. base record
    #       a. Get previous tail record, update new record's indirection column, splice in previous tail record values
    #       b. Splice base record into updatedRecord and update indirection column
    # 3. Increment and insert the new cumulative record. Update base record meta data
    def update(self, baseRID, updatedRecord):
        # 1.
        basePageIndex = self.calculateBasePageIndex(baseRID)
        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = self.basePages[basePageIndex].getRecord(basePageOffset)
        # 2.
        if baseRecord[SCHEMA_ENCODING_COLUMN] == 1 and not self.recordHasBeenMerged(baseRecord, basePageIndex):
            previousTailRecord = self.getPreviousTailRecord(baseRecord[INDIRECTION_COLUMN])
            cumulativeRecord = self.spliceRecord(previousTailRecord, updatedRecord)
            cumulativeRecord[INDIRECTION_COLUMN] = previousTailRecord[RID_COLUMN]
        else:
            cumulativeRecord = self.spliceRecord(baseRecord, updatedRecord)
            cumulativeRecord[INDIRECTION_COLUMN] = baseRecord[RID_COLUMN]
        # 3.
        self.tailRID += 1
        cumulativeRecord[RID_COLUMN] = self.tailRID
        cumulativeRecord[TIMESTAMP_COLUMN] = round(time.time() * 1000)
        cumulativeRecord[SCHEMA_ENCODING_COLUMN] = 1

        self.basePages[basePageIndex].newRecordAppended(self.tailRID, basePageOffset)
        self.tailInsert(cumulativeRecord)

    def getPreviousTailRecord(self, baseIndirectionRID):
        previousTailPageIndex = self.calculateTailPageIndex(baseIndirectionRID)
        previousTailPageOffset = self.calculatePageOffset(baseIndirectionRID)
        previouslyUpdatedRecord = self.tailPages[previousTailPageIndex].getRecord(previousTailPageOffset)
        return previouslyUpdatedRecord

    # Returns record objects
    # 1. Same as self.update step 1.
    # 2. Setup record object
    def select(self, key, baseRID):
        basePageIndex = self.calculateBasePageIndex(baseRID)
        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = self.basePages[basePageIndex].getRecord(basePageOffset)
        baseIndirectionRID = baseRecord[INDIRECTION_COLUMN]
        schemaBit = baseRecord[SCHEMA_ENCODING_COLUMN]
        if schemaBit == 1 and not self.recordHasBeenMerged(baseRecord, basePageIndex):
            tailPageIndex = self.calculateTailPageIndex(baseIndirectionRID)
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
        basePageIndex = self.calculateBasePageIndex(baseRID)
        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = self.basePages[basePageIndex].getRecord(basePageOffset)
        self.basePages[basePageIndex].invalidateRecord(basePageOffset)
        # 2.
        if baseRecord[SCHEMA_ENCODING_COLUMN] == 1:
            self.invalidateTailRecords(baseRecord[INDIRECTION_COLUMN], baseRID)

    def invalidateTailRecords(self, indirectionRID, baseIndirectionRID):
        if indirectionRID == baseIndirectionRID:
            return
        else:
            pageIndex = self.calculateTailPageIndex(indirectionRID)
            pageOffset = self.calculatePageOffset(indirectionRID)
            # Check to see if page already removed and return if so
            if pageIndex < 0 or pageIndex >= len(self.tailPages):
                return
            try:
                nextRID = self.tailPages[pageIndex].invalidateRecord(pageOffset)
                self.invalidateTailRecords(nextRID, baseIndirectionRID)
            except:
                print("race case occurred?")

    # Base record's indirection is pointing to a record that's already been merged
    def recordHasBeenMerged(self, baseRecord, basePageIndex):
        if baseRecord[INDIRECTION_COLUMN] <= self.basePages[basePageIndex].TPS:
            return True
        return False

    def calculateBasePageIndex(self, baseRID):
        pageRange = 0
        while baseRID >= RecordsPerPageRange:
            baseRID -= RecordsPerPageRange
        while baseRID >= ElementsPerPhysicalPage:
            pageRange += 1
            baseRID -= ElementsPerPhysicalPage
        return pageRange

    def calculateTailPageIndex(self, tailRID):
        pageIndex = 0
        while tailRID >= RecordsPerPageRange:
            pageIndex += PagesPerPageRange
            tailRID -= RecordsPerPageRange
        while tailRID >= ElementsPerPhysicalPage:
            pageIndex += 1
            tailRID -= ElementsPerPhysicalPage
        # each time we merge, our pageIndex calculations will be off by: numMerges * MergedPolicy
        return pageIndex - self.numMerges * MergePolicy

    # translate RID to actual pageOffset
    def calculatePageOffset(self, RID):
        offset = RID
        while offset >= RecordsPerPageRange:
            offset -= RecordsPerPageRange
        while offset >= ElementsPerPhysicalPage:
            offset -= ElementsPerPhysicalPage
        return offset

    # Cumulative splicing
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


    def open(self, path):
        pass

    def close(self, path):
        # path look like "./ECS165/table_<table.name>/pageRange_<pageRange index>"

        # we want pageRange.close to store the contents of the pageRange to a pageRange directory

        for index, basePage in enumerate(self.basePages):
            # we want basePage.writeToDisk to store the contents of the basePage to a Page directory
            basePagesDirPath = path + "/basePage_" + str(index)
            if not os.path.exists(basePagesDirPath):
                os.mkdir(basePagesDirPath)
            basePage.writeToDisk(basePagesDirPath)
        for index, tailPage in enumerate(self.tailPages):
            # we want basePage.writeToDisk to store the contents of the basePage to a Page directory
            tailPagesDirPath = path + "/tailPage_" + str(index)
            if not os.path.exists(tailPagesDirPath):
                os.mkdir(tailPagesDirPath)
            tailPage.writeToDisk(tailPagesDirPath)
        pass

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
        returned_record_columns = []
        for query_column in range(len(query_columns)):
            if (query_columns[query_column] == 1):
                returned_record_columns.append(record.columns[query_column])
            else:
                returned_record_columns.append(None)
        return [Record(record.rid, record.key, returned_record_columns)]

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

    def writeMetaJsonToDisk(self, path):
        MetaJsonPath = path + "/Meta.json"
        f = open(MetaJsonPath, "w")
        metaDictionary = {
            "name": self.name, 
            "key": self.key,
            "num_columns": self.num_columns,
            "baseRID": self.baseRID,
            "keyToRID": self.keyToRID
            # "index": self.index # python doesn't like this
        }
        json.dump(metaDictionary, f, indent=4)
        f.close()
        pass

    def readMetaJsonFromDisk(self, path):
        # reads the stored Meta.json and returns the constructed Dictionary
        MetaJsonPath = path + "/Meta.json"
        f = open(MetaJsonPath, "r")
        metaDictionary = json.load(f)
        f.close()
        return metaDictionary

    def open(self, path):
        # path look like "./ECS165/table_1"

        # we want table.open to populate the table with the data in the given table directory path

        pass

    def close(self, path):
        # path look like "./ECS165/table_1"
        
        self.writeMetaJsonToDisk(path);

        # we want table.close to store the contents of the table to a table directory

        for index, pageRange in enumerate(self.page_directory):
            # we want pageRange.close to store the contents of the pageRange to a pageRange directory
            pageRangeDirPath = path + "/pageRange_" + str(index)
            if not os.path.exists(pageRangeDirPath):
                os.mkdir(pageRangeDirPath)
            pageRange.close(pageRangeDirPath);
        pass
