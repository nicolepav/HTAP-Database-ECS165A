from template.page import *
from template.config import *
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

class PageRange: #TODO: eventually remove this class
    def __init__(self):
        self.basePages = []
        self.tailPages = []
        pass

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

    def open(self, path):
        pass

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    :variable keyToRID          #
    :variable baseRID           #The current RID used to store a new base record
    :variable tailRIDs          #The current RID used for updating a base record, used for tail record
    """
    def __init__(self, name, num_columns, key, path = "./", baseRID = -1, tailRIDs = [], keyToRID = {}):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        # map key to RID for query operations
        self.path = path
        self.baseRID = baseRID
        self.keyToRID = keyToRID
        self.index = Index(self)
        self.numMerges = 0
        # new tailRID array, each element holds the tailRID of each Page Range.
        self.tailRIDs = tailRIDs

        pass

    def __merge(self):
        pass

    # General Note: Will need to replace any instance of self.basePage[index] with our recreated page objects

    # Calls insert on the correct page range
    # 1. Check if page is already in bufferpool (getBasePagePath(self, baseRID) compared to BP.pages dictionary {page_path: page_object})
    #   a. If not, then recreate the page and call BP.handleReplacement(getBasePagePath)
    #   b. Else get page object from bufferpool queue
    # 2. Handle IsPageFull Logic: Check meta file or recreate base page and have it manually check to determine if full
    # 3. Then call recreatedPage.insert(RID, recordData)
    def insert(self, record):
        self.baseRID += 1
        key = record[0]
        self.keyToRID[key] = self.baseRID
        selectedPageRange = self.getPageRange(self.baseRID)
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        BasePagePath = self.getBasePagePath(self.baseRID)

        BPindex = BP.pathInBP(BasePagePath)
        # Page not in bufferpool
        if BPindex is None:
            # recreate page
            page = BasePage(self.num_columns, selectedPageRange, BasePagePath)
            # Create folder if needed
            if os.path.exists(BasePagePath):
                page.readPageFromDisk(BasePagePath)
            # add to bufferpool
            BPindex = BP.add(page)
        # Get page location in bufferpool
        else:
            BPindex = BP.refresh(BPindex)
        BP.bufferpool[BPindex].insert(self.baseRID, record)
        self.finishedModifyingRecord(BPindex)

    # m1_tester expects a list of record objects, but we should only be passing back certain columns
    def select(self, key, column, query_columns):
        if key not in self.keyToRID:
            print("No RID found for this key")
            return False

        baseRID = self.keyToRID[key]
        selectedPageRange = self.getPageRange(baseRID)
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        BasePagePath = self.getBasePagePath(baseRID)

        BPindex = self.getBasePageBPIndex(BasePagePath, selectedPageRange)

        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = BP.bufferpool[BPindex].getRecord(basePageOffset)

        if baseRecord[RID_COLUMN] == INVALID:
            BP.bufferpool[BPindex].pinned -=1
            return False

        mostUpdatedRecord = self.getMostUpdatedRecord(baseRecord, BPindex, selectedPageRange, key)
        returned_record_columns = self.setupReturnedRecord(mostUpdatedRecord, query_columns)
        BP.bufferpool[BPindex].pinned -=1
        return [Record(mostUpdatedRecord.rid, mostUpdatedRecord.key, returned_record_columns)]

    # Similar to insert steps but calls update at end and doesn't check page range
    def update(self, key, record):
        if key not in self.keyToRID:
            print("No RID found for this key")
            return False

        baseRID = self.keyToRID[key]
        selectedPageRange = self.getPageRange(baseRID)
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        BasePagePath = self.getBasePagePath(baseRID)

        baseBPindex = self.getBasePageBPIndex(BasePagePath, selectedPageRange)
        #here we have the base page in the bufferpool
        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = BP.bufferpool[baseBPindex].getRecord(basePageOffset)
        if baseRecord[RID_COLUMN] == INVALID:
            BP.bufferpool[baseBPindex].pinned -=1
            return False

        cumulativeRecord = self.setupCumulativeRecord(baseBPindex, basePageOffset, selectedPageRange, record, baseRecord)

        BP.bufferpool[baseBPindex].newRecordAppended(self.tailRIDs[selectedPageRange], basePageOffset)
        self.finishedModifyingRecord(baseBPindex)

        TailPagePath = self.getTailPagePath(self.tailRIDs[selectedPageRange], selectedPageRange)
        tailBPindex = self.getTailPageBufferIndex(selectedPageRange, TailPagePath)
        BP.bufferpool[tailBPindex].insert(cumulativeRecord)
        self.finishedModifyingRecord(tailBPindex)

        return True

    def setupCumulativeRecord(self, BPindex, basePageOffset, selectedPageRange, record, baseRecord):

        if baseRecord[SCHEMA_ENCODING_COLUMN] == 1: # and not self.recordHasBeenMerged(baseRecord, BP.bufferpool[index].TPS):
            previousTailPagePath = self.getTailPagePath(baseRecord[INDIRECTION_COLUMN], selectedPageRange)
            BPindexTP = self.getTailPageBufferIndex(selectedPageRange, previousTailPagePath)
            previousTailPageOffset = self.calculatePageOffset(baseRecord[INDIRECTION_COLUMN])
            previousTailRecord = BP.bufferpool[BPindexTP].getRecord(previousTailPageOffset)

            cumulativeRecord = self.spliceRecord(previousTailRecord, record)
            cumulativeRecord[INDIRECTION_COLUMN] = previousTailRecord[RID_COLUMN]

            BP.bufferpool[BPindexTP].pinned -= 1
        else:
            cumulativeRecord = self.spliceRecord(baseRecord, record)
            cumulativeRecord[INDIRECTION_COLUMN] = baseRecord[RID_COLUMN]
        self.tailRIDs[selectedPageRange] += 1
        cumulativeRecord[RID_COLUMN] = self.tailRIDs[selectedPageRange]
        cumulativeRecord[TIMESTAMP_COLUMN] = round(time.time() * 1000)
        cumulativeRecord[SCHEMA_ENCODING_COLUMN] = 1
        return cumulativeRecord

    def finishedModifyingRecord(self, BPindex):
        BP.bufferpool[BPindex].dirty = True
        BP.bufferpool[BPindex].pinned -= 1

    def sum(self, start_range, end_range, aggregate_column_index):
        summation = 0
        none_in_range = True
        for key in range(start_range, end_range + 1):
            if key not in self.keyToRID:
                continue
            baseRID = self.keyToRID[key]
            selectedPageRange = self.getPageRange(baseRID)
            PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
            BasePagePath = self.getBasePagePath(baseRID)

            BPindex = self.getBasePageBPIndex(BasePagePath, selectedPageRange)

            basePageOffset = self.calculatePageOffset(baseRID)
            baseRecord = BP.bufferpool[BPindex].getRecord(basePageOffset)

            if baseRecord[RID_COLUMN] == INVALID:
                BP.bufferpool[BPindex].pinned -=1
                continue

            mostUpdatedRecord = self.getMostUpdatedRecord(baseRecord, BPindex, selectedPageRange, key)
            query_columns = [1, 1, 1, 1, 1]
            returned_record_columns = self.setupReturnedRecord(mostUpdatedRecord, query_columns)

            BP.bufferpool[BPindex].pinned -=1

            none_in_range = False
            summation += mostUpdatedRecord.columns[aggregate_column_index]
        if (none_in_range):
            return False
        else:
            return summation

    def getMostUpdatedRecord(self, baseRecord, BPindex, selectedPageRange, key):
        if baseRecord[SCHEMA_ENCODING_COLUMN] == 1: #and not self.recordHasBeenMerged(baseRecord, BP.bufferpool[BPindex].TPS):
            previousTailRecord = self.getPreviousTailRecord(baseRecord, selectedPageRange)
            record = Record(previousTailRecord[RID_COLUMN], key, previousTailRecord[MetaElements:])
        else:
            record = Record(baseRecord[RID_COLUMN], key, baseRecord[MetaElements:])
        return record

    def setupReturnedRecord(self, record, query_columns):
        returned_record_columns = []
        for query_column in range(0, len(query_columns)):
            if (query_columns[query_column] == 1):
                returned_record_columns.append(record.columns[query_column])
            else:
                returned_record_columns.append(None)
        return returned_record_columns

    def delete(self, key):
        if key not in self.keyToRID:
            print("No RID found for this key")
            return False
        baseRID = self.keyToRID[key]
        selectedPageRange = self.getPageRange(baseRID)
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        BasePagePath = self.getBasePagePath(baseRID)
        baseBPindex = self.getBasePageBPIndex(BasePagePath, selectedPageRange)
        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = BP.bufferpool[baseBPindex].getRecord(basePageOffset)

        # Invalidate base record
        BP.bufferpool[baseBPindex].invalidateRecord(basePageOffset)
        self.finishedModifyingRecord(baseBPindex)

        # Recurse through tail record indirections, invalidating each tail record until invalidated base record reached
        if baseRecord[SCHEMA_ENCODING_COLUMN] == 1:
            self.invalidateTailRecords(baseRecord[INDIRECTION_COLUMN], baseRID, selectedPageRange)

    def invalidateTailRecords(self, indirectionRID, baseRID, selectedPageRange):
        if indirectionRID == baseRID:
            return
        else:
            # pageIndex = self.calculateTailPageIndex(indirectionRID)
            pageOffset = self.calculatePageOffset(indirectionRID)
            TailPagePath = self.getTailPagePath(indirectionRID, selectedPageRange)
            
            tailBPindex = BP.pathInBP(TailPagePath)
            if tailBPindex is None:
                # here we know that the page is not in the bufferpool (So the page exists only on disk)
                page = TailPage(self.num_columns, selectedPageRange, TailPagePath)
                page.readPageFromDisk(TailPagePath)
                tailBPindex = BP.add(page)
            else:
                # here the page is in the bufferpool, so we will refresh it.
                tailBPindex = BP.refresh(tailBPindex)

            nextRID = BP.bufferpool[tailBPindex].invalidateRecord(pageOffset)
            self.finishedModifyingRecord(tailBPindex)
            self.invalidateTailRecords(nextRID, baseRID, selectedPageRange)

    def getBasePageBPIndex(self, BasePagePath, selectedPageRange):
        BPindex = BP.pathInBP(BasePagePath)
        if BPindex is None:
            # the path does exist, so go read the basepage from disk
            page = BasePage(self.num_columns, selectedPageRange, BasePagePath)
            page.readPageFromDisk(BasePagePath)
            BPindex = BP.add(page)
        else:
            # here the page is in the bufferpool, so we will refresh it.
            BPindex = BP.refresh(BPindex)
        return BPindex

    def getTailPageBufferIndex(self, selectedPageRange, TailPagePath):
        BPindex = BP.pathInBP(TailPagePath)
        if BPindex is None:
            # here we know that the page is not in the bufferpool (So either the page exists, but is only on disk OR we are inserting a record into a new base page)
            if not os.path.exists(TailPagePath):
                # the page is not in the bufferpool and the path does not exist, so we must be inserting a record into a new tail page
                page = TailPage(self.num_columns, selectedPageRange, TailPagePath)
                BPindex = BP.add(page)
            else:
                # the path does exist, so go read the basepage from disk
                page = TailPage(self.num_columns, selectedPageRange, TailPagePath)
                page.readPageFromDisk(TailPagePath)
                BPindex = BP.add(page)
        else:
            # here the page is in the bufferpool, so we will refresh it.
            BPindex = BP.refresh(BPindex)
        return BPindex

    def getTailPagePath(self, tailRID, selectedPageRange):
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        selectedTailPage = self.calculateTailPageIndex(tailRID)
        TailPagePath = PageRangePath + "/tailPage_" + str(selectedTailPage)
        return TailPagePath

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

    def getPreviousTailRecord(self, baseRecord, selectedPageRange):
        previousTailPagePath = self.getTailPagePath(baseRecord[INDIRECTION_COLUMN], selectedPageRange)
        previousBufferIndex = self.getTailPageBufferIndex(selectedPageRange, previousTailPagePath)
        previousTailPageOffset = self.calculatePageOffset(baseRecord[INDIRECTION_COLUMN])
        previousTailRecord = BP.bufferpool[previousBufferIndex].getRecord(previousTailPageOffset)
        BP.bufferpool[previousBufferIndex].pinned -= 1
        return previousTailRecord

    def writeMetaJsonToDisk(self, path):
        MetaJsonPath = path + "/Meta.json"
        f = open(MetaJsonPath, "w")
        metaDictionary = {
            "name": self.name, 
            "key": self.key,
            "num_columns": self.num_columns,
            "baseRID": self.baseRID,
            "tailRIDs": self.tailRIDs,
            "keyToRID": self.keyToRID
            # "indexTo": self.index # python doesn't like this
            # TypeError: Object of type Index is not JSON serializable
        }
        json.dump(metaDictionary, f, indent=4)
        f.close()
        pass

    def close(self, path):
        self.writeMetaJsonToDisk(path)
        BP.kickAll()

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
        return pageIndex # - self.numMerges * MergePolicy

    def getBasePagePath(self, baseRID):
        selectedPageRange = self.getPageRange(baseRID)
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        if not os.path.exists(PageRangePath):
            os.mkdir(PageRangePath)
        if len(self.tailRIDs) <= selectedPageRange:
            self.tailRIDs.append(-1)
        selectedBasePage = self.calculateBasePageIndex(baseRID)
        BasePagePath = PageRangePath + "/basePage_" + str(selectedBasePage)
        return BasePagePath

    # translate RID to actual pageOffset
    def calculatePageOffset(self, RID):
        offset = RID
        while offset >= RecordsPerPageRange:
            offset -= RecordsPerPageRange
        while offset >= ElementsPerPhysicalPage:
            offset -= ElementsPerPhysicalPage
        return offset

    # 1. Iterate over all page ranges inside this tables folder
    # 2. For each base folder, recreate the base page
    #     a. Check base record, if updated, recreate tail page and go to tail record
    def getAllUpdatedRecords(self):
        allRecords = []
        for selectedPageRange in range(0, self.getPageRange(self.baseRID) + 1):
            PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
            for selectedBasePage in range(0, self.calculateBasePageIndex(self.baseRID) + 1):
                BasePagePath = PageRangePath + "/basePage_" + str(selectedBasePage)
                basePage = self.getBasePage(self, selectedPageRange, BasePagePath)
                basePageRecords = basePage.getAllRecords()
                for baseRecord in basePageRecords:
                    #check for tail page
                    if baseRecord[INDIRECTION_COLUMN] != 0:
                        tailIndex = self.calculateTailPageIndex(baseRecord[INDIRECTION_COLUMN])
                        TailPagePath = PageRangePath + "/tailPage_" + str(tailIndex)
                        tailPage = self.getTailPage(selectedPageRange, TailPagePath)
                        for tailRecord in tailPage:
                            if (tailRecord[RID_COLUMN] == baseRecord[INDIRECTION_COLUMN]):
                                allRecords.append(tailRecord)
                    elif baseRecord[INDIRECTION_COLUMN] == 0:
                        allRecords.append(baseRecord)
                    


        return allRecords
    
    def getAllBasePages(self):
        allBasePages = []
        # iterate from 0 to most recently updated pageRange (handle case for only 1 pageRange)
        for selectedPageRange in range(0, self.getPageRange(self.baseRID) + 1):
            PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
            for selectedBasePage in range(0, self.calculateBasePageIndex(self.baseRID) + 1):
                BasePagePath = PageRangePath + "/basePage_" + str(selectedBasePage)
                allBasePages.append(self.getBasePage(self, selectedPageRange, BasePagePath))
        return allBasePages
                    
    def getTailPage(self, selectedPageRange, TailPagePath): #TODO: not using the bufferpool, maybe there is reason for this?
        page = TailPage(self.num_columns, selectedPageRange, TailPagePath)
        page.readPageFromDisk(TailPagePath)
        return page

    def getBasePage(self, selectedPageRange, BasePagePath): #TODO: not using the bufferpool, maybe there is reason for this?
        page = BasePage(self.num_columns, selectedPageRange, BasePagePath)
        page.readPageFromDisk(BasePagePath)
        return page

    def getPageRange(self, baseRID):
        if baseRID > RecordsPerPageRange and floor(baseRID / RecordsPerPageRange) == 0:
            print("Error")
            # raise Exception("PageRange Error.") #TODO: Do we want actual error output here?
        return floor(baseRID / RecordsPerPageRange)

    # Base record's indirection is pointing to a record that's already been merged
    def recordHasBeenMerged(self, baseRecord, TPS):
        if baseRecord[INDIRECTION_COLUMN] <= TPS:
            return True
        return False

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