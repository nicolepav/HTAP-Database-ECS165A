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
BASE_RID = 4

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    :variable keyToRID          #
    :variable baseRID           #The current RID used to store a new base record
    :variable tailRIDs          #The current RID used for updating a base record, used for tail record
    """

    def __init__(self, name, num_columns, key, path = "./", baseRID = -1, tailRIDs = [], keyToRID = {}, numMerges = 0):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        # map key to RID for query operations
        self.path = path
        self.baseRID = baseRID
        self.keyToRID = keyToRID
        self.index = Index(self)
        #lock manager per table
        self.numMerges = numMerges
        # new tailRID array, each element holds the tailRID of each Page Range.
        self.tailRIDs = tailRIDs
        # used for latching Page Dir

    # Calls insert on the correct page range
    # 1. Check if page is already in bufferpool (getBasePagePath(self, baseRID) compared to BP.pages dictionary {page_path: page_object})
    #   a. If not, then recreate the page and call BP.handleReplacement(getBasePagePath)
    #   b. Else get page object from bufferpool queue
    # 2. Handle IsPageFull Logic: Check meta file or recreate base page and have it manually check to determine if full
    # 3. Then call recreatedPage.insert(RID, recordData)
    def insert(self, record):
        BP.latch.acquire()
        key = record[0]
        self.baseRID += 1
        currentBaseRID = self.baseRID
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
        # PD unlatch
        BP.latch.release()
        if self.index:
            self.index.latch.acquire()
            self.indexInsert(record)
            self.index.latch.release()
        return [self, currentBaseRID, key]

    # m1_tester expects a list of record objects, but we should only be passing back certain columns
    def select(self, key, column, query_columns):
        BP.latch.acquire()
        if key not in self.keyToRID:
            BP.latch.release()
            return False

        baseRID = self.keyToRID[key]
        selectedPageRange = self.getPageRange(baseRID)
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        BasePagePath = self.getBasePagePath(baseRID)
        basePageOffset = self.calculatePageOffset(baseRID)
        BPindex = self.getBasePageBPIndex(BasePagePath, selectedPageRange)
        baseRecord = BP.bufferpool[BPindex].getRecord(basePageOffset)

        if baseRecord[RID_COLUMN] == INVALID:
            BP.bufferpool[BPindex].pinned -=1
            BP.latch.release()
            return False

        mostUpdatedRecord = self.getMostUpdatedRecord(baseRecord, BPindex, selectedPageRange, key)
        BP.bufferpool[BPindex].pinned -=1
        # BP unlatch
        BP.latch.release()
        returned_record_columns = self.setupReturnedRecord(mostUpdatedRecord, query_columns)
        return [Record(mostUpdatedRecord.rid, mostUpdatedRecord.key, returned_record_columns)]

    # 1. Pull base record into BP if needed so we can get the record and update base record data/bp status
    # 2. Get the most updated tail record into BP so that we can create cumulative record
    # 3. Add tail page to BP if needed and insert the cumulative tail record into latest tail page
    # 4/5. Check if a merge should occur and udpate index
    def update(self, key, record, isTransaction = False):
        BP.latch.acquire()
        if key not in self.keyToRID:
            BP.latch.release()
            return False
        # 1.
        baseRID = self.keyToRID[key]
        selectedPageRange = self.getPageRange(baseRID)
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        BasePagePath = self.getBasePagePath(baseRID)
        baseBPindex = self.getBasePageBPIndex(BasePagePath, selectedPageRange)
        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = BP.bufferpool[baseBPindex].getRecord(basePageOffset)

        if baseRecord[RID_COLUMN] == INVALID:
            BP.bufferpool[baseBPindex].pinned -=1
            BP.latch.release()
            return False

        self.tailRIDs[selectedPageRange] += 1
        tailRID = self.tailRIDs[selectedPageRange]
        # if transaction, don't update indirection column yet
        if isTransaction:
            BP.bufferpool[baseBPindex].newRecordAppended(baseRecord[INDIRECTION_COLUMN], basePageOffset)
        else:
            BP.bufferpool[baseBPindex].newRecordAppended(tailRID, basePageOffset)
        self.finishedModifyingRecord(baseBPindex)
        # 2.
        if baseRecord[SCHEMA_ENCODING_COLUMN] == 1 and not self.recordHasBeenMerged(baseRecord, BP.bufferpool[baseBPindex].TPS):
            previousTailRecord = self.getPreviousTailRecord(baseRecord, selectedPageRange)
            cumulativeRecord = self.createCumulativeRecord(previousTailRecord, record, previousTailRecord[RID_COLUMN], baseRecord[RID_COLUMN], selectedPageRange, MetaElements + 1)
        else:
            cumulativeRecord = self.createCumulativeRecord(baseRecord, record, baseRecord[RID_COLUMN], baseRecord[RID_COLUMN], selectedPageRange, MetaElements)
        # 3.
        TailPagePath = self.getTailPagePath(tailRID, selectedPageRange)
        tailBPindex = self.getTailPageBufferIndex(selectedPageRange, TailPagePath)
        BP.bufferpool[tailBPindex].insert(cumulativeRecord)
        self.finishedModifyingRecord(tailBPindex)
        # 4.
        if self.numMerges == 0 and self.calculateTailPageIndex(tailRID) >= MergePolicy:
            self.initiateMerge(selectedPageRange)
        elif self.numMerges > 0 and self.calculateTailPageIndex(tailRID) >= self.numMerges * MergePolicy + MergePolicy:
            self.initiateMerge(selectedPageRange)
        BP.latch.release()
        # 5.
        if self.index:
            self.index.latch.acquire()
            self.indexUpdate(cumulativeRecord)
            self.index.latch.release()
        return [self, tailRID, selectedPageRange, baseRID]

    def deleteBaseRecord(self, baseRID):
        pageOffset = self.calculatePageOffset(baseRID)
        BasePagePath = self.getBasePagePath(baseRID)
        baseBPindex = BP.pathInBP(BasePagePath)
        if baseBPindex is None:
            page = BasePage(self.num_columns, 0, BasePagePath)
            page.readPageFromDisk(BasePagePath)
            baseBPindex = BP.add(page)
        else:
            baseBPindex = BP.refresh(baseBPindex)
        BP.bufferpool[baseBPindex].invalidateRecord(pageOffset)
        self.finishedModifyingRecord(baseBPindex)

    def deleteTailRecord(self, tailRID, selectedPageRange):
        pageOffset = self.calculatePageOffset(tailRID)
        TailPagePath = self.getTailPagePath(tailRID, selectedPageRange)
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


    def updateBaseIndirection(self, baseRID, tailRID):
        selectedPageRange = self.getPageRange(baseRID)
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        BasePagePath = self.getBasePagePath(baseRID)
        # BP latch
        BP.latch.acquire()
        baseBPindex = self.getBasePageBPIndex(BasePagePath, selectedPageRange)
        basePageOffset = self.calculatePageOffset(baseRID)
        baseRecord = BP.bufferpool[baseBPindex].getRecord(basePageOffset)

        if baseRecord[INDIRECTION_COLUMN] < tailRID:
            BP.bufferpool[baseBPindex].newRecordAppended(tailRID, basePageOffset)
        self.finishedModifyingRecord(baseBPindex)
        BP.latch.release()

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
            basePageOffset = self.calculatePageOffset(baseRID)
            # BP latch
            BP.latch.acquire()
            BPindex = self.getBasePageBPIndex(BasePagePath, selectedPageRange)
            baseRecord = BP.bufferpool[BPindex].getRecord(basePageOffset)

            if baseRecord[RID_COLUMN] == INVALID:
                BP.bufferpool[BPindex].pinned -=1
                # BP unlatch
                BP.latch.release()
                continue

            mostUpdatedRecord = self.getMostUpdatedRecord(baseRecord, BPindex, selectedPageRange, key)
            BP.bufferpool[BPindex].pinned -=1
            # BP unlatch
            BP.latch.release()
            query_columns = [1, 1, 1, 1, 1]
            returned_record_columns = self.setupReturnedRecord(mostUpdatedRecord, query_columns)

            none_in_range = False
            summation += mostUpdatedRecord.columns[aggregate_column_index]
        if (none_in_range):
            return False
        else:
            return summation

    def getMostUpdatedRecord(self, baseRecord, BPindex, selectedPageRange, key):
        if baseRecord[SCHEMA_ENCODING_COLUMN] == 1 and not self.recordHasBeenMerged(baseRecord, BP.bufferpool[BPindex].TPS):
            previousTailRecord = self.getPreviousTailRecord(baseRecord, selectedPageRange)
            record = Record(previousTailRecord[RID_COLUMN], key, previousTailRecord[MetaElements + 1:])
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
            return False
        baseRID = self.keyToRID[key]
        selectedPageRange = self.getPageRange(baseRID)
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        BasePagePath = self.getBasePagePath(baseRID)
        basePageOffset = self.calculatePageOffset(baseRID)
        # BP latch
        BP.latch.acquire()
        baseBPindex = self.getBasePageBPIndex(BasePagePath, selectedPageRange)
        baseRecord = BP.bufferpool[baseBPindex].getRecord(basePageOffset)

        # Invalidate base record
        BP.bufferpool[baseBPindex].invalidateRecord(basePageOffset)
        self.finishedModifyingRecord(baseBPindex)

        # Recurse through tail record indirections, invalidating each tail record until invalidated base record reached
        if baseRecord[SCHEMA_ENCODING_COLUMN] == 1:
            self.invalidateTailRecords(baseRecord[INDIRECTION_COLUMN], baseRID, selectedPageRange)
        # BP unlatch
        BP.latch.release()
        if self.index:
            # Index latch
            self.index.latch.acquire()
            self.indexDelete(baseRID)
            # Index unlatch
            self.index.latch.release()

    def invalidateTailRecords(self, indirectionRID, baseRID, selectedPageRange):
        if indirectionRID == baseRID:
            return
        else:
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
            "keyToRID": self.keyToRID,
            "tailRIDs": self.tailRIDs,
            "numMerges": self.numMerges
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
        return pageIndex

    # 1. Call perform merge on background thread
    # 2. Have BP only write metaData pages for any pages currently being merged which are also in the BP still
    # 3. Replace all returned consolidated base page data pages and Page_Meta at the path
    def initiateMerge(self, pageRange):
        # 1.
        with concurrent.futures.ThreadPoolExecutor() as executor:
            backgroundThread = executor.submit(self.performMerge, pageRange)
            mergedBasePages = backgroundThread.result()
            if mergedBasePages == None:
                return
            for mergedBasePage in mergedBasePages:
                BPIndex = BP.pathInBP(mergedBasePage.path)
                # 2.
                if BPIndex != None:
                    BP.bufferpool[BPIndex].consolidated = True
                # 3.
                mergedBasePage.writeDataToDisk(mergedBasePage.path)
            self.numMerges += 1

    # 1. Recreate all full base pages and tail pages
    #   a. if tail pages haven't been written out yet, don't perform (see function for more in-depth explanation)
    # 2. Map base pages to their path and keep track of updatedBaseRecords
    # 3. Iterate through reversed tail records
    #   a. Keep track of seen base records so they only get updated once
    # 4. Get base record by matching paths with tail records baseRID and update with tail page data
    def performMerge(self, pageRange):
        # 1.
        basePages = self.getAllFullBasePages(pageRange)
        tailPages = self.getAllFullTailPagesReversed(pageRange)
        if tailPages == None:
            return None
        # 2.
        updatedBaseRecords = set()
        mappedBasePages = {}
        for basePage in basePages:
            mappedBasePages[basePage.path] = basePage
        # 3.
        for tailPage in tailPages:
            allTailRecords = tailPage.getAllRecordsReversed()
            for tailRecord in allTailRecords:
                # 3a.
                if tailRecord[BASE_RID] in updatedBaseRecords:
                    continue
                else:
                    updatedBaseRecords.add(tailRecord[BASE_RID])
                # 4.
                basePagePath = self.getBasePagePath(tailRecord[BASE_RID])
                if basePagePath in mappedBasePages:
                    basePage = mappedBasePages[basePagePath]
                    pageOffset = self.calculatePageOffset(tailRecord[BASE_RID])
                    basePage.mergeTailRecord(pageOffset, tailRecord[RID_COLUMN], tailRecord[tailPage.numMetaElements():])
        return basePages

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

    # 1. If base page not committed yet, don't add to merge queue (dir path doesn't exist)
    # 2. Check if base page is full and therefore eligible for merge
    # 3. Return list of full base pages
    def getAllFullBasePages(self, selectedPageRange):
        allFullBasePages = []
        # iterate from 0 to most recently updated pageRange (handle case for only 1 pageRange)
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        for selectedBasePage in range(0, self.calculateBasePageIndex(self.baseRID) + 1):
            BasePagePath = PageRangePath + "/basePage_" + str(selectedBasePage)
            # 1.
            if not os.path.exists(BasePagePath):
                continue
            # 2.
            MetaPagePath = BasePagePath + "/Page_Meta.json"
            f = open(MetaPagePath, "r")
            metaDictionary = json.load(f)
            f.close()
            if ElementsPerPhysicalPage != metaDictionary["num_records"]:
                continue
            # 3.
            else:
                allFullBasePages.append(self.getBasePage(selectedPageRange, BasePagePath))
        return allFullBasePages

    # 1. Iterate in reverse from self.numMerges * MergePolicy + MergePolicy - 1 through self.numMerges * MergePolicy (9-0, 19-10, etc.)
    # 2. If tail page in range not committed yet (dir path doesn't exist), don't perform merge
    def getAllFullTailPagesReversed(self, selectedPageRange):
        allFullTailPages = []
        # 1.
        PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
        for selectedTailPage in range(self.numMerges * MergePolicy + MergePolicy, self.numMerges * MergePolicy - 1, -1):
            TailPagePath = PageRangePath + "/tailPage_" + str(selectedTailPage)
            # 2. Tail page in range not committed yet, don't perform merge
            if not os.path.isdir(TailPagePath):
                return None
            allFullTailPages.append(self.getTailPage(selectedPageRange, TailPagePath))
        return allFullTailPages

    # Getting pages outside of bufferpool for merge
    def getTailPage(self, selectedPageRange, TailPagePath):
        page = TailPage(self.num_columns, selectedPageRange, TailPagePath)
        page.readPageFromDisk(TailPagePath)
        return page

    # Getting pages outside of bufferpool for merge
    def getBasePage(self, selectedPageRange, BasePagePath):
        page = BasePage(self.num_columns, selectedPageRange, BasePagePath)
        page.readPageFromDisk(BasePagePath)
        return page

    def getPageRange(self, baseRID):
        return floor(baseRID / RecordsPerPageRange)

    # Base record's indirection is pointing to a record that's already been merged
    def recordHasBeenMerged(self, baseRecord, TPS):
        if baseRecord[INDIRECTION_COLUMN] <= TPS:
            return True
        return False

    # Cumulative splicing
    def createCumulativeRecord(self, oldRecord, updatedRecord, indirectionColumn, baseRID, selectedPageRange, NumMetaElements):
        createdRecord = []
        for metaIndex in range(0, MetaElements + 1):
            createdRecord.append(0)
        createdRecord[INDIRECTION_COLUMN] = indirectionColumn
        createdRecord[RID_COLUMN] = self.tailRIDs[selectedPageRange]
        createdRecord[TIMESTAMP_COLUMN] = round(time.time() * 1000)
        createdRecord[SCHEMA_ENCODING_COLUMN] = 1
        createdRecord[BASE_RID] = baseRID
        for columnIndex in range(0, len(updatedRecord)):
            #use data from the oldRecord
            if updatedRecord[columnIndex] == None:
                createdRecord.append(oldRecord[columnIndex + NumMetaElements])
            else:
                createdRecord.append(updatedRecord[columnIndex])
        return createdRecord

    def getAllUpdatedRecords(self):
        allRecords = []
        for selectedPageRange in range(0, self.getPageRange(self.baseRID) + 1):
            PageRangePath = self.path + "/pageRange_" + str(selectedPageRange)
            for selectedBasePage in range(0, self.calculateBasePageIndex(self.baseRID) + 1):
                BasePagePath = PageRangePath + "/basePage_" + str(selectedBasePage)
                BPindex = self.getBasePageBPIndex(BasePagePath, selectedPageRange)
                for baseRecord in BP.bufferpool[BPindex].getAllRecords():
                    if baseRecord[RID_COLUMN] == INVALID:
                        continue

                    #check for tail page
                    if baseRecord[INDIRECTION_COLUMN] != 0:
                        tailIndex = self.calculateTailPageIndex(baseRecord[INDIRECTION_COLUMN])
                        TailPagePath = PageRangePath + "/tailPage_" + str(tailIndex)

                        tailBPindex = BP.pathInBP(TailPagePath)
                        if tailBPindex is None:
                            # the path does exist, so go read the basepage from disk
                            page = TailPage(self.num_columns, selectedPageRange, TailPagePath)
                            page.readPageFromDisk(TailPagePath)
                            tailBPindex = BP.add(page)
                        else:
                            # here the page is in the bufferpool, so we will refresh it.
                            tailBPindex = BP.refresh(tailBPindex)

                        for tailRecord in BP.bufferpool[tailBPindex].getAllRecords():
                            if (tailRecord[RID_COLUMN] == baseRecord[INDIRECTION_COLUMN]):
                                allRecords.append(tailRecord)
                            elif baseRecord[INDIRECTION_COLUMN] == 0:
                                allRecords.append(baseRecord)
                        BP.bufferpool[tailBPindex].pinned -= 1
                BP.bufferpool[BPindex].pinned -=1
        return allRecords

    def indexInsert(self, record):
        RID = self.baseRID 
        newRecord = [record[0],record[1],record[2],record[3],record[4], RID]
        incrementer = 0
        for index in self.index.indices:
            incrementer += 1
            if index != None:
                index.insert(newRecord,incrementer)

    def indexUpdate(self, record):
        newRecord = [record[5],record[6],record[7],record[8],record[9], record[RID_COLUMN]]
        incrementer = 0
        for index in self.index.indices:
            incrementer += 1
            if index != None:
                index.findAndChange(newRecord,record[RID_COLUMN])

    def indexDelete(self,RID):
        newRecord = [-1,-1,-1,-1,-1,-1]
        incrementer = 0
        for index in self.index.indices:
            incrementer += 1
            if index != None:
                index.findAndChange(newRecord,RID)