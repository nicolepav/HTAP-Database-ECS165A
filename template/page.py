from template.config import *
import time
import os
import json

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID = 4


# aka base/tail pages
class Page:
    def __init__(self, num_columns, PageRange, path):
        # 1. initialize meta columns
        self.metaColumns = []
        for i in range(0, MetaElements):
            self.metaColumns.append(PhysicalPage())
        # 2. initialize data columns
        self.dataColumns = []
        for columns in range(0, num_columns):
            self.dataColumns.append(PhysicalPage())
        self.num_records = 0

        # still need to implement this logic
        self.PageRange = PageRange
        self.path = path
        self.dirty = False
        self.pinned = 0
        self.age = 1

    def getRecord(self, offset):
        record = []
        for metaData in self.metaColumns:
            record.append(metaData.read(offset))
        for dataColumn in self.dataColumns:
            record.append(dataColumn.read(offset))
        return record

    def getAllRecords(self):
        records = []
        recordsPerPage = int(ElementsPerPhysicalPage)
        for i in range(0, self.num_records):
            record = self.getRecord(i)
            records.append(self.getRecord(i))
        return records
        
    def getAllRecordsReversed(self):
        records = []
        recordsPerPage = int(ElementsPerPhysicalPage)
        for i in range(self.num_records - 1, -1, -1):
            record = self.getRecord(i)
            records.append(self.getRecord(i))
        return records

    def isFull(self):
        return self.num_records == ElementsPerPhysicalPage

    def invalidateRecord(self, pageOffset):
        self.dirty = True
        self.metaColumns[RID_COLUMN].update(INVALID, pageOffset)
        return self.metaColumns[INDIRECTION_COLUMN].read(pageOffset)

    def insertMetaData(self, metaData, PhysicalPageDir, numMetaElements):
        for i in range(numMetaElements):
            if int(PhysicalPageDir[-1]) == i:
                self.metaColumns[i] = metaData

    def insertData(self, data, PhysicalPageDir, numMetaElements):
        for i in range(len(self.dataColumns)):
            if (int(PhysicalPageDir[-1]) - numMetaElements) == i:
                self.dataColumns[i] = data

    def numMetaElements(self):
        return MetaElements

    def writePageToDisk(self, path):
        if self.consolidated:
            self.writeMetaToDisk(path)
            return
        self.writePageMeta(path)
        index = 0
        for metaData in self.metaColumns:
            PhysicalPagePath = path + "/metaData_" + str(index)
            metaData.writeToDisk(PhysicalPagePath)
            index += 1
        for dataIndex in range(0, len(self.dataColumns)):
            PhysicalPagePath = path + "/data_" + str(index)
            self.dataColumns[dataIndex].writeToDisk(PhysicalPagePath)
            index += 1

    def readPageFromDisk(self, path):
        # Setup physical pages from disk
        # 1. Iterate through physical pages and make path to physical page file
        # 2. Pass in physical page file path to PhysicalPage.readFromDisk

        numColumns = self.readPageMeta(path)
        numMetaElements = self.numMetaElements()

        self.metaColumns = [0 for x in range(numMetaElements)]
        self.dataColumns = [0 for x in range(numColumns)]

        for PhysicalPageDir in [f for f in os.listdir(path) if os.path.isfile(os.path.join(path,f))]:
            PhysicalPagePath = path + '/' + PhysicalPageDir
            if "metaData_" in PhysicalPageDir:
                metaData=PhysicalPage()
                metaData.readFromDisk(PhysicalPagePath)
                self.insertMetaData(metaData, PhysicalPagePath, numMetaElements)
            elif "data_" in PhysicalPageDir:
                dataColumn=PhysicalPage()
                dataColumn.readFromDisk(PhysicalPagePath)
                self.insertData(dataColumn, PhysicalPagePath, numMetaElements)

# End Page Class #




class BasePage(Page):
    def __init__(self, num_columns, PageRange, path):
        # 1. initialize meta columns
        self.TPS = -1
        self.metaColumns = []
        for i in range(0, MetaElements):
            self.metaColumns.append(PhysicalPage())
        # 2. initialize data columns
        self.dataColumns = []
        for columns in range(0, num_columns):
            self.dataColumns.append(PhysicalPage())
        self.num_records = 0

        # still need to implement this logic (what about for merge?)
        self.PageRange = PageRange
        self.path = path
        self.dirty = False
        self.pinned = 0
        # only updated when in bufferpool
        self.consolidated = False
        self.age = 1

    # Appends each record's element across all physical pages
    def insert(self, RID, record):
        self.dirty = True
        self.num_records += 1
        for index, dataColumn in enumerate(self.dataColumns):
            dataColumn.appendData(record[index])
        self.initializeRecordMetaData(RID)

    def newRecordAppended(self, RID, pageOffset):
        self.metaColumns[INDIRECTION_COLUMN].update(RID, pageOffset)
        self.metaColumns[SCHEMA_ENCODING_COLUMN].update(1, pageOffset)

    def initializeRecordMetaData(self, baseRID):
        self.metaColumns[INDIRECTION_COLUMN].appendData(0)
        self.metaColumns[RID_COLUMN].appendData(baseRID)
        self.metaColumns[TIMESTAMP_COLUMN].appendData(round(time.time() * 1000))
        self.metaColumns[SCHEMA_ENCODING_COLUMN].appendData(0)

    def mergeTailRecord(self, offset, tailRID, tailRecordData):
        if tailRID > self.TPS:
            self.TPS = tailRID
        for index, dataColumn in enumerate(self.dataColumns):
            dataColumn.update(tailRecordData[index], offset)

    def writePageMeta(self, path):
        #base page, so store page meta (numrecords and TPS)
        MetaJsonPath = path + "/Page_Meta.json"
        fullPath = os.path.join(MetaJsonPath)
        f = open(fullPath, "w")
        metaDictionary = {
            "num_records": self.num_records,
            "num_columns": len(self.dataColumns),
            "TPS": self.TPS
        }
        json.dump(metaDictionary, f, indent=4)
        f.close()

    def readPageMeta(self, path):
        #base page, so get page meta (numrecords and TPS)
        MetaJsonPath = path + "/Page_Meta.json"
        fullPath = os.path.join(MetaJsonPath)
        f = open(fullPath, "r")
        f.seek(0)
        metaDictionary = json.load(f)
        f.close()
        self.num_records = metaDictionary["num_records"]
        self.TPS = metaDictionary["TPS"]
        numColumns = metaDictionary["num_columns"]
        return numColumns

    def writeMetaToDisk(self, path):
        index = 0
        for metaData in self.metaColumns:
            PhysicalPagePath = path + "/metaData_" + str(index)
            metaData.writeToDisk(PhysicalPagePath)
            index += 1

    def writeDataToDisk(self, path):
        self.writePageMeta(path)
        index = MetaElements
        for dataIndex in range(0, len(self.dataColumns)):
            PhysicalPagePath = path + "/data_" + str(index)
            self.dataColumns[dataIndex].writeToDisk(PhysicalPagePath)
            index += 1


class TailPage(Page):
    def __init__(self, num_columns, PageRange, path):
        # 1. initialize meta columns
        self.metaColumns = []
        for i in range(0, self.numMetaElements()):
            self.metaColumns.append(PhysicalPage())
        # 2. initialize data columns
        self.dataColumns = []
        for columns in range(0, num_columns):
            self.dataColumns.append(PhysicalPage())
        self.num_records = 0

        # still need to implement this logic
        self.PageRange = PageRange
        self.path = path
        self.dirty = False
        self.pinned = 0
        self.consolidated = False
        self.age = 1

    # Appends meta data and record data
    def insert(self, record):
        self.dirty = True
        self.num_records += 1
        for index, metaColumn in enumerate(self.metaColumns):
            metaColumn.appendData(record[index])
        for index, dataColumn in enumerate(self.dataColumns):
            dataColumn.appendData(record[index + self.numMetaElements()])

    def writePageMeta(self, path):
        #tail page, so store page meta (numrecords and TPS)
        MetaJsonPath = path + "/Page_Meta.json"
        f = open(MetaJsonPath, "w")
        metaDictionary = {
            "num_records": self.num_records,
            "num_columns": len(self.dataColumns)
        }
        json.dump(metaDictionary, f, indent=4)
        f.close()

    def readPageMeta(self, path):
        #tail page, so get page meta (numrecords)
        MetaJsonPath = path + "/Page_Meta.json"
        f = open(MetaJsonPath, "r")
        metaDictionary = json.load(f)
        f.close()
        self.num_records = metaDictionary["num_records"]
        numColumns = metaDictionary["num_columns"]
        return numColumns

    def numMetaElements(self):
        return MetaElements + 1

class PhysicalPage:

    def __init__(self):
        self.data = bytearray()

    def appendData(self, value):
        self.data += value.to_bytes(BytesPerElement, byteorder='big')
        pass

    def read(self, location):
        # location should be the element value between 0 and 511 (ElementsPerPhysicalPage)
        # if the location is valid, then we can read the element from its location on the Physical Page
        if location >= ElementsPerPhysicalPage:
            raise Exception("Read Error: Record does not exist.")
        byte_location = int(location * BytesPerElement)
        return int.from_bytes(self.data[byte_location:byte_location + BytesPerElement], byteorder='big')

    def update(self, value, location):
        # location should be the element value between 0 and 511 (ElementsPerPhysicalPage)
        # if the location is valid, then we can read the element from its location on the Physical Page
        # Note that this update function is not used to update a record, it is used for updating meta data columns
        if location >= ElementsPerPhysicalPage:
            raise Exception("Update Error: Record does not exist.")
        byte_location = int(location * BytesPerElement)
        self.data[(byte_location):(byte_location + BytesPerElement)] = value.to_bytes(BytesPerElement, byteorder='big')

    def writeToDisk(self, path):
        f = open(path, "wb")
        f.write(self.data)
        f.close()

    def readFromDisk(self, path):
        f = open(path, "rb")
        self.data = bytearray(f.read())
        f.close()
