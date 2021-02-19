from template.config import *
import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

INVALID = 0
# aka base/tail pages
class Page:

    def __init__(self, num_columns):
        # 1. initialize meta columns
        self.metaColumns = []
        for i in range(0, MetaElements):
            self.metaColumns.append(PhysicalPage())
        # 2. initialize data columns
        self.dataColumns = []
        for columns in range(0, num_columns):
            self.dataColumns.append(PhysicalPage())
        self.numrecords = 0

        # still need to implement this logic
        self.dirty = False
        self.pinned = 0

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

    def isFull(self):
        return self.num_records == ElementsPerPhysicalPage

    def invalidateRecord(self, pageOffset):
        self.dirty = True
        self.metaColumns[RID_COLUMN].update(INVALID, pageOffset)
        return self.metaColumns[INDIRECTION_COLUMN].read(pageOffset)

    def writeToDisk(self, path):
        # path look like "./ECS165/table_<table.name>/pageRange_<pageRange index>/(base/tail)Page_<basePage or tailPage index>"
        # we want (base/tail)Page.writeToDisk to store the contents of the basePage to a Page directory

        for index, metaData in enumerate(self.metaColumns):
            PhysicalPagePath = path + "/metadata_" + str(index)
            metaData.writeToDisk(PhysicalPagePath)
        for index, dataColumn in enumerate(self.dataColumns):
            PhysicalPagePath = path + "/data_" + str(index)
            dataColumn.writeToDisk(PhysicalPagePath)
        pass

    def readFromDisk(self, path, index):
        # path look like "./ECS165/table_<table.name>/pageRange_<pageRange index>/(base/tail)Page_<basePage or tailPage index>"


        # PhysicalPagePath = path + "/metadata_" + str(index)
        # metaData.readFromDisk(PhysicalPagePath)

        # PhysicalPagePath = path + "/data_" + str(index)
        # dataColumn.readFromDisk(PhysicalPagePath)

        pass

class BasePage(Page):
    def __init__(self, num_columns):
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
        self.dirty = False
        self.pinned = 0

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
        # need to figure out what to do about disk management here (should we delete old tail page files and replace with the new merged file?)
        if tailRID > self.TPS:
            self.TPS = tailRID
        for index, dataColumn in enumerate(self.dataColumns):
            dataColumn.update(tailRecordData[index], offset)

class TailPage(Page):
    def __init__(self, num_columns):
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
        self.dirty = False
        self.pinned = 0

    # Appends meta data and record data
    def insert(self, record):
        self.dirty = True
        self.num_records += 1
        for index, metaColumn in enumerate(self.metaColumns):
            metaColumn.appendData(record[index])
        for index, dataColumn in enumerate(self.dataColumns):
            dataColumn.appendData(record[index + MetaElements])

class PhysicalPage:

    def __init__(self):
        self.data = bytearray()

    def appendData(self, value):
        # append and element to the Physical Page (isFull() checks if there is capacity before calling)
        self.data += value.to_bytes(BytesPerElement, byteorder='big')

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
        f = open(path, "w+b")
        f.write(self.data)
        f.close()

    def readFromDisk(self, path):
        f = open(path, "w+b")
        self.data = f.read()
        f.close()
