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
        for i in range(0, self.dataColumns[0].num_records):
            record = self.getRecord(i)
            records.append(self.getRecord(i))
        return records

    def isFull(self):
        return self.dataColumns[0].num_records == ElementsPerPhysicalPage

    def invalidateRecord(self, pageOffset):
        self.metaColumns[RID_COLUMN].update(INVALID, pageOffset)
        return self.metaColumns[INDIRECTION_COLUMN].read(pageOffset)

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

    # Appends each record's element across all physical pages
    def insert(self, RID, record):
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

    # Appends meta data and record data
    def insert(self, record):
        for index, metaColumn in enumerate(self.metaColumns):
            metaColumn.appendData(record[index])
        for index, dataColumn in enumerate(self.dataColumns):
            dataColumn.appendData(record[index + MetaElements])

class PhysicalPage:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray()

    def has_capacity(self):
        return self.num_records < ElementsPerPhysicalPage

    def appendData(self, value):
        # if a physical page has capacity, append and element to the Physical Page
        if not self.has_capacity():
            raise Exception("Insert Error: Physical Page is already full.")
        self.num_records += 1
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
