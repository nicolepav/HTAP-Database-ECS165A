from template.config import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

# aka base/tail pages
class Page:
    def __init__(self, record):
        # make meta PhysicalPages
        indirectionColumn = PhysicalPage()
        RID = PhysicalPage()
        timeStamp = PhysicalPage()
        schemaEncoding = PhysicalPage()
        # make data PhysicalPages from record
        data_columns = []
        for data in record:
            physicalPage = PhysicalPage(data)
            data_columns.append(physicalPage)
        pass

    def insertRecord(self, RID, record):
        # update meta columns - will need special case for 
        # update data_columns: write each value of record

        pass
    
    # updatedDataColumns - 0 1 0
    def updateRecord(self, RID, updatedDataColumns):
        # 1. translate RID to page offset
        # 2. 
        pass

# make up Page class
class PhysicalPage:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(BytesPerPhysicalPage)

    def has_capacity(self):
        return ((BytesPerPhysicalPage - self.num_records) > 0)
        pass

    def insert(self, value):

        if self.has_capacity():
            self.num_records += 1
            self.data += value.to_bytes(8, byteorder='big')
            return num_records
        else:
            return False
        pass

    def read(self, location): # NOT WORKING
        # location should be the element value between 0 and 512 (ElementsPerPhysicalPage)
        byte_location = location * BytesPerElement
        return int.from_bytes(self.data[(byte_location):(byte_location + 7)], byteorder='big')
        pass

    def update(self, value, location): # NOT WORKING
        # location should be the element value between 0 and 512 (ElementsPerPhysicalPage)
        byte_location = location * BytesPerElement

        self.data[(byte_location):(byte_location + 7)] = value.to_bytes(8, byteorder='big')

        pass

