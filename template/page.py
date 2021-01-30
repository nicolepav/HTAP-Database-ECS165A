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
        return ((ElementsPerPhysicalPage - self.num_records) > 0)

    def insert(self, value):
        if not self.has_capacity():
            raise Exception("Insert Error: Physical Page is already full.")
        self.num_records += 1
        update(self, value, num_records)
        return num_records

    def read(self, location):
        # location should be the element value between 0 and 512 (ElementsPerPhysicalPage)
        if location > self.num_records:
            raise Exception("Read Error: Record does not exist.")
        byte_location = location * BytesPerElement
        return int.from_bytes(self.data[(byte_location):(byte_location + 7)], byteorder='big')
        

    def update(self, value, location):
        # location should be the element value between 0 and 512 (ElementsPerPhysicalPage)
        if location > self.num_records:
            raise Exception("Update Error: Record does not exist.")
        byte_location = location * BytesPerElement
        self.data[(byte_location):(byte_location + 7)] = value.to_bytes(7, byteorder='big')
        

