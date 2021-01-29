from template.config import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
# aka base/tail pages
class Page:
    def __init__(self, record):
        # make meta PhysicalPages
        indirectionColumn = Page()
        RID = Page()
        timeStamp = Page()
        schemaEncoding = Page()
        # make data PhysicalPages from record
        data_columns = []
        for data in record:
            physicalPage = PhysicalPage(data)
            data_columns.append(physicalPage)
        pass

# make up Page class
class PhysicalPage:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PageSize)

    def has_capacity(self):
        return ((PageRecords - self.num_records) > 0)
        pass

    def write(self, value):
        if self.has_capacity():
            self.num_records += 1
            return 0
        else:
            return 1
        pass

