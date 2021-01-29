# Global Setting for the Database
# PageSize, StartRID, etc..

PhysicalPageBytes = 4096                             # Physical Pages Size
Data_Bytes = 8                                        # Each data within a record stored as 8 bytes
RecordsPerPage = PhysicalPageBytes / Data_Bytes  
BasePagesPerPageRange = 16                                        # Number of






# Data(8 bytes)                                             8 "bytes" in one "PhysicalPage"

# PhysicalPage(Data, Data, Data, ...)                       500 "data"s in one "PhysicalPage"

# BasePage(PhysicalPage, PhysicalPage, PhysicalPage, ...)   9 (4 are meta filled, 5 are data filled) "PhysicalPage"s in one "BasePage"

# PageRange(BasePage, BasePage, BasePage, ...)              16 "BasePage"s in one "PageRange"


# BytesPerData = 8
# DatasPerPhysicalPage = 500
# MetaPhysical Pages = 4
# DataPhysical Pages = 4
# PhysicalPagesPerBasePage = 9 (4 are meta filled, 5 are data filled)

