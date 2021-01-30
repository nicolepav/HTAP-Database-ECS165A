# Global Setting for the Database
# PageSize, StartRID, etc..

# PhysicalPageBytes = 4096                              # Physical Pages Size
# Data_Bytes = 8                                        # Each data within a record stored as 8 bytes
# RecordsPerPhysicalPage = PhysicalPageBytes / Data_Bytes       # records per physical page
# BasePagesPerPageRange = 16                            # Number of





# Element(8 bytes)                                                              # 8 "bytes" in one "PhysicalPage"
# PhysicalPage(Element, Element, Element, ...)                                  # 512 "element"s in one "PhysicalPage"
# BasePage(PhysicalPage, PhysicalPage, PhysicalPage, ...)                       # 9 (4 are meta filled, 5 are data filled) "PhysicalPage"s in one "BasePage"
# PageRange(BasePage, BasePage, BasePage, ...)                                  # 16 "BasePage"s in one "PageRange"

BytesPerElement = 8
PhysicalPageBytes = 4096
ElementsPerPhysicalPage = PhysicalPageBytes /  BytesPerElement                                                # aka records per Physical Page
# MetaColumnsPhysicalPages = 4
# DataColumnsPerPhysicalPage = 5
# PhysicalPagesPerPage = MetaColumnsPhysicalPages + DataColumnsPerPhysicalPage        # 9 (4 are meta filled, 5 are data filled)
PagesPerPageRange = 16
BytesPerPhysicalPage = ElementsPerPhysicalPage * BytesPerElement              # 4096 "bytes"

# ElementsPerPage = PhysicalPagesPerPage * ElementsPerPhysicalPage
# ElementsPerPageRange = PagesPerPageRange * ElementsPerPage