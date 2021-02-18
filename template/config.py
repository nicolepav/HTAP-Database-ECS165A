# Global Setting for the Database
# PageSize, StartRID, etc..

# Element(byte, byte, byte, byte, byte, byte, byte, '\x00')                     # 8 "bytes" in one "element" Note that only 7 of the bytes can be written to!
# PhysicalPage(Element, Element, Element, ...)                                  # 512 "element"s in one "PhysicalPage"
# BasePage(PhysicalPage, PhysicalPage, PhysicalPage, ...)                       # 9 (4 are meta filled, 5 are data filled) "PhysicalPage"s in one "BasePage"
# PageRange(BasePage, BasePage, BasePage, ...)                                  # 16 "BasePage"s in one "PageRange"
def init():
    pass
BytesPerElement = 8
PhysicalPageBytes = 4096
# aka records per base page
ElementsPerPhysicalPage = PhysicalPageBytes /  BytesPerElement
MetaElements = 4
# When we get 10 filled up tail pages, merge
MergePolicy = 10
PagesPerPageRange = 16
# records per base page * number of base pages per range = records per page range
RecordsPerPageRange = PagesPerPageRange * ElementsPerPhysicalPage
