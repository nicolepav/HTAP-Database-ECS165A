from template.config import *


class Page:

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

