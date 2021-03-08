from template.table import Table, Record, LockManager
from template.index import Index
from template.config import *

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.insertedBaseData = []
        self.insertedTailData = []
        self.ID = -1

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        if self.ID == -1:
                self.ID = LM.getTransactionID()
        for query, args in self.queries:
            if query.__name__ == "insert":
                LM.latch.acquire()
                hasLock = LM.obtainXLock(args[0], self.ID)
                LM.latch.release()
                if not hasLock:
                    result = False
                else:
                    result = query(*args)
                    self.insertedBaseData.append(result)
            elif query.__name__ == "update":
                LM.latch.acquire()
                hasLock = LM.obtainXLock(args[0], self.ID)
                LM.latch.release()
                if not hasLock:
                    result = False
                else:
                    result = query(*args)
                    self.insertedTailData.append(result)
            elif query.__name__ == "select":
                LM.latch.acquire()
                hasLock = LM.obtainSLock(args[0], self.ID)
                LM.latch.release()
                if not hasLock:
                    result = False
                else:
                    result = query(*args)
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        self.rollbackChanges()
        self.releaseLocks()
        return False

    def commit(self):
        self.commitUpdatedRecords()
        self.releaseLocks()
        return True

    # TODO: get tail record from tailRID, then get base record, then check if base indirection is less than tailRID and update if so
    def commitUpdatedRecords(self):
        for data in self.insertedTailData:
            table = data[0]
            tailRID = data[1]
            selectedPageRange = data[2]
            baseRID = data[3]
            table.updateBaseIndirection(baseRID, tailRID)

    #TODO: iterate through any insertedBase or tail RIDs and delete
    def rollbackChanges(self):
        for data in self.insertedTailData:
            table = data[0]
            tailRID = data[1]
            selectedPageRange = data[2]
            baseRID = data[3]
            table.deleteTailRecord(tailRID, selectedPageRange)
        for data in self.insertedBaseData:
            table = data[0]
            key = data[1]
            table.delete(key)

    def releaseLocks(self):
        for query, args in self.queries:
            if query.__name__ == "insert":
                LM.giveUpXLock(args[0], self.ID)
            elif query.__name__ == "update":
                LM.giveUpXLock(args[0], self.ID)
            elif query.__name__ == "select":
                LM.giveUpSLock(args[0], self.ID)
