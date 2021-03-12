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

    def run(self):
        if self.ID == -1:
                self.ID = LM.getTransactionID()
        for query, args in self.queries:
            if query.__name__ == "insert":
                LM.latch.acquire()
                hasLock = LM.obtainXLock(query.__self__.table.name + '_' + str(args[0]), self.ID)
                LM.latch.release()
                if not hasLock:
                    result = False
                else:
                    result = query(*args)
                    if result != False:
                        LM.latch.acquire()
                        self.insertedBaseData.append(result)
                        LM.latch.release()
            elif query.__name__ == "update":
                LM.latch.acquire()
                hasLock = LM.obtainXLock(query.__self__.table.name + '_' + str(args[0]), self.ID)
                LM.latch.release()
                if not hasLock:
                    result = False
                else:
                    result = query(*args)
                    if result != False:
                        LM.latch.acquire()
                        self.insertedTailData.append(result)
                        LM.latch.release()
            elif query.__name__ == "select":
                LM.latch.acquire()
                hasLock = LM.obtainSLock(query.__self__.table.name + '_' + str(args[0]), self.ID)
                LM.latch.release()
                if not hasLock:
                    result = False
                else:
                    result = query(*args)
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        LM.latch.acquire()
        self.rollbackChanges()
        self.releaseLocks()
        LM.latch.release()
        return False

    def commit(self):
        LM.latch.acquire()
        self.commitUpdatedRecords()
        self.releaseLocks()
        LM.latch.release()
        return True

    # make base page point to committed tail record and map key to committed base record
    def commitUpdatedRecords(self):
        for data in self.insertedTailData:
            table = data[0]
            tailRID = data[1]
            selectedPageRange = data[2]
            baseRID = data[3]
            table.updateBaseIndirection(baseRID, tailRID)
        for data in self.insertedBaseData:
            table = data[0]
            baseRID = data[1]
            key = data[2]
            table.keyToRID[key] = baseRID

    def rollbackChanges(self):
        for data in self.insertedTailData:
            table = data[0]
            tailRID = data[1]
            selectedPageRange = data[2]
            baseRID = data[3]
            table.deleteTailRecord(tailRID, selectedPageRange)
        for data in self.insertedBaseData:
            table = data[0]
            baseRID = data[1]
            table.deleteBaseRecord(baseRID)

    def releaseLocks(self):
        for query, args in self.queries:
            if query.__name__ == "insert":
                LM.giveUpXLock(query.__self__.table.name + '_' + str(args[0]), self.ID)
            elif query.__name__ == "update":
                LM.giveUpXLock(query.__self__.table.name + '_' + str(args[0]), self.ID)
            elif query.__name__ == "select":
                LM.giveUpSLock(query.__self__.table.name + '_' + str(args[0]), self.ID)
