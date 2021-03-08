from template.table import Table, Record, LockManager
from template.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.insertedBaseRIDs = []
        self.insertedTailRIDs = []
        # maps baseRID to previous indirection value
        self.updatedIndirectionColumns = {}
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
        # Before we perform any queries, do the following:
        for query, args in self.queries:
            # a. If insert, add baseRID to self.insertedBaseRIDs
            # b. If update, then map baseRID to indirection and add tailRID to self.insertedTailRIDs
            pass
        for query, args in self.queries:
            if self.ID != -1:
                self.ID = query.table.lockManager.getTransactionID()
            # TODO if Query.insert can't be used to check function type, refactor this into Query class (try to print the self.queries to see how stuff is stored, this way we can do a comparison)
            if query.__name__ == "insert":
                if not query.table.lockManger.obtainSLock(args[0], self.ID):
                    self.abort()
                else:
                    # get baseRID for inserted record (insert needs to happen atomically after this if we use this logic)
                    #TODO query.table.latch.aquire()????
                    self.insertedBaseRIDS.append(query.table.baseRID + 1)
                    # call insert query
            elif query.__name__ == "update":
                if not query.table.lockManger.obtainXLock(args[0], self.ID):
                    self.abort()
                else:
                    queryColumns = []
                    for i in query.table.num_columns:
                        queryColumns.append(1)
                    baseRecord = query.table.select(args[0], 1, queryColumns)[0]
                    self.updatedIndirectionColumns[baseRecord[RID_COLUMN]] = baseRecord[INDIRECTION_COLUMN]
                    self.insertedTailRIDS.append(query.table.tailRIDs + 1)
            elif query.__name__ == "select":
                if not query.table.lockManger.obtainSLock(args[0], self.ID):
                    self.abort()
            result = query(*args)

            # Insert: need baseRID for inserted record
            # Update: need previus base record indirection with indirection column (indirection column is tailRID) 
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        self.rollbackChanges()
        self.releaseLocks()
        return False

    def commit(self):
        self.releaseLocks()
        return True

    #TODO: iterate through any insertedBase or tail RIDs and delete
    # and then replace any base record's updatedIndirectionColumns with their previously mapped value
    def rollbackChanges(self):

        # for thing in self.insertedBaseRIDS:
        #     thing.rollbackChange()

        # for thing in self.insertedTailRIDS:
        #     thing.rollbackChange()

        pass

    def releaseLocks(self):
        for query, args in self.queries:
            if query == Query.insert:
                query.table.lockManger.giveUpSLock(args[0], self.ID)
            elif query == Query.update:
                query.table.lockManger.giveUpXLock(args[0], self.ID)
            elif query == Query.select:
                query.table.lockManger.giveUpSLock(args[0], self.ID)
