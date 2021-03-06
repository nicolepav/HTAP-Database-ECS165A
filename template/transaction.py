from template.table import Table, Record
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
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    # On abort, iterate through any insertedBase or tail RIDs and delete
    # and then replace any base record's updatedIndirectionCOlumns with their previously mapped value
    # Release locks
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    # Release locks, won't undo changes (don't think we write to disk)
    def commit(self):
        # TODO: commit to database
        return True

