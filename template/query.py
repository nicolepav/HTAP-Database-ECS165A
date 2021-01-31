from template.table import Table, Record
from template.index import Index
from math import floor


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    
    When a record is deleted, the base record will be
    invalidated by setting the RID of itself and all its tail records to a special value. These
    invalidated records will be removed during the next merge cycle for the corresponding
    page range. The invalidation part needs to be implemented during this milestone. The
    removal of invalidated records will be implemented in the merge routine of the next
    milestone.

    """
    def delete(self, key):
        pass

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        self.table.insert(columns)
        pass

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns):
        pass

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # call on page update function
        # are we calling on the base or tail page
        # if tail page, which tail page (since there may be multiple)
        # call page update function once correct page is found
        

        # Use table class and its "keyToRID" dictonary to find the rid, if key not found return -1
        updateRID  = self.table.keyToRID.setdefault(key, -1)

        #return false if there is no RID associated for given key, else update the key and return true
        if (updateRID == -1):
            print("No RID found for this key")
            return False
        
        
        #Use that RID and math too its in and the page range index
        pageRangeIndex = floor(updateRID/(PagesPerPageRange * ElementsPerPage))
        currentpageRangeofRID = self.table.page_directory[pageRangeIndex]
        
        #find what basepage its on (get the remainder to get size within that page range and divide it by the size of a basepage)
        basePageIndex = floor(updateRID % (PagesPerPageRange * ElementsPerPage) / (PhysicalPagesPerPage * ElementsPerPhysicalPage))
                                            # ( 16 * 500 )/( 9 * )
        #updatedBasePage = currentpageRangeofRID.basePages[basePageIndex].updateRecord(updateRID, *columns)
        
        baseRecordMetaData = currentpageRangeofRID.basePages[basePageIndex].recordMetaData(updateRID)


        # TODO: append to available tail page with respects to the Base Page see if there's space, else create a new tail page
        
        # check schema column if need to go to tail page (do this in lower lvl function?)
        tailPageRID = 0
        if baseRecordMetaData[3] == 1:                  # if schema column = 1
            # indirection column of base points to tail RID
            tailPageRID = baseRecordMetaData[0]
            # find room in tailpage and append accordingly
        else:
            pass

        #Use the pageUpdate method to update the appropiate column


        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

