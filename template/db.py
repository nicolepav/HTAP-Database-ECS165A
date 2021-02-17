from template.table import Table
import os

class Database():

    def __init__(self):
        self.tables = []
        self.path = None
        pass

    def open(self, path):
        if path == None:
            raise Exception("Open Error: there is no defined path.")
        if not os.path.exists(path):
            os.mkdir(path)
        self.path = path

        # put the tables found at the self.path into the bufferpool (aka load it into ram)

        # for each table directory in the database directory,
        for tableDir in [dI for dI in os.listdir(path) if os.path.isdir(os.path.join(path,dI))]:
            tableDirPath = self.path + '/' + tableDir
            print("Table Directory: " + tableDirPath)
            # open the table directory, 
                # can't use the table.open without first having a table object
                # table = Table()
                # we want table.open to populate the table with the data in the Table's Directory
                # table.open(tableDirPath);
            # then load the table directory to self.tables
                # self.tables.append(table)
        pass

    def close(self):
        if self.path == None:
            raise Exception("Close Error: there is no defined path.")
        if not os.path.exists(self.path):
            raise Exception("Close Error: there is no file at this path.")

        # How should we deal with old tables? (is this even something we need to do?)

        for table in self.tables:
            # we want table.close to store the contents of the table to a table directory
            tableDirPath = self.path + "/table_" + table.name
            if not os.path.exists(tableDirPath):
                os.mkdir(tableDirPath)
            table.close(tableDirPath);
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for table in self.tables:
            if table.name == name:
                del table

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
