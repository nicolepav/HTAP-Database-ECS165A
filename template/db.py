from template.table import Table
from template.page import *
from template.config import *
import os
import json
import shutil


class Database():

    def __init__(self, path='./ECS165'):
        self.tables = []
        self.path = path
        pass

    def open(self, path):
        if not os.path.exists(path):
            os.mkdir(path)
        self.path = path
        pass

    def close(self):
        if not os.path.exists(self.path):
            raise Exception("Close Error: there is no file at this path.")

        for table in self.tables:
            # we want table.close to store the contents of the table to a table directory
            tableDirPath = self.path + "/table_" + table.name
            if not os.path.exists(tableDirPath):
                os.mkdir(tableDirPath)
            table.close(tableDirPath)
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        Tablepath = self.path + "/table_" + name
        if os.path.exists(Tablepath):
            # TODO uncomment while testing for faster testing
            shutil.rmtree(Tablepath)
            os.mkdir(Tablepath)
            # raise Exception("Create Table: Cannot create a table that already exists on disk. Use get_table or uncomment code to test quickly")
        table = Table(name, num_columns, key, Tablepath)
        self.tables.append(table)
        return table

    # #debugging create table:
    # def create_table(self, name, num_columns, key):
    #     Tablepath = self.path + "/table" + name
    #     if not os.path.exists(Tablepath):
    #         os.mkdir(Tablepath)
    #     else:
    #         shutil.rmtree(Tablepath)
    #         os.mkdir(Tablepath)
    #     table = Table(name, num_columns, key, Tablepath)
    #     self.tables.append(table)
    #     return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for table in self.tables:
            if table.name == name:
                del table
        Tablepath = self.path + "/table_" + name
        DroppedTablePath = self.path + "/DROPPED_table_" + name
        if os.path.exists(Tablepath):
            os.rename(Tablepath, DroppedTablePath)


    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        # search for the table at the self.path on the disk
        # for each table directory in the database directory,
        tableName = "/table_" + name
        for tableDir in [dI for dI in os.listdir(self.path) if os.path.isdir(os.path.join(self.path,dI))]:
            tableDirPath = self.path + '/' + tableDir
            if tableName in tableDir:
                # get the table object
                # reads the stored Meta.json and returns the constructed Dictionary
                MetaJsonPath = tableDirPath + "/Meta.json"
                f = open(MetaJsonPath, "r")
                metaDictionary = json.load(f)
                # json.decoder.JSONDecodeError: Expecting value: line 1008 column 16 (char 25009)
                f.close()
                # metaDictionary is a dictionary filled with the table's meta info
                # open the table directory, 
                    # can't use the table.open without first having a table object
                table = Table(metaDictionary["name"],
                    metaDictionary["num_columns"],
                    metaDictionary["key"],
                    tableDirPath,
                    metaDictionary["baseRID"],
                    metaDictionary["keyToRID"],
                    metaDictionary["tailRIDs"]
                )
                # we want table.open to populate the table with the data in the Table's Directory
                # then load the table directory to self.tables
                self.tables.append(table)
                return table