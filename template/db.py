from template.table import Table
from template.page import *
import os
import json

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

        # How should we deal with old tables? (is this even something we need to do?)

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

            # put the tables found at the self.path into the bufferpool (aka load it into ram)

            # # for each table directory in the database directory,
            # for tableDir in [dI for dI in os.listdir(path) if os.path.isdir(os.path.join(path,dI))]:
            #     tableDirPath = self.path + '/' + tableDir

                # # reads the stored Meta.json and returns the constructed Dictionary
                # MetaJsonPath = tableDirPath + "/Meta.json"
                # f = open(MetaJsonPath, "r")
                # metaDictionary = json.load(f)
                # json.decoder.JSONDecodeError: Expecting value: line 1008 column 16 (char 25009)
                # f.close()
                # metaDictionary is a dictionary filled with the table's meta info


                # print("Table Directory: " + tableDirPath)
                # print("MetaJson File:   " + MetaJsonPath)
                # open the table directory, 
                    # can't use the table.open without first having a table object
                    # table = Table()
                    # we want table.open to populate the table with the data in the Table's Directory
                    # table.open(tableDirPath);
                # then load the table directory to self.tables
                    # self.tables.append(table)



class Bufferpool():
    
    def __init__(self,):
        self.bufferpool = []
        #initialize queue
        #bufferpool.pop(0)
        #bufferpool.append(<page>)
        pass


    def BufferpoolIsFull(self):
        return len(self.bufferpool) >= BufferpoolSize
        
    def add(self, page):

        #check if page if page is already in bufferpool
        #has to check the rid range of that page if it's a base page
        #else if tailpage, you have to check the page for that record
        if page in self.bufferpool: 
            return #pointer to that page? so it can be used? index to page??


        if (self.BufferpoolIsFull()):
            self.kick()
            
        #add the new page here

        bufferpool.append(page)

        # if new page is added, return pointer to that page? so it can be used?

        pass

    #need a way to perform operation on that page in the bufferpool
    
    def kick(self):
        # called when we need to kick a page

        kicked = self.bufferpool.pop(0)
        
        while (kicked.pinned > 0):
            # throw it to the back of the bufferpool so next object can be kicked
            bufferpool.append(kicked)
            kicked = self.bufferpool.pop(0)

        if (kicked.dirty == True):
            # write the dirty page to disk

            # get the correct path of where we need to write to

            # path = 

            # path should look like: "./ECS165/table_<table.name>/pageRange_<pageRange index>/(base/tail)Page_<basePage or tailPage index>"
            kicked.writeToDisk(path)
        # when not dirty, we don't have to worry about writing it back to the disk

        pass



    '''
    our bufferpool will act as the intermediary between the physical table and our operations
    so when we insert: we create a page in memeory and perform operations on it
    when that page is kicked out (kick()) of the bufferpool, we write it onto the disk
        pages will created in order in the bufferpool, and then written to physical memory in order (unless pinned)

    

    updates will have to pull the physical base page and then tail pages into memory/create a tail page in memory, and then update the record

    deletions will function similarly to updates

    base functionality
        rewrite all function to hook into bufferpool
    merge/dirty functionality
    pinning pages, locking from getting kicked


    Hooke bufferpool into:
        insert

        update

        delete

    '''



