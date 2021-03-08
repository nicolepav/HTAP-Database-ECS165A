# from blist import blist
from sys import maxsize
from template.config import *
import threading
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
# TODO: to begin with, let's lock at start of every function then unlock at end
class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.indices[0] = BTree()
        self.table = table
        self.latch = threading.Semaphore()

    """
    # returns the location of all records with the given value on column "column"
    """
    #TODO Finish this
    def locate(self, column, value):
        returningRIDs = []
        allRecords = self.indices[column-1].find(value , column)
        for record in allRecords:
            returningRIDs.append(record)
        return returningRIDs

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        returningRIDs = []
        records = self.indices[column-1].findRange(begin,end,column)
        for record in records:
            returningRIDs.append(record)

        return returningRIDs

    """
    # optional: Create index on specific column
    """
    #record data = array of pageranges
    #column_number assumes the user passes the number of the table column from their view
    def create_index(self, column_number):
        # print("index created on ", column_number)
        self.indices[column_number-1] = BTree()
        # print(self.table.getAllUpdatedRecords())
        allRecords = self.table.getAllUpdatedRecords()
        for record in allRecords:
            insertedRecord = [record[4],record[5],record[6],record[7], record[RID_COLUMN]]
            self.indices[column_number-1].insert(insertedRecord,column_number)


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number-1] = None
        pass


returningData = []

class BNode:
    def __init__(self, data, par=None):
        #print ("Node __init__: " + str(data))
        self.data = list([data])
        self.parent = par
        self.child = list()
        self.returningData = []

    def __str__(self):
        if self.parent:
            return str(self.parent.data) + ' : ' + str(self.data)
        return 'Root : ' + str(self.data)

    def __lt__(self, node):
        return self.data[0] < node.data[0]

    def _isLeaf(self):
        return len(self.child) == 0

    # merge new_node sub-tree into self node
    def _add(self, new_node):
        # print ("Node _add: " + str(new_node.data) + ' to ' + str(self.data))
        for child in new_node.child:
            child.parent = self
        self.data.extend(new_node.data)
        self.data.sort()
        self.child.extend(new_node.child)
        if len(self.child) > 1:
            self.child.sort()
        if len(self.data) > 2:
            self._split()

    # find correct node to insert new node into tree
    def _insert(self, new_node, keyColumn):
        # print ('Node _insert: ' + str(new_node.data) + ' into ' + str(self.data))

        # leaf node - add data to leaf and rebalance tree
        if self._isLeaf():
            self._add(new_node)

        # not leaf - find correct child to descend, and do recursive insert
        elif new_node.data[0][keyColumn] > self.data[-1][keyColumn]:
            self.child[-1]._insert(new_node, keyColumn)
        else:
            for i in range(0, len(self.data)):
                if new_node.data[0][keyColumn] <= self.data[i][keyColumn]:
                    self.child[i]._insert(new_node, keyColumn)
                    break

    # 3 items in node, split into new sub-tree and add to parent
    def _split(self):
        # print("Node _split: " + str(self.data))
        left_child = BNode(self.data[0], self)
        right_child = BNode(self.data[2], self)
        if self.child:
            self.child[0].parent = left_child
            self.child[1].parent = left_child
            self.child[2].parent = right_child
            self.child[3].parent = right_child
            left_child.child = [self.child[0], self.child[1]]
            right_child.child = [self.child[2], self.child[3]]

        self.child = [left_child]
        self.child.append(right_child)
        self.data = [self.data[1]]

        # now have new sub-tree, self. need to add self to its parent node
        if self.parent:
            if self in self.parent.child:
                self.parent.child.remove(self)
            self.parent._add(self)
        else:
            left_child.parent = self
            right_child.parent = self

    # find all item in the tree; USED FOR SELECT(key) 
    def _find(self, key, keyColumn):
        # print ("Find " + str(item))
        for record in self.data:
            if record[keyColumn] == key:
                returningData.append(record)

        for child in self.child:
            child._find(key, keyColumn)

        return returningData

    def _findRange(self, begin, end, keyColumn):
        # print ("Find " + str(item))
        for record in self.data:
            if record[keyColumn] >= begin and record[keyColumn] <= end:
                returningData.append(record)

        for child in self.child:
            child._findRange(begin, end, keyColumn)

        return returningData

    def _findAndChange(self, newRecord, RID):
        for i in range(len(self.data)):
            if self.data[i][-1] == RID:
                self.data[i] = newRecord

        for child in self.child:
            child._findAndChange(newRecord, RID)


    def _remove(self, item):
        pass

    # print preorder traversal
    def _preorder(self):
        print(self)
        for child in self.child:
            child._preorder()


class BTree:
    def __init__(self):
        # print("Tree __init__")
        self.root = None

    def insert(self, record, keyColumn):
        # print("Tree insert: " + str(record[keyColumn-1]))
        if self.root is None:
            self.root = BNode(record)
        else:
            self.root._insert(BNode(record), keyColumn-1)
            while self.root.parent:
                self.root = self.root.parent
        return True

    def find(self, key, keyColumn):
        global returningData
        returningData = []
        return self.root._find(key, keyColumn-1)

    def findRange(self,begin,end, keyColumn):
            global returningData
            returningData = []
            return self.root._findRange(begin,end,keyColumn-1)

    def remove(self, record):
        self.root.remove(record)

    def printTop2Tiers(self):
        print('----Top 2 Tiers----')
        print(str(self.root.data))
        for child in self.root.child:
            print(str(child.data), end=' ')
        print(' ')

    def preorder(self):
        print('----Preorder----')
        self.root._preorder()

    def findAndChange(self, record, RID):
        if self.root != None:
            self.root._findAndChange(record,RID)
