# from blist import blist
from sys import maxsize
from template.config import *

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table


    """
    # returns the location of all records with the given value on column "column"
    """
    #TODO Finish this
    def locate(self, column, value):
        self.indices[column-1].clearReturningData()
        self.indices[column-1].returnRangeData(self.indices[column-1].root, value, value)
        returningRecords = self.indices[column-1].returnData
        #return the location
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        self.indices[column-1].clearReturnData()
        self.indices[column-1].returnRangeData(self.indices[column-1].root, begin, end)
        returningRecords = self.indices[column-1].returnData
        return returningRecords

    """
    # optional: Create index on specific column
    """
    #record data = array of pageranges
    #column_number assumes the user passes the number of the table column from their view
    def create_index(self, column_number):
        pass
                        


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number-1] = None
        pass



class BNode:
    def __init__(self, data, par=None):
        #print ("Node __init__: " + str(data))
        self.data = list([data])
        self.parent = par
        self.child = list()

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
                if new_node.data[0][keyColumn] < self.data[i][keyColumn]:
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

    # find an item in the tree; return item, or False if not found
    def _find(self, key, keyColumn):
        # print ("Find " + str(item))
        for record in self.data:
            if record[keyColumn] == key:
                return record

        if self._isLeaf():
        	return False
        elif key > self.data[-1][keyColumn]:
        	return self.child[-1]._find(key, keyColumn)
        else:
        	for i in range(len(self.data)):
        		if key < self.data[i][keyColumn]:
        			return self.child[i]._find(key, keyColumn)

    def _remove(self, item):
        pass

    # print preorder traversal
    def _preorder(self):
        print(self)
        for child in self.child:
            child._preorder()


class BTree:
    def __init__(self):
        print("Tree __init__")
        self.root = None

    def insert(self, record, keyColumn):
        print("Tree insert: " + str(record[keyColumn-1]))
        if self.root is None:
            self.root = BNode(record)
        else:
            self.root._insert(BNode(record), keyColumn-1)
            while self.root.parent:
                self.root = self.root.parent
        return True

    # TODO: change to find range of keys
    def find(self, key, keyColumn):
        return self.root._find(key, keyColumn-1)

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


