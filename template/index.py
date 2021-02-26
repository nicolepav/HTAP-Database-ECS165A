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



# class Node:
#     def __init__(self, data = None, record = None):
#         self.data = data
#         self.record = record #pointer
#         self.left = None
#         self.right = None

# class BST:
#     def __init__(self):
#         self.root = None
#         self.returnData = []

#     def insert(self, data, record):
#         # check if root node is none
#         if self.root is None:
#             self.root = Node(data, record)
#         #tree has at least one node in it and find appro location to put the new node
#         #create a helper method _insert
#         else:
#             self._insert(data, record, self.root)

#     def _insert(self, data, record, cur_node):
#         #data is less than cur_node data
#         if data < cur_node.data:
#             #left node is avaliable for insert
#             if cur_node.left is None:
#                 cur_node.left = Node(data, record)
#             #else traverse down
#             else:
#                 self._insert(data, record, cur_node.left)
#         elif data >= cur_node.data:
#             if cur_node.right is None:
#                 cur_node.right = Node(data , record)
#             else:
#                 self._insert(data, record, cur_node.right)

#     # TODO change append data to record when data insertion is figured out
#     def returnRangeData(self, cur_node, begin, end):
        
#     # Base Case Start at root
        
#         if cur_node is None:
#             return
 
#     # Since the desired o/p is sorted, recurse for left
#     # subtree first. If root.data is greater than k1, then
#     # only we can get o/p keys in left subtree
#         if begin < cur_node.data :
#             self.returnRangeData(cur_node.left, begin, end)
 
#     # If root's data lies in range, then prints root's data
#         if begin <= cur_node.data and end >= cur_node.data:
#             self.returnData.append(cur_node.record[0])
 
#     # If root.data is smaller than k2, then only we can get
#     # o/p keys in right subtree
#         if end >= cur_node.data:
#             self.returnRangeData(cur_node.right, begin, end)

#     def clearReturnData(self):
#         self.returnData = []




#     def print_tree(self, traversal_type):
#         if traversal_type == "preorder":
#             return self.preorder_print(self.root, "")

#     def preorder_print(self,start,traversal):
#         if start:
#             traversal += (str(start.data) + "-")
#             traversal = self.preorder_print(start.left, traversal)
#             traversal = self.preorder_print(start.right, traversal)
#         return traversal





class BNode:
    def __init__(self, record, par = None):
        # each node will have a list of record objects within it
        self.records = list([record])
        self.parent = par
        self.child = list()
    #tester to print out node data
    def __str__(self):
        if self.parent:
            return str(self.parent.records) + ' : ' + str(self.records)
        return "Root: " + str(self.records)

    def _isLeaf(self):
        return len(self.child) == 0

    def _add(self, new_node):
        for child in new_node.child:
            child.parent = self

        self.records.extend(new_node.records)
        self.records.sort()
        self.child.extend(new_node.child)
        if len(self.child) > 1:
            self.child.sort()
            for child in self.child:
                child.parent = self

            if len(self.records) > 3:
                self._split()
        

    def _insert(self, new_node, keyColumn):
        #if the node is a leaf: add record to leaf and rebalance tree
        if self._isLeaf():
            self._add(new_node)
        # not leaf: find correct child to descend and do recursive insert
        elif new_node.record[0][keyColumn-1] > self.records[-1][keyColumn-1]:
            self.child[-1]._insert(new_node, keyColumn)
        else:
            for i in range(0, len(self.records)):
                if new_node.records[0][keyColumn-1] < self.records[i][keyColumn-1]:
                    self.child[i]._insert(new_node, keyColumn)
                    break

    def _split(self):
        left_child = BNode(self.record[0], self)
        right_child = BNode(self.record[2], self)
        #if self is not a leaf, reattach its child with the new node
        if self.child:
            self.child[0].parent = left_child
            self.child[1].parent = left_child
            self.child[2].parent = right_child
            self.child[3].parent = right_child
            left_child.child = [self.child[0], self.child[1]]
            right_child.child = [self.child[2], self.child[3]]
        self.child = [left_child]
        self.child.append(right_child)
        #now have new sub-tree, self. needs to add self to its parent node
        if self.parent:
            if self in self.parent.child:
                self.parent.child.remove(self)
        else:
            left_child.parent = self
            right_child.parent = self

    def _preorder(self):
        print(self)
        for child in self.child:
            child._preorder()
class BTree:
    def __init__(self):
        print("Tree __init__")
        self.root = None

    def insert(self, record, keyColumn):
        print("Tree insert key: " + str(record[keyColumn-1]))
        if self.root is None:
            self.root = BNode(record)
        else:
            self.root._insert(BNode(record), keyColumn)
            while self.root.parent:
                self.root = self.root.parent

        return True

    def preorder(self):
        print("----Preorder----")
        self.root._preorder()