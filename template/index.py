# from blist import blist

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One tree for each column. All empty initially.
        self.indices = []
        for x in range(table.num_columns):
            self.indices.append(BST())
        print(self.indices)
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass



class Node:
    def __init__(self, data = None, record = None):
        self.data = data
        self.record = record
        self.left = None
        self.right = None

class BST:
    def __init__(self):
        self.root = None
        self.returnData = []

    def insert(self, data, record):
        # check if root node is none
        if self.root is None:
            self.root = Node(data, record)
        #tree has at least one node in it and find appro location to put the new node
        #create a helper method _insert
        else:
            self._insert(data, record, self.root)

    def _insert(self, data, record, cur_node):
        #data is less than cur_node data
        if data < cur_node.data:
            #left node is avaliable for insert
            if cur_node.left is None:
                cur_node.left = Node(data, record)
            #else traverse down
            else:
                self._insert(data, record, cur_node.left)
        elif data >= cur_node.data:
            if cur_node.right is None:
                cur_node.right = Node(data , record)
            else:
                self._insert(data, record, cur_node.right)

    def findLocation(self, value):
        
        pass

    def returnRangeData(self, cur_node, begin, end):
    # Base Case Start at root
        
        if cur_node is None:
            return
 
    # Since the desired o/p is sorted, recurse for left
    # subtree first. If root.data is greater than k1, then
    # only we can get o/p keys in left subtree
        if begin < cur_node.data :
            self.returnRangeData(cur_node.left, begin, end)
 
    # If root's data lies in range, then prints root's data
        if begin <= cur_node.data and end >= cur_node.data:
            self.returnData.append(cur_node.data)
 
    # If root.data is smaller than k2, then only we can get
    # o/p keys in right subtree
        if end > cur_node.data:
            self.returnRangeData(cur_node.right, begin, end)

    def clearReturnData(self):
        self.returnData = []




    def print_tree(self, traversal_type):
        if traversal_type == "preorder":
            return self.preorder_print(self.root, "")

    def preorder_print(self,start,traversal):
        if start:
            traversal += (str(start.data) + "-")
            traversal = self.preorder_print(start.left, traversal)
            traversal = self.preorder_print(start.right, traversal)
        return traversal