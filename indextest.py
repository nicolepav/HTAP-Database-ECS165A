from template.index import *


tree = BTree()

tree.insert([0,0,0,0,0], 3)

tree.insert([33,1,1,1,1], 3)
tree.insert([22,2,2,2,2], 3)
tree.insert([332,3,3,3,3], 3)
tree.insert([3321,4,3,3,3], 3)

tree.preorder()