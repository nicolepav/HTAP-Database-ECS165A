from template.index import *



tree = BST()

tree.insert(3, [1,3,4])
tree.insert(6, [1,3,4])
tree.insert(1, [1,3,4])
tree.insert(2, [1,3,4])
tree.insert(78, [1,3,4])
tree.insert(3, [1,3,4])
tree.insert(9, [1,3,4])
tree.insert(2, [1,3,4])
tree.insert(6, [1,3,4])
tree.insert(3, [1,3,4])

print(tree.returnRangeData(tree.root, 3, 10))
print(tree.returnData)

tree.clearReturnData()
print(tree.returnData)