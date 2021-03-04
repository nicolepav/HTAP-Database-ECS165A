from template.index import *


tree = BTree()

tree.insert([1,16,0,0,0],2)
tree.insert([1,17,0,0,0],2)
tree.insert([1,15,0,0,0],2)
tree.insert([1,12,0,0,0],2)
tree.insert([3,16,0,0,0],2)
tree.insert([1,16,0,0,0],2)
tree.insert([1,17,0,0,0],2)
tree.insert([1,15,0,0,0],2)
tree.insert([1,12,0,0,0],2)
tree.insert([3,16,0,0,0],2)
tree.insert([1,16,0,0,0],2)
tree.insert([1,17,0,0,0],2)
tree.insert([1,15,0,0,0],2)
tree.insert([1,12,0,0,0],2)
tree.insert([3,16,0,0,0],2)


print(tree.find(16, 2))

print(tree.findRange(16, 18, 2))

