from template.db import Database
from template.query import Query
from template.transaction import Transaction
from template.transaction_worker import TransactionWorker
from template.config import init

from random import choice, randint, sample, seed

init()
db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)
num_threads = 8

try:
    grades_table.index.create_index(1)
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

keys = []
records = {}
seed(3562901)

insert_transactions = []
transaction_workers = []
for i in range(num_threads):
    insert_transactions.append(Transaction())
    transaction_workers.append(TransactionWorker())
    transaction_workers[i].add_transaction(insert_transactions[i])

for i in range(0, 1000):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    q = Query(grades_table)
    t = insert_transactions[i % num_threads]
    t.add_query(q.insert, *records[key])

# Commit to disk
for i in range(num_threads):
    transaction_workers[i].run()

db.close()