from template.table import Table, Record
from template.index import Index
from template.config import *
import threading
class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.thread = 0
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs a transaction
    """
    def run(self):
        x = threading.Thread(target=self.run_thread, args=())
        self.thread = x
        threads.append(x)
        x.start()
        #self.run_thread()

    def run_thread(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

    def join(self):
        self.thread.join()
