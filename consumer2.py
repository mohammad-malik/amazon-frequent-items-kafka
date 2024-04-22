from kafka import KafkaConsumer
from collections import defaultdict, deque
from itertools import combinations
import json

class SlidingWindow:
    def __init__(self, size=100):
        self.window = deque()
        self.size = size
        self.itemsets = defaultdict(int)

    def add_transaction(self, transaction):
        if len(self.window) >= self.size:
            self.remove_transaction()
        self.window.append(transaction)
        self.update_counts(transaction, 1)

    def remove_transaction(self):
        old_transaction = self.window.popleft()
        self.update_counts(old_transaction, -1)

    def update_counts(self, transaction, increment):
        max_length = 4
        for r in range(1, min(max_length + 1, len(transaction) + 1)):
            for itemset in combinations(transaction, r):
                self.itemsets[itemset] += increment
                if self.itemsets[itemset] <= 0:
                    del self.itemsets[itemset]

    def get_frequent_itemsets(self, min_support):
        return {
            itemset: count
            for itemset, count in self.itemsets.items()
            if count >= min_support
        }

def json_deserializer(data):
    return json.loads(data.decode("utf-8"))

def process_messages(consumer):
    window = SlidingWindow(size=100)
    for message in consumer:
        if "also_buy" in message.value: 
            transaction = message.value["also_buy"]
            window.add_transaction(transaction)
            min_support = 2  
            frequent_itemsets = window.get_frequent_itemsets(min_support)
            if frequent_itemsets:
                print(f"Frequent Itemsets: {frequent_itemsets}")

if __name__ == "__main__":
    consumer = KafkaConsumer(
        "transaction_topic",
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=json_deserializer
    )
    process_messages(consumer)
