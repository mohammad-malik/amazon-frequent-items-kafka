from kafka import KafkaConsumer
from pymongo import MongoClient
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


def generate_rules_with_interest(itemsets, min_confidence, total_transactions):
    rules = []
    for itemset in itemsets.keys():
        itemset_support = itemsets[itemset] / total_transactions  # P(A ∩ B)
        if len(itemset) > 1:
            for i in range(1, len(itemset)):
                for antecedent in combinations(itemset, i):
                    consequent = tuple(
                        item for item in itemset if item not in antecedent
                    )
                    antecedent_support = (
                        itemsets.get(antecedent, 0) / total_transactions
                    )  # P(A)
                    consequent_support = (
                        itemsets.get(consequent, 0) / total_transactions
                    )  # P(B)
                    if antecedent_support > 0 and consequent_support > 0:
                        confidence = itemset_support / antecedent_support
                        interest = itemset_support - (
                            antecedent_support * consequent_support
                        )
                        if confidence >= min_confidence:
                            rules.append(
                                (antecedent, consequent, confidence, interest))
    return rules


def process_messages(consumer, db):
    window = SlidingWindow(size=100)
    min_support = 2
    min_confidence = 0.5

    for message in consumer:
        if "also_buy" not in message.value or not message.value["also_buy"]:
            continue

        transaction = message.value["also_buy"]
        window.add_transaction(transaction)

        frequent_itemsets = window.get_frequent_itemsets(min_support)
        if not frequent_itemsets:
            continue

        print(f"Frequent Itemsets: {frequent_itemsets}")

        total_transactions = len(window.window)
        rules = generate_rules_with_interest(
            frequent_itemsets,
            min_confidence,
            total_transactions
        )
        if not rules:
            print("No rules generated.")
            continue

        print("Generated rules:")
        for rule in rules:
            antecedent_str = str(rule[0])
            consequent_str = str(rule[1])
            print(f"Rule: {antecedent_str} -> {consequent_str}")
            print(f"Confidence: {rule[2]}")
            print(f"Interest: {rule[3]}")
            print()

        db.rules.insert_many(
            [
                {
                    "antecedent": rule[0],
                    "consequent": rule[1],
                    "confidence": rule[2],
                    "interest": rule[3],
                }
                for rule in rules
            ]
        )


if __name__ == "__main__":
    client = MongoClient("mongodb://10.0.0.4:27017/")
    db = client["apriori_db"]
    consumer = KafkaConsumer(
        "main_topic",
        bootstrap_servers=[
            "10.0.0.4:9092",
            "10.0.0.5:9092",
            "192.168.0.4:9092",
            "192.168.0.5:9092"
        ],
        auto_offset_reset="earliest",
        value_deserializer=json_deserializer,
    )
    process_messages(consumer, db)
