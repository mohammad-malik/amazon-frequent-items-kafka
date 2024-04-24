from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations
import time
from pymongo import MongoClient


class EnhancedPCY:
    def __init__(self, bucket_size, support_threshold):
        self.bucket_size = bucket_size
        self.support_threshold = support_threshold
        self.data_window = []
        self.hash_buckets = [0] * bucket_size
        self.item_counts = defaultdict(int)

    def hash_combination(self, combination):
        return hash(combination) % self.bucket_size

    def process_transaction(self, transaction):
        if len(self.data_window) >= self.bucket_size:
            self.data_window.pop(0)

        self.data_window.append(transaction)
        for item in transaction:
            self.item_counts[item] += 1
        for pair in combinations(sorted(transaction), 2):
            self.hash_buckets[self.hash_combination(pair)] += 1

    def get_frequent_itemsets(self):
        frequent_itemsets = {}
        for item, count in self.item_counts.items():
            if count >= self.support_threshold:
                frequent_itemsets[(item,)] = count

        for transaction in self.data_window:
            for r in range(2, len(transaction) + 1):
                for itemset in combinations(sorted(transaction), r):
                    count = min(self.item_counts[item] for item in itemset)
                    if count >= self.support_threshold:
                        frequent_itemsets[itemset] = count

        return frequent_itemsets


def generate_association_rules(itemsets, min_confidence, total_transactions):
    rules = []
    for itemset in itemsets.keys():
        itemset_support = itemsets[itemset] / total_transactions  # P(A ∩ B).
        if len(itemset) > 1:
            for i in range(1, len(itemset)):
                for antecedent in combinations(itemset, i):
                    consequent = tuple(
                        item for item in itemset if item not in antecedent
                    )
                    antecedent_support = (
                        itemsets.get(antecedent, 0) / total_transactions
                    )  # P(A).
                    if antecedent_support > 0:
                        confidence = itemset_support / antecedent_support
                        if confidence >= min_confidence:
                            # Support for union of antecedent and consequent.
                            union_support = (
                                itemsets.get(itemset, 0) / total_transactions
                            )
                            # Calculating interest
                            interest = (
                                union_support
                                - antecedent_support
                                * itemsets.get(consequent, 0)
                                / total_transactions
                            )
                            rules.append(
                                (antecedent, consequent, confidence, interest))
    return rules


def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


def process_messages(consumer, db):
    window = EnhancedPCY(bucket_size=100, support_threshold=2)
    transactions = []
    print("Starting message processing...")

    # Track the start time
    start_time = time.time()

    while True:
        # Poll for messages
        messages = consumer.poll(timeout_ms=1000)

        if messages:
            start_time = time.time()

            for _, message in messages.items():
                for msg in message:
                    print("Received message:", msg.value)
                    if "also_buy" in msg.value:
                        transaction = msg.value["also_buy"]
                        transactions.append(transaction)
                        print("Added transaction to list:", transaction)
        else:
            #
            if time.time() - start_time > 10:
                print("No messages received within timeout period. Exiting...")
                break

    print("All messages received. Processing transactions...")
    print(
        "Number of transactions:", len(transactions)
    )  # Print the number of transactions
    for transaction in transactions:
        print(
            "Processing transaction:", transaction
        )  # Print the transaction being processed
        window.process_transaction(transaction)

    frequent_itemsets = window.get_frequent_itemsets()
    if frequent_itemsets:
        print(f"Frequent Itemsets: {frequent_itemsets}")
        total_transactions = len(transactions)
        min_confidence = 0.5
        association_rules = generate_association_rules(
            frequent_itemsets, min_confidence, total_transactions
        )
        if association_rules:
            print("Generated association rules:")
            for rule in association_rules:
                antecedent_str = str(rule[0])
                consequent_str = str(rule[1])
                print(
                    f"Rule: {antecedent_str} -> {consequent_str}" +
                    f"Confidence: {rule[2]}, Interest: {rule[3]}"
                )
                db.rules.insert_many(
                    [
                        {
                            "antecedent": antecedent_str,
                            "consequent": consequent_str,
                            "confidence": rule[2],
                            "interest": rule[3],
                        }
                    ]
                )
        else:
            print("No association rules generated.")
    else:
        print("No frequent itemsets found in the current window.")


if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017/")
    db = client["association_rules_db"]
    consumer = KafkaConsumer(
        "topic9",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        value_deserializer=json_deserializer,
    )
    process_messages(consumer, db)
