import json
from kafka import KafkaProducer
import time

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def read_transactions_from_json(file_path):
    with open(file_path, "r") as file:
        products = json.load(file)
    transactions = []
    for product in products:
        if "also_buy" in product and product["also_buy"]:
            transactions.append(product)
    return transactions

def produce_transactions(producer, topic, transactions):
    for transaction in transactions:
        producer.send(topic, value=transaction)
        producer.flush()
        time.sleep(2)

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"], value_serializer=json_serializer
    )
    transactions = read_transactions_from_json("preprocessed_for_itemsets.json")
    
    # Send transactions to PCY topic
    produce_transactions(producer, "topic11", transactions)
    
    # Send transactions to Apriori topic
    produce_transactions(producer, "topic12", transactions)
