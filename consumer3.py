import json
from kafka import KafkaConsumer
import numpy as np
from pymongo import MongoClient
from scipy.stats import iqr
import matplotlib.pyplot as plt


class AnomalyDetectionConsumer:
    def __init__(
        self,
        topic,
        bootstrap_servers,
        db_uri,
        db_name,
        collection_name,
        output_file
    ):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.transactions = []
        self.window_size = 50
        self.low_value_threshold = 1.0  # For low value anomaly detection.
        self.output_file = output_file
        self.amounts = []
        self.times = []
        self.anomalies = []

        # Plot setup.
        plt.ion()
        self.fig, self.ax = plt.subplots()
        self.ax.set_title("Real-time Transaction Monitoring")
        self.ax.set_xlabel("Transaction Count")
        self.ax.set_ylabel("Transaction Amount")

    def update_plot(self):
        self.ax.clear()
        self.ax.plot(self.times, self.amounts, label="Transaction Amount")

        # Highlighting anomalies.
        anomaly_indices = [
            idx - 1 for idx in self.anomalies if idx - 1 < len(self.amounts)
        ]
        anomaly_values = [self.amounts[i] for i in anomaly_indices]
        self.ax.scatter(
            anomaly_indices,
            anomaly_values,
            color="red",
            label="Anomalies"
        )
        self.ax.legend()
        plt.draw()
        plt.pause(0.01)

    def detect_anomalies(self, new_transaction, idx):
        self.transactions.append(new_transaction)
        if len(self.transactions) > self.window_size:
            self.transactions.pop(0)

        if len(self.transactions) >= 30:
            amounts = [t["amount"] for t in self.transactions]
            median_amount = np.median(amounts)
            iqr_amount = iqr(amounts)
            lower_bound = median_amount - 2 * iqr_amount
            upper_bound = median_amount + 2 * iqr_amount

            if (
                (new_transaction["amount"] > upper_bound)
                or (new_transaction["amount"] < lower_bound)
                or (new_transaction["amount"] < self.low_value_threshold)
            ):
                anomaly_msg = \
                    "Anomaly detected: Transaction ID" + \
                    f" {new_transaction['transaction_id']}" + \
                    f" with amount {new_transaction['amount']}"

                print(anomaly_msg)
                self.anomalies.append(len(self.amounts))

    def run(self):
        idx = 0
        print("Starting the consumer for anomaly detection...")
        for message in self.consumer:
            transaction_data = message.value
            self.amounts.append(transaction_data["amount"])
            self.times.append(idx)
            self.detect_anomalies(transaction_data, idx)
            self.update_plot()
            idx += 1


if __name__ == "__main__":
    consumer = AnomalyDetectionConsumer(
        topic="transaction_topic",
        bootstrap_servers=["localhost:9092"],
        db_uri="mongodb://localhost:27017/",
        db_name="anomaly_detection",
        collection_name="transactions",
        output_file="anomaly_detection_output.txt",
    )
    consumer.run()
