import json
from kafka import KafkaConsumer
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import iqr
from pymongo import MongoClient


class AnomalyDetectionConsumer:
    def __init__(
        self,
        topic,
        bootstrap_servers,
        window_size,
        moving_average_window,
        mongo_uri,
        db_name,
        collection_name,
    ):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        self.window_size = window_size
        self.moving_average_window = moving_average_window
        self.transactions = []
        self.amounts = []
        self.times = []
        self.anomalies = []
        self.moving_averages = []

        # MongoDB setup
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

        # Plot setup
        plt.ion()
        self.fig, self.ax = plt.subplots()
        self.ax.set_title(
            "Real-time Transaction Monitoring with Moving Average")
        self.ax.set_xlabel(
            "Transaction Count")
        self.ax.set_ylabel(
            "Transaction Amount")

    def insert_anomaly_to_db(self, transaction):
        if transaction["is_anomaly"]:
            self.collection.insert_one(transaction)

    def update_plot(self):
        self.ax.clear()
        self.ax.plot(
            self.times,
            self.amounts,
            label="Transaction Amount",
            marker="o"
        )
        self.ax.plot(
            self.times,
            self.moving_averages,
            label="Moving Average",
            linestyle="--",
            color="orange",
        )

        anomaly_indices = [
            idx for idx, val in enumerate(self.anomalies) if val]
        anomaly_values = [
            self.amounts[i] for i in anomaly_indices]
        self.ax.scatter(
            anomaly_indices,
            anomaly_values,
            color="red",
            label="Anomalies",
            zorder=5
        )
        self.ax.legend()
        plt.draw()
        plt.pause(0.01)

    def detect_anomalies(self, new_transaction):
        self.transactions.append(new_transaction)
        if len(self.transactions) > self.window_size:
            self.transactions.pop(0)

        amounts = [t["amount"] for t in self.transactions]
        median_amount = np.median(amounts)
        iqr_amount = iqr(amounts)
        lower_bound = median_amount - 1.5 * iqr_amount
        upper_bound = median_amount + 1.5 * iqr_amount

        is_anomaly = False
        if new_transaction["amount"] < 1.0:
            is_anomaly = True
            print(
                f"Low value anomaly detected at ${new_transaction['amount']}")
        elif (
            new_transaction["amount"] > upper_bound
            or new_transaction["amount"] < lower_bound
        ):
            is_anomaly = True
            print(
                f"Anomaly detected at ${new_transaction['amount']}" +
                f" outside bounds [{lower_bound}, {upper_bound}]"
            )

        self.anomalies.append(is_anomaly)
        new_transaction["is_anomaly"] = is_anomaly
        self.insert_anomaly_to_db(new_transaction)
        return is_anomaly

    def calculate_moving_average(self):
        if len(self.amounts) >= self.moving_average_window:
            window_amounts = self.amounts[-self.moving_average_window:]
            average = np.mean(window_amounts)
            self.moving_averages.append(average)
        else:
            self.moving_averages.append(np.mean(self.amounts))

    def run(self):
        print("Starting the consumer for anomaly detection...")
        idx = 0
        while True:
            # 10 seconds timeout.
            message = self.consumer.poll(timeout_ms=10000)
            if not message:
                print("No message received for 10 seconds. Exiting...")
                break
            for _, records in message.items():
                for record in records:
                    transaction_data = record.value
                    self.amounts.append(transaction_data["amount"])
                    self.times.append(idx)
                    self.calculate_moving_average()
                    self.detect_anomalies(transaction_data)
                    self.update_plot()
                    idx += 1


if __name__ == "__main__":
    consumer = AnomalyDetectionConsumer(
        topic="topic_for_consumer3",
        bootstrap_servers=[
            "10.0.0.4:9092",
            "10.0.0.5:9092",
            "192.168.0.4:9092",
            "192.168.0.5:9092"
        ],
        window_size=50,
        moving_average_window=10,
        mongo_uri="mongodb://10.0.0.4:27017",
        db_name="anomaly_detection_db",
        collection_name="anomaly_detection_collection",
    )
    consumer.run()
