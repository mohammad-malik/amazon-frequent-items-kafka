from kafka import KafkaProducer
import json
import time


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def get_data_from_file(file_path):
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return []
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []


def parse_price(price_str):
    if price_str is None:
        return 0.0
    price_str = str(price_str).replace("$", "").replace(" ", "")
    if "-" in price_str:
        prices = list(map(float, price_str.split("-")))
        return sum(prices) / len(prices)
    try:
        return float(price_str)
    except ValueError:
        return 0.0


def on_send_success(record_metadata):
    print(
        f"Message sent to {record_metadata.topic} partition "
        + f"{record_metadata.partition} offset {record_metadata.offset}"
    )


def on_send_error(excp):
    print("Error:", excp)


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"], value_serializer=json_serializer
)

data = get_data_from_file("Processed_Amazon_Meta.json")

for item in data:
    asin = item.get("asin", "")
    price_str = item.get("price", "0.0")
    price = parse_price(price_str)
    category = item.get("category", [])

    message = {"transaction_id": asin, "amount": price, "category": category}
    future = producer.send("consumer3", value=message)
    future.add_callback(on_send_success)
    future.add_errback(on_send_error)
    print(f"Sending: {message}")
    time.sleep(1)

producer.flush()
print("All messages sent successfully!")
