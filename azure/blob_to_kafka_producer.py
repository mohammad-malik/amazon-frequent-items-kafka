from azure.storage.blob import BlobServiceClient
from kafka import KafkaProducer
import json

# Reading file containing connection string
with open("azure_blob_data.txt", "r") as file:
    connection_string = file.readline().replace("\n", "")
    container_name = file.readline().replace("\n", "")
    blob_name = file.readline().replace("\n", "")

# Configuration for Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(
    connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Configuration for Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[
        "10.0.0.4:9092",
        "10.0.0.5:9092",
        "192.168.0.4:9092",
        "192.168.0.5:9092",
    ],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

# Fetch a specific blob and stream it to Kafka
blob_name = "processed-" + blob_name
blob_client = container_client.get_blob_client(blob_name)

# Download the blob and parse it as JSON
stream = blob_client.download_blob().chunks()

buffer = ""
for chunk in stream:
    buffer += chunk.decode("utf-8")
    while "\n" in buffer:
        record, buffer = buffer.split("\n", 1)
        producer.send("sample_data", json.loads(record))

producer.flush()
