from azure.storage.blob import BlobServiceClient
from kafka import KafkaProducer
import json

# Reading file containing connection string
with open('azure_blob_data.txt', 'r') as file:
    connection_string = file.read().replace('\n', '')
    container_name = file.read().replace('\n', '')
    blob_name = file.read().replace('\n', '')

# Configuration for Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Configuration for Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[
        '10.0.0.4:9092',
        '10.0.0.5:9092',
        '192.168.0.4:9092',
        '192.168.0.5:9092'
        ],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Fetch a specific blob and stream it to Kafka
blob_name = 'processed-' + blob_name
blob_client = container_client.get_blob_client(blob_name)

# Download the blob and parse it as JSON
stream = blob_client.download_blob().chunks()

for data in stream:
    record = json.loads(data)
    producer.send('sample_data', record)

producer.flush()
