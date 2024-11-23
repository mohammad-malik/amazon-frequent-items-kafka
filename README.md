# Amazon Metadata Streaming Data Pipeline and Itemset Mining

#### This repository houses an implementation of finding similar items utilising A-Priori and PCY Algorithms on Apache Kafka. 
Using a 12GB .json file as a sample of the 100+GB Amazon_Reviews Dataset, it was developed as part of an assignment for the course Fundamentals of Big Data Analytics (DS2004). 

### The project leverages:
1. Apache Kafka for robust real-time data streaming.
2. (Optional) Use of Azure VMs and Blobs, providing a scalable solution for large datasets.

## Repository Structure:
```
└── preprocessing.py            # Script for preprocessing data locally
└── sampling.py                 # Script used to randomly sample the original 100+GB into 15 GB.
├── preprocessing_for_azure.py  # Script for preprocessing and loading data to Azure Blob Storage
├── blob_to_kafka_producer.py  # Script for streaming data from Azure Blob to Kafka
├── consumer1.py                 # Kafka consumer implementing the Apriori algorithm
├── consumer2.py                 # Kafka consumer implementing the PCY algorithm
└── consumer3.py                # Kafka consumer for anomaly detection 
└── producer_for_1_2.py          # Kafka producer for Apriori and PCY consumers
└── producer_for_3.py            # Kafka producer for Anomaly detection consumer
```

## Setup Instructions
### 1. Data Preparation
 The first step is to download and preprocess the Amazon Metadata dataset.
    <li>Download the dataset from the provided Amazon link. Use EITHER of:
        <br> &emsp;&emsp;&emsp;&emsp;└── Preprocessing_for_azure.py if using Azure, <br> &emsp;&emsp;&emsp;&emsp;└── Preprocessing.py if not.</li><br>
    <li>Upload the preprocessed data to Azure Blob Storage (set blob and connection string in the script) (If not using Azure, skip this step).</li>
    <li>The original dataset's size necessitated sampling for efficient analysis. We ensured a good mix of metadata for our analysis.</li>
### 2. Streaming Pipeline</li>
  Next up is setting up Kafka (and optionally Azure Blob Storage):</li>
   <li>Deploy Apache Kafka. Ensure Kafka brokers are accessible.</li>
   <li>Modify azure_blob_data.py with your Azure Blob Storage connection details and Kafka bootstrap servers.</li>
   <li>Run blob_to_kafka_producer.py to stream data from Azure Blob Storage to Kafka.</li>

### 3. Consumer Applications
   Then deploy the consumer scripts:
    <li>consumer1.py: Consumes data for frequent itemset mining using Apriori. Adjust Kafka topic and MongoDB details.
    <li>consumer2.py: Similar setup as Apriori, but implements the PCY algorithm.
    <li>consumer3.py: Implements anomaly detection. Configure for the relevant Kafka topic.


## Technologies and Challenges:
### Used Technologies:
  <li>Azure Blob Storage: For storing and managing large-scale dataset preprocessing.
  <li>Apache Kafka: Utilized for robust real-time data streaming.
  <li>Python: Scripting language for data processing and mining algorithms.
  <li>MongoDB (optional): Recommended for storing consumer application outputs for persistent analysis

### Streaming Challenges and Solutions:
  Sliding Window Approach
  Approximation Techniques
    
## Why This Implementation with Kafka and Sliding Window Approach?

This project leverages Apache Kafka and a sliding window approach for real-time data processing due to several key advantages:
### Scalability of Kafka: 
Kafka's distributed architecture allows for horizontal scaling by adding more nodes to the cluster. This ensures the system can handle ever-increasing data volumes in e-commerce scenarios without performance degradation.

### Real-time Processing with Sliding Window: 
Traditional batch processing wouldn't be suitable for real-time analytics. The sliding window approach, implemented within Kafka consumers, enables processing data chunks (windows) as they arrive in the stream. This provides near real-time insights without waiting for the entire dataset.

### Low Latency with Kafka: 
Kafka's high throughput and low latency are crucial for e-commerce applications. With minimal delays in data processing, businesses can gain quicker insights into customer behavior and product trends, allowing for faster decision-making.

<br>
While Azure Blob Storage provides excellent cloud storage for the preprocessed data, and Azure VMs allow for easier clustering, it's Kafka that facilitates the real-time processing aspects crucial for this assignment's goals. The combination of Kafka's streaming capabilities and the sliding window approach within consumers unlocks the power of real-time analytics for e-commerce data.

## Team:
- **Manal Aamir**: [GitHub](https://github.com/manal-aamir)
- **Mohammad Malik**: [GitHub](https://github.com/mohammad-malik)
- **Aqsa Fayaz**: [GitHub](https://github.com/Aqsa-Fayaz)
