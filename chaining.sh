#!/bin/bash

# Ensure the script exits cleanly handling background processes
trap "exit" INT TERM ERR
trap "kill 0" EXIT

echo "Starting the first producer: blob_to_kafka_producer.py..."
python3 blob_to_kafka_producer.py
echo "First producer finished."

echo "Starting the second producer: producer_for_3.py..."
python3 producer_for_3.py
echo "Second producer finished."

# Keep the script running until all background processes are finished
wait
