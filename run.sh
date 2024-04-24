#!/bin/bash

# Ensure the script exits cleanly handling background processes
trap "exit" INT TERM ERR
trap "kill 0" EXIT

# Define the path to Kafka configuration
KAFKA_CONF=$KAFKA_HOME/config

# Start Zookeeper in a new terminal window
echo "Starting Zookeeper..."
gnome-terminal -- bash -c "zookeeper-server-start.sh $KAFKA_CONF/zookeeper.properties; exec bash"

# Start Kafka server in a new terminal window
echo "Starting Kafka..."
gnome-terminal -- bash -c "kafka-server-start.sh $KAFKA_CONF/server.properties; exec bash"

# Allow the user to choose which consumers to run
echo "Select the consumer(s) to run:"
echo "1. Apriori Algorithm"
echo "2. PCY Algorithm"
echo "3. Anomaly Detection"
read -p "Enter your choice (e.g., 1, 2, 3, 1 2, etc.): " choice


# Function to start a consumer
start_consumer() {
    if [[ $1 == "1" ]]; then
        echo "Starting the producer..."
        python3 producer_for_1_2.py &
        PROD_PID=$!
        echo "Producer started with PID $PROD_PID"

        echo "Starting Apriori Consumer..."
        python3 A-priori.py &
    elif [[ $1 == "2" ]]; then
        echo "Starting the producer..."
        python3 producer_for_1_2.py &
        PROD_PID=$!
        echo "Producer started with PID $PROD_PID"

        echo "Starting PCY Consumer..."
        python3 PCY.py &
    elif [[ $1 == "3" ]]; then
        echo "Starting the producer..."
        python3 producer_for_3.py &
        PROD_PID=$!
        echo "Producer started with PID $PROD_PID"
        echo "Starting Anomaly Detection Consumer..."
        python3 consumer3.py &
    else
        echo "Invalid choice: $1"
    fi
}

# Iterate over the user's choices and start the selected consumers
for c in $choice; do
    start_consumer $c
done

# Keep the script running until all background processes are finished
wait

