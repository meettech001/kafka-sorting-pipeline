#!/bin/bash
# Script to initialize Kafka topics

echo "Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server kafka:9092 --list &>/dev/null; do
  echo "Kafka is not ready yet, sleeping for 2 seconds..."
  sleep 20
done

echo "Kafka is ready, creating topics..."

# Create topics with 4 partitions each
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --partitions 4 --replication-factor 1 --topic source
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --partitions 4 --replication-factor 1 --topic id
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --partitions 4 --replication-factor 1 --topic name
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --partitions 4 --replication-factor 1 --topic continent

echo "Topics created successfully!"
echo "Listing topics:"
kafka-topics --bootstrap-server kafka:9092 --list

exit 0