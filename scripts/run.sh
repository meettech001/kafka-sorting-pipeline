#!/usr/bin/env bash
set -euo pipefail

echo "This script assumes you have docker-compose and a local Kafka instance."
echo "Example docker-compose.yaml snippet is documented in README.md."

echo "To run the pipeline container against an existing Kafka broker, use:"
echo
echo "  docker run --rm \\"
echo "    --network=<your-docker-network> \\"
echo "    -e KAFKA_BROKER=<kafka-host>:9092 \\"
echo "    -e RECORDS=50000000 \\"
echo "    -e CHUNK_SIZE=2000000 \\"
echo "    kafka-sorting-pipeline:latest"
