#!/usr/bin/env bash
set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka:9092}"
SOURCE_TOPIC="${SOURCE_TOPIC:-source}"
ID_TOPIC="${ID_TOPIC:-id}"
NAME_TOPIC="${NAME_TOPIC:-name}"
CONTINENT_TOPIC="${CONTINENT_TOPIC:-continent}"
RECORDS="${RECORDS:-50000000}"
CHUNK_SIZE="${CHUNK_SIZE:-2000000}"
WORKDIR="${WORKDIR:-/tmp/pipeline-data}"

echo "Starting pipeline..."
echo "Broker          : ${BROKER}"
echo "Source topic    : ${SOURCE_TOPIC}"
echo "ID topic        : ${ID_TOPIC}"
echo "Name topic      : ${NAME_TOPIC}"
echo "Continent topic : ${CONTINENT_TOPIC}"
echo "Records         : ${RECORDS}"
echo "Chunk size      : ${CHUNK_SIZE}"
echo "Workdir         : ${WORKDIR}"

# Add a small delay to ensure Kafka is fully ready
echo "Waiting for Kafka to be fully ready..."
sleep 5

exec /app/pipeline \
  -broker="${BROKER}" \
  -source-topic="${SOURCE_TOPIC}" \
  -id-topic="${ID_TOPIC}" \
  -name-topic="${NAME_TOPIC}" \
  -continent-topic="${CONTINENT_TOPIC}" \
  -records="${RECORDS}" \
  -chunk-size="${CHUNK_SIZE}" \
  -workdir="${WORKDIR}"