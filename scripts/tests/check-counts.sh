#!/usr/bin/env bash
set -euo pipefail

TOPICS=("source" "id" "name" "continent")

echo "Checking message counts..."

for topic in "${TOPICS[@]}"; do
    COUNT=$(kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list kafka:9092 \
        --topic ${topic} | awk -F ':' '{print $3}')

    echo "$topic => $COUNT messages"

    if [[ -z "$COUNT" ]]; then
        echo "✖ Failed to fetch message count for topic: $topic"
        exit 1
    fi
done

echo "✔ Count check completed (ensure counts match)."
