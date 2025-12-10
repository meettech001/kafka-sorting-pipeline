#!/usr/bin/env bash
set -euo pipefail

REQUIRED_TOPICS=("source" "id" "name" "continent")

echo "Checking topic existence..."

for topic in "${REQUIRED_TOPICS[@]}"; do
    if kafka-topics --bootstrap-server kafka:9092 --list | grep -q "^${topic}$"; then
        echo "✔ Topic exists: $topic"
    else
        echo "✖ MISSING topic: $topic"
        exit 1
    fi
done

echo "All required topics exist."
