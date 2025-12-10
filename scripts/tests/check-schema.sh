#!/usr/bin/env bash
set -euo pipefail

echo "Checking source schema..."

kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic source \
  --from-beginning \
  --max-messages 100000 \
  > /tmp/schema_check.csv

awk -F',' '
  NF != 4                        { print "✖ Invalid field count"; exit 1 }
  length($2) < 10 || length($2) > 15 { print "✖ Invalid name length"; exit 1 }
  length($3) < 15 || length($3) > 20 { print "✖ Invalid address length"; exit 1 }
  !($4=="North America" || $4=="Asia" || $4=="South America" || \
    $4=="Europe" || $4=="Africa" || $4=="Australia") {
       print "✖ Invalid continent: " $4; exit 1
  }
  END { print "✔ Schema OK" }
' /tmp/schema_check.csv
