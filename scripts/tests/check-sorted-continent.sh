#!/usr/bin/env bash
set -euo pipefail

echo "Checking CONTINENT sorted ordering..."

kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic continent \
  --from-beginning \
  --max-messages 200000 \
  > /tmp/continent_check.csv

awk -F',' '
  NR==1 {prev=$4}
  NR>1 {
     if ($4 < prev) {
       print "✖ ERROR: CONTINENT out of order : " $4 " < " prev
       exit 1
     }
     prev=$4
  }
  END { print "✔ Continent ordering OK" }
' /tmp/continent_check.csv
