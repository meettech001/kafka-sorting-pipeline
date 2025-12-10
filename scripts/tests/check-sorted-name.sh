#!/usr/bin/env bash
set -euo pipefail

echo "Checking NAME sorted ordering..."

kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic name \
  --from-beginning \
  --max-messages 200000 \
  > /tmp/name_check.csv

awk -F',' '
  NR==1 {prev=$2}
  NR>1 {
     if ($2 < prev) {
       print "✖ ERROR: NAME out of order : " $2 " < " prev
       exit 1
     }
     prev=$2
  }
  END { print "✔ NAME ordering OK" }
' /tmp/name_check.csv
