#!/usr/bin/env bash
set -euo pipefail

echo "Checking ID sorted ordering..."

kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic id \
  --from-beginning \
  --max-messages 200000 \
  > /tmp/id_check.csv

awk -F',' '
  NR==1 {prev=$1}
  NR>1 {
     if ($1 < prev) {
       print "✖ ERROR: ID out of order : " $1 " < " prev
       exit 1
     }
     prev=$1
  }
  END { print "✔ ID ordering OK" }
' /tmp/id_check.csv
