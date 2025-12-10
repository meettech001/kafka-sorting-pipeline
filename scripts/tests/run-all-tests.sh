#!/usr/bin/env bash
set -euo pipefail

echo "Running all automated tests..."

tests/check-topics.sh
tests/check-counts.sh
tests/check-schema.sh
tests/check-sorted-id.sh
tests/check-sorted-name.sh
tests/check-sorted-continent.sh

echo "======================================="
echo "✔ ALL TESTS PASSED SUCCESSFULLY"
echo "======================================="
