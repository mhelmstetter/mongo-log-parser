#!/bin/bash

# Script to analyze unknown operations in MongoDB logs

LOG_FILE="$1"

if [ -z "$LOG_FILE" ]; then
    echo "Usage: $0 <log_file>"
    exit 1
fi

echo "=== ANALYZING UNKNOWN OPERATIONS ==="
echo

echo "=== SAMPLE COMMAND OPERATIONS (first 20) ==="
grep '"c":"COMMAND"' "$LOG_FILE" | head -20 | jq -r '.attr.command | keys[]' 2>/dev/null | sort | uniq -c | sort -nr

echo -e "\n=== ALL COMMAND TYPES IN LOG ==="
grep '"c":"COMMAND"' "$LOG_FILE" | jq -r '.attr.command | keys[]' 2>/dev/null | sort | uniq -c | sort -nr | head -30

echo -e "\n=== SAMPLE WRITE OPERATIONS ==="
grep '"c":"WRITE"' "$LOG_FILE" | head -5

echo -e "\n=== SAMPLE UNKNOWN STRUCTURES ==="
echo "Operations that might be missed:"
grep '"c":"COMMAND"' "$LOG_FILE" | grep -v '"find":' | grep -v '"aggregate":' | grep -v '"insert":' | grep -v '"update":' | grep -v '"delete":' | grep -v '"count":' | grep -v '"getMore":' | head -5

echo -e "\n=== GETMORE OPERATIONS ==="
echo "getMore operations count:"
grep '"getMore":' "$LOG_FILE" | wc -l

echo -e "\n=== SAMPLE GETMORE ==="
grep '"getMore":' "$LOG_FILE" | head -2

