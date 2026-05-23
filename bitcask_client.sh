#!/bin/bash

CENTRAL_STATION_URL=${CENTRAL_STATION_URL:-"http://localhost:8080"}

if [ "$1" == "--view-all" ]; then
    TIMESTAMP=$(date +%s)
    FILENAME="${TIMESTAMP}.csv"
    echo "key,value" > "$FILENAME"
    curl -s "${CENTRAL_STATION_URL}/bitcask/all" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for key, value in data.items():
    v = json.dumps(value) if isinstance(value, dict) else str(value)
    print(f'{key},{v}')
" >> "$FILENAME"
    echo "Written to $FILENAME"

elif [ "$1" == "--view" ]; then
    KEY=$(echo "$2" | sed 's/--key=//')
    curl -s "${CENTRAL_STATION_URL}/bitcask/key/${KEY}"
    echo

elif [ "$1" == "--perf" ]; then
    CLIENTS=$(echo "$2" | sed 's/--clients=//')
    TIMESTAMP=$(date +%s)
    for i in $(seq 1 "$CLIENTS"); do
        (
            FILENAME="${TIMESTAMP}_thread_${i}.csv"
            echo "key,value" > "$FILENAME"
            curl -s "${CENTRAL_STATION_URL}/bitcask/all" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for key, value in data.items():
    v = json.dumps(value) if isinstance(value, dict) else str(value)
    print(f'{key},{v}')
" >> "$FILENAME"
            echo "Thread $i written to $FILENAME"
        ) &
    done
    wait
    echo "All $CLIENTS threads done"

else
    echo "Usage:"
    echo "  ./bitcask_client.sh --view-all"
    echo "  ./bitcask_client.sh --view --key=SOME_KEY"
    echo "  ./bitcask_client.sh --perf --clients=100"
fi