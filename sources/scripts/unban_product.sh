#!/bin/bash

PRODUCT_ID="$1"

if [ -z "$PRODUCT_ID" ]; then
    echo "Использование: $0 <product_id>"
    exit 1
fi

curl -s -X DELETE "http://127.0.0.1:8003/api/bans/$PRODUCT_ID"

echo ""
