#!/bin/bash

PRODUCT_ID="$1"
REASON="${2:-запрещено вручную}"

if [ -z "$PRODUCT_ID" ]; then
    echo "Использование: $0 <product_id> [reason]"
    exit 1
fi

curl -s -X POST "http://127.0.0.1:8003/api/bans" \
      -H "Content-Type: application/json; charset=UTF-8" \
      -d "{\"product_id\":\"$PRODUCT_ID\",\"reason\":\"$REASON\"}"

echo ""
