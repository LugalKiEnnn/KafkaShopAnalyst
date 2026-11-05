#!/bin/bash

curl -X POST "http://127.0.0.1:8002/api/recommendations" \
      -H "Content-Type: application/json; charset=UTF-8" \
      -d '{"user_id": "'"$1"'"}'

echo ""
