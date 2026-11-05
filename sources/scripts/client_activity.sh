#!/bin/bash

USERS=(user_1 user_2 user_3 user_4 user_5)
QUERIES=("часы" "наушники" "смартфон" "ноутбук" "кроссовки" "гантели" "велосипед" "кофемашина" "пылесос" "чайник" "книга" "игра")

for i in $(seq 1 ${1:-20}); do
    USER=${USERS[$RANDOM % ${#USERS[@]}]}
    QUERY=${QUERIES[$RANDOM % ${#QUERIES[@]}]}

    echo -n "$USER: $QUERY - "
    curl -s -X POST "http://127.0.0.1:8002/api/search" \
          -H "Content-Type: application/json; charset=UTF-8" \
          -d '{"user_id": "'"$USER"'", "query": "'"$QUERY"'"}'
    echo ""
done
