#!/bin/bash
#
# Сквозная проверка пайплайна:
#   1. добавляем запреты через ban-api;
#   2. грузим товары через shop-api (Streams фильтрует, Connect пишет в ES);
#   3. ищем забаненный и разрешённый товар через client-api.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FILE="${DATA_FILE:-$SCRIPT_DIR/../data/products.json}"
CONNECTOR_CONFIG="${CONNECTOR_CONFIG:-$SCRIPT_DIR/../../elasticsearch-connect/connector-config.json}"

SHOP_API_URL="${SHOP_API_URL:-http://127.0.0.1:8001}"
CLIENT_API_URL="${CLIENT_API_URL:-http://127.0.0.1:8002}"
BAN_API_URL="${BAN_API_URL:-http://127.0.0.1:8003}"
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://127.0.0.1:9200}"
ELASTICSEARCH_INDEX="${ELASTICSEARCH_INDEX:-filtered-product-topic}"
CONNECT_URL="${CONNECT_URL:-http://127.0.0.1:8085}"
CONNECTOR_NAME="${CONNECTOR_NAME:-elasticsearch}"

BANNED_IDS=(10005 10009 10013)
ALLOWED_ID="${ALLOWED_ID:-10001}"

#BAN_WAIT_SECONDS="${BAN_WAIT_SECONDS:-5}"
BAN_WAIT_SECONDS=5
ES_WAIT_SECONDS="${ES_WAIT_SECONDS:-90}"
CONNECTOR_WAIT_SECONDS="${CONNECTOR_WAIT_SECONDS:-60}"

info() { echo "[INFO] $*"; }
ok()   { echo "[OK]   $*"; }
fail() { echo "[FAIL] $*" >&2; exit 1; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || fail "Не найдена команда: $1"
}

http_code() {
    curl -s -o /dev/null -w '%{http_code}' "$@" 2>/dev/null || true
}

wait_for_http() {
    local url="$1" name="$2" timeout="${3:-60}" i code

    for ((i = 0; i < timeout; i += 2)); do
        code=$(http_code -m 2 "$url")
        if [ "$code" = "200" ]; then
            ok "$name доступен"
            return 0
        fi
        sleep 2
    done

    fail "$name недоступен: $url"
}

product_name() {
    python3 - "$DATA_FILE" "$1" <<'PY'
import json
import sys

for product in json.load(open(sys.argv[1], encoding="utf-8")):
    if str(product.get("product_id")) == str(sys.argv[2]):
        print(product.get("name", ""))
        break
PY
}

expected_allowed_count() {
    python3 - "$DATA_FILE" "${BANNED_IDS[@]}" <<'PY'
import json
import sys

products = json.load(open(sys.argv[1], encoding="utf-8"))
banned = {str(value) for value in sys.argv[2:]}
print(sum(1 for product in products if str(product.get("product_id")) not in banned))
PY
}

json_has_product() {
    python3 -c 'import json,sys
target = str(sys.argv[1])
try:
    data = json.load(sys.stdin)
except Exception:
    print("false")
    raise SystemExit(0)
print("true" if any(str(item.get("product_id")) == target for item in data.get("items", [])) else "false")' "$1"
}

pretty_json() {
    python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin), ensure_ascii=False, indent=2))'
}

search_client() {
    local query="$1"
    local payload

    payload=$(python3 -c 'import json,sys; print(json.dumps({"user_id": "verify_pipeline", "query": sys.argv[1]}, ensure_ascii=False))' "$query")
    curl -sf -m 20 -X POST "$CLIENT_API_URL/api/search" \
        -H 'Content-Type: application/json; charset=UTF-8' \
        -d "$payload"
}

require_cmd curl
require_cmd python3

[ -f "$DATA_FILE" ] || fail "Не найден файл с товарами: $DATA_FILE"
[ -f "$CONNECTOR_CONFIG" ] || fail "Не найден конфиг коннектора: $CONNECTOR_CONFIG"

info "Проверяю доступность сервисов..."
wait_for_http "$SHOP_API_URL/api/health" "shop-api" 60
wait_for_http "$CLIENT_API_URL/api/health" "client-api" 60
wait_for_http "$BAN_API_URL/api/health" "ban-api" 60
wait_for_http "$ELASTICSEARCH_URL/_cluster/health" "Elasticsearch" 90
wait_for_http "$CONNECT_URL/connectors" "Kafka Connect" 120

info "Регистрирую/обновляю коннектор $CONNECTOR_NAME из $CONNECTOR_CONFIG..."
curl -sf -m 10 -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" \
    -H 'Content-Type: application/json' \
    --data @"$CONNECTOR_CONFIG" >/dev/null \
    || fail "Не удалось зарегистрировать коннектор $CONNECTOR_NAME"

# После изменения конфига Connect может не поднять ранее упавшие tasks сам
curl -sf -m 10 -X POST "$CONNECT_URL/connectors/$CONNECTOR_NAME/restart?includeTasks=true" >/dev/null || true

connector_state=""
tasks_state=""
last_status=""
for ((i = 0; i < CONNECTOR_WAIT_SECONDS; i += 2)); do
    last_status=$(curl -s -m 3 "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null || true)
    connector_state=$(printf '%s' "$last_status" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("connector",{}).get("state",""))' 2>/dev/null || true)
    tasks_state=$(printf '%s' "$last_status" | python3 -c 'import json,sys
d=json.load(sys.stdin)
tasks=d.get("tasks",[])
print("RUNNING" if tasks and all(t.get("state")=="RUNNING" for t in tasks) else (tasks[0].get("state","") if tasks else "NO_TASKS"))' 2>/dev/null || true)

    if [ "$connector_state" = "RUNNING" ] && [ "$tasks_state" = "RUNNING" ]; then
        break
    fi
    sleep 2
done

if [ "$connector_state" != "RUNNING" ] || [ "$tasks_state" != "RUNNING" ]; then
    printf '%s' "$last_status" | pretty_json 2>/dev/null || echo "$last_status"
    fail "Коннектор $CONNECTOR_NAME не RUNNING (connector=${connector_state:-unknown}, tasks=${tasks_state:-unknown})"
fi
ok "Коннектор $CONNECTOR_NAME и его задачи RUNNING"

info "Очищаю индекс $ELASTICSEARCH_INDEX..."
curl -s -m 5 -X DELETE "$ELASTICSEARCH_URL/$ELASTICSEARCH_INDEX" >/dev/null || true

info "Добавляю запреты: ${BANNED_IDS[*]}"
for product_id in "${BANNED_IDS[@]}"; do
    payload=$(python3 -c 'import json,sys; print(json.dumps({"product_id": sys.argv[1], "reason": "verify_pipeline"}))' "$product_id")
    curl -sf -m 10 -X POST "$BAN_API_URL/api/bans" \
        -H 'Content-Type: application/json; charset=UTF-8' \
        -d "$payload" >/dev/null \
        || fail "Не удалось добавить запрет $product_id"
    ok "Запрет добавлен: $product_id"
done

info "Жду ${BAN_WAIT_SECONDS} с, чтобы Streams обновил GlobalKTable..."
sleep "$BAN_WAIT_SECONDS"

info "Загружаю товары через shop-api..."
load_response=$(curl -sf -m 30 -X POST "$SHOP_API_URL/api/products/load") \
    || fail "Не удалось загрузить товары"
echo "  $load_response"

expected_count=$(expected_allowed_count)
info "Ожидаемое количество разрешённых товаров: $expected_count"

info "Жду, пока Connect запишет $expected_count документов в Elasticsearch..."
count=-1
for ((i = 0; i < ES_WAIT_SECONDS; i += 2)); do
    count=$(curl -s -m 3 "$ELASTICSEARCH_URL/$ELASTICSEARCH_INDEX/_count" 2>/dev/null \
        | python3 -c 'import json,sys
try:
    print(json.load(sys.stdin).get("count", -1))
except Exception:
    print(-1)' 2>/dev/null \
        || echo -1)

    if [[ "$count" =~ ^[0-9]+$ ]] && [ "$count" -eq "$expected_count" ]; then
        break
    fi
    sleep 2
done

if ! [[ "$count" =~ ^[0-9]+$ ]] || [ "$count" -ne "$expected_count" ]; then
    fail "В Elasticsearch $count документов, ожидалось $expected_count. Проверь Streams и Connect."
fi
ok "В Elasticsearch $count документов"

allowed_name=$(product_name "$ALLOWED_ID")
banned_id="${BANNED_IDS[0]}"
banned_name=$(product_name "$banned_id")

[ -n "$allowed_name" ] || fail "В $DATA_FILE не найден товар $ALLOWED_ID"
[ -n "$banned_name" ] || fail "В $DATA_FILE не найден товар $banned_id"

info "Ищу разрешённый товар '$allowed_name' ($ALLOWED_ID)..."
allowed_response=$(search_client "$allowed_name") \
    || fail "Ошибка запроса к client-api для разрешённого товара"
printf '%s' "$allowed_response" | pretty_json 2>/dev/null || echo "$allowed_response"

if [ "$(printf '%s' "$allowed_response" | json_has_product "$ALLOWED_ID")" != "true" ]; then
    fail "Разрешённый товар $ALLOWED_ID не найден в результатах поиска"
fi
ok "Разрешённый товар найден"

info "Ищу забаненный товар '$banned_name' ($banned_id)..."
banned_response=$(search_client "$banned_name") \
    || fail "Ошибка запроса к client-api для забаненного товара"
printf '%s' "$banned_response" | pretty_json 2>/dev/null || echo "$banned_response"

if [ "$(printf '%s' "$banned_response" | json_has_product "$banned_id")" = "true" ]; then
    fail "Забаненный товар $banned_id всё ещё находится в результатах поиска"
fi
ok "Забаненный товар в поиске отсутствует"

echo
ok "Проверка пройдена: фильтрация, запись в Elasticsearch и поиск работают."
