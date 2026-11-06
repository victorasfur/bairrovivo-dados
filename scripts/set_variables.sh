#!/usr/bin/env bash
# Cria as Airflow Variables do pipeline (requisito 03: chaves FORA do código).
# Rode com o Airflow de pé:  bash scripts/set_variables.sh
# Edite os valores abaixo ou passe por ambiente (BAIRROVIVO_LAT=... bash scripts/set_variables.sh).
set -euo pipefail

LAT="${BAIRROVIVO_LAT:--23.5646}"
LON="${BAIRROVIVO_LON:--46.6527}"
API_URL="${BAIRROVIVO_API_URL:-mock}"        # "mock" = usa o gerador sintético; ou a URL do export do backend
API_TOKEN="${BAIRROVIVO_API_TOKEN:-nao-definido}"   # token do backend (NUNCA commitar o valor real)
WEBHOOK="${BAIRROVIVO_WEBHOOK_URL:-}"        # opcional: URL de webhook (Discord/Slack) para a notificação

if command -v airflow >/dev/null 2>&1; then
  AF="airflow"
else
  AF="docker compose exec -T airflow-scheduler airflow"   # modo Docker
fi

$AF variables set bairrovivo_lat "$LAT"
$AF variables set bairrovivo_lon "$LON"
$AF variables set bairrovivo_api_url "$API_URL"
$AF variables set bairrovivo_api_token "$API_TOKEN"
[ -n "$WEBHOOK" ] && $AF variables set bairrovivo_webhook_url "$WEBHOOK"

$AF variables list
