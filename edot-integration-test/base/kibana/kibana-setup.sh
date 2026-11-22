#!/usr/bin/env bash
#
# Job kibana-setup (hook PostSync) : crée ou met à jour la vue de données
# « Logs EDOT (OTel) » sur logs-*.otel-* et en fait la vue par défaut de
# Discover. Idempotent (identifiant fixe, override).
set -euo pipefail
: "${KIBANA_URL:?}" "${ELASTIC_PASSWORD:?}"

CURLCFG="$(mktemp)"
trap 'rm -f "$CURLCFG"' EXIT
printf 'user = "elastic:%s"\n' "$ELASTIC_PASSWORD" > "$CURLCFG"

kb() {  # kb CHEMIN CORPS
  curl -fsS -m 30 -K "$CURLCFG" -X POST -H 'Content-Type: application/json' \
    -H 'kbn-xsrf: edot-integration-test' "$KIBANA_URL$1" --data-binary "$2" -o /dev/null
}

echo "==> Attente de Kibana ($KIBANA_URL)"
for i in $(seq 1 120); do
  code="$(curl -s -m 5 -K "$CURLCFG" -o /dev/null -w '%{http_code}' "$KIBANA_URL/api/status" || true)"
  [[ "$code" == 200 ]] && break
  [[ $i -eq 120 ]] && { echo "Kibana indisponible après 10 min (HTTP $code)"; exit 1; }
  sleep 5
done

echo "==> Vue de données logs-*.otel-*"
kb /api/data_views/data_view '{
  "override": true,
  "data_view": {
    "id": "edot-otel-logs",
    "name": "Logs EDOT (OTel)",
    "title": "logs-*.otel-*",
    "timeFieldName": "@timestamp"
  }
}'
kb /api/data_views/default '{"data_view_id": "edot-otel-logs", "force": true}'
echo "==> OK"
