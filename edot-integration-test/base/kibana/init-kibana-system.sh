#!/usr/bin/env bash
#
# initContainer du pod Kibana : définit (idempotent) le mot de passe de
# l'utilisateur intégré kibana_system avec le superutilisateur elastic, puis
# rend la main. Le conteneur Kibana ne reçoit QUE le mot de passe de
# kibana_system. Dans le même pod parce qu'Argo CD attend que Kibana soit
# sain avant ses hooks PostSync : un Job ne pourrait pas le faire avant.
set -euo pipefail
: "${ES_URL:?}" "${ELASTIC_PASSWORD:?}" "${KIBANA_SYSTEM_PASSWORD:?}"

# Identifiants passés à curl par un fichier (jamais en argument, visible dans /proc).
CURLCFG="$(mktemp)"
trap 'rm -f "$CURLCFG"' EXIT
printf 'user = "elastic:%s"\n' "$ELASTIC_PASSWORD" > "$CURLCFG"

echo "==> Attente d'Elasticsearch ($ES_URL)"
for i in $(seq 1 120); do
  curl -fsS -m 5 -K "$CURLCFG" -o /dev/null "$ES_URL/_cluster/health?wait_for_status=yellow&timeout=4s" 2>/dev/null && break
  [[ $i -eq 120 ]] && { echo "Elasticsearch indisponible après 10 min"; exit 1; }
  sleep 5
done

echo "==> Mot de passe de kibana_system"
# Mots de passe alphanumériques (scripts/create-secrets.sh) : rien à échapper.
printf '{"password":"%s"}' "$KIBANA_SYSTEM_PASSWORD" |
  curl -fsS -m 30 -K "$CURLCFG" -X POST -H 'Content-Type: application/json' \
    "$ES_URL/_security/user/kibana_system/_password" --data-binary @- >/dev/null
echo "==> OK"
