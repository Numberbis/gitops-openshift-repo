#!/usr/bin/env bash
#
# Préparation IDEMPOTENTE d'Elasticsearch pour les tests (Job
# elasticsearch-setup), avec le superutilisateur elastic :
#
#   1. rôle edot_integration_test_writer : privilèges MINIMAUX d'écriture,
#      identiques à ceux de l'API Key de production (infra/edot-collector
#      README §13) — monitor + auto_configure/create_doc sur logs-*.otel-* ;
#   2. utilisateur edot-integration-test (mot de passe du Secret) avec ce rôle :
#      identité utilisée par l'EDOT Collector de test ;
#   3. component template logs-otel@custom : 0 réplica pour les data streams
#      logs-*.otel-* (un seul nœud : un réplica ne serait jamais alloué et
#      le cluster resterait yellow).
#
# Relancer le Job réapplique ces objets (PUT) sans effet de bord.

set -euo pipefail

: "${ES_URL:?}" "${ELASTIC_PASSWORD:?}" "${EDOT_USERNAME:?}" "${EDOT_PASSWORD:?}"

# Identifiants passés à curl par un fichier de configuration (jamais en
# argument de ligne de commande, visible dans /proc).
CURLCFG="$(mktemp)"
trap 'rm -f "$CURLCFG"' EXIT
printf 'user = "elastic:%s"\n' "$ELASTIC_PASSWORD" > "$CURLCFG"

es() {  # es MÉTHODE CHEMIN [CORPS]
  curl -fsS -m 30 -K "$CURLCFG" -X "$1" -H 'Content-Type: application/json' \
    "$ES_URL$2" ${3:+--data-binary "$3"}
  echo
}

echo "==> Attente d'Elasticsearch ($ES_URL)"
for i in $(seq 1 60); do
  if es GET "/_cluster/health?wait_for_status=yellow&timeout=5s" >/dev/null 2>&1; then
    break
  fi
  [[ $i -eq 60 ]] && { echo "Elasticsearch indisponible après 5 min"; exit 1; }
  sleep 5
done

echo "==> Rôle edot_integration_test_writer"
es PUT /_security/role/edot_integration_test_writer '{
  "cluster": ["monitor"],
  "indices": [
    { "names": ["logs-*.otel-*"], "privileges": ["auto_configure", "create_doc"] }
  ]
}'

echo "==> Utilisateur $EDOT_USERNAME"
# Le mot de passe est transmis dans le corps JSON (stdin), pas en argument.
# Mots de passe générés par scripts/create-secrets.sh : alphanumériques, donc
# sans caractère à échapper en JSON.
body="$(printf '{"password":"%s","roles":["edot_integration_test_writer"],"full_name":"EDOT Collector (environnement de test)"}' "$EDOT_PASSWORD")"
curl -fsS -m 30 -K "$CURLCFG" -X PUT -H 'Content-Type: application/json' \
  "$ES_URL/_security/user/$EDOT_USERNAME" --data-binary @- <<<"$body"
echo

echo "==> Component template logs-otel@custom (0 réplica)"
es PUT /_component_template/logs-otel@custom '{
  "template": { "settings": { "index": { "number_of_replicas": 0 } } },
  "_meta": { "managed_by": "edot-integration-test", "description": "Environnement de test mono-nœud : aucun réplica" }
}'

# Data streams de test déjà créés avant ce template : réplicas retirés.
es PUT "/logs-*.otel-*/_settings?expand_wildcards=all&allow_no_indices=true" \
  '{ "index": { "number_of_replicas": 0 } }' >/dev/null || true

es GET "/_cluster/health?filter_path=status,number_of_nodes"
echo "==> OK"
