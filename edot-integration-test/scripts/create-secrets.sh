#!/usr/bin/env bash
#
# Crée le Secret elasticsearch-test-credentials s'il n'existe pas, avec des
# valeurs ALÉATOIRES (jamais dans Git, jamais affichées) :
#   elastic-password        superutilisateur elastic (Jobs de setup, tests, connexion à Kibana) ;
#   edot-username           utilisateur d'écriture de l'EDOT de test ;
#   edot-password           son mot de passe ;
#   kibana-system-password  utilisateur intégré kibana_system (Kibana → Elasticsearch) ;
#   kibana-encryption-key   clé de chiffrement de Kibana (sessions, objets enregistrés).
# Secret existant : seules les clés ABSENTES sont ajoutées (un Secret créé
# avant l'ajout de Kibana reçoit les clés Kibana) ; une valeur existante
# n'est jamais modifiée. Rotation : README §5.2.
#
# Environnement de TEST uniquement : pas de Vault / External Secrets ici
# (les secrets de production d'EDOT restent gérés par infra/edot-collector).

. "$(dirname "$0")/lib.sh"

gen() { LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c "${1:-32}"; }
SECRET=elasticsearch-test-credentials

if ! k get secret "$SECRET" >/dev/null 2>&1; then
  k create secret generic "$SECRET" \
    --from-literal=elastic-password="$(gen)" \
    --from-literal=edot-username=edot-integration-test \
    --from-literal=edot-password="$(gen)" \
    --from-literal=kibana-system-password="$(gen)" \
    --from-literal=kibana-encryption-key="$(gen 48)" >/dev/null
  k label secret "$SECRET" \
    app.kubernetes.io/part-of=edot-integration-test edot-integration-test/environment=test >/dev/null
  echo "    Secret $SECRET : créé (valeurs aléatoires)"
  exit 0
fi

# Clés présentes (noms seulement, jamais les valeurs).
keys="$(k get secret "$SECRET" -o go-template='{{range $k, $v := .data}}{{$k}} {{end}}')"
added=()
for spec in kibana-system-password:32 kibana-encryption-key:48; do
  key="${spec%%:*}" len="${spec##*:}"
  [[ " $keys " == *" $key "* ]] && continue
  # Valeur transmise par stdin (jamais en argument de ligne de commande).
  printf '{"data":{"%s":"%s"}}' "$key" "$(gen "$len" | base64 -w0)" |
    k patch secret "$SECRET" --type merge --patch-file /dev/stdin >/dev/null
  added+=("$key")
done
if [[ ${#added[@]} -gt 0 ]]; then
  echo "    Secret $SECRET : existant, clé(s) ajoutée(s) : ${added[*]}"
else
  echo "    Secret $SECRET : existant, conservé"
fi
