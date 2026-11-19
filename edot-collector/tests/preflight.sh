#!/usr/bin/env bash
#
# Contrôle PRÉALABLE à la mise en service de l'EDOT Collector.
#
#   preflight.sh          statique : liste chaque TODO restant (fichier:ligne)
#                         dans les fichiers DÉPLOYÉS. Code retour 1 s'il en reste.
#   preflight.sh --live   + contrôles EN LECTURE SEULE sur le cluster courant
#                         (oc ou kubectl) : namespace, StorageClass,
#                         ExternalSecrets synchronisés, clés des Secrets
#                         présentes (JAMAIS leurs valeurs), droits du compte.
#
# À lancer avant la première synchronisation Argo CD (README §16). Il ne
# modifie rien, ni dans le dépôt ni dans le cluster.
set -uo pipefail

ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
D="$ROOT/edot-collector"
LIVE=false
[[ "${1:-}" == "--live" ]] && LIVE=true

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
rc=0

# Fichiers réellement déployés (les tests, README et TODO.md sont exclus).
files=(
  "$D"/values.yaml "$D"/values-openshift.yaml "$D"/values-kubernetes.yaml
  "$D"/values-kafka.yaml "$D"/values-elasticsearch.yaml "$D"/values-processing.yaml
  "$D"/network-policies/allows/*.yaml "$D"/prometheusrule.yaml
  "$ROOT"/apps/40-edot-collector-app.yaml
)

bold "==> Paramètres TODO restants (fichiers déployés)"
# Seules les VALEURS comptent : une ligne de commentaire qui cite TODO.md n'est pas un paramètre.
hits="$(grep -nH -E '^[^#]*TODO_[A-Z0-9_]+' "${files[@]}" 2>/dev/null | sed "s|$ROOT/||")"
if [[ -n "$hits" ]]; then
  count="$(wc -l <<<"$hits")"
  red "    ✗ $count ligne(s) à renseigner :"
  sed 's/^/      /' <<<"$hits"
  echo "    Signification de chaque paramètre : edot-collector/TODO.md"
  rc=1
else
  green "    ✓ aucun TODO dans les fichiers déployés"
fi
# Paramètres commentés à décider (storageClassName, managed-by…).
optional="$(grep -nH -E '^\s*#\s*[a-zA-Z.-]+/?[a-zA-Z.-]*:\s*TODO_' "${files[@]}" 2>/dev/null | sed "s|$ROOT/||")"
if [[ -n "$optional" ]]; then
  echo "    Paramètres OPTIONNELS commentés (décision à prendre, cf. TODO.md) :"
  sed 's/^/      /' <<<"$optional"
fi

if $LIVE; then
  CLI="$(command -v oc || command -v kubectl)" || { red "oc / kubectl introuvable"; exit 2; }
  NS=edot
  bold "==> Cluster (lecture seule) : $("$CLI" config current-context 2>/dev/null || echo '?')"
  ok()  { echo "    ✓ $*"; }
  ko()  { red "    ✗ $*"; rc=1; }

  "$CLI" get namespace "$NS" >/dev/null 2>&1 && ok "namespace $NS présent" || ko "namespace $NS absent (créé par Argo CD, CreateNamespace=true)"
  sc="$("$CLI" get storageclass -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")]}{.metadata.name}{end}' 2>/dev/null)"
  [[ -n "$sc" ]] && ok "StorageClass par défaut : $sc" || ko "aucune StorageClass par défaut : fixer storageClassName (values.yaml)"
  "$CLI" api-resources --api-group=external-secrets.io -o name 2>/dev/null | grep -q '^externalsecrets' \
    && ok "External Secrets Operator installé" || ko "CRD ExternalSecret absente"
  "$CLI" api-resources --api-group=monitoring.coreos.com -o name 2>/dev/null | grep -q '^servicemonitors' \
    && ok "CRD ServiceMonitor présente" || ko "CRD ServiceMonitor absente (désactiver serviceMonitor ou installer la supervision)"
  for es in edot-kafka-credentials edot-elasticsearch-credentials; do
    st="$("$CLI" -n "$NS" get externalsecret "$es" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)"
    [[ "$st" == "True" ]] && ok "ExternalSecret $es synchronisé" || ko "ExternalSecret $es non synchronisé (valeurs absentes de Vault ?)"
    # Noms des clés uniquement — les valeurs ne sont ni lues ni affichées.
    keys="$("$CLI" -n "$NS" get secret "$es" -o go-template='{{range $k, $v := .data}}{{$k}} {{end}}' 2>/dev/null)"
    [[ -n "$keys" ]] && ok "Secret $es : clés [$keys]" || ko "Secret $es absent"
  done
  if "$CLI" api-resources -o name 2>/dev/null | grep -q '^securitycontextconstraints'; then
    ok "OpenShift détecté : profil values-openshift.yaml attendu dans l'Application"
    grep -q 'values-openshift.yaml' "$ROOT/apps/40-edot-collector-app.yaml" \
      || ko "l'Application n'utilise pas values-openshift.yaml"
  else
    ok "Kubernetes standard détecté : profil values-kubernetes.yaml attendu dans l'Application"
    grep -q 'values-kubernetes.yaml' "$ROOT/apps/40-edot-collector-app.yaml" \
      || ko "l'Application utilise le profil OpenShift sur un cluster sans SCC (runAsNonRoot refusé)"
  fi
fi

echo
if [[ $rc -eq 0 ]]; then
  green "==> Prêt pour la mise en service (README §16). Étapes suivantes : tests/network-diagnostics.sh, puis sync manuel."
else
  red "==> PAS prêt pour la mise en service : corriger les points ci-dessus."
fi
exit $rc
