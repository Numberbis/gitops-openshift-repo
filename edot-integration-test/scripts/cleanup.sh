#!/usr/bin/env bash
#
# Supprime l'environnement de test, et LUI SEUL : toutes les commandes portent
# sur le namespace edot-integration-test. Aucun Operator, CRD ni objet
# cluster-wide n'est concerné (l'environnement n'en crée aucun).
#
#   (défaut)            workloads, Services, ConfigMaps, NetworkPolicies,
#                       Jobs, pods de test, release Helm edot-collector-test.
#                       CONSERVÉS : PVC (données Kafka/Elasticsearch/file
#                       EDOT), Secret d'identifiants, namespace.
#   --delete-pvc        supprime AUSSI les PVC de test et le Secret, après
#                       confirmation (ou --yes).
#   --delete-namespace  supprime AUSSI le namespace (implique --delete-pvc).
#
# Usage : scripts/cleanup.sh [--delete-pvc] [--delete-namespace] [--yes]

. "$(dirname "$0")/lib.sh"
parse_yes "$@"
DEL_PVC=false DEL_NS=false
for arg in "$@"; do
  case "$arg" in
    --delete-pvc) DEL_PVC=true ;;
    --delete-namespace) DEL_NS=true; DEL_PVC=true ;;
    --yes|-y) ;;
    *) die "option inconnue : $arg" ;;
  esac
done
[[ "$NS" == edot-integration-test ]] || die "namespace inattendu : $NS"

detect_platform
confirm_context
"$KUBECTL" get namespace "$NS" >/dev/null 2>&1 || { echo "Namespace $NS absent : rien à faire."; exit 0; }

if "$KUBECTL" -n argocd get application edot-integration-test >/dev/null 2>&1; then
  red "ATTENTION : l'Application Argo CD edot-integration-test existe. La supprimer"
  red "            (kubectl -n argocd delete application edot-integration-test) pour"
  red "            qu'Argo CD ne recrée pas les objets lors d'une synchronisation."
fi

bold "==> EDOT Collector de test (release Helm $EDOT_RELEASE)"
if command -v helm >/dev/null && helm status "$EDOT_RELEASE" -n "$NS" >/dev/null 2>&1; then
  helm uninstall "$EDOT_RELEASE" -n "$NS" --wait
else
  echo "    absente (ou helm non installé)"
fi

bold "==> Kafka, Elasticsearch, NetworkPolicies (overlays/$PLATFORM)"
"$KUBECTL" delete -k "$TEST_DIR/overlays/$PLATFORM" --ignore-not-found --wait=true
"$KUBECTL" delete -k "$TEST_DIR/overlays/$PLATFORM/producer" --ignore-not-found --wait=true
# Restes éventuels : Job producteur, pods clients des tests.
k delete job,pod -l app.kubernetes.io/part-of=edot-integration-test --ignore-not-found --wait=true

bold "==> Ressources persistantes restantes"
k get pvc,secret -l app.kubernetes.io/part-of=edot-integration-test 2>/dev/null | sed 's/^/    /' || true
if ! $DEL_PVC; then
  echo "    Conservées. Supprimer : scripts/cleanup.sh --delete-pvc [--delete-namespace]"
  exit 0
fi

# PVC de test : noms connus (volumeClaimTemplates) ET label du namespace de test.
pvcs="$(k get pvc -o name 2>/dev/null | grep -E '^persistentvolumeclaim/(data-kafka-0|data-elasticsearch-0|queue-edot-collector-test-[0-9]+)$' || true)"
bold "==> Suppression DÉFINITIVE dans $NS : ${pvcs//$'\n'/ } secret/elasticsearch-test-credentials$($DEL_NS && echo " namespace/$NS")"
if [[ "${ASSUME_YES:-0}" != 1 ]]; then
  read -r -p "Confirmer (données de test perdues) ? [o/N] " ans
  [[ "$ans" =~ ^[oOyY]$ ]] || die "abandon"
fi
[[ -n "$pvcs" ]] && k delete $pvcs --wait=true
k delete secret elasticsearch-test-credentials --ignore-not-found
if $DEL_NS; then
  "$KUBECTL" delete namespace "$NS" --wait=true
fi
green "==> Nettoyage terminé"
