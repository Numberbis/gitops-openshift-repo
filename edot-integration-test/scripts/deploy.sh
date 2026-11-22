#!/usr/bin/env bash
#
# Déploie l'environnement de test dans le namespace edot-integration-test :
#
#   1. vérifie le contexte Kubernetes actif (garde-fou, lib.sh) ;
#   2. crée le namespace s'il n'existe pas (Pod Security « restricted ») ;
#   3. crée le Secret d'identifiants Elasticsearch s'il n'existe pas ;
#   4. déploie Kafka, attend sa disponibilité, crée le topic (Job) ;
#   5. déploie Elasticsearch, attend sa disponibilité, crée rôle/utilisateur (Job) ;
#      puis Kibana (vue de données par défaut : Job kibana-setup) ;
#   6. installe l'EDOT Collector de TEST (release Helm edot-collector-test,
#      chart et values de production + edot/values-test*.yaml).
#
# Idempotent : relancer le script met à jour les objets et relance les Jobs.
# N'agit QUE dans le namespace edot-integration-test : l'instance EDOT de
# production (namespace edot) n'est jamais lue ni modifiée.
#
# Usage : scripts/deploy.sh [--yes] [--skip-edot]
#   PLATFORM=kubernetes|openshift  forcer le profil (défaut : détection)
#   EDOT_TEST_CONTEXT=<contexte>   contexte attendu (sinon confirmation)
#   KUBECTL=oc                     binaire à utiliser

. "$(dirname "$0")/lib.sh"
parse_yes "$@"
SKIP_EDOT=false
for arg in "$@"; do [[ "$arg" == "--skip-edot" ]] && SKIP_EDOT=true; done
$SKIP_EDOT || command -v helm >/dev/null || die "helm introuvable (ou --skip-edot)"

detect_platform
confirm_context

# Gestion par Argo CD ET par ce script = conflit : l'un ou l'autre.
if "$KUBECTL" -n argocd get application edot-integration-test >/dev/null 2>&1; then
  die "l'Application Argo CD edot-integration-test existe : déployer par Argo CD (README §5.3), pas par ce script"
fi

bold "==> 1/6 Namespace $NS"
if ! "$KUBECTL" get namespace "$NS" >/dev/null 2>&1; then
  "$KUBECTL" create namespace "$NS" >/dev/null
  echo "    créé"
fi
"$KUBECTL" label namespace "$NS" --overwrite \
  app.kubernetes.io/part-of=edot-integration-test \
  edot-integration-test/environment=test \
  pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/audit=restricted \
  pod-security.kubernetes.io/warn=restricted >/dev/null

bold "==> 2/6 Secret"
"$(dirname "$0")/create-secrets.sh"

bold "==> 3/6 Manifests (overlays/$PLATFORM)"
# Jobs immuables : supprimés puis recréés (création du topic, setup ES).
k delete job kafka-topic-init elasticsearch-setup kibana-setup --ignore-not-found --wait=true >/dev/null
"$KUBECTL" apply -k "$TEST_DIR/overlays/$PLATFORM"

bold "==> 4/6 Kafka"
# 15 min : le premier démarrage inclut le téléchargement des images
# (Kafka ~240 Mo, Elasticsearch ~800 Mo depuis Docker Hub / docker.elastic.co).
k rollout status statefulset/kafka --timeout=900s
k wait --for=condition=complete job/kafka-topic-init --timeout=600s \
  || { k logs job/kafka-topic-init --tail=30 || true; die "création du topic en échec"; }
k logs job/kafka-topic-init --tail=4 | sed 's/^/    /'

bold "==> 5/6 Elasticsearch"
k rollout status statefulset/elasticsearch --timeout=900s
k wait --for=condition=complete job/elasticsearch-setup --timeout=600s \
  || { k logs job/elasticsearch-setup --tail=30 || true; die "préparation d'Elasticsearch en échec"; }
k logs job/elasticsearch-setup --tail=2 | sed 's/^/    /'

bold "==> 5b/6 Kibana"
k rollout status deployment/kibana --timeout=900s
k wait --for=condition=complete job/kibana-setup --timeout=600s \
  || { k logs job/kibana-setup --tail=30 || true; die "préparation de Kibana en échec"; }
k get ingress edot-kibana-tailscale -o jsonpath='    URL : https://{.status.loadBalancer.ingress[0].hostname}{"\n"}' 2>/dev/null || true

if $SKIP_EDOT; then
  bold "==> 6/6 EDOT : ignoré (--skip-edot)"
else
  edot_chart
  edot_values_args
  bold "==> 6/6 EDOT Collector de test (chart opentelemetry-collector $EDOT_CHART_VERSION, release $EDOT_RELEASE)"
  helm upgrade --install "$EDOT_RELEASE" opentelemetry-collector \
    --repo "$EDOT_CHART_REPO" --version "$EDOT_CHART_VERSION" \
    --namespace "$NS" "${EDOT_VALUES[@]}" --wait --timeout 10m
fi

bold "==> Environnement prêt"
k get pods,pvc -l app.kubernetes.io/part-of=edot-integration-test
echo
echo "Santé : scripts/check-health.sh   Tests : scripts/run-tests.sh   Nettoyage : scripts/cleanup.sh"
