# shellcheck shell=bash
# Fonctions communes aux scripts de l'environnement edot-integration-test.
# Source : . "$(dirname "$0")/lib.sh"

set -euo pipefail

# Namespace FIXE : il figure aussi dans les noms DNS des manifests
# (advertised.listeners, endpoints EDOT). Le changer impose de modifier
# base/ et edot/values-test.yaml.
NS=edot-integration-test
EDOT_RELEASE=edot-collector-test

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$TEST_DIR/../.." && pwd)"
EDOT_DIR="$REPO_ROOT/infra/edot-collector"

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
die()   { red "ERREUR : $*" >&2; exit 2; }

# kubectl ou oc : variable KUBECTL, sinon oc s'il est installé, sinon kubectl.
if [[ -z "${KUBECTL:-}" ]]; then
  if command -v oc >/dev/null; then KUBECTL=oc; else KUBECTL=kubectl; fi
fi
command -v "$KUBECTL" >/dev/null || die "$KUBECTL introuvable"
k() { "$KUBECTL" -n "$NS" "$@"; }

# Plateforme : PLATFORM=openshift|kubernetes, sinon détection (API des SCC).
detect_platform() {
  if [[ -n "${PLATFORM:-}" ]]; then
    [[ "$PLATFORM" == openshift || "$PLATFORM" == kubernetes ]] || die "PLATFORM=$PLATFORM invalide"
  elif "$KUBECTL" api-resources --api-group=security.openshift.io -o name 2>/dev/null \
       | grep -q securitycontextconstraints; then
    PLATFORM=openshift
  else
    PLATFORM=kubernetes
  fi
  export PLATFORM
}

# Garde-fou : le contexte actif doit être le cluster de test autorisé.
#   * EDOT_TEST_CONTEXT=<nom> : le contexte DOIT porter ce nom (CI, scripts) ;
#   * sinon confirmation interactive, ou ASSUME_YES=1 / option --yes.
confirm_context() {
  local ctx server
  ctx="$("$KUBECTL" config current-context 2>/dev/null || true)"
  server="$("$KUBECTL" config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)"
  [[ -n "$ctx" ]] || die "aucun contexte Kubernetes actif"
  bold "Contexte : $ctx ($server) — namespace $NS — plateforme $PLATFORM"
  if [[ -n "${EDOT_TEST_CONTEXT:-}" ]]; then
    [[ "$ctx" == "$EDOT_TEST_CONTEXT" ]] || die "contexte « $ctx » ≠ EDOT_TEST_CONTEXT « $EDOT_TEST_CONTEXT »"
    return
  fi
  [[ "${ASSUME_YES:-0}" == 1 ]] && return
  [[ -t 0 ]] || die "terminal non interactif : passer --yes ou EDOT_TEST_CONTEXT=$ctx"
  read -r -p "Agir sur ce cluster, namespace $NS uniquement ? [o/N] " ans
  [[ "$ans" =~ ^[oOyY]$ ]] || die "abandon"
}

# Option commune --yes.
parse_yes() {
  for arg in "$@"; do [[ "$arg" == "--yes" || "$arg" == "-y" ]] && ASSUME_YES=1; done
  return 0
}

# Chart EDOT : MÊME chart et MÊME version que l'Application de production.
edot_chart() {
  local app="$REPO_ROOT/argocd/applications/infra/edot-collector.yaml"
  EDOT_CHART_REPO="$(sed -n 's/^ *- repoURL: \(https:\/\/open-telemetry.*\)$/\1/p' "$app" | head -1)"
  EDOT_CHART_VERSION="$(sed -n 's/^ *targetRevision: "\([0-9.]*\)"$/\1/p' "$app" | head -1)"
  [[ -n "$EDOT_CHART_REPO" && -n "$EDOT_CHART_VERSION" ]] || die "chart EDOT introuvable dans $app"
}

# Fichiers de values EDOT de test, dans l'ordre de fusion.
edot_values_args() {
  local profile=values-kubernetes.yaml
  [[ "$PLATFORM" == openshift ]] && profile=values-openshift.yaml
  EDOT_VALUES=(
    -f "$EDOT_DIR/values.yaml"
    -f "$EDOT_DIR/$profile"
    -f "$EDOT_DIR/values-kafka.yaml"
    -f "$EDOT_DIR/values-elasticsearch.yaml"
    -f "$EDOT_DIR/values-processing.yaml"
    -f "$TEST_DIR/edot/values-test.yaml"
  )
  [[ "$PLATFORM" == kubernetes ]] && EDOT_VALUES+=(-f "$TEST_DIR/edot/values-test-kubernetes.yaml")
  return 0
}
