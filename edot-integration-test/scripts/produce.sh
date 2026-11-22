#!/usr/bin/env bash
#
# Publie une campagne d'événements OTLP JSON dans Kafka par le Job
# otlp-producer (producer/), puis affiche son résultat.
#
# Usage :
#   scripts/produce.sh [--events all|info,debug,…] [--count N] [--interval S]
#                      [--campaign-id ID] [--yes]
# Exemples :
#   scripts/produce.sh                                  # les 6 événements + témoin
#   scripts/produce.sh --events info,nested --count 50 --interval 0.2
# Documents : scripts/run-tests.sh fait la vérification automatique ; à la
# main, README §7.

. "$(dirname "$0")/lib.sh"
parse_yes "$@"
EVENTS=all COUNT=1 INTERVAL=0 CAMPAIGN_ID="manual-$(date +%Y%m%d-%H%M%S)"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --events) EVENTS="$2"; shift 2 ;;
    --count) COUNT="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --campaign-id) CAMPAIGN_ID="$2"; shift 2 ;;
    --yes|-y) shift ;;
    *) die "option inconnue : $1" ;;
  esac
done
[[ "$EVENTS" =~ ^[a-z,]+$ && "$COUNT" =~ ^[0-9]+$ && "$INTERVAL" =~ ^[0-9.]+$ && "$CAMPAIGN_ID" =~ ^[A-Za-z0-9._-]+$ ]] \
  || die "paramètre invalide"

detect_platform
confirm_context

k delete job otlp-producer --ignore-not-found --wait=true >/dev/null
# Paramètres injectés dans la ConfigMap rendue (valeurs contrôlées ci-dessus).
"$KUBECTL" kustomize "$TEST_DIR/overlays/$PLATFORM/producer" \
  | sed -e "s/^  EVENTS: .*/  EVENTS: \"$EVENTS\"/" -e "s/^  COUNT: .*/  COUNT: \"$COUNT\"/" \
        -e "s/^  INTERVAL: .*/  INTERVAL: \"$INTERVAL\"/" -e "s/^  CAMPAIGN_ID: .*/  CAMPAIGN_ID: \"$CAMPAIGN_ID\"/" \
  | "$KUBECTL" apply -f - >/dev/null
bold "==> Job otlp-producer : campagne $CAMPAIGN_ID ($EVENTS ×$COUNT, intervalle ${INTERVAL}s)"
k wait --for=condition=complete job/otlp-producer --timeout=3600s \
  || { k logs job/otlp-producer --all-containers --tail=30 || true; die "Job en échec"; }
k logs job/otlp-producer -c generator | sed 's/^/    /'
green "==> Publié. Recherche : attributes.test.campaign_id = $CAMPAIGN_ID"
