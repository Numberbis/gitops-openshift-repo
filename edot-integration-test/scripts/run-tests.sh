#!/usr/bin/env bash
#
# Tests de bout en bout Kafka → EDOT → Elasticsearch (tests/validate_pipeline.py).
# Crée un pod client éphémère et publie dans le topic de test : contexte
# vérifié comme pour le déploiement.
#
# Usage : scripts/run-tests.sh [--yes] [--timeout 180] [--settle 5] [--keep-client] …
#         (options : python3 tests/validate_pipeline.py --help)

. "$(dirname "$0")/lib.sh"
parse_yes "$@"
args=()
for arg in "$@"; do [[ "$arg" == "--yes" || "$arg" == "-y" ]] || args+=("$arg"); done
detect_platform
confirm_context
exec python3 "$TEST_DIR/tests/validate_pipeline.py" --kubectl "$KUBECTL" "${args[@]}"
