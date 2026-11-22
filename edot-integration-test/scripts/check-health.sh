#!/usr/bin/env bash
#
# État et consommation de l'environnement de test, en LECTURE SEULE :
# pods, Jobs, PVC, CPU/mémoire par composant (metrics-server), disque
# utilisé dans chaque volume persistant, santé d'Elasticsearch.
#
# Code de sortie 1 si un composant n'est pas prêt.
#
# Usage : scripts/check-health.sh

. "$(dirname "$0")/lib.sh"
"$KUBECTL" get namespace "$NS" >/dev/null 2>&1 || die "namespace $NS absent"
status=0

ready() {  # ready LIBELLÉ SÉLECTEUR
  local r t
  r="$(k get pods -l "$2" -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' | grep -c True || true)"
  t="$(k get pods -l "$2" --no-headers 2>/dev/null | wc -l)"
  if [[ "$t" -gt 0 && "$r" == "$t" ]]; then green "    $1 : $r/$t prêt(s)"; else red "    $1 : $r/$t prêt(s)"; status=1; fi
}

bold "==> Pods"
ready "Kafka         " app.kubernetes.io/name=kafka
ready "Elasticsearch " app.kubernetes.io/name=elasticsearch
ready "Kibana        " app.kubernetes.io/name=kibana
ready "EDOT (test)   " app.kubernetes.io/instance=$EDOT_RELEASE

bold "==> Jobs"
k get jobs -o custom-columns='JOB:.metadata.name,TERMINÉS:.status.succeeded,ÉCHECS:.status.failed' 2>/dev/null | sed 's/^/    /'

bold "==> CPU / mémoire (kubectl top)"
for sel in app.kubernetes.io/name=kafka app.kubernetes.io/name=elasticsearch app.kubernetes.io/name=kibana app.kubernetes.io/instance=$EDOT_RELEASE; do
  k top pods -l "$sel" --containers --no-headers 2>/dev/null | sed 's/^/    /' \
    || echo "    (metrics-server indisponible pour $sel)"
done

bold "==> Requests / limits"
k get pods -l app.kubernetes.io/part-of=edot-integration-test -o \
  custom-columns='POD:.metadata.name,REQ_CPU:.spec.containers[0].resources.requests.cpu,REQ_MEM:.spec.containers[0].resources.requests.memory,LIM_CPU:.spec.containers[0].resources.limits.cpu,LIM_MEM:.spec.containers[0].resources.limits.memory' \
  --field-selector=status.phase=Running | sed 's/^/    /'

bold "==> Stockage persistant"
k get pvc -o custom-columns='PVC:.metadata.name,STATUT:.status.phase,TAILLE:.status.capacity.storage,CLASSE:.spec.storageClassName' | sed 's/^/    /'
# Taille réelle des données (du) : avec local-path, le volume est un
# répertoire de l'hôte SANS quota — df afficherait le disque du nœud, et les
# métriques kubelet_volume_stats_* ne sont pas publiées (README §9).
for spec in "kafka-0:kafka:/var/lib/kafka/data:data-kafka-0" \
            "elasticsearch-0:elasticsearch:/usr/share/elasticsearch/data:data-elasticsearch-0" \
            "$EDOT_RELEASE-0:opentelemetry-collector:/var/lib/edot:queue-$EDOT_RELEASE-0"; do
  IFS=: read -r pod ctr path pvc <<<"$spec"
  if k get pod "$pod" >/dev/null 2>&1; then
    size="$(k get pvc "$pvc" -o jsonpath='{.status.capacity.storage}' 2>/dev/null || true)"
    used="$(k exec "$pod" -c "$ctr" -- du -sh "$path" 2>/dev/null | awk '{print $1}' || true)"
    printf '    %-26s %s utilisés / %s demandés\n' "$pvc" "${used:-?}" "${size:-?}"
  fi
done

bold "==> Elasticsearch"
if k get pod elasticsearch-0 >/dev/null 2>&1; then
  # Mot de passe lu DANS le conteneur (variable d'environnement), jamais affiché.
  k exec elasticsearch-0 -c elasticsearch -- bash -c \
    'curl -s -m 10 -u "elastic:$ELASTIC_PASSWORD" "http://127.0.0.1:9200/_cluster/health?filter_path=status,number_of_nodes,active_shards,unassigned_shards" ; echo;
     curl -s -m 10 -u "elastic:$ELASTIC_PASSWORD" "http://127.0.0.1:9200/_cat/indices/.ds-logs-*?h=index,health,docs.count,store.size&s=index"' \
    | sed 's/^/    /' || status=1
fi
bold "==> Kibana"
k get ingress edot-kibana-tailscale -o jsonpath='    URL tailnet : https://{.status.loadBalancer.ingress[0].hostname}{"\n"}' 2>/dev/null \
  || echo "    pas d'Ingress Tailscale (profil OpenShift ?) : kubectl -n $NS port-forward svc/kibana 5601"
exit $status
