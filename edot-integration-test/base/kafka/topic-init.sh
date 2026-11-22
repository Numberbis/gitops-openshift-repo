#!/usr/bin/env bash
#
# Création IDEMPOTENTE du topic de test (Job kafka-topic-init).
#
#   * topic absent  → créé avec TOPIC_PARTITIONS partitions, RF 1 ;
#   * topic présent → rétention et politique de nettoyage réappliquées ;
#     partitions AUGMENTÉES si TOPIC_PARTITIONS est supérieur au nombre
#     actuel (Kafka ne permet pas de les réduire : avertissement) ;
#   * relancer le Job ne modifie rien si la configuration est inchangée.
#
# Paramètres : ConfigMap kafka-topic-config (base/kafka/kustomization.yaml).

set -euo pipefail

: "${BOOTSTRAP:?}" "${TOPIC_NAME:?}" "${TOPIC_PARTITIONS:?}" "${TOPIC_RETENTION_MS:?}"
BIN=/opt/kafka/bin

echo "==> Attente du broker $BOOTSTRAP"
for i in $(seq 1 60); do
  if "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    break
  fi
  [[ $i -eq 60 ]] && { echo "Broker injoignable après 5 min"; exit 1; }
  sleep 5
done

echo "==> Topic $TOPIC_NAME (partitions=$TOPIC_PARTITIONS, RF=1, retention.ms=$TOPIC_RETENTION_MS)"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$TOPIC_NAME" --partitions "$TOPIC_PARTITIONS" --replication-factor 1 \
  --config retention.ms="$TOPIC_RETENTION_MS" --config cleanup.policy=delete

current="$("$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC_NAME" \
  | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -1)"
if (( current < TOPIC_PARTITIONS )); then
  echo "==> Partitions : $current → $TOPIC_PARTITIONS"
  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --alter \
    --topic "$TOPIC_NAME" --partitions "$TOPIC_PARTITIONS"
elif (( current > TOPIC_PARTITIONS )); then
  echo "AVERTISSEMENT : $current partitions existantes > $TOPIC_PARTITIONS demandées."
  echo "                Kafka ne réduit pas les partitions : supprimer le topic pour le recréer."
fi

# Configuration réappliquée à chaque exécution (sans effet si identique).
"$BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --alter \
  --entity-type topics --entity-name "$TOPIC_NAME" \
  --add-config retention.ms="$TOPIC_RETENTION_MS",cleanup.policy=delete

"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC_NAME"
echo "==> OK"
