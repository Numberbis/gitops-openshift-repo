#!/usr/bin/env bash
#
# Diagnostic réseau OpenShift → Kafka EXTERNE et OpenShift → Elasticsearch
# EXTERNE, DEPUIS le namespace edot, avec la même identité réseau qu'EDOT.
#
#   network-diagnostics.sh kafka           DNS, TCP, TLS, SASL, advertised.listeners, ACL
#   network-diagnostics.sh elasticsearch   DNS, TCP, TLS (chaîne, nom), HTTPS, API Key, privilèges
#   network-diagnostics.sh all             les deux (défaut)
#
# Lit les adresses dans values-kafka.yaml / values-elasticsearch.yaml et
# REFUSE de s'exécuter tant qu'elles contiennent des TODO : aucune adresse
# n'est devinée ni contactée par défaut.
#
# Ce que fait le script (aucune autre modification du cluster) :
#   * crée dans le namespace edot deux pods ÉPHÉMÈRES de diagnostic
#     (edot-netdiag : image EDOT ; edot-kafkadiag : image officielle Apache
#     Kafka, épinglée par digest), non privilégiés, compatibles restricted-v2,
#     portant le label app.kubernetes.io/instance=edot-collector : ils sont
#     soumis aux MÊMES NetworkPolicies et à la même EgressIP qu'EDOT, mais ne
#     sont pas sélectionnés par le Service ;
#   * injecte les Secrets edot-kafka-credentials / edot-elasticsearch-credentials
#     dans les pods (secretKeyRef) : identifiants jamais lus par le script, jamais affichés ;
#   * supprime les deux pods à la fin (même en cas d'erreur).
#
# Prérequis : oc ou kubectl connecté au cluster cible, droits create/delete
# pods et pods/exec dans edot, Secrets déjà synchronisés (preflight.sh --live).
set -euo pipefail

ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
D="$ROOT/infra/edot-collector"
NS=edot
WHAT="${1:-all}"
EDOT_IMAGE="docker.elastic.co/elastic-agent/elastic-otel-collector@sha256:9caf6481865156cc1ae52992be3b0596719a849501d677e36f38312e8c8ee0f1"
KAFKA_IMAGE="apache/kafka@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7"  # 4.3.2
CLI="$(command -v oc || command -v kubectl)" || { echo "oc / kubectl introuvable"; exit 2; }

red()  { printf '\033[31m%s\033[0m\n' "$*"; }
bold() { printf '\033[1m%s\033[0m\n' "$*"; }

# ------------------------------------------------------- paramètres (Git)
eval "$(python3 - "$D" <<'EOF'
import shlex, sys, yaml
d = sys.argv[1]
k = yaml.safe_load(open(f"{d}/values-kafka.yaml"))["config"]["receivers"]["kafka"]
e = yaml.safe_load(open(f"{d}/values-elasticsearch.yaml"))["config"]
es = e["exporters"]["elasticsearch"]
stmts = " ".join(s for g in e["processors"]["transform/routing"]["log_statements"] for s in g["statements"])
import re
ds = re.search(r'data_stream\.dataset"\], "([^"]+)"', stmts).group(1)
dn = re.search(r'data_stream\.namespace"\], "([^"]+)"', stmts).group(1)
vals = {"BROKERS": " ".join(k["brokers"]), "TOPICS": " ".join(k["logs"]["topics"]), "GROUP": k["group_id"],
        "MECHANISM": k["auth"]["sasl"]["mechanism"], "ES_ENDPOINTS": " ".join(es["endpoints"]),
        "DATA_STREAM": f"logs-{ds}.otel-{dn}"}
for key, v in vals.items():
    print(f"{key}={shlex.quote(v)}")
EOF
)"
if grep -q TODO_ <<<"$BROKERS $TOPICS $GROUP $MECHANISM $ES_ENDPOINTS $DATA_STREAM"; then
  red "Des paramètres sont encore à TODO (values-kafka.yaml / values-elasticsearch.yaml) :"
  red "  aucun test n'est lancé. Renseigner TODO.md, puis relancer."
  exit 1
fi

# ------------------------------------------------------- pods éphémères
# Les Secrets sont lus par les PODS (secretKeyRef), jamais par ce script.
openshift=false
"$CLI" api-resources -o name 2>/dev/null | grep -q '^securitycontextconstraints' && openshift=true
pod_manifest() {  # nom image secret montage
  python3 - "$1" "$2" "$3" "$4" "$openshift" <<'EOF'
import json, sys
name, image, secret, mount, openshift = sys.argv[1:6]
psc = {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}}
if openshift != "true":
    psc.update(runAsUser=1000, runAsGroup=1000)
env = [{"name": "LOG_DIR", "value": "/tmp"}]
if secret == "edot-kafka-credentials":
    env += [{"name": n, "valueFrom": {"secretKeyRef": {"name": secret, "key": k}}} for n, k in (("KAFKA_USERNAME", "username"), ("KAFKA_PASSWORD", "password"))]
else:
    env += [{"name": "ELASTICSEARCH_API_KEY", "valueFrom": {"secretKeyRef": {"name": secret, "key": "api-key"}}}]
print(json.dumps({"apiVersion": "v1", "kind": "Pod",
  "metadata": {"name": name, "labels": {"app.kubernetes.io/instance": "edot-collector", "edot.diagnostic": "true"}},
  "spec": {"restartPolicy": "Never", "automountServiceAccountToken": False, "securityContext": psc,
    "activeDeadlineSeconds": 900,
    "containers": [{"name": "diag", "image": image, "command": ["sleep", "900"], "env": env,
      "resources": {"requests": {"cpu": "50m", "memory": "256Mi"}, "limits": {"cpu": "500m", "memory": "768Mi"}},
      "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
      "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}, {"name": "kafka", "mountPath": "/etc/edot/kafka", "readOnly": True},
                       {"name": "es", "mountPath": "/etc/edot/elasticsearch", "readOnly": True}]}],
    "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "64Mi"}},
      {"name": "kafka", "secret": {"secretName": "edot-kafka-credentials", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
      {"name": "es", "secret": {"secretName": "edot-elasticsearch-credentials", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}]}}))
EOF
}
cleanup() { "$CLI" -n "$NS" delete pod edot-netdiag edot-kafkadiag --ignore-not-found --wait=false >/dev/null 2>&1 || true; }
trap cleanup EXIT

bold "==> Pods de diagnostic éphémères dans $NS ($("$CLI" config current-context 2>/dev/null))"
pod_manifest edot-netdiag "$EDOT_IMAGE" edot-elasticsearch-credentials x | "$CLI" -n "$NS" apply -f - >/dev/null
[[ "$WHAT" != elasticsearch ]] && pod_manifest edot-kafkadiag "$KAFKA_IMAGE" edot-kafka-credentials x | "$CLI" -n "$NS" apply -f - >/dev/null
"$CLI" -n "$NS" wait --for=condition=Ready pod/edot-netdiag --timeout=180s >/dev/null
[[ "$WHAT" != elasticsearch ]] && "$CLI" -n "$NS" wait --for=condition=Ready pod/edot-kafkadiag --timeout=180s >/dev/null
node="$("$CLI" -n "$NS" get pod edot-netdiag -o jsonpath='{.spec.nodeName} {.status.hostIP}')"
echo "    nœud (IP) : $node — IP de sortie vue par les pare-feu : EgressIP du namespace si définie, sinon IP du nœud (README §7)"

# Contrôles bas niveau d'une adresse host:port depuis le pod EDOT.
probe() {  # host port cafile
  "$CLI" -n "$NS" exec edot-netdiag -- bash -c '
    h="$1"; p="$2"; ca="$3"
    if ips="$(getent ahosts "$h" | awk "{print \$1}" | sort -u | tr "\n" " ")" && [ -n "$ips" ]; then
      echo "      ✓ DNS  $h → $ips"; else echo "      ✗ DNS  $h : non résolu"; exit 1; fi
    if timeout 5 bash -c "</dev/tcp/$h/$p" 2>/dev/null; then echo "      ✓ TCP  $h:$p"; else
      echo "      ✗ TCP  $h:$p : refusé ou filtré (pare-feu, NetworkPolicy allow-egress-*, EgressIP)"; exit 1; fi
    out="$(timeout 10 openssl s_client -connect "$h:$p" -servername "$h" -CAfile "$ca" -verify_hostname "$h" \
            -verify_return_error -showcerts </dev/null 2>&1)" || true
    if grep -q "Verify return code: 0 (ok)" <<<"$out"; then
      chain="$(grep -c "BEGIN CERTIFICATE" <<<"$out")"
      end="$(openssl x509 -noout -enddate <<<"$out" 2>/dev/null | cut -d= -f2)"
      echo "      ✓ TLS  certificat valide pour $h (chaîne : $chain certificat(s), expire le $end)"
    else
      echo "      ✗ TLS  $(grep -m1 -E "Verify return code|verify error|error" <<<"$out")"; exit 1
    fi' _ "$1" "$2" "$3"
}

rc=0
# ---------------------------------------------------------------- Kafka
if [[ "$WHAT" == kafka || "$WHAT" == all ]]; then
  bold "==> OpenShift → Kafka : brokers de bootstrap"
  for b in $BROKERS; do
    echo "    $b"; probe "${b%:*}" "${b##*:}" /etc/edot/kafka/ca.crt || rc=1
  done

  bold "==> OpenShift → Kafka : authentification SASL ($MECHANISM) et advertised.listeners"
  out="$("$CLI" -n "$NS" exec edot-kafkadiag -- bash -c '
    case "$1" in SCRAM-*) mod=org.apache.kafka.common.security.scram.ScramLoginModule ;;
                 *)       mod=org.apache.kafka.common.security.plain.PlainLoginModule ;; esac
    umask 077
    { echo "security.protocol=SASL_SSL"; echo "sasl.mechanism=$1"
      echo "sasl.jaas.config=$mod required username=\"$KAFKA_USERNAME\" password=\"$KAFKA_PASSWORD\";"
      echo "ssl.truststore.type=PEM"; echo "ssl.truststore.location=/etc/edot/kafka/ca.crt"
      echo "ssl.endpoint.identification.algorithm=https"; } > /tmp/client.properties
    timeout 60 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$2" --command-config /tmp/client.properties 2>&1 \
      | grep -E "^[^ ].*\(id: [0-9]+|Exception|ERROR" | sed "s/ -> .*//" | sort -u' _ "$MECHANISM" "${BROKERS// /,}")" || true
  if grep -q "(id:" <<<"$out"; then
    echo "      ✓ authentification réussie ; brokers ANNONCÉS (advertised.listeners) :"
    sed 's/^/        /' <<<"$(grep "(id:" <<<"$out")"
  else
    echo "      ✗ échec : $(grep -m1 -E "Exception|ERROR" <<<"$out")"; rc=1
  fi
  bold "==> OpenShift → Kafka : chaque broker annoncé (le client s'y connecte après le bootstrap)"
  for a in $(grep -oE "^[^ ]+:[0-9]+" <<<"$out" | sort -u); do
    echo "    $a"; probe "${a%:*}" "${a##*:}" /etc/edot/kafka/ca.crt || rc=1
  done

  bold "==> Kafka : ACL du compte EDOT (Describe des topics, groupe $GROUP)"
  # kafka-get-offsets (ListOffsets) ne demande que Describe : kafka-topics
  # --describe exigerait en plus DescribeConfigs, inutile à EDOT (constaté
  # avec Kafka 4.3).
  for t in $TOPICS; do
    [[ "$t" == ^* ]] && { echo "      - $t : motif d'expression régulière, non vérifiable ici"; continue; }
    parts="$("$CLI" -n "$NS" exec edot-kafkadiag -- bash -c 'timeout 60 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$1" \
         --command-config /tmp/client.properties --topic "$2" 2>&1' _ "${BROKERS// /,}" "$t")" || true
    n="$(grep -cE "^$t:[0-9]+:" <<<"$parts" || true)"
    if [[ "$n" -gt 0 ]]; then
      echo "      ✓ topic $t accessible : $n partition(s) — au-delà de $n réplicas, EDOT aurait des réplicas inactifs"
    else
      echo "      ✗ topic $t : $(tail -1 <<<"$parts") (topic absent ou ACL Describe manquante)"; rc=1
    fi
  done
  "$CLI" -n "$NS" exec edot-kafkadiag -- bash -c 'timeout 60 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$1" \
     --command-config /tmp/client.properties --describe --group "$2" 2>&1 | grep -vE "^\s*$" | head -20' _ "${BROKERS// /,}" "$GROUP" \
     | sed 's/^/      /' || true
fi

# -------------------------------------------------------- Elasticsearch
if [[ "$WHAT" == elasticsearch || "$WHAT" == all ]]; then
  bold "==> OpenShift → Elasticsearch"
  for u in $ES_ENDPOINTS; do
    hp="${u#https://}"; hp="${hp%%/*}"
    echo "    $u"; probe "${hp%:*}" "${hp##*:}" /etc/edot/elasticsearch/ca.crt || { rc=1; continue; }
    # API Key injectée dans le pod par le Secret (comme pour EDOT) : elle ne
    # transite jamais par le poste de l'opérateur et n'est jamais affichée.
    "$CLI" -n "$NS" exec edot-netdiag -- bash -c '
        u="$1"; ds="$2"; ca=/etc/edot/elasticsearch/ca.crt
        hdr=(-H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" -H "Content-Type: application/json")
        code="$(curl -s -o /tmp/root.json -w "%{http_code}" --cacert "$ca" "${hdr[@]}" "$u/")"
        case "$code" in
          200) echo "      ✓ HTTPS  $u/ → 200, version $(sed -n "s/.*\"number\" *: *\"\([^\"]*\)\".*/\1/p" /tmp/root.json | head -1)";;
          401) echo "      ✗ HTTPS  401 : API Key invalide, révoquée ou expirée"; exit 1;;
          403) echo "      ✗ HTTPS  403 : privilège cluster « monitor » manquant (ou refus d’un proxy)"; exit 1;;
          *)   echo "      ✗ HTTPS  code $code (proxy, load balancer ?)"; exit 1;;
        esac
        who="$(curl -s --cacert "$ca" "${hdr[@]}" "$u/_security/_authenticate")"
        type="$(grep -o "\"authentication_type\":\"[^\"]*\"" <<<"$who" | cut -d\" -f4)"
        name="$(grep -o "\"api_key\":{[^}]*}" <<<"$who" | grep -o "\"name\":\"[^\"]*\"" | cut -d\" -f4)"
        echo "      ✓ authentifié (type ${type:-?}, clé ${name:-?})"
        priv="$(curl -s --cacert "$ca" "${hdr[@]}" -X POST "$u/_security/user/_has_privileges" \
          -d "{\"cluster\":[\"monitor\"],\"index\":[{\"names\":[\"$ds\"],\"privileges\":[\"create_doc\",\"auto_configure\"]}]}")"
        if grep -q "\"has_all_requested\":true" <<<"$priv"; then
          echo "      ✓ privilèges monitor + create_doc/auto_configure sur $ds"
        else
          echo "      ✗ privilèges insuffisants sur $ds : $priv"; exit 1
        fi' _ "$u" "$DATA_STREAM" || rc=1
  done
fi

echo
[[ $rc -eq 0 ]] && bold "==> Diagnostic réseau : OK" || red "==> Diagnostic réseau : ÉCHECS ci-dessus (README §18)."
exit $rc
