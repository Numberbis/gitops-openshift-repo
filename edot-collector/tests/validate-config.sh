#!/usr/bin/env bash
#
# Validation STATIQUE de l'EDOT Collector AVANT merge, sans accès au cluster
# ni aux infrastructures Kafka / Elasticsearch :
#
#   1. image : le tag épinglé pointe toujours vers le digest vérifié ;
#   2. helm lint + helm template du chart, exactement comme Argo CD (chart,
#      version et valueFiles lus dans apps/40-edot-collector-app.yaml) ;
#   3. contrôles des manifests rendus : StatefulSet, PVC, Secrets référencés,
#      ServiceMonitor, sécurité OpenShift (pas d'UID fixe, pas de privilège,
#      pas de hostPath / hostNetwork, aucun RBAC, aucun jeton d'API), aucune
#      exposition (Ingress, Route, HTTPRoute, Service non ClusterIP) ;
#   4. contrôles de la configuration du Collector : ordre du pipeline, TLS
#      vérifié, otlp_json, file persistante, backpressure, mapping otel,
#      liste des clés sensibles identique aux deux endroits ;
#   5. `elastic-agent otel validate` (binaire EDOT officiel de l'image) :
#        a. configuration de DÉPLOIEMENT (avec ses TODO) → DOIT être REFUSÉE :
#           preuve qu'une activation accidentelle échoue ;
#        b. même configuration + tests/values-test.yaml (valeurs fictives) →
#           DOIT être ACCEPTÉE : composants, options, OTTL, extensions ;
#   6. rendu Kustomize du répertoire compagnon (ExternalSecrets, alertes,
#      dashboard, NetworkPolicies).
#
# Une configuration YAML valide peut être refusée par le Collector : seule
# l'étape 5 le voit. Un Collector valide peut échouer à se connecter : c'est
# l'objet de tests/functional/ (banc local) et de tests/network-diagnostics.sh.
#
# Usage :
#   edot-collector/tests/validate-config.sh              # validation
#   edot-collector/tests/validate-config.sh --self-test  # + vérifie que
#                                       des configurations cassées sont rejetées
#
# Prérequis : helm, kubectl (kustomize), docker, python3 (+ PyYAML).

set -euo pipefail

# Doivent correspondre à image.tag de edot-collector/values.yaml.
EDOT_IMAGE="docker.elastic.co/elastic-agent/elastic-otel-collector"
EDOT_VERSION="9.5.6"
EDOT_DIGEST="sha256:9caf6481865156cc1ae52992be3b0596719a849501d677e36f38312e8c8ee0f1"
# Collector OpenTelemetry embarqué (NOTICE.txt de l'image) : documentation de référence.
OTELCOL_VERSION="v0.163.0"

SELF_TEST=false
[[ "${1:-}" == "--self-test" ]] && SELF_TEST=true

ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
EDOT_DIR="$ROOT/edot-collector"
WORK="$(mktemp -d)"
chmod 755 "$WORK"
trap 'rm -rf "$WORK"' EXIT
HELM="${HELM:-helm}"

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }

for bin in "$HELM" kubectl docker python3; do
  command -v "$bin" >/dev/null || { red "Commande introuvable : $bin"; exit 2; }
done

# ------------------------------------------------------------- 1. image
bold "==> Image EDOT $EDOT_IMAGE:$EDOT_VERSION"
docker pull -q "$EDOT_IMAGE:$EDOT_VERSION" >/dev/null
if ! docker image inspect "$EDOT_IMAGE:$EDOT_VERSION" --format '{{join .RepoDigests "\n"}}' | grep -qx "$EDOT_IMAGE@$EDOT_DIGEST"; then
  red "    ✗ le tag $EDOT_VERSION ne pointe plus vers $EDOT_DIGEST (tag déplacé ?)"
  exit 1
fi
version="$(docker run --rm "$EDOT_IMAGE@$EDOT_DIGEST" components 2>/dev/null | awk '/^ *version:/ {print $2; exit}')"
[[ "$version" == "$EDOT_VERSION" ]] || { red "    ✗ buildinfo $version ≠ $EDOT_VERSION"; exit 1; }
echo "    ✓ digest $EDOT_DIGEST, buildinfo $version (Collector $OTELCOL_VERSION)"
if command -v cosign >/dev/null; then
  curl -sSfL https://artifacts.elastic.co/cosign.pub -o "$WORK/cosign.pub"
  cosign verify --key "$WORK/cosign.pub" "$EDOT_IMAGE@$EDOT_DIGEST" >/dev/null 2>&1 \
    && echo "    ✓ signature cosign Elastic vérifiée" \
    || { red "    ✗ signature cosign invalide"; exit 1; }
else
  echo "    - cosign absent : signature non vérifiée (cosign verify --key https://artifacts.elastic.co/cosign.pub ...)"
fi

# ------------------------------------------------------ 2. rendu du chart
bold "==> helm lint et helm template (comme Argo CD)"
python3 - "$ROOT" "$WORK" "$HELM" <<'EOF'
import subprocess, sys, yaml
root, work, helm = sys.argv[1:4]
spec = yaml.safe_load(open(f"{root}/apps/40-edot-collector-app.yaml"))["spec"]
src = next(s for s in spec["sources"] if "chart" in s)
values = [v.replace("$values/", root + "/") for v in src["helm"]["valueFiles"]]
open(f"{work}/values-files", "w").write("\n".join(values))
pull = subprocess.run([helm, "pull", src["chart"], "--repo", src["repoURL"], "--version", src["targetRevision"],
                       "--untar", "--untardir", f"{work}/chart"], capture_output=True, text=True)
if pull.returncode:
    sys.exit(f"    ÉCHEC du téléchargement du chart : {pull.stderr.strip()}")
chart = f"{work}/chart/{src['chart']}"
args = [x for v in values for x in ("-f", v)]
lint = subprocess.run([helm, "lint", chart, *args], capture_output=True, text=True)
if lint.returncode:
    sys.exit(f"    ÉCHEC de helm lint :\n{lint.stdout}{lint.stderr}")
base = [helm, "template", src["helm"]["releaseName"], chart, "--namespace", spec["destination"]["namespace"],
        "--kube-version", "1.35.8", "--api-versions", "monitoring.coreos.com/v1"]
for name, extra in (("deploy", []), ("test", ["-f", f"{root}/edot-collector/tests/values-test.yaml"]),
                    ("kubernetes", [x for v in values for x in ("-f", v.replace("values-openshift.yaml", "values-kubernetes.yaml"))])):
    cmd = base + (extra if name == "kubernetes" else args + extra)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode:
        sys.exit(f"    ÉCHEC du rendu ({name}) : {proc.stderr.strip()}")
    open(f"{work}/rendered-{name}.yaml", "w").write(proc.stdout)
    cm = next(d for d in yaml.safe_load_all(proc.stdout) if d and d["kind"] == "ConfigMap")
    open(f"{work}/relay-{name}.yaml", "w").write(cm["data"]["relay"])
print(f"    ✓ chart {src['chart']} {src['targetRevision']}, helm lint OK, {len(values)} fichiers de values")
EOF

# ---------------------------------------- 3. et 4. contrôles du dépôt
bold "==> Contrôles des manifests rendus et de la configuration"
python3 - "$ROOT" "$WORK" "$EDOT_IMAGE" "$EDOT_VERSION" <<'EOF'
import re, sys, yaml
root, work, image, version = sys.argv[1:5]
errors, ok = [], []
def check(cond, good, bad):
    (ok if cond else errors).append(good if cond else bad)

docs = [d for d in yaml.safe_load_all(open(f"{work}/rendered-deploy.yaml")) if d]
kinds = sorted(d["kind"] for d in docs)
check(kinds == ["ConfigMap", "PodDisruptionBudget", "Service", "ServiceAccount", "ServiceMonitor", "StatefulSet"],
      f"objets rendus : {', '.join(kinds)}", f"objets rendus inattendus : {kinds}")
by = {d["kind"]: d for d in docs}
sts = by["StatefulSet"]
pod = sts["spec"]["template"]["spec"]
c = pod["containers"][0]
check(sts["spec"]["replicas"] == 2, "StatefulSet : 2 réplicas", f"replicas={sts['spec']['replicas']}")
check(c["image"] == f"{image}:{version}", f"image {c['image']}", f"image {c['image']} ≠ {image}:{version}")
check(c["command"] == ["/usr/share/elastic-agent/otelcol"] and c["args"] == ["--config=/conf/relay.yaml"],
      "commande EDOT : /usr/share/elastic-agent/otelcol --config=/conf/relay.yaml", f"commande {c.get('command')} {c.get('args')}")
vct = sts["spec"].get("volumeClaimTemplates", [])
check(len(vct) == 1 and vct[0]["metadata"]["name"] == "queue" and vct[0]["spec"]["resources"]["requests"]["storage"] == "10Gi",
      "volumeClaimTemplates : PVC « queue » 10Gi par réplica", f"volumeClaimTemplates inattendus : {vct}")
pol = sts["spec"].get("persistentVolumeClaimRetentionPolicy", {})
check(pol == {"whenDeleted": "Retain", "whenScaled": "Retain"}, "PVC conservés (Retain / Retain)", f"rétention des PVC : {pol}")
check(any(m["mountPath"] == "/var/lib/edot" and m["name"] == "queue" for m in c["volumeMounts"]),
      "PVC monté sur /var/lib/edot", "PVC queue non monté sur /var/lib/edot")
# Sécurité
psc, sc = pod.get("securityContext", {}), c.get("securityContext", {})
check(not any(k in psc or k in sc for k in ("runAsUser", "runAsGroup", "fsGroup")),
      "profil OpenShift : aucun UID/GID/fsGroup fixe (attribués par la SCC)", "UID/GID fixe dans le profil OpenShift")
check(psc.get("runAsNonRoot") is True and psc.get("seccompProfile", {}).get("type") == "RuntimeDefault",
      "runAsNonRoot, seccomp RuntimeDefault", f"podSecurityContext {psc}")
check(sc.get("allowPrivilegeEscalation") is False and sc.get("readOnlyRootFilesystem") is True
      and sc.get("capabilities", {}).get("drop") == ["ALL"] and not sc.get("privileged") and not sc.get("capabilities", {}).get("add"),
      "conteneur : non privilégié, rootfs en lecture seule, capabilities drop ALL", f"securityContext {sc}")
check(not pod.get("hostNetwork") and not pod.get("hostPID") and not any("hostPath" in v for v in pod.get("volumes", [])),
      "ni hostNetwork, ni hostPID, ni hostPath", "hostNetwork / hostPID / hostPath présent")
check(pod.get("automountServiceAccountToken") is False and by["ServiceAccount"].get("automountServiceAccountToken") is not True,
      "aucun jeton d'API Kubernetes monté", "jeton de ServiceAccount monté")
check(all(c.get(p) for p in ("startupProbe", "livenessProbe", "readinessProbe")), "probes startup, liveness, readiness", "probe manquante")
res = c.get("resources", {})
check(res.get("requests") == {"cpu": "500m", "memory": "1Gi"} and res.get("limits") == {"cpu": "2", "memory": "3Gi"},
      "ressources 500m/1Gi → 2/3Gi", f"ressources {res}")
check(pod.get("terminationGracePeriodSeconds", 0) >= 60, "terminationGracePeriodSeconds ≥ 60", "arrêt trop court")
check(by["PodDisruptionBudget"]["spec"].get("maxUnavailable") == 1, "PodDisruptionBudget maxUnavailable 1", "PDB inattendu")
# Exposition
svc = by["Service"]
check(svc["spec"].get("type", "ClusterIP") == "ClusterIP" and [p["port"] for p in svc["spec"]["ports"]] == [8888],
      "Service ClusterIP, port 8888 (métriques) uniquement", f"Service {svc['spec'].get('type')} ports {svc['spec']['ports']}")
check(by["ServiceMonitor"]["metadata"]["labels"].get("release") == "kube-prometheus-stack", "ServiceMonitor découvert (release)", "label release absent")
# Secrets référencés
env = {e["name"]: e for e in c.get("env", [])}
refs = {(e["valueFrom"]["secretKeyRef"]["name"], e["valueFrom"]["secretKeyRef"]["key"]) for e in env.values()
        if "secretKeyRef" in e.get("valueFrom", {})}
refs |= {(v["secret"]["secretName"], i["key"]) for v in pod["volumes"] if "secret" in v for i in v["secret"].get("items", [])}
check(refs == {("edot-kafka-credentials", "ca.crt"), ("edot-kafka-credentials", "tls.crt"), ("edot-kafka-credentials", "tls.key"),
               ("edot-elasticsearch-credentials", "api-key"), ("edot-elasticsearch-credentials", "ca.crt")},
      f"Secrets référencés : {len(refs)} clés (edot-kafka-credentials, edot-elasticsearch-credentials)", f"références de Secrets : {sorted(refs)}")
open(f"{work}/secret-refs", "w").write("\n".join(f"{n} {k}" for n, k in sorted(refs)))

# Configuration du Collector
relay = open(f"{work}/relay-deploy.yaml").read()
cfg = yaml.safe_load(relay)
for var in set(re.findall(r"\$\{env:([A-Z0-9_]+)\}", relay)):
    check(var in env, f"variable {var} fournie au conteneur", f"variable {var} utilisée mais non définie")
pipes = cfg["service"]["pipelines"]
check(set(pipes) == {"logs"}, "un seul pipeline : logs", f"pipelines {sorted(pipes)}")
p = pipes["logs"]
procs = p["processors"]
check(p["receivers"] == ["kafka"] and p["exporters"] == ["elasticsearch"], "kafka → elasticsearch", f"pipeline {p}")
check(procs[0] == "memory_limiter", "memory_limiter en premier", f"processors {procs}")
check(max(procs.index(x) for x in procs if x.startswith("filter/")) < procs.index("transform/redact") < procs.index("transform/routing") == len(procs) - 1,
      f"ordre : {' → '.join(procs)}", f"ordre des processors incorrect : {procs}")
check("debug" not in cfg.get("exporters", {}), "pas de debug exporter", "debug exporter actif")
k = cfg["receivers"]["kafka"]
check(k["logs"]["encoding"] == "otlp_json", "encodage otlp_json", "encodage ≠ otlp_json")
check(k["message_marking"] == {"after": True, "on_error": False, "on_permanent_error": True},
      "offsets marqués après traitement, erreurs transitoires retentées", f"message_marking {k['message_marking']}")
es = cfg["exporters"]["elasticsearch"]
for name, tls in (("Kafka", k.get("tls", {})), ("Elasticsearch", es.get("tls", {}))):
    check(tls.get("insecure") is False and tls.get("insecure_skip_verify") is False and tls.get("ca_file"),
          f"TLS {name} : certificat vérifié avec CA dédiée", f"TLS {name} non vérifié : {tls}")
check(all(u.startswith("https://") for u in es["endpoints"]), "Elasticsearch en HTTPS", "endpoint non HTTPS")
check(not any(re.search(r"svc(\.cluster\.local)?(:|$)", u) for u in es["endpoints"] + k["brokers"]),
      "aucune adresse de Service Kubernetes pour Kafka/Elasticsearch", "adresse de Service Kubernetes utilisée")
check(es["api_key"] == "${env:ELASTICSEARCH_API_KEY}",
      "API Key Elasticsearch injectée par variable (aucune valeur en clair)", "identifiant en clair dans la configuration")
# Kafka : TLS mutuel (security.protocol=SSL). Certificat client et clé lus
# depuis le volume du Secret ; aucun SASL (ni mot de passe, ni mécanisme).
kt = k.get("tls", {})
check("auth" not in k and kt.get("cert_file") == "/etc/edot/kafka/tls.crt" and kt.get("key_file") == "/etc/edot/kafka/tls.key",
      "Kafka en mTLS : certificat client /etc/edot/kafka/tls.{crt,key}, aucun SASL", f"Kafka : mTLS attendu, auth={k.get('auth')} tls={kt}")
kvol = next((v for v in pod["volumes"] if v.get("secret", {}).get("secretName") == "edot-kafka-credentials"), {})
check(kvol.get("secret", {}).get("defaultMode") == 0o440 and
      any(m["name"] == kvol.get("name") and m["mountPath"] == "/etc/edot/kafka" and m.get("readOnly") for m in c["volumeMounts"]),
      "clé privée Kafka montée en lecture seule, mode 0440 (lisible via le fsGroup de la SCC)",
      f"volume du certificat client Kafka inattendu : {kvol}")
q = es["sending_queue"]
check(q.get("storage") == "file_storage/queue" and "file_storage/queue" in cfg["service"]["extensions"] and q.get("block_on_overflow") is True,
      "file persistante file_storage/queue, block_on_overflow", f"sending_queue {q}")
check(es["mapping"]["mode"] == "otel" and es["discover"] == {"on_start": False, "interval": "0s"},
      "mapping otel, découverte des nœuds désactivée", "mapping ou discover inattendu")
check(cfg["extensions"]["file_storage/queue"]["directory"].startswith("/var/lib/edot/"), "file sur le PVC", "file hors PVC")
# Liste des clés sensibles : une seule expression, recopiée pour le Body.
regs = set(re.findall(r'delete_matching_keys\((?:attributes|body), "([^"]+)"\)', relay))
check(len(regs) == 1, "liste des clés sensibles identique (attributs et Body)", f"listes de clés sensibles divergentes : {regs}")

# Profil Kubernetes : mêmes objets, UID fixe.
kdocs = {d["kind"]: d for d in yaml.safe_load_all(open(f"{work}/rendered-kubernetes.yaml")) if d}
kpsc = kdocs["StatefulSet"]["spec"]["template"]["spec"]["securityContext"]
check(kpsc.get("runAsUser") == 10001 and kpsc.get("fsGroup") == 10001, "profil Kubernetes : UID 10001 / fsGroup 10001", f"profil Kubernetes {kpsc}")

# Les valeurs de test ne sont référencées par aucune Application.
import glob
apps = "".join(open(f).read() for f in glob.glob(f"{root}/apps/**/*.yaml", recursive=True))
check("values-test" not in apps, "tests/values-test.yaml référencé par aucune Application", "values-test.yaml référencé par une Application")
# Aucune synchronisation automatique.
app = yaml.safe_load(open(f"{root}/apps/40-edot-collector-app.yaml"))
check("automated" not in app["spec"].get("syncPolicy", {}), "Application en synchronisation MANUELLE", "syncPolicy.automated présent")

for m in ok:
    print(f"    ✓ {m}")
for e in errors:
    print(f"    \033[31m✗ {e}\033[0m")
sys.exit(1 if errors else 0)
EOF

# ------------------------------------------------ 5. validation EDOT
otel_env=(-e MY_POD_IP=127.0.0.1 -e OTEL_K8S_POD_NAME=edot-collector-0 -e OTEL_K8S_NAMESPACE=edot
          -e ELASTICSEARCH_API_KEY=ci)
validate_edot() {
  docker run --rm --network none -v "$1:/conf/relay.yaml:ro" "${otel_env[@]}" \
    "$EDOT_IMAGE@$EDOT_DIGEST" validate --config=file:/conf/relay.yaml
}

bold "==> EDOT : elastic-agent otel validate"
if out="$(validate_edot "$WORK/relay-deploy.yaml" 2>&1)"; then
  red "    ✗ la configuration de DÉPLOIEMENT est acceptée alors qu'elle contient des TODO :"
  red "      une activation accidentelle ne serait plus bloquée (ou les TODO ont été renseignés :"
  red "      lancer alors tests/preflight.sh et adapter cette étape)."
  exit 1
fi
todo_hits="$(grep -o 'TODO_[A-Z0-9_]*' <<<"$out" | sort -u | tr '\n' ' ')"
echo "    ✓ configuration de déploiement REFUSÉE tant que des TODO subsistent (${todo_hits:-TODO})"
validate_edot "$WORK/relay-test.yaml"
green "    ✓ configuration + valeurs de test : VALIDE (composants, options, OTTL, extensions)"

# ------------------------------------------------ 6. répertoire compagnon
bold "==> kubectl kustomize edot-collector"
kubectl kustomize "$EDOT_DIR" > "$WORK/companion.yaml"
python3 - "$WORK" <<'EOF'
import json, sys, yaml
work = sys.argv[1]
docs = [d for d in yaml.safe_load_all(open(f"{work}/companion.yaml")) if d]
kinds = sorted(f"{d['kind']}/{d['metadata']['name']}" for d in docs)
errors = []
targets = {d["spec"]["target"]["name"]: [x["secretKey"] for x in d["spec"]["data"]] for d in docs if d["kind"] == "ExternalSecret"}
for line in open(f"{work}/secret-refs").read().splitlines():
    name, key = line.split()
    if key not in targets.get(name, []):
        errors.append(f"Secret {name}/{key} référencé par le pod mais produit par aucun ExternalSecret")
cm = next(d for d in docs if d["kind"] == "ConfigMap")
json.loads(cm["data"]["edot-collector.json"])
if cm["metadata"]["labels"].get("grafana_dashboard") != "1" or cm["metadata"]["namespace"] != "monitoring":
    errors.append("dashboard : label grafana_dashboard ou namespace monitoring manquant")
rules = [r for d in docs if d["kind"] == "PrometheusRule" for g in d["spec"]["groups"] for r in g["rules"]]
import re
if any(re.search(r"otelcol_\w+_total", r["expr"]) for r in rules):
    errors.append("PrometheusRule : suffixe _total (les compteurs EDOT 9.5.6 n'en ont pas)")
for d in docs:
    if d["kind"] in ("NetworkPolicy",) and d["metadata"]["namespace"] != "edot":
        errors.append(f"NetworkPolicy {d['metadata']['name']} hors namespace edot")
for e in errors:
    print(f"    \033[31m✗ {e}\033[0m")
if errors:
    sys.exit(1)
print(f"    ✓ {len(docs)} objets : " + ", ".join(kinds))
print(f"    ✓ chaque Secret référencé par le pod est produit par un ExternalSecret ; {len(rules)} alertes ; dashboard JSON valide")
EOF

# ------------------------------------------------------------ self-test
if $SELF_TEST; then
  bold "==> Self-test : des configurations cassées doivent être rejetées"
  for pat in 's/IsMatch(log.attributes\["http.route"\]/IsMatchz(log.attributes["http.route"]/' \
             's/delete_key(body\["user"\], "email")/delete_key(body["user"] "email")/' \
             's/flush_timeout: 5s/flush_timeoutt: 5s/' \
             's/block_on_overflow: true/block_on_overflow: peut-etre/'; do
    # LIMITE CONSTATÉE (EDOT 9.5.6) : un encodage Kafka inconnu
    # (encoding: otlp_jsonn) n'est PAS détecté par `validate`, seulement au
    # démarrage. D'où le contrôle explicite « encodage otlp_json » de l'étape 4.
    sed "$pat" "$WORK/relay-test.yaml" > "$WORK/relay-bad.yaml"
    cmp -s "$WORK/relay-bad.yaml" "$WORK/relay-test.yaml" && { red "    ✗ motif non appliqué : $pat"; exit 1; }
    if validate_edot "$WORK/relay-bad.yaml" >/dev/null 2>&1; then
      red "    ✗ configuration cassée NON détectée : $pat"; exit 1
    fi
    echo "    ✓ rejetée : $pat"
  done
fi

green "==> OK : configuration EDOT valide (et bloquée tant que les TODO ne sont pas renseignés)."
