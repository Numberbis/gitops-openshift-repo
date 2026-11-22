#!/usr/bin/env python3
"""Test de bout en bout Kafka → EDOT Collector → Elasticsearch (edot-integration-test).

Déroulé :
  1. préconditions : Kafka joignable PAR SON SERVICE, topic présent,
     Elasticsearch disponible, EDOT prêt ET membre du consumer group ;
  2. publication d'une campagne d'événements OTLP JSON (producer/produce_otlp.py),
     tous sur la MÊME partition (clé = identifiant de campagne), terminée par
     un événement TÉMOIN ;
  3. attente (timeout configurable) : événements attendus ET témoin indexés,
     puis lag du consumer group nul sur le topic, puis délai de stabilisation
     (> flush_timeout de l'exporter) et _refresh Elasticsearch ;
  4. assertions sur les VRAIS documents Elasticsearch de la campagne.

Une absence n'est comptée comme « filtrée » QUE si le témoin (publié après
elle, sur la même partition) est indexé et que le lag est nul : sinon le
test échoue comme « non vérifiable » (jamais de faux succès).

Accès au cluster par kubectl (ou oc) uniquement, depuis le poste :
  * Kafka : pod client éphémère (image Kafka officielle, label
    kafka-client) qui exécute les outils Kafka via le Service ; supprimé à la
    fin. Les outils ne tournent PAS dans le pod du broker (mémoire limitée) ;
  * Elasticsearch : kubectl port-forward (local).
Les compteurs internes d'EDOT (filtres) se lisent dans Prometheus (README §9) :
le port 8888 écoute sur l'IP du pod, hors de portée d'un port-forward.

Codes de sortie : 0 tout réussi, 1 au moins un test en échec, 2 préconditions
non remplies ou erreur d'exécution.
"""
from __future__ import annotations

import argparse
import base64
import json
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "producer"))
import produce_otlp  # noqa: E402

KAFKA_IMAGE = "apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7"
WIDTH = 41


# ------------------------------------------------------------------ affichage
class Report:
    def __init__(self):
        self.pre: list[tuple[str, bool, str]] = []
        self.tests: list[tuple[str, bool, str]] = []

    @staticmethod
    def _line(name, ok, detail, labels):
        status = labels[0] if ok else labels[1]
        tty = sys.stdout.isatty()
        color = ("\033[32m" if ok else "\033[31m") if tty else ""
        reset = "\033[0m" if tty else ""
        s = f"{name:<29}{color}{status}{reset}"
        print(s + (f"   ({detail})" if detail else ""), flush=True)

    def precondition(self, name, ok, detail=""):
        self.pre.append((name, ok, detail))
        self._line(name, ok, detail, ("OK", "FAIL"))
        return ok

    def test(self, name, ok, detail=""):
        self.tests.append((name, ok, detail))
        self._line(name, ok, detail, ("PASS", "FAIL"))
        return ok


def banner(title):
    print("=" * WIDTH)
    print(title.center(WIDTH).rstrip())
    print("=" * WIDTH)


def step(msg):
    print(f"  … {msg}", file=sys.stderr, flush=True)


# ------------------------------------------------------------------ cluster
class Cluster:
    def __init__(self, kubectl: str, namespace: str):
        self.k = kubectl
        self.ns = namespace

    def run(self, *args, input: str | None = None, check=True, timeout=180) -> subprocess.CompletedProcess:
        cmd = [self.k, "-n", self.ns, *args]
        r = subprocess.run(cmd, input=input, capture_output=True, text=True, timeout=timeout)
        if check and r.returncode != 0:
            raise RuntimeError(f"{' '.join(cmd[:6])}… : {r.stderr.strip()[-500:]}")
        return r

    def json(self, *args) -> dict:
        return json.loads(self.run(*args, "-o", "json").stdout)

    def is_openshift(self) -> bool:
        r = subprocess.run([self.k, "api-resources", "--api-group=security.openshift.io", "-o", "name"],
                           capture_output=True, text=True)
        return "securitycontextconstraints" in r.stdout

    def pods_ready(self, selector: str) -> tuple[int, int]:
        items = self.json("get", "pods", "-l", selector)["items"]
        ready = sum(1 for p in items if any(c["type"] == "Ready" and c["status"] == "True"
                                            for c in p.get("status", {}).get("conditions", [])))
        return ready, len(items)

    def secret_value(self, name: str, key: str) -> str:
        data = self.json("get", "secret", name)["data"]
        return base64.b64decode(data[key]).decode()


class PortForward:
    """kubectl port-forward vers un Service, sur un port local libre."""

    def __init__(self, cluster: Cluster, target: str, port: int):
        self.proc = subprocess.Popen([cluster.k, "-n", cluster.ns, "port-forward", target, f":{port}"],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        deadline = time.time() + 20
        self.local = None
        while time.time() < deadline:
            line = self.proc.stdout.readline()
            m = re.search(r"127\.0\.0\.1:(\d+)", line or "")
            if m:
                self.local = int(m.group(1))
                break
            if self.proc.poll() is not None:
                break
        if not self.local:
            self.close()
            raise RuntimeError(f"port-forward {target} impossible : {self.proc.stderr.read()[-300:]}")

    def close(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(5)
            except subprocess.TimeoutExpired:
                self.proc.kill()


class KafkaClient:
    """Pod client éphémère : outils Kafka exécutés contre le Service."""

    def __init__(self, cluster: Cluster, bootstrap: str, openshift: bool, keep: bool):
        self.c = cluster
        self.bootstrap = bootstrap
        self.keep = keep
        self.name = f"edot-test-client-{uuid.uuid4().hex[:6]}"
        sec = {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}}
        if not openshift:
            sec.update(runAsUser=10001, runAsGroup=10001)
        pod = {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": self.name, "labels": {
                "app.kubernetes.io/name": "edot-test-client",
                "app.kubernetes.io/component": "test",
                "app.kubernetes.io/part-of": "edot-integration-test",
                "edot-integration-test/environment": "test",
                "edot-integration-test/kafka-client": "true"}},
            "spec": {
                "restartPolicy": "Never",
                "enableServiceLinks": False,
                "automountServiceAccountToken": False,
                # « sleep » en PID 1 ignore SIGTERM : arrêt immédiat à la suppression.
                "terminationGracePeriodSeconds": 1,
                # Filet de sécurité : le pod s'arrête seul au bout d'une heure.
                "activeDeadlineSeconds": 3600,
                # Sur le nœud du broker si possible : image Kafka déjà présente.
                "affinity": {"podAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [{
                    "weight": 100, "podAffinityTerm": {
                        "topologyKey": "kubernetes.io/hostname",
                        "labelSelector": {"matchLabels": {"app.kubernetes.io/name": "kafka"}}}}]}},
                "securityContext": sec,
                "containers": [{
                    "name": "kafka-tools", "image": KAFKA_IMAGE, "imagePullPolicy": "IfNotPresent",
                    "command": ["sleep", "3600"],
                    "env": [{"name": "KAFKA_HEAP_OPTS", "value": "-Xms64m -Xmx192m"},
                            {"name": "LOG_DIR", "value": "/tmp/kafka-logs"}],
                    "resources": {"requests": {"cpu": "50m", "memory": "256Mi"},
                                  "limits": {"cpu": "1", "memory": "512Mi"}},
                    "securityContext": {"allowPrivilegeEscalation": False, "privileged": False,
                                        "readOnlyRootFilesystem": True, "runAsNonRoot": True,
                                        "capabilities": {"drop": ["ALL"]}},
                    "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}]}],
                "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "64Mi"}}]}}
        cluster.run("apply", "-f", "-", input=json.dumps(pod))
        cluster.run("wait", "--for=condition=Ready", f"pod/{self.name}", "--timeout=180s", timeout=200)

    def sh(self, script: str, input: str | None = None, check=True, timeout=120) -> subprocess.CompletedProcess:
        return self.c.run("exec", "-i", self.name, "--", "bash", "-c", script,
                          input=input, check=check, timeout=timeout)

    def tool(self, cmd: str, check=True, timeout=120):
        return self.sh(f"/opt/kafka/bin/{cmd} --bootstrap-server {self.bootstrap}", check=check, timeout=timeout)

    def produce(self, topic: str, lines: list[str]):
        self.sh(f"/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server {self.bootstrap} "
                f"--topic {topic} --property parse.key=true --property key.separator=$'\\t' "
                f"--producer-property acks=all", input="\n".join(lines) + "\n")

    def group_state(self, group: str, topic: str) -> tuple[int, int | None]:
        """(nombre de membres, lag total sur le topic ou None si inconnu)."""
        members = self.tool(f"kafka-consumer-groups.sh --describe --group {group} --members", check=False)
        n = 0
        for ln in members.stdout.splitlines():
            parts = ln.split()
            if parts and parts[0] == group and len(parts) >= 2 and parts[1] != "-":
                n += 1
        offs = self.tool(f"kafka-consumer-groups.sh --describe --group {group}", check=False)
        lag, known = 0, False
        for ln in offs.stdout.splitlines():
            parts = ln.split()
            # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG …
            if len(parts) >= 6 and parts[0] == group and parts[1] == topic:
                if parts[5].isdigit():
                    lag += int(parts[5])
                    known = True
                elif parts[4] == "0":
                    # Partition vide : aucun offset validé (« - ») mais rien à consommer.
                    known = True
                else:
                    # Partition non vide sans offset validé : consommation non prouvée.
                    return n, None
        return n, (lag if known else None)

    def close(self):
        if not self.keep:
            self.c.run("delete", "pod", self.name, "--wait=false", "--ignore-not-found", check=False)


# ------------------------------------------------------------------ Elasticsearch
class Elasticsearch:
    def __init__(self, port: int, password: str):
        self.base = f"http://127.0.0.1:{port}"
        self.auth = "Basic " + base64.b64encode(f"elastic:{password}".encode()).decode()

    def req(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(self.base + path, data=data, method=method,
                                   headers={"Authorization": self.auth, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(r, timeout=30) as resp:
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def campaign_docs(self, data_stream: str, campaign: str) -> list[dict]:
        self.req("POST", f"/{data_stream}/_refresh")
        code, r = self.req("POST", f"/{data_stream}/_search", {
            "size": 200, "query": {"term": {"attributes.test.campaign_id": campaign}}})
        if code == 404:
            return []
        if code != 200:
            raise RuntimeError(f"recherche Elasticsearch : HTTP {code} {str(r)[:300]}")
        return [dict(h["_source"], _index=h["_index"]) for h in r["hits"]["hits"]]


def lookup(doc, path: str):
    """(trouvé, valeur) d'un champ du _source, clés imbriquées ou « aplaties »
    avec des points (mapping otel : attributes."user.email")."""
    parts = path.split(".")

    def rec(obj, parts):
        if not parts:
            return True, obj
        if not isinstance(obj, dict):
            return False, None
        for i in range(len(parts), 0, -1):
            k = ".".join(parts[:i])
            if k in obj:
                found, v = rec(obj[k], parts[i:])
                if found:
                    return True, v
        return False, None
    return rec(doc, parts)


def by_case(docs: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for d in docs:
        out.setdefault(lookup(d, "attributes.test.case")[1], []).append(d)
    return out


# ------------------------------------------------------------------ programme
def main() -> int:
    p = argparse.ArgumentParser(description="Test de bout en bout Kafka → EDOT → Elasticsearch")
    p.add_argument("--namespace", default="edot-integration-test")
    p.add_argument("--kubectl", default=None, help="binaire kubectl ou oc (défaut : oc si OpenShift, sinon kubectl)")
    p.add_argument("--bootstrap", default="kafka.edot-integration-test.svc.cluster.local:9092")
    p.add_argument("--topic", default="otlp-logs-test")
    p.add_argument("--group", default="edot-integration-test", help="consumer group de l'EDOT de test")
    p.add_argument("--edot-instance", default="edot-collector-test", help="release / Service de l'EDOT de test")
    p.add_argument("--data-stream", default="logs-edotintegration.otel-test")
    p.add_argument("--timeout", type=int, default=180, help="attente maximale du traitement (s, défaut 180)")
    p.add_argument("--settle", type=float, default=5.0,
                   help="stabilisation après témoin + lag nul (s, défaut 5 ; > flush_timeout de 1 s)")
    p.add_argument("--campaign-id", default=None)
    p.add_argument("--keep-client", action="store_true", help="conserver le pod client Kafka")
    a = p.parse_args()

    kubectl = a.kubectl or ("oc" if shutil.which("oc") else "kubectl")
    if not shutil.which(kubectl):
        print(f"{kubectl} introuvable", file=sys.stderr)
        return 2
    cluster = Cluster(kubectl, a.namespace)
    campaign = a.campaign_id or f"e2e-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    rep = Report()
    client = es_pf = None

    print()
    banner("EDOT INTEGRATION TEST")
    print(f"namespace={a.namespace}  campagne={campaign}")
    print()
    try:
        openshift = cluster.is_openshift()
        # ---------------------------------------------------- 1. préconditions
        step("Kafka : pod du broker et pod client éphémère")
        ready, total = cluster.pods_ready("app.kubernetes.io/name=kafka")
        kafka_ok = False
        detail = f"broker {ready}/{total} prêt"
        if ready:
            client = KafkaClient(cluster, a.bootstrap, openshift, a.keep_client)
            r = client.tool("kafka-broker-api-versions.sh", check=False)
            kafka_ok = r.returncode == 0 and "id: 1" in r.stdout
            detail = f"{a.bootstrap}" if kafka_ok else f"{detail} ; {r.stderr.strip()[-200:]}"
        rep.precondition("Kafka connection", kafka_ok, detail)

        topic_ok, detail = False, ""
        if kafka_ok:
            r = client.tool(f"kafka-topics.sh --describe --topic {a.topic}", check=False)
            m = re.search(r"PartitionCount:\s*(\d+).*?ReplicationFactor:\s*(\d+)", r.stdout)
            rm = re.search(r"retention\.ms=(\d+)", r.stdout)
            topic_ok = bool(m)
            detail = (f"{a.topic} : {m.group(1)} partitions, RF {m.group(2)}"
                      + (f", rétention {int(rm.group(1)) / 3_600_000:g} h" if rm else "")) if m else r.stderr.strip()[-200:]
        rep.precondition("Kafka topic", topic_ok, detail)

        step("Elasticsearch : port-forward et santé du cluster")
        es = None
        try:
            es_pf = PortForward(cluster, "svc/elasticsearch", 9200)
            es = Elasticsearch(es_pf.local, cluster.secret_value("elasticsearch-test-credentials", "elastic-password"))
            code, h = es.req("GET", "/_cluster/health")
            _, info = es.req("GET", "/")
            version = info.get("version", {}).get("number", "?")
            es_ok = code == 200 and h.get("status") in ("green", "yellow") and h.get("number_of_nodes") == 1
            detail = f"{version}, {h.get('status')}, {h.get('number_of_nodes')} nœud" if code == 200 else f"HTTP {code}"
        except Exception as e:  # noqa: BLE001
            es_ok, detail = False, str(e)[:200]
        rep.precondition("Elasticsearch connection", es_ok, detail)

        step("EDOT : pods prêts et membre du consumer group")
        ready, total = cluster.pods_ready(f"app.kubernetes.io/instance={a.edot_instance}")
        edot_ok, detail = False, f"{ready}/{total} pod(s) prêt(s)"
        if ready and kafka_ok:
            deadline = time.time() + 90
            members = 0
            while time.time() < deadline:
                members, _ = client.group_state(a.group, a.topic)
                if members:
                    break
                time.sleep(5)
            edot_ok = members > 0
            detail += f", {members} membre(s) du groupe {a.group}"
        rep.precondition("EDOT Collector", edot_ok, detail)
        print()

        if not all(ok for _, ok, _ in rep.pre):
            banner("RESULT: PRÉCONDITIONS NON REMPLIES — TESTS NON EXÉCUTÉS")
            return 2

        # ---------------------------------------------------- 2. publication
        msgs = produce_otlp.generate(campaign, produce_otlp.ALL, 1, witness=True)
        step(f"publication de {len(msgs)} messages ({', '.join(n for n, _, _ in msgs)})")
        client.produce(a.topic, [f"{campaign}\t{line}" for _, _, line in msgs])

        # ---------------------------------------------------- 3. attente
        expected = [n for n, _, _ in msgs if produce_otlp.EXPECTED_INDEXED[n]]
        step(f"attente de l'indexation de : {', '.join(expected)} (timeout {a.timeout} s)")
        deadline = time.time() + a.timeout
        cases: dict[str, list[dict]] = {}
        while time.time() < deadline:
            cases = by_case(es.campaign_docs(a.data_stream, campaign))
            if all(c in cases for c in expected):
                break
            time.sleep(2)
        witness = "witness" in cases
        lag = None
        if witness:
            step("témoin indexé ; attente d'un lag nul du consumer group")
            while time.time() < deadline:
                _, lag = client.group_state(a.group, a.topic)
                if lag == 0:
                    break
                time.sleep(2)
        consumed = witness and lag == 0
        step(f"stabilisation {a.settle:g} s puis _refresh")
        time.sleep(a.settle)
        docs = es.campaign_docs(a.data_stream, campaign)
        cases = by_case(docs)

        print(f"Témoin indexé                {'oui' if witness else 'NON'}")
        print(f"Lag du consumer group        {lag if lag is not None else 'inconnu'}")
        print(f"Documents de la campagne     {len(docs)} dans {a.data_stream}")
        print()

        # ---------------------------------------------------- 4. assertions
        def one(case):
            d = cases.get(case, [])
            return (d[0] if len(d) == 1 else None), len(d)

        def check_fields(doc, present: dict, absent: list) -> list[str]:
            problems = []
            for f, v in present.items():
                found, got = lookup(doc, f)
                if not found or got != v:
                    problems.append(f"{f}={got!r} ≠ {v!r}")
            for f in absent:
                if lookup(doc, f)[0]:
                    problems.append(f"{f} PRÉSENT")
            return problems

        info, n = one("info")
        pb = [f"{n} document(s)"] if not info else check_fields(info, {
            "body.text": "Test OpenTelemetry", "severity_text": "INFO",
            "resource.attributes.service.name": "application-test",
            "attributes.http.route": "/api/orders"}, [])
        rep.test("INFO log indexed", not pb, "; ".join(pb))

        not_verified = "NON VÉRIFIABLE : témoin non indexé ou lag non nul"
        for case, label in (("debug", "DEBUG log filtered"), ("health", "Healthcheck filtered"),
                            ("excluded", "Excluded service filtered")):
            n = len(cases.get(case, []))
            if n:
                rep.test(label, False, f"{n} document(s) indexé(s)")
            else:
                rep.test(label, consumed, "" if consumed else not_verified)

        for field, label in (("user.email", "user.email removed"), ("client.ip", "client.ip removed"),
                             ("debug.payload", "debug.payload removed")):
            if not info:
                rep.test(label, False, "document INFO absent")
            else:
                pb = check_fields(info, {}, [f"attributes.{field}"])
                rep.test(label, not pb, "; ".join(pb))

        normal, n = one("normal")
        pb = [f"{n} document(s)"] if not normal else check_fields(normal, {
            "body.text": "Commande traitée", "severity_text": "WARN",
            "attributes.http.route": "/api/orders/{id}", "attributes.order.id": "o-1001",
            "attributes.http.response.status_code": 200, "attributes.http.request.method": "GET",
            "resource.attributes.service.name": "application-test",
            "resource.attributes.service.version": "1.0.0"}, [])
        rep.test("Normal log kept unchanged", not pb, "; ".join(pb))

        nested, n = one("nested")
        if not nested:
            rep.test("Nested field removed", False, f"{n} document(s)")
            rep.test("Nested siblings kept", False, f"{n} document(s)")
        else:
            pb = check_fields(nested, {}, ["body.structured.user.email"])
            rep.test("Nested field removed", not pb, "; ".join(pb))
            pb = check_fields(nested, {
                "body.structured.message": "Commande créée", "body.structured.user.id": "u-42",
                "body.structured.user.name": "Jeanne Test", "body.structured.order.id": "o-2002",
                "body.structured.order.amount": 42.5}, [])
            rep.test("Nested siblings kept", not pb, "; ".join(pb))

        # Mapping natif OpenTelemetry (exporter mapping.mode: otel).
        ref = info or normal
        pb = ["aucun document"] if not ref else []
        if ref:
            if not ref["_index"].startswith(f".ds-{a.data_stream}-"):
                pb.append(f"index {ref['_index']}")
            pb += check_fields(ref, {"data_stream.type": "logs", "data_stream.dataset": "edotintegration.otel",
                                     "data_stream.namespace": "test",
                                     "scope.name": "edot-integration-test.producer"}, [])
            for f in ("@timestamp", "severity_number", "resource.attributes", "attributes"):
                if not lookup(ref, f)[0]:
                    pb.append(f"{f} absent")
        rep.test("OTel native mapping", not pb, "; ".join(pb))
    except Exception as e:  # noqa: BLE001
        print(f"\nERREUR : {e}", file=sys.stderr)
        return 2
    finally:
        if es_pf:
            es_pf.close()
        if client:
            client.close()

    passed = sum(1 for _, ok, _ in rep.tests if ok)
    print()
    banner(f"RESULT: {passed}/{len(rep.tests)} TESTS PASSED")
    print(f"Documents : GET {a.data_stream}/_search?q=attributes.test.campaign_id:{campaign}")
    return 0 if passed == len(rep.tests) else 1


if __name__ == "__main__":
    sys.exit(main())
