#!/usr/bin/env python3
"""Tests fonctionnels et de résilience de l'EDOT Collector, sur un banc LOCAL.

Monte un Kafka 4.3 (SASL_SSL, SCRAM-SHA-512, ACL) et un Elasticsearch 9.5.6
(HTTPS, API Key à privilèges minimaux) JETABLES dans un réseau Docker
`internal`, rend la configuration EXACTE du dépôt (helm template des values
de déploiement + tests/values-test.yaml qui remplace les TODO), lance deux
réplicas EDOT dans les conditions d'OpenShift (UID arbitraire, rootfs en
lecture seule, aucune capability, file persistante sur volume), puis :

  filtering   12 échantillons OTLP JSON (samples/) : suppression d'événements,
              suppression de champs (ressource, scope, log, Body imbriqué,
              Body JSON texte), conservation du reste, message invalide.
  resilience  indisponibilité d'Elasticsearch (arrêt, gel → timeouts),
              redémarrage et crash d'EDOT pendant la consommation, crash avec
              file persistante non vide, rebalance, API Key révoquée (401),
              saturation de la file (backpressure vers Kafka).
  all         les deux (défaut).

AUCUN service réel n'est contacté. Les identifiants sont générés
aléatoirement dans un répertoire temporaire, jamais affichés, et détruits à
la fin (sauf --keep).

Usage :
  infra/edot-collector/tests/functional/run-tests.py [filtering|resilience|all] [--keep]

Prérequis : docker (compose v2), helm, openssl, python3 + PyYAML. Variable
HELM pour un binaire précis. Durée : ~3 min (filtering), ~15 min (all).
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
EDOT_DIR = HERE.parent.parent
ROOT = EDOT_DIR.parent.parent
COMPOSE_FILE = HERE / "compose.yaml"
PROJECT = "edot-functional-test"
TOPIC = "otlp-logs-test"
GROUP = "edot-functional-test"
PARTITIONS = 4
DATA_STREAM = "logs-edottest.otel-functional"
EDOT = ("edot-collector-0", "edot-collector-1")
HELM = os.environ.get("HELM", "helm")
CHART_APP = ROOT / "argocd/applications/infra/edot-collector.yaml"

RESULTS: list[tuple[str, bool, str]] = []


def bold(s): print(f"\033[1m{s}\033[0m", flush=True)
def info(s): print(f"    {s}", flush=True)


def record(name: str, ok: bool, detail: str = "") -> bool:
    RESULTS.append((name, ok, detail))
    mark = "\033[32m✓\033[0m" if ok else "\033[31m✗\033[0m"
    print(f"    {mark} {name}{' — ' + detail if detail else ''}", flush=True)
    return ok


def run(cmd: list[str], *, input: str | None = None, check: bool = True, env=None,
        capture: bool = True, timeout: int = 600) -> subprocess.CompletedProcess:
    p = subprocess.run(cmd, input=input, text=True, capture_output=capture, env=env, timeout=timeout)
    if check and p.returncode != 0:
        # Ne JAMAIS afficher la ligne de commande (elle peut contenir un
        # identifiant de test) : seulement le binaire et la sortie d'erreur.
        raise RuntimeError(f"échec de {cmd[0]} {cmd[1] if len(cmd) > 1 else ''} : {(p.stderr or '').strip()[-800:]}")
    return p


class Bench:
    def __init__(self, keep: bool):
        self.keep = keep
        self.work = Path(tempfile.mkdtemp(prefix="edot-test-"))
        self.work.chmod(0o755)
        self.secrets = {
            "ELASTIC_PASSWORD": secrets.token_urlsafe(24),
            "KAFKA_KEYSTORE_PASSWORD": secrets.token_urlsafe(24),
            "KAFKA_EDOT_PASSWORD": secrets.token_urlsafe(24),
            "KAFKA_PRODUCER_PASSWORD": secrets.token_urlsafe(24),
        }
        self.api_key_id = ""
        self.api_key = ""
        self.env = dict(os.environ, EDOT_TEST_WORK=str(self.work),
                        ELASTIC_PASSWORD=self.secrets["ELASTIC_PASSWORD"],
                        KAFKA_KEYSTORE_PASSWORD=self.secrets["KAFKA_KEYSTORE_PASSWORD"])

    # ------------------------------------------------------------ docker
    def compose(self, *args, **kw):
        return run(["docker", "compose", "-f", str(COMPOSE_FILE), "-p", PROJECT, *args], env=self.env, **kw)

    def exec(self, service: str, *cmd, **kw):
        return self.compose("exec", "-T", service, *cmd, **kw)

    def secret_values(self) -> list[str]:
        return [v for v in (*self.secrets.values(), self.api_key) if v]

    # ------------------------------------------------------------ setup
    def certificates(self):
        w = self.work
        ossl = lambda *a: run(["openssl", *a])
        ossl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", "/CN=edot-functional-test-ca",
             "-keyout", str(w / "ca.key"), "-out", str(w / "ca.crt"))
        for name in ("kafka-test", "elasticsearch-test"):
            ossl("req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={name}",
                 "-keyout", str(w / f"{name}.key"), "-out", str(w / f"{name}.csr"))
            (w / f"{name}.ext").write_text(f"subjectAltName=DNS:{name}\nextendedKeyUsage=serverAuth\n")
            ossl("x509", "-req", "-in", str(w / f"{name}.csr"), "-CA", str(w / "ca.crt"), "-CAkey", str(w / "ca.key"),
                 "-CAcreateserial", "-days", "2", "-extfile", str(w / f"{name}.ext"), "-out", str(w / f"{name}.crt"))
        # Mot de passe du keystore passé par variable d'environnement (jamais en argument).
        run(["openssl", "pkcs12", "-export", "-in", str(w / "kafka-test.crt"), "-inkey", str(w / "kafka-test.key"),
             "-name", "kafka-test", "-out", str(w / "kafka.p12"), "-passout", "env:KAFKA_KEYSTORE_PASSWORD"],
            env=self.env)
        es = w / "es-certs"
        es.mkdir()
        shutil.copy(w / "elasticsearch-test.crt", es / "es.crt")
        shutil.copy(w / "elasticsearch-test.key", es / "es.key")
        shutil.copy(w / "ca.crt", es / "ca.crt")
        for p in [*w.iterdir(), *es.iterdir()]:
            if p.is_file():
                p.chmod(0o644)  # banc jetable : lisible par les UID des conteneurs

    def render(self, extra_values: dict | None = None) -> Path:
        """Rend le chart comme Argo CD (chart, version et valueFiles lus dans
        l'Application) + tests/values-test.yaml (+ surcharges du scénario)."""
        app = yaml.safe_load(CHART_APP.read_text())["spec"]
        src = next(s for s in app["sources"] if "chart" in s)
        cmd = [HELM, "template", src["helm"]["releaseName"], src["chart"], "--repo", src["repoURL"],
               "--version", src["targetRevision"], "--namespace", app["destination"]["namespace"],
               "--kube-version", "1.35.8", "--api-versions", "monitoring.coreos.com/v1"]
        for v in src["helm"]["valueFiles"]:
            cmd += ["-f", v.replace("$values/", f"{ROOT}/")]
        cmd += ["-f", str(EDOT_DIR / "tests/values-test.yaml")]
        if extra_values:
            p = self.work / "values-scenario.yaml"
            p.write_text(yaml.safe_dump(extra_values))
            cmd += ["-f", str(p)]
        out = run(cmd).stdout
        relay = next(d for d in yaml.safe_load_all(out) if d and d["kind"] == "ConfigMap")["data"]["relay"]
        path = self.work / "relay.yaml"
        path.write_text(relay)
        path.chmod(0o644)
        return path

    def start_backends(self):
        self.compose("up", "-d", "--quiet-pull", "kafka-test", "elasticsearch-test")
        self.wait(lambda: self.exec("kafka-test", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                                    "localhost:19092", "--list", check=False).returncode == 0, 120, "Kafka")
        k = lambda *a: self.exec("kafka-test", *a)
        kb = ["--bootstrap-server", "localhost:19092"]
        k("/opt/kafka/bin/kafka-topics.sh", *kb, "--create", "--topic", TOPIC, "--partitions", str(PARTITIONS),
          "--replication-factor", "1")
        for user, pw in (("edot", self.secrets["KAFKA_EDOT_PASSWORD"]), ("producer", self.secrets["KAFKA_PRODUCER_PASSWORD"])):
            k("/opt/kafka/bin/kafka-configs.sh", *kb, "--alter", "--entity-type", "users", "--entity-name", user,
              "--add-config", f"SCRAM-SHA-512=[password={pw}]")
        acl = ["/opt/kafka/bin/kafka-acls.sh", *kb, "--add"]
        # Privilèges minimaux d'EDOT (README §9) : lecture du topic et du groupe.
        k(*acl, "--allow-principal", "User:edot", "--operation", "Read", "--operation", "Describe", "--topic", TOPIC)
        k(*acl, "--allow-principal", "User:edot", "--operation", "Read", "--group", GROUP)
        k(*acl, "--allow-principal", "User:producer", "--operation", "Write", "--operation", "Describe", "--topic", TOPIC)
        (self.work / "producer.properties").write_text(
            "security.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\n"
            "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required "
            f"username=\"producer\" password=\"{self.secrets['KAFKA_PRODUCER_PASSWORD']}\";\n"
            "ssl.truststore.type=PEM\nssl.truststore.location=/work/ca.crt\n"
            "linger.ms=20\nbatch.size=262144\n")
        (self.work / "producer.properties").chmod(0o644)
        self.wait(lambda: self.es("GET", "/_cluster/health", check=False).get("status") in ("green", "yellow"), 180,
                  "Elasticsearch")
        self.create_api_key()

    def create_api_key(self):
        # Privilèges MINIMAUX documentés (README §13) : c'est cette clé qu'EDOT utilise.
        r = self.es("POST", "/_security/api_key", {
            "name": f"edot-functional-test-{uuid.uuid4().hex[:6]}",
            "role_descriptors": {"edot_logs_writer": {
                "cluster": ["monitor"],
                "indices": [{"names": ["logs-*.otel-*"], "privileges": ["auto_configure", "create_doc"]}]}}})
        self.api_key_id, self.api_key = r["id"], r["encoded"]
        env = self.work / "edot.env"
        env.write_text(f"KAFKA_USERNAME=edot\nKAFKA_PASSWORD={self.secrets['KAFKA_EDOT_PASSWORD']}\n"
                       f"ELASTICSEARCH_API_KEY={self.api_key}\n")
        env.chmod(0o600)

    def start_edot(self, *services: str, recreate: bool = False):
        services = services or EDOT
        args = ["up", "-d", "--quiet-pull"] + (["--force-recreate"] if recreate else []) + list(services)
        self.compose(*args)
        for s in services:
            self.wait(lambda s=s: self.exec(s, "curl", "-sf", "localhost:13133/", check=False).returncode == 0, 120,
                      f"{s} (health_check)")

    def down(self):
        if self.keep:
            info(f"--keep : banc conservé ({self.work}). Destruction : "
                 f"docker compose -f {COMPOSE_FILE} -p {PROJECT} down -v")
            return
        self.compose("down", "-v", "--remove-orphans", check=False, timeout=180)
        shutil.rmtree(self.work, ignore_errors=True)

    # ------------------------------------------------------------ helpers
    @staticmethod
    def wait(cond, timeout: int, what: str, interval: float = 2):
        end = time.time() + timeout
        while time.time() < end:
            try:
                if cond():
                    return True
            except Exception:
                pass
            time.sleep(interval)
        raise TimeoutError(f"{what} : délai de {timeout} s dépassé")

    def es(self, method: str, path: str, body: dict | None = None, check: bool = True) -> dict:
        cmd = ["curl", "-s", "--cacert", "config/certs/ca.crt", "-u", f"elastic:{self.secrets['ELASTIC_PASSWORD']}",
               "--resolve", "elasticsearch-test:9200:127.0.0.1",
               "-X", method, "-H", "Content-Type: application/json", f"https://elasticsearch-test:9200{path}"]
        if body is not None:
            cmd += ["--data-binary", "@-"]
        p = self.exec("elasticsearch-test", *cmd, input=json.dumps(body) if body is not None else None, check=check)
        try:
            return json.loads(p.stdout or "{}")
        except json.JSONDecodeError:
            return {}

    def produce(self, lines: list[str], key: str | None = None):
        """Publie des messages (une ligne = un message) en SASL_SSL, comme un
        producteur réel, via le listener annoncé kafka-test:9093."""
        props = ["--producer.config", "/work/producer.properties"]
        if key:
            lines = [f"{key}\t{line}" for line in lines]
            props += ["--property", "parse.key=true", "--property", "key.separator=\t"]
        self.exec("kafka-test", "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "kafka-test:9093",
                  "--topic", TOPIC, *props, input="\n".join(lines) + "\n", timeout=300)

    def metrics(self, service: str) -> dict[str, float]:
        """Métriques internes EDOT (:8888), sommées par nom + labels choisis."""
        p = self.exec(service, "curl", "-sf", "localhost:8888/metrics", check=False)
        out: dict[str, float] = {}
        for line in (p.stdout or "").splitlines():
            if line.startswith("#") or not line.strip():
                continue
            m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9.eE+-]+|NaN)", line)
            if not m:
                continue
            name, labels, val = m.group(1), m.group(2) or "", m.group(3)
            try:
                v = float(val)
            except ValueError:
                continue
            out[name] = out.get(name, 0) + v
            for lab in ("filter", "outcome", "exporter", "data_type", "http_response_status_code"):
                lm = re.search(rf'{lab}="([^"]*)"', labels)
                if lm:
                    k = f"{name}{{{lab}={lm.group(1)}}}"
                    out[k] = out.get(k, 0) + v
        return out

    def msum(self, name: str) -> float:
        return sum(self.metrics(s).get(name, 0) for s in EDOT if self.is_running(s))

    def is_running(self, service: str) -> bool:
        p = self.compose("ps", "--status", "running", "-q", service, check=False)
        return bool(p.stdout.strip())

    def lag(self) -> int:
        p = self.exec("kafka-test", "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:19092",
                      "--describe", "--group", GROUP, check=False)
        total = 0
        for line in p.stdout.splitlines():
            cols = line.split()
            if len(cols) >= 6 and cols[1] == TOPIC and cols[5].isdigit():
                total += int(cols[5])
        return total

    def refresh(self):
        self.es("POST", f"/{DATA_STREAM}/_refresh", check=False)

    def search(self, query: dict, size: int = 50) -> list[dict]:
        self.refresh()
        r = self.es("POST", f"/{DATA_STREAM}/_search", {"size": size, "query": query}, check=False)
        return [h["_source"] for h in r.get("hits", {}).get("hits", [])]

    def run_seqs(self, run_id: str) -> tuple[int, set[int]]:
        """(nombre de documents, ensemble EXACT des test.seq) d'un lot."""
        self.refresh()
        q = {"term": {"attributes.test.run": run_id}}
        total = self.es("POST", f"/{DATA_STREAM}/_count", {"query": q}, check=False).get("count", 0)
        seqs: set[int] = set()
        after = None
        while True:
            comp = {"size": 5000, "sources": [{"s": {"terms": {"field": "attributes.test.seq"}}}]}
            if after:
                comp["after"] = after
            r = self.es("POST", f"/{DATA_STREAM}/_search",
                        {"size": 0, "query": q, "aggs": {"c": {"composite": comp}}}, check=False)
            agg = r.get("aggregations", {}).get("c", {})
            for b in agg.get("buckets", []):
                seqs.add(int(b["key"]["s"]))
            after = agg.get("after_key")
            if not agg.get("buckets") or not after:
                break
        return total, seqs

    def edot_logs(self) -> str:
        return self.compose("logs", "--no-color", *EDOT, check=False).stdout


# ---------------------------------------------------------------- messages
def now_ns() -> str:
    return str(time.time_ns())


def load_sample(path: Path, run_id: str) -> tuple[str, dict, str]:
    s = json.loads(path.read_text())
    msg = s["message"]
    for rl in msg["resourceLogs"]:
        for sl in rl["scopeLogs"]:
            for r in sl["logRecords"]:
                r["timeUnixNano"] = r["observedTimeUnixNano"] = now_ns()
                r["attributes"].append({"key": "test.run", "value": {"stringValue": run_id}})
    return s["description"], s["expect"], json.dumps(msg, separators=(",", ":"), ensure_ascii=False)


def load_lines(run_id: str, count: int, per_msg: int = 10, start: int = 0) -> list[str]:
    """`count` enregistrements numérotés (test.seq) répartis en messages de
    `per_msg` enregistrements : permet de compter EXACTEMENT pertes et doublons."""
    lines = []
    for m in range(start, start + count, per_msg):
        recs = [{"timeUnixNano": now_ns(), "severityNumber": 9, "severityText": "INFO",
                 "body": {"stringValue": f"load record {i}"},
                 "attributes": [{"key": "test.run", "value": {"stringValue": run_id}},
                                {"key": "test.seq", "value": {"intValue": str(i)}},
                                {"key": "http.route", "value": {"stringValue": "/api/load"}}]}
                for i in range(m, min(m + per_msg, start + count))]
        lines.append(json.dumps({"resourceLogs": [{"resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": "load-generator"}}]},
            "scopeLogs": [{"scope": {"name": "edot.functional.test"}, "logRecords": recs}]}]},
            separators=(",", ":")))
    return lines


def lookup(doc, path: str):
    """Valeur d'un champ du _source, que les clés soient imbriquées ou
    « aplaties » avec des points (mapping otel : attributes."user.email")."""
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


# ---------------------------------------------------------------- scénarios
def scenario_filtering(b: Bench):
    bold("==> Filtrage et suppression de champs (samples/)")
    run_id = f"filtering-{uuid.uuid4().hex[:8]}"
    samples = sorted(HERE.glob("samples/t*.json"))
    expected = {}
    before = {s: b.metrics(s) for s in EDOT}
    for p in samples:
        desc, expect, line = load_sample(p, run_id)
        expected[p.stem] = (desc, expect)
        b.produce([line])
    # Message indécodable PUIS message valide sur la MÊME partition (même clé) :
    # le second doit être indexé (partition non bloquée par l'erreur permanente).
    poison = (HERE / "samples/t13-invalid-otlp-json.txt").read_text().strip()
    _, _, after_poison = load_sample(HERE / "samples/t11-info-kept.json", run_id)
    after_poison = after_poison.replace('"t11"', '"t13-after-poison"')
    b.produce([poison, after_poison], key="poison-partition")

    kept = [n.split("-")[0] for n, (_, e) in expected.items() if e["indexed"]] + ["t13-after-poison"]
    case = lambda c: {"bool": {"filter": [{"term": {"attributes.test.run": run_id}},
                                          {"term": {"attributes.test.case": c}}]}}
    try:
        b.wait(lambda: all(b.search(case(c), 1) for c in kept), 90, "indexation des logs conservés")
    except TimeoutError as e:
        record("indexation des logs conservés", False, str(e))
    time.sleep(12)  # > flush_timeout (5 s) : un log qui devait être supprimé aurait eu le temps d'arriver

    for name, (desc, expect) in expected.items():
        c = name.split("-")[0]
        docs = b.search(case(c))
        if not expect["indexed"]:
            record(f"{c} {desc}", len(docs) == 0, f"{len(docs)} document(s) indexé(s)" if docs else "")
            continue
        problems = []
        if len(docs) != expect.get("count", 1):
            problems.append(f"{len(docs)} document(s) au lieu de {expect.get('count', 1)}")
        d = docs[0] if docs else {}
        for f in expect.get("absent", []):
            if lookup(d, f)[0]:
                problems.append(f"{f} PRÉSENT")
        for f, v in expect.get("present", {}).items():
            found, got = lookup(d, f)
            if not found or got != v:
                problems.append(f"{f}={got!r} (attendu {v!r})")
        text = json.dumps(lookup(d, "body.text")[1] or "", ensure_ascii=False)
        for s in expect.get("body_text_contains", []):
            if json.dumps(s, ensure_ascii=False)[1:-1] not in text:
                problems.append(f"body.text ne contient pas {s}")
        for s in expect.get("body_text_absent", []):
            if s in text:
                problems.append(f"body.text contient {s}")
        record(f"{c} {desc}", not problems and bool(docs), "; ".join(problems))
        if c == "t09" and docs:
            (b.work / "t09-document.json").write_text(json.dumps(d, indent=2, ensure_ascii=False))

    record("t13 message OTLP JSON invalide ignoré, message suivant de la même partition indexé",
           bool(b.search(case("t13-after-poison"))))
    after = {s: b.metrics(s) for s in EDOT}
    delta = lambda k: sum(after[s].get(k, 0) - before[s].get(k, 0) for s in EDOT)
    unmarshal = delta("otelcol_kafka_receiver_unmarshal_failed_log_records")
    record("erreur de décodage comptée (otelcol_kafka_receiver_unmarshal_failed_log_records)", unmarshal == 1,
           f"+{unmarshal:g}")
    for flt, n in (("filter/severity", 3), ("filter/resource", 1), ("filter/log-attributes", 3), ("filter/values", 1)):
        got = delta(f"otelcol_processor_filter_logs_filtered{{filter={flt}}}")
        record(f"suppressions comptées par {flt}", got == n, f"{got:g} (attendu {n})")
    failed = delta("otelcol_exporter_send_failed_log_records")
    record("aucun échec d'export", failed == 0, f"{failed:g}")


def produce_load(b: Bench, run_id: str, count: int, start: int = 0):
    b.produce(load_lines(run_id, count, start=start))


def expect_complete(b: Bench, name: str, run_id: str, count: int, timeout: int = 240):
    try:
        b.wait(lambda: len(b.run_seqs(run_id)[1]) >= count, timeout, name, interval=5)
    except TimeoutError:
        pass
    total, seqs = b.run_seqs(run_id)
    lost = count - len(seqs)
    dup = total - len(seqs)
    record(name, lost == 0, f"{len(seqs)}/{count} indexés, {lost} perdu(s), {dup} doublon(s)")
    return lost, dup


def scenario_resilience(b: Bench):
    bold("==> Résilience")
    t = lambda: uuid.uuid4().hex[:8]

    # R1 — Elasticsearch arrêté : retries, file persistante, reprise.
    info("R1 Elasticsearch arrêté pendant la collecte (connexion refusée)")
    rid = f"r1-{t()}"
    failed0 = b.msum("otelcol_exporter_send_failed_log_records")
    b.compose("stop", "elasticsearch-test")
    produce_load(b, rid, 5000)
    time.sleep(40)
    q = b.msum("otelcol_exporter_queue_size")
    record("R1 file d'attente alimentée pendant l'indisponibilité", q > 0, f"otelcol_exporter_queue_size={q:g}")
    record("R1 offsets validés après écriture dans la file (lag Kafka nul)", b.lag() == 0, f"lag={b.lag()}")
    b.compose("start", "elasticsearch-test")
    b.wait(lambda: b.es("GET", "/_cluster/health", check=False).get("status") in ("green", "yellow"), 180, "ES")
    expect_complete(b, "R1 reprise après rétablissement : aucune perte", rid, 5000)
    record("R1 aucun lot abandonné", b.msum("otelcol_exporter_send_failed_log_records") - failed0 == 0)

    # R2 — Elasticsearch gelé : TCP accepté mais aucune réponse → timeouts.
    info("R2 Elasticsearch gelé (requêtes en timeout)")
    rid = f"r2-{t()}"
    failed0 = b.msum("otelcol_exporter_send_failed_log_records")
    b.compose("pause", "elasticsearch-test")
    produce_load(b, rid, 3000)
    time.sleep(75)  # > timeout de l'exporter (30 s) : au moins deux timeouts
    b.compose("unpause", "elasticsearch-test")
    expect_complete(b, "R2 reprise après timeouts : aucune perte", rid, 3000)
    lost_batches = b.msum("otelcol_exporter_send_failed_log_records") - failed0
    record("R2 aucun lot abandonné sur timeout", lost_batches == 0, f"send_failed=+{lost_batches:g}")

    # R3 — redémarrage propre et crash d'EDOT pendant la consommation.
    info("R3 redémarrage (SIGTERM) et crash (SIGKILL) d'EDOT pendant la consommation")
    rid = f"r3-{t()}"
    produce_load(b, rid, 10000)
    b.compose("restart", "edot-collector-0")
    b.compose("kill", "-s", "SIGKILL", "edot-collector-1")
    produce_load(b, rid, 10000, start=10000)
    b.start_edot("edot-collector-1")
    b.wait(lambda: b.exec("edot-collector-0", "curl", "-sf", "localhost:13133/", check=False).returncode == 0, 120, "edot-0")
    expect_complete(b, "R3 reprise après redémarrage/crash : aucune perte", rid, 20000, timeout=300)

    # R4 — crash avec file persistante NON VIDE : les données ne sont plus que sur le volume.
    info("R4 crash des deux réplicas avec file persistante non vide")
    rid = f"r4-{t()}"
    b.compose("stop", "elasticsearch-test")
    produce_load(b, rid, 4000)
    b.wait(lambda: b.lag() == 0, 90, "consommation complète (offsets validés)")
    q = b.msum("otelcol_exporter_queue_size")
    record("R4 données uniquement dans la file persistante avant le crash", q > 0 and b.lag() == 0,
           f"queue_size={q:g}, lag={b.lag()}")
    b.compose("kill", "-s", "SIGKILL", *EDOT)
    b.compose("start", "elasticsearch-test")
    b.wait(lambda: b.es("GET", "/_cluster/health", check=False).get("status") in ("green", "yellow"), 180, "ES")
    b.start_edot()
    expect_complete(b, "R4 file persistante relue après crash (fsync) : aucune perte", rid, 4000)

    # R5 — rebalance : un réplica disparaît, ses partitions passent à l'autre.
    info("R5 perte d'un réplica : rebalance après session_timeout (45 s)")
    rid = f"r5-{t()}"
    b.compose("stop", "edot-collector-1")
    t0 = time.time()
    produce_load(b, rid, 4000)
    expect_complete(b, "R5 partitions reprises par le réplica restant : aucune perte", rid, 4000, timeout=180)
    info(f"R5 délai de reprise mesuré : {time.time() - t0:.0f} s (membre statique : attente de session_timeout)")
    b.start_edot("edot-collector-1")

    # R6 — API Key révoquée (401) : arrêt de la chaîne, aucune perte, reprise.
    info("R6 API Key révoquée (HTTP 401), puis rotation de la clé")
    rid = f"r6-{t()}"
    failed0 = b.msum("otelcol_exporter_send_failed_log_records")
    b.es("DELETE", "/_security/api_key", {"ids": [b.api_key_id]})
    time.sleep(5)
    produce_load(b, rid, 2000)
    time.sleep(45)
    # Constaté : le client HTTP retente les 401 SANS log warn/error ; la
    # tentative est visible dans http_client_request_duration_count.
    rejected = b.msum("http_client_request_duration_count{http_response_status_code=401}")
    record("R6 401 retenté sans abandon de lot (visible dans http_client_request_duration_count)",
           rejected > 0 and b.msum("otelcol_exporter_send_failed_log_records") - failed0 == 0,
           f"{rejected:g} tentative(s) 401, send_failed=+{b.msum('otelcol_exporter_send_failed_log_records') - failed0:g}")
    b.create_api_key()  # nouvelle clé = mise à jour du Secret + rollout
    b.start_edot(recreate=True)
    expect_complete(b, "R6 reprise après rotation de la clé : aucune perte", rid, 2000)

    # R7 — saturation de la file : backpressure jusqu'à Kafka, aucun refus.
    info("R7 saturation de la file (file réduite à 2 Mo pour le test)")
    rid = f"r7-{t()}"
    b.render({"config": {"exporters": {"elasticsearch": {"sending_queue": {"queue_size": 2000000}}}}})
    b.start_edot(recreate=True)
    enq0 = b.msum("otelcol_exporter_enqueue_failed_log_records")
    b.compose("stop", "elasticsearch-test")
    produce_load(b, rid, 30000)
    time.sleep(40)
    lag = b.lag()
    q, cap = b.msum("otelcol_exporter_queue_size"), b.msum("otelcol_exporter_queue_capacity")
    record("R7 file pleine : consommation Kafka suspendue (lag > 0)", lag > 0 and q > 0,
           f"lag={lag}, queue_size={q:g}/{cap:g}")
    record("R7 aucun refus d'écriture dans la file", b.msum("otelcol_exporter_enqueue_failed_log_records") - enq0 == 0)
    b.compose("start", "elasticsearch-test")
    b.wait(lambda: b.es("GET", "/_cluster/health", check=False).get("status") in ("green", "yellow"), 180, "ES")
    expect_complete(b, "R7 reprise : arriéré Kafka entièrement indexé", rid, 30000, timeout=420)
    b.render()
    b.start_edot(recreate=True)


def scenario_secrets(b: Bench):
    bold("==> Aucun identifiant dans les logs EDOT")
    logs = b.edot_logs()
    leaked = [i for i, v in enumerate(b.secret_values()) if v and v in logs]
    record("aucun mot de passe ni API Key dans les logs des conteneurs EDOT", not leaked,
           f"{len(leaked)} valeur(s) trouvée(s)" if leaked else "")


def inventory_metrics(b: Bench):
    names = sorted({k.split("{")[0] for k in b.metrics(EDOT[0])})
    (b.work / "metrics.txt").write_text("\n".join(names) + "\n")
    bold(f"==> {len(names)} métriques internes exposées (inventaire : {b.work}/metrics.txt)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", nargs="?", default="all", choices=["filtering", "resilience", "all"])
    ap.add_argument("--keep", action="store_true", help="conserver le banc (diagnostic)")
    args = ap.parse_args()
    for binary in ("docker", HELM, "openssl"):
        if not shutil.which(binary):
            sys.exit(f"Commande introuvable : {binary}")
    b = Bench(args.keep)
    try:
        bold("==> Banc de test : certificats, Kafka, Elasticsearch, rendu de la configuration")
        b.certificates()
        b.render()
        b.start_backends()
        b.start_edot()
        info("2 réplicas EDOT démarrés (UID 1000680000, rootfs en lecture seule, sans capability)")
        if args.scenario in ("filtering", "all"):
            scenario_filtering(b)
        if args.scenario in ("resilience", "all"):
            scenario_resilience(b)
        scenario_secrets(b)
        inventory_metrics(b)
    except Exception as e:  # noqa: BLE001 — rapport lisible, puis nettoyage
        record("exécution du banc", False, f"{type(e).__name__}: {e}")
        if not args.keep:
            print(b.edot_logs()[-4000:])
    finally:
        b.down()
    failed = [r for r in RESULTS if not r[1]]
    bold(f"==> {len(RESULTS) - len(failed)}/{len(RESULTS)} vérifications réussies")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
