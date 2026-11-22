#!/usr/bin/env python3
"""Générateur d'événements OTLP JSON pour l'environnement edot-integration-test.

Chaque message est un ExportLogsServiceRequest OTLP/JSON (encodage `otlp_json`
du receiver Kafka d'EDOT) :

    resourceLogs[] → resource.attributes
                   → scopeLogs[] → scope
                                 → logRecords[]

Une ligne = un message Kafka. Le script n'a AUCUNE dépendance (bibliothèque
standard) : il écrit les messages sur la sortie standard (ou un fichier / une
FIFO), et un producteur Kafka les publie :

  * Job Kubernetes (producer/job.yaml) : conteneur Python → FIFO →
    kafka-console-producer.sh de l'image officielle Kafka ;
  * tests (tests/validate_pipeline.py) : importe ce module et publie via un
    pod client éphémère ;
  * à la main :
      ./produce_otlp.py --events all | kafka-console-producer.sh \
        --bootstrap-server kafka:9092 --topic otlp-logs-test \
        --property parse.key=true --property key.separator=$'\\t'

Format de sortie : « clé<TAB>message-json ». La clé est l'identifiant de
campagne : tous les messages d'une campagne vont dans la MÊME partition,
donc sont consommés dans l'ordre d'émission (base de la vérification par
événement témoin des tests).

Chaque logRecord porte les attributs test.campaign_id, test.case et
test.seq : ils permettent de retrouver les documents dans Elasticsearch.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from typing import Callable

# --------------------------------------------------------------- encodage OTLP


def any_value(v):
    """Valeur Python → AnyValue OTLP/JSON (int64 en chaîne, conforme à la
    spécification protobuf-JSON)."""
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    if isinstance(v, float):
        return {"doubleValue": v}
    if isinstance(v, dict):
        return {"kvlistValue": {"values": kvs(v)}}
    if isinstance(v, (list, tuple)):
        return {"arrayValue": {"values": [any_value(x) for x in v]}}
    return {"stringValue": str(v)}


def kvs(d: dict) -> list[dict]:
    return [{"key": k, "value": any_value(v)} for k, v in d.items()]


SEVERITY = {"TRACE": 1, "DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17, "FATAL": 21}


def otlp_message(*, resource: dict, severity: str, body, attributes: dict,
                 scope: str = "edot-integration-test.producer") -> dict:
    now = str(time.time_ns())
    return {
        "resourceLogs": [{
            "resource": {"attributes": kvs(resource)},
            "scopeLogs": [{
                "scope": {"name": scope, "version": "1.0.0"},
                "logRecords": [{
                    "timeUnixNano": now,
                    "observedTimeUnixNano": now,
                    "severityNumber": SEVERITY[severity],
                    "severityText": severity,
                    "body": any_value(body),
                    "attributes": kvs(attributes),
                }],
            }],
        }],
    }


# ------------------------------------------------------------ types d'événements
# Chaque type : fonction(attributs de test) → message, et le résultat ATTENDU
# après le pipeline EDOT (filtres et suppressions de values-processing.yaml).

APP = {"service.name": "application-test", "service.version": "1.0.0",
       "deployment.environment.name": "integration-test"}


def ev_info(t: dict) -> dict:
    """1. INFO normal avec attributs sensibles → indexé, user.email,
    client.ip et debug.payload supprimés."""
    return otlp_message(resource=APP, severity="INFO", body="Test OpenTelemetry", attributes={
        **t,
        "user.email": "test@example.com",
        "client.ip": "192.0.2.10",
        "debug.payload": "{\"internal\": true}",
        "http.route": "/api/orders",
        "http.request.method": "POST",
    })


def ev_debug(t: dict) -> dict:
    """2. DEBUG → supprimé (filter/severity)."""
    return otlp_message(resource=APP, severity="DEBUG", body="Debug event", attributes=t)


def ev_health(t: dict) -> dict:
    """3. Healthcheck http.route=/health → supprimé (filter/log-attributes)."""
    return otlp_message(resource=APP, severity="INFO", body="GET /health 200", attributes={
        **t, "http.route": "/health", "http.request.method": "GET",
        "http.response.status_code": 200,
    })


def ev_excluded(t: dict) -> dict:
    """4. service.name=excluded-service → supprimé (filter/resource)."""
    return otlp_message(resource={**APP, "service.name": "excluded-service"}, severity="INFO",
                        body="Event from excluded service", attributes=t)


def ev_normal(t: dict) -> dict:
    """5. Log normal sans attribut sensible → indexé sans modification."""
    return otlp_message(resource=APP, severity="WARN", body="Commande traitée", attributes={
        **t, "http.route": "/api/orders/{id}", "http.request.method": "GET",
        "http.response.status_code": 200, "order.id": "o-1001",
    })


def ev_nested(t: dict) -> dict:
    """6. Body JSON imbriqué (kvlistValue) avec un champ sensible
    body.user.email → supprimé, le reste conservé."""
    return otlp_message(resource=APP, severity="INFO", attributes=t, body={
        "message": "Commande créée",
        "user": {"id": "u-42", "email": "nested@example.com", "name": "Jeanne Test"},
        "order": {"id": "o-2002", "amount": 42.5, "items": 3},
    })


def ev_witness(t: dict) -> dict:
    """Événement TÉMOIN, publié EN DERNIER sur la même partition : quand il
    est indexé, tous les messages précédents de la campagne ont été consommés
    et traités par le pipeline."""
    return otlp_message(resource=APP, severity="INFO", body="Témoin de fin de campagne", attributes=t)


EVENTS: dict[str, Callable[[dict], dict]] = {
    "info": ev_info,
    "debug": ev_debug,
    "health": ev_health,
    "excluded": ev_excluded,
    "normal": ev_normal,
    "nested": ev_nested,
    "witness": ev_witness,
}
# Résultat attendu dans Elasticsearch, par type.
EXPECTED_INDEXED = {"info": True, "debug": False, "health": False, "excluded": False,
                    "normal": True, "nested": True, "witness": True}
ALL = ["info", "debug", "health", "excluded", "normal", "nested"]


def parse_events(spec: str) -> list[str]:
    names = ALL if spec == "all" else [s.strip() for s in spec.split(",") if s.strip()]
    unknown = [n for n in names if n not in EVENTS]
    if unknown:
        raise SystemExit(f"type(s) inconnu(s) : {', '.join(unknown)} — valides : all, {', '.join(EVENTS)}")
    return names


def generate(campaign_id: str, events: list[str], count: int = 1, witness: bool = True):
    """Messages d'une campagne : `count` tours de `events`, puis le témoin.
    Renvoie une liste de (type, seq, message JSON sur une ligne)."""
    out = []
    seq = 0
    for _ in range(count):
        for name in events:
            seq += 1
            t = {"test.campaign_id": campaign_id, "test.case": name, "test.seq": seq}
            out.append((name, seq, json.dumps(EVENTS[name](t), ensure_ascii=False, separators=(",", ":"))))
    if witness:
        seq += 1
        t = {"test.campaign_id": campaign_id, "test.case": "witness", "test.seq": seq}
        out.append(("witness", seq, json.dumps(ev_witness(t), ensure_ascii=False, separators=(",", ":"))))
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--events", default="all",
                   help="types séparés par des virgules, ou « all » (défaut) : " + ", ".join(EVENTS))
    p.add_argument("--count", type=int, default=1, help="nombre de tours de la liste d'événements (défaut 1)")
    p.add_argument("--interval", type=float, default=0.0,
                   help="secondes entre deux messages (défaut 0 : au plus vite)")
    p.add_argument("--campaign-id", default=None, help="identifiant de campagne (défaut : généré)")
    p.add_argument("--no-witness", action="store_true", help="ne pas ajouter l'événement témoin final")
    p.add_argument("--output", default="-", help="fichier ou FIFO de sortie (défaut : sortie standard)")
    p.add_argument("--no-key", action="store_true", help="message seul, sans « clé<TAB> »")
    # --bootstrap / --topic : informatifs ici (le producteur Kafka les utilise),
    # acceptés pour une ligne de commande homogène avec le Job.
    p.add_argument("--bootstrap", default="kafka.edot-integration-test.svc.cluster.local:9092")
    p.add_argument("--topic", default="otlp-logs-test")
    a = p.parse_args()

    cid = a.campaign_id or f"campaign-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    msgs = generate(cid, parse_events(a.events), a.count, witness=not a.no_witness)
    print(f"campagne={cid} messages={len(msgs)} topic={a.topic} bootstrap={a.bootstrap}", file=sys.stderr)

    out = sys.stdout if a.output == "-" else open(a.output, "w", encoding="utf-8")
    try:
        for i, (_, _, line) in enumerate(msgs):
            out.write(line + "\n" if a.no_key else f"{cid}\t{line}\n")
            out.flush()
            if a.interval and i < len(msgs) - 1:
                time.sleep(a.interval)
    finally:
        if out is not sys.stdout:
            out.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
