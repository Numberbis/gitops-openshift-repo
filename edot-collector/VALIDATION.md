# Compte rendu de validation — EDOT Collector

Validations réalisées le **2026-11-19** sur la branche `feature/develop_EDOT_ETL`,
**sans accès** à un cluster OpenShift ni aux infrastructures Kafka et
Elasticsearch réelles (aucun contexte Kubernetes configuré sur le poste).
Tout ce qui est marqué ✅ a été **exécuté** ; ce qui ne l'a pas été est listé
au §4. Commandes de reproduction : [README §20](README.md#20-validations-et-tests).

## 1. Versions vérifiées

| Élément | Version retenue | Vérification |
|---|---|---|
| Chart | `open-telemetry/opentelemetry-collector` 0.178.3 (2026-11-10, appVersion 0.163.2) | index du dépôt Helm ; 0.179.x / 0.180.0 (≤ 1 jour) écartées, écarts limités aux presets non utilisés |
| Image EDOT | `docker.elastic.co/elastic-agent/elastic-otel-collector:9.5.6` | tag présent sur le registre (9.5.7 et 9.6.0 : artefacts seulement, pas d'image) ; digest `sha256:9caf6481865156cc1ae52992be3b0596719a849501d677e36f38312e8c8ee0f1` ; `buildinfo` 9.5.6 ; image du 2026-10-29, signature cosign publiée (`.sig`) |
| Collector embarqué | v0.163.0 (core et contrib), client Kafka franz-go 1.22.1 | `NOTICE.txt` de l'image |
| Commande | `/usr/share/elastic-agent/otelcol --config=/conf/relay.yaml` (`exec elastic-agent otel`) | inspection de l'image ; démarrage réel |
| Banc : Kafka | `apache/kafka:4.3.2` (`sha256:852a0d49…c227a7`) | |
| Banc : Elasticsearch | `docker.elastic.co/elasticsearch/elasticsearch:9.5.6` | |

## 2. Validations statiques

| Validation | Résultat |
|---|---|
| `helm lint` (5 fichiers de values) | ✅ |
| `helm template` comme Argo CD (profil OpenShift, profil Kubernetes, valeurs de test) | ✅ StatefulSet, ConfigMap, Service, ServiceAccount, ServiceMonitor, PDB |
| 47 contrôles des manifests et de la configuration rendus (`validate-config.sh`) : sécurité OpenShift, PVC, Secrets référencés, ordre du pipeline, TLS, file persistante, exposition… | ✅ |
| `elastic-agent otel validate` — configuration de **déploiement** (TODO) | ✅ **refusée** (mécanisme SASL, ports et URL invalides) |
| `elastic-agent otel validate` — configuration + valeurs de test | ✅ acceptée |
| Self-test : OTTL invalide (fonction, syntaxe, chemin), option inconnue, type invalide | ✅ rejetés |
| `kubectl kustomize` du répertoire compagnon (11 objets) ; chaque Secret consommé est produit par un ExternalSecret | ✅ |
| `scripts/vault-secrets.py render --check` | ✅ fichiers générés à jour |
| `scripts/argocd-projects-check.py` (Applications ↔ AppProjects) | ✅ |
| Policies Kyverno du dépôt (`infra/kyverno-policies/validate.sh`, CLI 1.19.1) | ✅ `edot-collector` : 6 policies passées, 0 audit |
| `trivy config` 0.75.0 (`.github/scripts/trivy-config-scan.sh`) | ✅ aucun constat bloquant ; voir §3 |
| `promtool check rules` (Prometheus 3.5.0) : 21 alertes, 28 requêtes du dashboard | ✅ |

## 3. Constats `trivy config` (reporting) et traitement

| Constat | Analyse | Traitement |
|---|---|---|
| KSV-0109 (HIGH) / KSV-01010 « ConfigMap with secrets » | faux positif : heuristique sur les **noms** `password:` / `api_key:` / `username:`, dont les valeurs sont `${env:…}` | aucun secret dans la ConfigMap (contrôlé par `validate-config.sh`) |
| KSV-0020 / KSV-0021 UID / GID ≤ 10000 | profil OpenShift : UID attribué par la SCC (≥ 1 000 000 000) ; profil Kubernetes : était 1000 | **corrigé** : profil Kubernetes en 10001 (démarrage vérifié) |
| KSV-0125 registre non approuvé | `docker.elastic.co` hors de la liste par défaut de trivy | registre officiel Elastic ; miroir interne possible (TODO.md) |
| KSV-0039 / KSV-0040 LimitRange / ResourceQuota | gouvernance du namespace, hors périmètre de l'application | à décider par l'équipe plateforme |

## 4. Tests fonctionnels et de résilience (banc Docker local)

Banc `tests/functional/run-tests.py all` : Kafka SASL_SSL/SCRAM-SHA-512 avec ACL
minimales, Elasticsearch HTTPS avec API Key minimale, **2 réplicas EDOT dans les
conditions d'OpenShift** (UID 1000680000, GID 0, fsGroup simulé, rootfs en
lecture seule, aucune capability, file persistante sur volume), configuration
**identique** à celle du dépôt hormis les TODO remplacés par `tests/values-test.yaml`.

Exécuté deux fois (la seconde sur un banc neuf, après correctifs) : **35/35
vérifications réussies** à la seconde exécution.

| # | Scénario | Résultat |
|---|---|---|
| t01–t02 | log DEBUG (severity_number, libellé) | ✅ supprimés |
| t03 | ressource `service.name=excluded-service` | ✅ supprimé |
| t04–t05 | healthchecks `http.route=/health`, `url.path=/readyz` | ✅ supprimés |
| t06 / t07 | critères combinés / un seul critère | ✅ supprimé / conservé |
| t08 | `user.email`, `client.ip`, `http.request.header.authorization` (tableau), clé en majuscules (log), `host.mac` (ressource), `debug.payload` (scope) | ✅ log conservé, champs absents d'Elasticsearch, autres champs intacts |
| t09 | Body structuré imbriqué (`user.email`, `client.ip`, `http.request.headers.authorization`) et attribut objet | ✅ seuls ces champs supprimés, voisins conservés |
| t10 | Body texte JSON | ✅ valeurs masquées, JSON conservé |
| t11 | log INFO ordinaire | ✅ conservé intact (attributs, ressource, scope, sévérité, data stream `logs-edottest.otel-functional`) |
| t12 | un message, trois enregistrements | ✅ seul l'INFO conservé |
| t13 | message OTLP JSON invalide puis message valide sur la même partition | ✅ invalide ignoré et compté (`unmarshal_failed`=1), suivant indexé (partition non bloquée) |
| — | compteurs de filtres par catégorie | ✅ 3 / 1 / 3 / 1 conformes |
| R1 | Elasticsearch arrêté (connexion refusée), 5 000 logs | ✅ file alimentée, offsets validés, 5 000/5 000 après reprise, 0 doublon, 0 lot abandonné |
| R2 | Elasticsearch gelé (timeouts), 3 000 logs | ✅ 3 000/3 000, 0 lot abandonné |
| R3 | redémarrage SIGTERM + crash SIGKILL pendant 20 000 logs | ✅ 0 perte ; **doublons : 3 230 puis 820** (at-least-once) |
| R4 | crash des 2 réplicas, 4 000 logs uniquement dans la file persistante (lag Kafka 0) | ✅ 4 000/4 000 après redémarrage : persistance `fsync` démontrée |
| R5 | perte d'un réplica (rebalance) | ✅ 4 000/4 000 ; reprise en 10 s puis 53 s (≤ session_timeout + traitement) |
| R6 | API Key révoquée (401) puis rotation | ✅ 401 retentés (14 tentatives comptées par `http_client_request_duration_count`), **aucun log émis par EDOT** (constat), 0 lot abandonné, 2 000/2 000 après rotation |
| R7 | file saturée (réduite à 2 Mo), 30 000 logs | ✅ consommation Kafka suspendue (lag > 0), 0 refus, 30 000/30 000 après reprise |
| S1 | identifiants dans les logs EDOT | ✅ aucun mot de passe ni API Key |

Constats issus des tests et intégrés à la solution :

- compteurs exposés **sans** suffixe `_total` ; compteurs d'échec absents avant
  le premier échec → alertes écrites en conséquence ;
- retries 401/403/429/5xx **silencieux** dans les logs → alertes sur
  `http_client_request_duration_count` ;
- `kafka-topics --describe` exige `DescribeConfigs` (Kafka 4.3) → diagnostic
  basé sur `kafka-get-offsets` (ACL minimales suffisantes) ;
- `elastic-agent otel validate` ne détecte pas un encodage Kafka inconnu →
  contrôle statique dédié ;
- option `resolve_canonical_bootstrap_servers_only` documentée mais refusée par
  cette version → retirée.

## 5. Non exécuté (et pourquoi)

| Test | Raison | Comment le faire |
|---|---|---|
| Déploiement sur OpenShift (SCC réelle, StorageClass, supervision UWM, OpenShift GitOps) | aucun cluster accessible depuis le poste | README §16 (sync partielle, `preflight.sh --live`) |
| Connectivité vers le Kafka et l'Elasticsearch réels (DNS, pare-feu, EgressIP, advertised.listeners, certificats réels) | adresses non renseignées (TODO), aucun accès | `tests/network-diagnostics.sh` ; ses commandes ont été exécutées avec succès sur le banc (DNS, TCP, TLS dont rejet d'un nom non conforme, SASL dont rejet d'un mauvais mot de passe, advertised.listeners, offsets du topic avec ACL minimales, privilèges de l'API Key dont refus hors périmètre) ; le script complet (`oc exec`) n'a pas pu être exécuté |
| Comportement sur un 403 de requête et sur une erreur TLS en cours d'exploitation | non simulé | déduit du code de l'exporter (README §19.3) |
| Perte d'un worker OpenShift réel, PVC répliqué, drain | pas de cluster | simulé par arrêt / crash de conteneur (R3 à R5) |
| Débit et dimensionnement réels | banc mono-hôte non représentatif | mesurer en recette (README §11, §14) |
| Rendu visuel du dashboard Grafana | pas de Grafana sur le banc | requêtes PromQL validées par promtool, métriques présentes sur le banc |
| Vérification de la signature cosign | `cosign` absent du poste | `validate-config.sh` la vérifie si `cosign` est installé |
| Exécution du job CI `edot-config` sur GitHub Actions | pas de push effectué | s'exécutera à la PR |
