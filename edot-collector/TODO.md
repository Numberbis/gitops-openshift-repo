# EDOT Collector — paramètres à fournir avant la mise en production

Ce fichier recense **tout** ce qui reste à renseigner ou à décider. Tant qu'une
ligne « Bloquant » n'est pas traitée, la chaîne **ne peut pas démarrer** :

- la configuration contenant des `TODO_*` est **refusée** par EDOT (`elastic-agent
  otel validate`, vérifié en CI) : pods en échec, aucune connexion tentée ;
- les Secrets n'existent pas tant que Vault n'est pas alimenté : pods en
  `CreateContainerConfigError` ;
- les NetworkPolicies de sortie contiennent des CIDR `TODO` : objets refusés
  par l'API server ;
- l'Application Argo CD est en **synchronisation manuelle** et n'est pas
  enregistrée.

Contrôle à tout moment :

```bash
edot-collector/tests/preflight.sh          # liste chaque TODO restant (fichier:ligne)
edot-collector/tests/preflight.sh --live   # + cluster, en lecture seule
```

Les valeurs **sensibles** (clé privée du certificat client Kafka, API Key) ne vont **jamais** dans Git :
la colonne « Valeur » indique alors l'emplacement Vault à alimenter
(procédure : [README §10](README.md#10-gestion-des-secrets)).

---

## 1. Kafka (cluster externe)

| Paramètre | Description | Où le renseigner | Valeur |
|---|---|---|---|
| **Kafka brokers** | Au moins 2 brokers de *bootstrap*, `hôte:port`, adresses **externes** joignables depuis OpenShift (pas de Service Kubernetes). Le nom doit figurer dans le certificat TLS du broker. | `values-kafka.yaml` → `brokers` (`TODO_KAFKA_BROKER_n:TODO_KAFKA_PORT`) | TODO |
| **Kafka advertised listeners** | Liste de **tous** les `hôte:port` annoncés par les brokers pour le listener utilisé (le client s'y connecte après le bootstrap). Chacun doit être résolu en DNS et joignable depuis les pods. | Documentation réseau + `network-policies/allows/allow-egress-kafka.yaml` ; vérification : `tests/network-diagnostics.sh kafka` | TODO |
| Kafka port | Port du listener **SSL** à authentification client (mTLS) — souvent distinct du port SASL_SSL. | `TODO_KAFKA_PORT` (values-kafka.yaml et allow-egress-kafka.yaml) | TODO |
| **Kafka topics** | Topic(s) contenant les logs **OTLP JSON** (`ExportLogsServiceRequest`). Liste, ou motif `^regex`. | `values-kafka.yaml` → `logs.topics` | TODO |
| Nombre de partitions | Par topic. Le nombre de réplicas EDOT utiles est ≤ au nombre total de partitions. | Information (dimensionnement, README §11) | TODO |
| **Kafka consumer group** | Groupe **dédié** à EDOT, jamais partagé avec Logstash ou un autre consommateur. Ex. `edot-logs-prod`. | `values-kafka.yaml` → `group_id` | TODO |
| **Kafka protocol** | `security.protocol=SSL` avec `ssl.client.auth=required` côté brokers (mTLS) : **configuration fournie**. `PLAINTEXT` / `SASL_PLAINTEXT` : refusés (TLS obligatoire). | `values-kafka.yaml` → bloc `tls` (`cert_file`, `key_file`), aucun bloc `auth` | ✅ SSL (mTLS) |
| **Kafka principal** | Forme exacte du principal dérivé du certificat, selon `ssl.principal.mapping.rules` des brokers : DN complet (`User:CN=…,OU=…,O=…,C=…`, défaut) ou CN seul. Les ACL portent sur cette chaîne exacte. | Équipe Kafka (tutoriel §A1) | TODO |
| **Kafka certificat client** | Certificat **dédié** à EDOT (un par environnement), DN convenu avec l'équipe Kafka, usage `clientAuth`, signé par une CA du truststore des brokers, + intermédiaires. Date d'expiration et rappel de renouvellement. | Vault `secret/kubernetes/edot/edot-kafka-credentials`, clé `tls.crt` (tutoriel §A2, §A4) | TODO (Vault) |
| **Kafka clé privée** | Clé du certificat client, PEM **PKCS#8 non chiffrée** (`BEGIN PRIVATE KEY`). | Vault `…/edot-kafka-credentials`, clé `tls.key` | TODO (Vault) |
| **Kafka CA** | Certificat(s) PEM de l'autorité qui signe les certificats **des brokers** (chaîne complète si intermédiaire) — à ne pas confondre avec la CA du certificat client. | Vault `…/edot-kafka-credentials`, clé `ca.crt` | TODO (Vault) |
| **ACL Kafka** | À créer par l'équipe Kafka pour le **principal du certificat** d'EDOT : `Read` + `Describe` sur chaque topic, `Read` sur le groupe. Aucun droit d'écriture. Si ACL restreintes par hôte : EgressIP du namespace. | Équipe Kafka (README §9, tutoriel §A3) | TODO |
| Rétention Kafka | `retention.ms` / `retention.bytes` des topics : durée maximale d'indisponibilité d'Elasticsearch ou d'EDOT absorbable sans perte, et fenêtre de rejeu. | Information (README §19) | TODO |
| `offsets.retention.minutes` | Rétention des offsets d'un groupe sans consommateur (défaut Kafka : 7 jours). | Information (README §19.2) | TODO |
| Initial offset (1ʳᵉ mise en service) | `latest` (défaut configuré : pas de rejeu de l'historique) ou `earliest` (indexe toute la rétention : doublons si ces logs sont déjà indexés par Logstash). | `values-kafka.yaml` → `initial_offset` | À CONFIRMER (latest) |
| Double ingestion | Ces topics sont-ils déjà indexés dans Elasticsearch par un pipeline Logstash existant ? Si oui : data streams distincts ou arrêt coordonné. | Décision | TODO |
| `group.max.session.timeout.ms` | Doit être ≥ `session_timeout` configuré (45 s). Défaut Kafka : 30 min. | Vérification côté brokers | TODO |
| `max.message.bytes` | Taille maximale d'un message des topics (≤ 1 Mo par défaut : réglages de fetch par défaut adaptés). | Information | TODO |

## 2. Elasticsearch (cluster externe 9.x)

| Paramètre | Description | Où le renseigner | Valeur |
|---|---|---|---|
| **Elasticsearch endpoints** | URL(s) **HTTPS externes** : nœuds, ou VIP d'un load balancer / reverse proxy. Le nom doit figurer dans le certificat. | `values-elasticsearch.yaml` → `endpoints` (`https://TODO_ELASTICSEARCH_HOST_n:TODO_ELASTICSEARCH_PORT`) | TODO |
| Elasticsearch port | Port HTTPS (souvent 9200 ou 443). | `TODO_ELASTICSEARCH_PORT` (values et allow-egress-elasticsearch.yaml) | TODO |
| Version Elasticsearch | Version exacte (9.x). Templates `logs-*.otel-*` intégrés requis. | Information (README §13) | TODO |
| **Elasticsearch API Key** | API Key **encodée** (`base64(id:api_key)`, champ `encoded` de la réponse de création), privilèges minimaux du README §13, avec date d'expiration et procédure de rotation. | Vault `secret/kubernetes/edot/edot-elasticsearch-credentials`, clé `api-key` | TODO (Vault) |
| **Elasticsearch CA** | Certificat(s) PEM de l'autorité du certificat HTTP (ou du reverse proxy). | Vault `…/edot-elasticsearch-credentials`, clé `ca.crt` | TODO (Vault) |
| **Elasticsearch data stream** | Destination des logs : `logs-<dataset>.otel-<namespace>`. Dataset et namespace en minuscules, sans `-` dans le dataset. Ex. dataset `securite`, namespace `prod` → `logs-securite.otel-prod`. | `values-elasticsearch.yaml` → `transform/routing` (`TODO_ELASTICSEARCH_DATASET`, `TODO_ELASTICSEARCH_NAMESPACE`) | TODO |
| Cycle de vie des données | Politique ILM / data stream lifecycle des data streams cibles (rétention, rollover). Non créée par ce dépôt. | Équipe Elasticsearch | TODO |
| Failure store | Activer le failure store des data streams cibles (quarantaine des documents refusés, README §19.4) ? | Équipe Elasticsearch | TODO (recommandé) |
| `http.max_content_length` | ≥ 10 Mo (lots EDOT ≤ 5 Mo). Défaut ES : 100 Mo. Vérifier aussi la limite d'un éventuel reverse proxy. | Vérification | TODO |
| Reverse proxy / load balancer | Présent ? Délai d'inactivité, taille maximale de requête, TLS terminé où ? | Information (README §7) | TODO |

## 3. Réseau et pare-feu

| Paramètre | Description | Où le renseigner | Valeur |
|---|---|---|---|
| **Firewall Kafka** | Ouverture **OpenShift → Kafka** : IP source (EgressIP ou IP des workers) vers **chaque** broker annoncé, TCP sur le port du listener SSL (mTLS). | Demande d'ouverture de flux (équipe réseau) | TODO |
| **Firewall Elasticsearch** | Ouverture **OpenShift → Elasticsearch** : IP source vers les endpoints (ou le load balancer), TCP sur le port HTTPS. Liste blanche d'IP éventuelle côté Elasticsearch / proxy. | Demande d'ouverture de flux | TODO |
| IP de sortie OpenShift | IP vues par les pare-feu : EgressIP du namespace `edot` si l'équipe plateforme en attribue une (recommandé), sinon IP des workers (NAT). **Aucune EgressIP n'est créée par ce dépôt.** | Équipe plateforme (README §7) | TODO |
| CIDR Kafka (NetworkPolicy) | CIDR couvrant les IP de tous les brokers (bootstrap + annoncés). | `network-policies/allows/allow-egress-kafka.yaml` (`TODO_KAFKA_BROKERS_CIDR_n`) | TODO |
| CIDR Elasticsearch (NetworkPolicy) | CIDR des endpoints / du load balancer. | `network-policies/allows/allow-egress-elasticsearch.yaml` (`TODO_ELASTICSEARCH_CIDR`) | TODO |
| Proxy sortant | Un proxy HTTP(S) est-il imposé vers Elasticsearch ? Si oui : `HTTPS_PROXY` / `NO_PROXY` dans `extraEnvs` (values.yaml) et NetworkPolicy vers le proxy. Kafka ne passe pas par un proxy HTTP. | values.yaml / README §7 | TODO (aucun ?) |
| DNS | Les noms des brokers et d'Elasticsearch sont-ils résolus par le DNS d'OpenShift (zones internes, forwarders) ? | Vérification : `tests/network-diagnostics.sh` | TODO |
| EgressFirewall / AdminNetworkPolicy | Politiques réseau de cluster existantes à adapter (OVN-Kubernetes) ? | Équipe plateforme | TODO |

## 4. OpenShift et GitOps

| Paramètre | Description | Où le renseigner | Valeur |
|---|---|---|---|
| Cluster cible | OpenShift (profil `values-openshift.yaml`, défaut) ou Kubernetes standard (`values-kubernetes.yaml`, p. ex. cluster kubeadm de ce dépôt). | `apps/40-edot-collector-app.yaml` → `valueFiles` | À CONFIRMER (OpenShift) |
| Version d'OpenShift | Pour vérifier SCC `restricted-v2`, OVN-Kubernetes, supervision des projets utilisateur. | Information | TODO |
| Instance Argo CD OpenShift | OpenShift GitOps : namespace de l'instance qui gère `edot` (label `argocd.argoproj.io/managed-by`), AppProject à utiliser. | `apps/40-edot-collector-app.yaml` (namespace `openshift-gitops`, projet `default`, comme les autres Applications de `apps/`) | À CONFIRMER |
| StorageClass | Classe des PVC de file (RWO, bloc ou fichier, `allowVolumeExpansion` souhaité, `reclaimPolicy` Retain souhaitée). Défaut du cluster si non fixée. | `values.yaml` → `storageClassName` (commenté) | TODO |
| Registre d'images | Accès direct à `docker.elastic.co` depuis les nœuds, ou miroir interne (`ImageDigestMirrorSet`, registre Quay/Nexus) : alors surcharger `image.repository`. | `values.yaml` → `image.repository` | TODO |
| Pull secret | Requis si miroir authentifié. | `imagePullSecrets` (values.yaml) + Vault | TODO (aucun ?) |
| Supervision | Prometheus de ce dépôt (label `release`) ou supervision des projets utilisateur d'OpenShift (activée ? namespace `edot` non exclu ?) ; Grafana : où importer le dashboard ? | README §15 | TODO |
| Alertmanager | Routage des alertes `category: security` (destinataires). | Configuration Alertmanager | TODO |
| Vault / ESO | Vault et External Secrets Operator disponibles sur ce cluster ? Rôle Vault `eso-edot` à créer (`scripts/vault-secrets.py setup-vault`). Sinon, adapter `external-secrets/store/` au mécanisme du cluster. | README §10 | TODO |
| Sizing | Débit attendu (logs/s, taille moyenne), pics, nombre de partitions : ajuster réplicas, ressources, taille des PVC. | README §11, §14 | TODO |
| Seuils d'alerte | Lag « élevé » (100 000 par défaut), latence bulk (10 s) : à recalibrer d'après le débit réel. | `prometheusrule.yaml` | À AJUSTER |
| Règles de filtrage | Les règles fournies sont des **exemples** (DEBUG, `excluded-service`, healthchecks, `load-test`) : à valider ou remplacer. | `values-processing.yaml` | À VALIDER |
| Champs sensibles | Liste des clés à supprimer (exemples fournis) et champs imbriqués propres à vos applications. | `values-processing.yaml` | À VALIDER |

---

## Ordre conseillé

1. Équipes PKI / Kafka / Elasticsearch : certificat client, principal, ACL, API Key, CA, data stream (§1, §2).
2. Équipe réseau / plateforme : IP de sortie, ouvertures de flux, DNS (§3).
3. Renseigner les fichiers values et NetworkPolicies (PR), alimenter Vault.
4. `tests/validate-config.sh` (adapter son étape 5a, qui exige aujourd'hui le refus des TODO), `tests/preflight.sh --live`.
5. Synchronisation manuelle, puis `tests/network-diagnostics.sh`, puis tests d'intégration : [README §16](README.md#16-déploiement-argo-cd-et-mise-en-service).
