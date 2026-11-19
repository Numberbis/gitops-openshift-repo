# Tutoriel — Connecter l'EDOT Collector à Kafka (mTLS + ACL sur CN) et à Elasticsearch

> **Public visé** : une personne qui découvre EDOT, Kafka et Elasticsearch, et qui
> doit mettre en service la chaîne `Kafka → EDOT Collector → Elasticsearch` sur
> OpenShift avec ce dépôt.
>
> **Ce que vous saurez faire à la fin** :
>
> 1. faire reconnaître l'EDOT Collector par un cluster Kafka **très sécurisé** qui
>    identifie ses clients **par certificat** (mTLS) et applique des **ACL sur le
>    CN** de ce certificat ;
> 2. connecter l'EDOT Collector à Elasticsearch avec une **API Key** aux privilèges
>    minimaux ;
> 3. **prouver**, étape par étape, que chaque maillon fonctionne, et savoir quoi
>    faire quand ce n'est pas le cas.
>
> **Durée** : comptez une demi-journée de travail effectif, plus les délais des
> équipes Kafka, Elasticsearch, PKI et réseau (souvent plusieurs jours : faites
> vos demandes **en premier**, voir §2.3).

---

## Sommaire

- [0. Lire avant de commencer](#0-lire-avant-de-commencer)
- [1. Comprendre ce qu'on construit](#1-comprendre-ce-quon-construit)
- [2. Prérequis](#2-prérequis)
- **Partie A — Kafka (mTLS + ACL sur CN)**
  - [A1. Comment Kafka reconnaît EDOT : le « principal »](#a1-comment-kafka-reconnaît-edot--le--principal-)
  - [A2. Obtenir le certificat client d'EDOT](#a2-obtenir-le-certificat-client-dedot)
  - [A3. Faire créer les ACL par l'équipe Kafka](#a3-faire-créer-les-acl-par-léquipe-kafka)
  - [A4. Ranger le certificat dans Vault](#a4-ranger-le-certificat-dans-vault)
  - [A5. Adapter l'ExternalSecret Kafka](#a5-adapter-lexternalsecret-kafka)
  - [A6. Adapter `values.yaml` (variables et volumes)](#a6-adapter-valuesyaml-variables-et-volumes)
  - [A7. Adapter `values-kafka.yaml`](#a7-adapter-values-kafkayaml)
  - [A8. Ouvrir le réseau vers Kafka](#a8-ouvrir-le-réseau-vers-kafka)
  - [A9. Adapter les tests du dépôt](#a9-adapter-les-tests-du-dépôt)
  - [A10. Vérifier la connexion Kafka](#a10-vérifier-la-connexion-kafka)
  - [A11. Dépannage Kafka](#a11-dépannage-kafka)
- **Partie B — Elasticsearch**
  - [B1. Comment EDOT écrit dans Elasticsearch](#b1-comment-edot-écrit-dans-elasticsearch)
  - [B2. Récupérer l'autorité de certification d'Elasticsearch](#b2-récupérer-lautorité-de-certification-delasticsearch)
  - [B3. Créer l'API Key](#b3-créer-lapi-key)
  - [B4. Ranger l'API Key et la CA dans Vault](#b4-ranger-lapi-key-et-la-ca-dans-vault)
  - [B5. Adapter `values-elasticsearch.yaml`](#b5-adapter-values-elasticsearchyaml)
  - [B6. Ouvrir le réseau vers Elasticsearch](#b6-ouvrir-le-réseau-vers-elasticsearch)
  - [B7. Vérifier la connexion Elasticsearch](#b7-vérifier-la-connexion-elasticsearch)
  - [B8. Dépannage Elasticsearch](#b8-dépannage-elasticsearch)
- **Partie C — Mise en service et preuve de bout en bout**
  - [C1. Ordre de déploiement](#c1-ordre-de-déploiement)
  - [C2. Test de bout en bout avec un message marqué](#c2-test-de-bout-en-bout-avec-un-message-marqué)
  - [C3. Checklist finale « tout fonctionne »](#c3-checklist-finale--tout-fonctionne-)
- [Annexes](#annexes) : rotation des secrets, Application Argo CD, aide-mémoire, glossaire

---

## 0. Lire avant de commencer

### 0.1 Conventions de ce guide

- `$ commande` : à taper dans **votre terminal** (poste de travail, connecté à
  OpenShift avec `oc`).
- `diag$ commande` : à taper **dans un pod de diagnostic** du namespace `edot`
  (on y entre avec `oc exec -it …`, voir §A10).
- Les blocs `GET …` / `POST …` sont à exécuter dans **Kibana → Dev Tools**.
- Tous les chemins de fichiers sont relatifs au répertoire
  `edot-collector/` du dépôt, sauf mention contraire.

**Valeurs d'exemple utilisées dans tout le guide** (remplacez-les par les
vôtres partout où elles apparaissent) :

| Élément | Valeur d'exemple |
|---|---|
| Brokers Kafka (bootstrap) | `kafka-b1.corp.example:9093`, `kafka-b2.corp.example:9093`, `kafka-b3.corp.example:9093` |
| Topic contenant les logs OTLP JSON | `otlp-logs-prod` |
| Consumer group dédié à EDOT | `edot-logs-prod` |
| CN du certificat client d'EDOT | `edot-collector-prod` |
| Sujet complet (DN) du certificat | `CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR` |
| Elasticsearch | `https://es-prod.corp.example:9200` |
| Data stream cible | `logs-app.otel-prod` (dataset `app`, namespace `prod`) |
| Namespace OpenShift | `edot` |

### 0.2 ⚠️ État du dépôt : ce qu'il faut savoir

1. **La configuration est déjà en mTLS.** Le receiver Kafka présente un
   certificat client (`security.protocol=SSL`, aucun SASL) : `values-kafka.yaml`,
   `values.yaml`, l'ExternalSecret Kafka et les tests ont été adaptés (§A5 à
   §A9 décrivent ce qui est en place et pourquoi). **Vous n'avez qu'à** obtenir
   le certificat, faire créer les ACL, alimenter Vault et renseigner les
   `TODO_*` (adresses, port, topic, groupe, data stream, CIDR).
2. **Le mTLS est validé sur le banc local** (`tests/functional/run-tests.py`) :
   Kafka 4.3 avec certificat client obligatoire et ACL sur le DN, certificat
   d'EDOT signé par une CA intermédiaire, refus vérifiés (sans certificat, CA
   inconnue, CN sans ACL, tentative d'écriture). Résultats : `VALIDATION.md`.
3. **L'Application Argo CD** est `apps/40-edot-collector-app.yaml` ; elle n'est
   volontairement **pas** listée dans `apps/kustomization.yaml` (voir
   l'[annexe 2](#annexe-2--application-argo-cd)). Les scripts `tests/` la lisent.
   `scripts/vault-secrets.py` n'existe pas dans ce dépôt : les ExternalSecrets
   sont maintenus à la main et les commandes Vault sont données en clair
   (§A4, §B4).
4. **Le `SecretStore` de `external-secrets/store/secretstore.yaml` pointe vers
   `https://vault-active.vault.svc.cluster.local:8200`** avec le rôle Vault
   `eso-edot`. Ce dépôt utilise ailleurs un autre Vault
   (`components/elastic-platform/cluster-secret-store.yaml` :
   `https://vault.example.com`, rôle `elastic-prod`). Faites valider par
   l'administrateur Vault **l'adresse, la CA (`caBundle`) et le rôle** à
   utiliser pour le namespace `edot` (§2.2).
5. **Version d'Elasticsearch** : la configuration EDOT (mapping `otel`) a été
   validée avec Elasticsearch **9.x** (9.5.6). Le cluster ECK déployé par ce
   dépôt (`components/elasticsearch/elasticsearch.yaml`) est en **8.16.1** : si
   c'est lui que vous visez, cette combinaison **n'a pas été validée** — faites
   un essai en recette ou prévoyez la montée de version.

---

## 1. Comprendre ce qu'on construit

### 1.1 Le schéma

```
                        OpenShift — namespace « edot »
                 ┌──────────────────────────────────────────────┐
                 │                                              │
  ┌─────────┐    │   ┌───────────────────┐   ┌───────────────┐  │    ┌────────────────┐
  │  Kafka  │◀───┼───│  EDOT Collector   │──▶│ file d'attente│──┼───▶│ Elasticsearch  │
  │ externe │ ①  │   │  (2 pods)         │   │ persistante   │ ②│    │ externe        │
  └─────────┘    │   │  receiver kafka   │   │ (PVC)         │  │    └────────────────┘
   TLS mutuel    │   │  → filtres OTTL   │   └───────────────┘  │     HTTPS + API Key
   (mTLS)        │   │  → exporter ES    │                      │
   ACL sur CN    │   └───────────────────┘                      │
                 │        ▲          ▲                          │
                 │        │ fichiers │ variable d'environnement │
                 │   ┌────┴──────────┴────┐                     │
                 │   │ Secrets Kubernetes │◀── External Secrets ◀── Vault
                 │   └────────────────────┘     Operator (ESO)  │
                 └──────────────────────────────────────────────┘
```

**Point important** : EDOT **n'ouvre aucun port de réception**. C'est **lui qui
se connecte** à Kafka (connexion ①, il *lit* des messages) puis à Elasticsearch
(connexion ②, il *écrit* des documents). Les deux connexions sont **sortantes**
depuis OpenShift.

### 1.2 Le vocabulaire minimal

| Terme | Explication simple |
|---|---|
| **TLS** | Chiffrement de la connexion. Le **serveur** (broker Kafka, Elasticsearch) présente un certificat ; le **client** (EDOT) vérifie qu'il est signé par une autorité de confiance et que le nom du serveur y figure. |
| **mTLS** (TLS mutuel) | En plus, le **client présente lui aussi un certificat**, que le serveur vérifie. C'est ainsi que Kafka sait « qui » est EDOT, **sans mot de passe**. |
| **CA** (autorité de certification) | Le certificat « racine » (ou intermédiaire) qui signe les autres. Faire confiance à une CA = accepter tous les certificats qu'elle a signés. |
| **CN** (Common Name) | Un champ du **sujet** d'un certificat, p. ex. `CN=edot-collector-prod`. Votre cluster Kafka s'en sert pour identifier le client. |
| **DN** (Distinguished Name) | Le sujet **complet** : `CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR`. |
| **SAN** (Subject Alternative Name) | La liste des noms DNS pour lesquels un certificat **serveur** est valable. Le nom que vous configurez (`kafka-b1.corp.example`) **doit** y figurer. |
| **Principal Kafka** | L'identité que Kafka attribue à un client authentifié, p. ex. `User:CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR`. Les ACL portent sur ce principal. |
| **ACL Kafka** | Règle « tel principal peut faire telle opération sur telle ressource » (p. ex. `Read` sur le topic `otlp-logs-prod`). Sans ACL, un cluster sécurisé **refuse tout**. |
| **Consumer group** | Nom partagé par les consommateurs qui se répartissent les partitions d'un topic. Kafka y mémorise la position de lecture (**offsets**). |
| **Advertised listeners** | Les adresses que **chaque broker** annonce aux clients. EDOT contacte d'abord un broker de « bootstrap », puis se reconnecte **directement à chaque broker annoncé**. |
| **API Key Elasticsearch** | Un identifiant (sans mot de passe utilisateur) porteur de privilèges précis. EDOT l'envoie dans l'en-tête HTTP `Authorization: ApiKey …`. |
| **Data stream** | La « destination » des logs dans Elasticsearch, p. ex. `logs-app.otel-prod`. Elle est créée automatiquement au premier document. |
| **Vault / ESO** | Vault stocke les secrets. External Secrets Operator (ESO) les recopie dans des **Secrets Kubernetes**. Aucun secret n'est jamais écrit dans Git. |

### 1.3 Ce qui va où (vue d'ensemble des fichiers)

| Information | Où elle vit | Sensible ? |
|---|---|---|
| Adresses des brokers, topic, consumer group | `values-kafka.yaml` (Git) | non |
| CA des brokers, certificat client, **clé privée** client | Vault → Secret `edot-kafka-credentials` → fichiers `/etc/edot/kafka/` | **la clé privée : oui** |
| Adresse d'Elasticsearch, data stream | `values-elasticsearch.yaml` (Git) | non |
| CA d'Elasticsearch, **API Key** | Vault → Secret `edot-elasticsearch-credentials` → fichier + variable `ELASTICSEARCH_API_KEY` | **l'API Key : oui** |
| Règles réseau internes au cluster | `network-policies/allows/*.yaml` (Git) | non |
| Ouvertures de pare-feu, EgressIP | équipes réseau / plateforme (hors Git) | non |

---

## 2. Prérequis

### 2.1 Outils sur votre poste

| Outil | Pour quoi faire | Vérification |
|---|---|---|
| `oc` | piloter OpenShift | `oc version` puis `oc whoami` |
| `openssl` ≥ 1.1.1 | générer et contrôler les certificats | `openssl version` |
| `vault` (CLI) | écrire les secrets | `vault version` |
| `git` | modifier le dépôt | `git --version` |
| `argocd` (CLI, facultatif) | synchroniser l'Application | `argocd version --client` |
| `jq` (facultatif) | lire du JSON | `jq --version` |

Connectez-vous et placez-vous dans le namespace :

```bash
$ oc login https://api.<votre-cluster>:6443
$ oc project edot          # le namespace est créé par Argo CD (CreateNamespace=true)
```

### 2.2 Droits nécessaires

| Où | Droit | Pour |
|---|---|---|
| OpenShift, namespace `edot` | créer/supprimer des pods, `pods/exec`, lire les `externalsecrets` | pods de diagnostic (§A10, §B7) |
| Vault | écrire sous `secret/kubernetes/edot/*` | §A4, §B4 |
| Vault (administrateur) | créer la policy et le rôle `eso-edot` | une seule fois, voir ci-dessous |
| Kibana | un utilisateur pouvant créer des API Keys (`manage_api_key` ou `manage_own_api_key`) **et** possédant lui-même les privilèges qu'il délègue | §B3 |

**Une seule fois, côté Vault (administrateur)** — autoriser ESO à lire les
secrets du namespace `edot`, et uniquement ceux-là :

```bash
$ vault policy write eso-edot - <<'EOF'
path "secret/data/kubernetes/edot/*"     { capabilities = ["read"] }
path "secret/metadata/kubernetes/edot/*" { capabilities = ["read", "list"] }
EOF

$ vault write auth/kubernetes/role/eso-edot \
    bound_service_account_names=eso-vault \
    bound_service_account_namespaces=edot \
    audience=vault \
    policies=eso-edot \
    ttl=15m
```

> Le ServiceAccount `eso-vault` et le `SecretStore` `vault` sont créés par
> `external-secrets/store/`. Si votre méthode d'authentification Kubernetes de
> Vault n'a pas de jeton « reviewer » propre, le ServiceAccount présenté doit
> pouvoir appeler l'API TokenReview (ClusterRole `system:auth-delegator`) —
> c'est ce que fait `components/elastic-platform/cluster-secret-store.yaml`
> pour le namespace `elastic`. Vérifiez avec l'administrateur Vault.

### 2.3 Les demandes à envoyer **dès maintenant** aux autres équipes

Ces demandes sont souvent ce qui prend le plus de temps. Envoyez-les le jour 1.

| Équipe | Demande | Section |
|---|---|---|
| **PKI** | Signer le certificat **client** d'EDOT (CSR fournie), usage *TLS Web Client Authentication* | §A2 |
| **Kafka** | (1) CA qui signe les certificats **des brokers** ; (2) liste des **advertised listeners** ; (3) format du **principal** (règles `ssl.principal.mapping.rules`) ; (4) création des **ACL** ; (5) la CA qui a signé votre certificat client est-elle dans le **truststore des brokers** ? | §A1, §A3, §A8 |
| **Elasticsearch** | Adresse HTTPS, version exacte, CA du certificat HTTP, création (ou délégation) de l'**API Key** | §B2, §B3 |
| **Réseau** | Ouverture de flux OpenShift → **chaque** broker Kafka (port SSL) et OpenShift → Elasticsearch (HTTPS) | §A8, §B6 |
| **Plateforme OpenShift** | Une **EgressIP** pour le namespace `edot` (IP source stable vue par les pare-feu et par d'éventuelles ACL Kafka « par hôte ») | §A8 |

---

# Partie A — Kafka (mTLS + ACL sur CN)

## A1. Comment Kafka reconnaît EDOT : le « principal »

Quand EDOT se connecte à un listener Kafka `SSL` avec `ssl.client.auth=required` :

1. le broker présente **son** certificat → EDOT le vérifie avec `ca.crt` ;
2. EDOT présente **son** certificat client → le broker le vérifie avec **son
   truststore** ;
3. le broker transforme le **sujet** du certificat client en **principal** ;
4. à chaque requête (lire un topic, rejoindre un groupe…), le broker consulte
   les **ACL** de ce principal.

### Le piège n° 1 : la forme exacte du principal

Par défaut (`ssl.principal.mapping.rules=DEFAULT`), le principal est le **DN
complet**, au format RFC 2253 : **CN en premier**, séparateurs `,` **sans
espace**.

```
User:CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR
```

Si l'équipe Kafka a configuré une règle de transformation, le principal peut
être **réduit au CN**. Exemple de règle :

```properties
ssl.principal.mapping.rules=RULE:^CN=([^,]+),.*$/$1/,DEFAULT
```

donne :

```
User:edot-collector-prod
```

> **Demandez à l'équipe Kafka la règle en vigueur** et calculez le principal
> exact. Une ACL écrite pour `User:edot-collector-prod` ne s'applique **pas** à
> `User:CN=edot-collector-prod,OU=…` (et inversement). Une seule lettre, un
> espace ou un ordre différent suffit à tout refuser.

Pour afficher le sujet d'un certificat **dans le format que Kafka utilise** :

```bash
$ openssl x509 -in edot-collector-prod.crt -noout -subject -nameopt RFC2253
subject=CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR
```

### Le piège n° 2 : il y a **deux** CA différentes

| CA | Sert à | Qui doit l'avoir |
|---|---|---|
| **CA des brokers** | signe les certificats **serveur** des brokers | **EDOT** (fichier `ca.crt`) |
| **CA du client** | signe le certificat **client** d'EDOT | **les brokers** (leur truststore) |

Elles sont souvent identiques (PKI d'entreprise), mais pas toujours. Si elles
diffèrent, `ca.crt` côté EDOT contient **uniquement** la CA des brokers.

---

## A2. Obtenir le certificat client d'EDOT

### A2.1 Exigences sur le certificat

| Exigence | Pourquoi |
|---|---|
| **CN** (et DN) exactement celui convenu avec l'équipe Kafka | c'est la base des ACL |
| *Extended Key Usage* = **`TLS Web Client Authentication`** (`clientAuth`) | sinon le broker refuse le certificat (`unsupported certificate purpose`) |
| Signé par une CA présente dans le **truststore des brokers** | sinon `unknown_ca` / `bad_certificate` |
| Clé privée au format **PEM PKCS#8 non chiffrée** (`-----BEGIN PRIVATE KEY-----`) | EDOT ne sait pas lire une clé protégée par mot de passe ; le format PKCS#8 convient aussi aux outils Kafka utilisés pour les tests (§A10) |
| Fichier certificat = **certificat client + intermédiaires** éventuels | le broker doit pouvoir reconstruire la chaîne jusqu'à sa racine |
| Durée de validité connue et **rappel de renouvellement** planifié | un certificat expiré coupe l'ingestion (voir [annexe 1](#annexe-1--rotation-des-secrets)) |

> **Un seul certificat pour tous les pods EDOT.** Les deux réplicas utilisent la
> même identité (même CN) : c'est voulu, ils partagent le même consumer group.
> Un certificat **par environnement** (recette, production) est en revanche
> recommandé (CN `edot-collector-rec`, `edot-collector-prod`…).

### A2.2 Générer la clé privée et la demande de signature (CSR)

Travaillez dans un répertoire temporaire protégé :

```bash
$ mkdir -p ~/edot-certs && cd ~/edot-certs
$ umask 077                          # fichiers lisibles par vous seul

$ openssl req -new \
    -newkey rsa:3072 -nodes \
    -keyout edot-collector-prod.key \
    -out    edot-collector-prod.csr \
    -subj "/C=FR/O=MaSociete/OU=Observabilite/CN=edot-collector-prod" \
    -addext "extendedKeyUsage=clientAuth" \
    -addext "keyUsage=critical,digitalSignature,keyEncipherment"
```

Explications :

- `-newkey rsa:3072 -nodes` : nouvelle clé RSA 3072 bits **non chiffrée**
  (`-nodes` = *no DES*). Si votre PKI impose ECDSA, utilisez
  `-newkey ec -pkeyopt ec_paramgen_curve:P-256`.
- `-subj` s'écrit dans l'ordre **inverse** du format Kafka : `/C=…/O=…/OU=…/CN=…`
  donnera `CN=…,OU=…,O=…,C=…`. Respectez **exactement** les attributs convenus
  avec l'équipe Kafka (certaines PKI ajoutent `L=`, `ST=` : le DN change alors).
- `-addext` demande l'usage « client ». La PKI peut l'imposer elle-même : ce
  qui compte, c'est le certificat **final** (contrôle au §A2.4).

Contrôlez la CSR avant de l'envoyer :

```bash
$ openssl req -in edot-collector-prod.csr -noout -verify -subject -nameopt RFC2253
verify OK
subject=CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR
```

Envoyez **uniquement le fichier `.csr`** à la PKI. **La clé `.key` ne quitte
jamais votre poste** (sauf vers Vault, §A4).

### A2.3 Cas où la PKI vous fournit un `.p12` / `.pfx` ou un `.jks`

Certaines PKI génèrent elles-mêmes la clé et livrent un conteneur protégé par
mot de passe. Convertissez-le en PEM :

```bash
# Depuis un JKS (Java), passer d'abord en PKCS#12 :
$ keytool -importkeystore -srckeystore edot.jks -destkeystore edot.p12 -deststoretype PKCS12

# PKCS#12 → certificat client, chaîne, clé PKCS#8 non chiffrée
$ openssl pkcs12 -in edot.p12 -clcerts -nokeys -out edot-collector-prod.crt
$ openssl pkcs12 -in edot.p12 -cacerts -nokeys -out edot-collector-prod-intermediates.crt
$ openssl pkcs12 -in edot.p12 -nocerts -nodes | openssl pkey -out edot-collector-prod.key
```

> Avec OpenSSL 3, un `.p12` ancien peut exiger l'option `-legacy`
> (message `unsupported algorithm … RC2-40-CBC`).

### A2.4 Contrôler le certificat reçu (à ne pas sauter)

On suppose que vous avez maintenant :

- `edot-collector-prod.key` — la clé privée ;
- `edot-collector-prod.crt` — le certificat signé ;
- `client-ca-chain.pem` — la ou les CA qui ont signé le certificat client
  (intermédiaire + racine) ;
- `kafka-brokers-ca.pem` — la CA des **brokers** (fournie par l'équipe Kafka).

**1. Sujet, émetteur, dates, usages :**

```bash
$ openssl x509 -in edot-collector-prod.crt -noout \
    -subject -issuer -dates -nameopt RFC2253 \
    -ext extendedKeyUsage
subject=CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR
issuer=CN=PKI Interne Issuing CA,O=MaSociete,C=FR
notBefore=Nov 19 10:00:00 2026 GMT
notAfter=Nov 19 10:00:00 2027 GMT
X509v3 Extended Key Usage:
    TLS Web Client Authentication
```

→ le `subject` doit être **exactement** celui prévu ; l'EKU doit contenir
`TLS Web Client Authentication`.

**2. La clé correspond bien au certificat** (les deux empreintes doivent être
identiques) :

```bash
$ openssl x509 -in edot-collector-prod.crt -noout -pubkey | sha256sum
$ openssl pkey -in edot-collector-prod.key -pubout          | sha256sum
```

**3. La chaîne est valide :**

```bash
$ openssl verify -CAfile client-ca-chain.pem edot-collector-prod.crt
edot-collector-prod.crt: OK
```

**4. Format de la clé** (première ligne) :

```bash
$ head -1 edot-collector-prod.key
-----BEGIN PRIVATE KEY-----
```

| Première ligne | Que faire |
|---|---|
| `-----BEGIN PRIVATE KEY-----` | ✅ parfait (PKCS#8 non chiffrée) |
| `-----BEGIN RSA PRIVATE KEY-----` ou `-----BEGIN EC PRIVATE KEY-----` | convertir : `openssl pkey -in edot-collector-prod.key -out edot.key.pk8 && mv edot.key.pk8 edot-collector-prod.key` |
| `-----BEGIN ENCRYPTED PRIVATE KEY-----` | retirer le mot de passe : `openssl pkey -in edot-collector-prod.key -out edot.key.pk8` (le mot de passe est demandé), puis `mv` comme ci-dessus |

**5. Construire le fichier « certificat + intermédiaires »** que présentera EDOT
(le certificat client **en premier**, puis le ou les intermédiaires, **sans**
la racine) :

```bash
$ cat edot-collector-prod.crt intermediaire.pem > edot-collector-prod-chain.crt
```

S'il n'y a pas d'intermédiaire (certificat signé directement par la racine),
utilisez simplement `edot-collector-prod.crt`.

### A2.5 Variante : faire émettre le certificat par cert-manager

Ce dépôt installe cert-manager (`bootstrap/06-cert-manager-subscription.yaml`).
Si l'équipe Kafka accepte une CA disponible comme `ClusterIssuer` (par exemple
un émetteur `vault` ou `ca`), cert-manager peut **émettre et renouveler
automatiquement** le certificat client :

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: edot-kafka-client
  namespace: edot
spec:
  secretName: edot-kafka-client-tls        # clés tls.crt, tls.key, ca.crt
  issuerRef:
    kind: ClusterIssuer
    name: TODO_ISSUER_RECONNU_PAR_KAFKA
  # DN imposé à l'identique (cert-manager ≥ 1.13) : c'est lui que lisent les ACL.
  literalSubject: "CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR"
  usages:
    - client auth
    - digital signature
    - key encipherment
  duration: 2160h      # 90 jours
  renewBefore: 720h    # renouvellement à J-30
  privateKey:
    algorithm: RSA
    size: 3072
    encoding: PKCS8
    rotationPolicy: Always
```

Attention dans ce cas : la clé `ca.crt` de ce Secret est la CA **de l'émetteur**
(CA du client), **pas forcément** celle des brokers. Il faut alors monter
`tls.crt`/`tls.key` depuis `edot-kafka-client-tls` et garder `ca.crt` (CA des
brokers) depuis `edot-kafka-credentials`. Le reste de ce tutoriel suit la voie
**Vault**, plus simple à expliquer ; adaptez les volumes du §A6 si vous
choisissez cert-manager.

---

## A3. Faire créer les ACL par l'équipe Kafka

### A3.1 Les droits strictement nécessaires

EDOT est un **consommateur** : il lit, il n'écrit jamais.

| Ressource | Nom | Opérations | Pourquoi |
|---|---|---|---|
| Topic | `otlp-logs-prod` (ou un préfixe) | `Read`, `Describe` | lire les messages, connaître les partitions |
| Group | `edot-logs-prod` | `Read` | rejoindre le groupe et enregistrer les offsets (`Read` inclut `Describe`) |

Rien d'autre : **aucun** `Write`, aucun droit `Cluster`, aucun `Create`. Le
mode « membre statique » utilisé par EDOT (`group_instance_id`) ne demande pas
de droit supplémentaire.

### A3.2 Le message type à envoyer à l'équipe Kafka

> Bonjour,
>
> Pour l'EDOT Collector (consommateur de logs, OpenShift namespace `edot`),
> merci de créer les ACL suivantes pour le principal correspondant au
> certificat client de DN
> `CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR`
> (merci de me confirmer la forme exacte du principal selon vos
> `ssl.principal.mapping.rules`) :
>
> - Topic `otlp-logs-prod` (LITERAL) : `Read`, `Describe` ;
> - Group `edot-logs-prod` (LITERAL) : `Read`.
>
> Si vos ACL sont restreintes par hôte (`--allow-host`), l'IP source sera
> l'EgressIP du namespace `edot` : `<IP>` (ou, à défaut, les IP des workers :
> `<liste>`).
>
> Merci de me confirmer également : la CA qui a signé ce certificat client est
> bien présente dans le truststore des brokers, et la liste des advertised
> listeners du listener SSL.

### A3.3 Les commandes correspondantes (pour information, exécutées par l'équipe Kafka)

```bash
# admin.properties : configuration d'un compte administrateur Kafka (non fournie ici)

kafka-acls.sh --bootstrap-server kafka-b1.corp.example:9093 \
  --command-config admin.properties \
  --add --allow-principal 'User:CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR' \
  --operation Read --operation Describe \
  --topic otlp-logs-prod

kafka-acls.sh --bootstrap-server kafka-b1.corp.example:9093 \
  --command-config admin.properties \
  --add --allow-principal 'User:CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR' \
  --operation Read \
  --group edot-logs-prod

# Vérification :
kafka-acls.sh --bootstrap-server kafka-b1.corp.example:9093 \
  --command-config admin.properties \
  --list --principal 'User:CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR'
```

Variantes utiles :

- plusieurs topics de même préfixe : `--topic otlp-logs- --resource-pattern-type prefixed` ;
- restriction par hôte : ajouter `--allow-host <EgressIP>` ;
- les apostrophes `'…'` sont **indispensables** : le DN contient des virgules
  et parfois des espaces.

---

## A4. Ranger le certificat dans Vault

Le Secret Kubernetes `edot-kafka-credentials` contiendra désormais **trois
clés** :

| Clé | Contenu | Fichier dans le pod |
|---|---|---|
| `ca.crt` | CA des **brokers** (PEM, chaîne complète si intermédiaire) | `/etc/edot/kafka/ca.crt` |
| `tls.crt` | certificat client **+ intermédiaires** (§A2.4, étape 5) | `/etc/edot/kafka/tls.crt` |
| `tls.key` | clé privée PKCS#8 non chiffrée | `/etc/edot/kafka/tls.key` |

Il n'y a **pas** de clés `username` / `password` : pas de SASL.

```bash
$ export VAULT_ADDR=https://<votre-vault>:8200
$ vault login -method=<votre méthode>        # oidc, userpass, ldap…

$ cd ~/edot-certs
$ vault kv put secret/kubernetes/edot/edot-kafka-credentials \
    ca.crt=@kafka-brokers-ca.pem \
    tls.crt=@edot-collector-prod-chain.crt \
    tls.key=@edot-collector-prod.key
```

- La syntaxe `clé=@fichier` lit le **contenu** du fichier : rien n'apparaît
  dans l'historique du shell.
- `vault kv put` **remplace toutes les clés** du secret : c'est voulu ici
  (suppression de `username`/`password`). Pour modifier une seule clé plus
  tard : `vault kv patch`.

Contrôlez **sans afficher la clé privée** :

```bash
# Noms des clés présentes
$ vault kv get -format=json secret/kubernetes/edot/edot-kafka-credentials | jq -r '.data.data | keys[]'
ca.crt
tls.crt
tls.key

# Le certificat stocké est le bon
$ vault kv get -field=tls.crt secret/kubernetes/edot/edot-kafka-credentials \
    | openssl x509 -noout -subject -enddate -nameopt RFC2253
```

Supprimez ensuite vos copies locales de la clé privée (Vault est désormais la
référence) :

```bash
$ shred -u ~/edot-certs/*.key ~/edot-certs/*.p12 2>/dev/null; ls ~/edot-certs
```

---

## A5. Adapter l'ExternalSecret Kafka

> ✅ **Déjà en place dans le dépôt** — section explicative : rien à modifier,
> sauf si vous changez de chemin Vault.

Fichier : `external-secrets/edot-kafka-credentials.externalsecret.yaml`.

L'ExternalSecret dit à ESO quelles clés recopier depuis Vault. ESO réécrit le
Secret **entier** : une clé absente de la liste disparaît du Secret. Le bloc
`data:` contient les trois clés du mTLS :

```yaml
  data:
    - secretKey: ca.crt
      remoteRef:
        key: kubernetes/edot/edot-kafka-credentials
        property: ca.crt
    - secretKey: tls.crt
      remoteRef:
        key: kubernetes/edot/edot-kafka-credentials
        property: tls.crt
    - secretKey: tls.key
      remoteRef:
        key: kubernetes/edot/edot-kafka-credentials
        property: tls.key
```

> `key: kubernetes/edot/…` (sans `secret/` ni `data/`) est normal : le
> `SecretStore` définit déjà `path: "secret"` et `version: "v2"`.

---

## A6. Adapter `values.yaml` (variables et volumes)

> ✅ **Déjà en place dans le dépôt** — section explicative.

### A6.1 Aucune variable d'identifiant Kafka

Dans `extraEnvs`, il n'y a plus de `KAFKA_USERNAME` ni de `KAFKA_PASSWORD`.
Il ne reste que :

```yaml
extraEnvs:
  - name: ELASTICSEARCH_API_KEY
    valueFrom:
      secretKeyRef:
        name: edot-elasticsearch-credentials
        key: api-key
```

> Si elles revenaient (fusion d'une ancienne branche…), le pod resterait
> bloqué en `CreateContainerConfigError` (« couldn't find key username in
> Secret »). `tests/validate-config.sh` le détecte.

### A6.2 Monter le certificat et la clé

Dans `extraVolumes`, le volume `kafka-tls` projette les trois clés :

```yaml
  # CA des brokers + certificat client + clé privée (mTLS Kafka).
  # defaultMode 0440 et NON 0400 : sur OpenShift le conteneur tourne avec un
  # UID arbitraire ; les fichiers d'un volume Secret appartiennent à root et
  # au groupe fsGroup injecté par la SCC. Seule la lecture par le GROUPE
  # permet au Collector de lire sa clé.
  - name: kafka-tls
    secret:
      secretName: edot-kafka-credentials
      defaultMode: 0440
      items:
        - key: ca.crt
          path: ca.crt
        - key: tls.crt
          path: tls.crt
        - key: tls.key
          path: tls.key
```

Et dans `extraVolumeMounts` :

```yaml
  - name: kafka-tls
    mountPath: /etc/edot/kafka
    readOnly: true
```

> **Pourquoi pas `0400` ?** C'est le réflexe habituel pour une clé privée, mais
> ici le propriétaire des fichiers est `root` et le processus n'est **pas**
> root : avec `0400`, EDOT obtiendrait `permission denied` sur `tls.key`.

---

## A7. Adapter `values-kafka.yaml`

### A7.1 Le bloc de sécurité

> ✅ **Déjà en place dans le dépôt** — section explicative. Ce qu'il vous
> reste à renseigner est au §A7.2.

La section « Sécurité » de `values-kafka.yaml` contient (commentaires abrégés) :

```yaml
      # ── Sécurité : KAFKA_SECURITY_PROTOCOL = SSL (TLS mutuel) ───────────
      # Le broker authentifie EDOT par son certificat client ; le principal
      # Kafka est dérivé du DN du certificat (ACL sur le CN). AUCUN bloc
      # `auth` : pas de SASL.
      tls:
        # CA des BROKERS (vérification du certificat serveur).
        ca_file: /etc/edot/kafka/ca.crt
        # Certificat client (+ intermédiaires) et clé privée PKCS#8 non chiffrée.
        cert_file: /etc/edot/kafka/tls.crt
        key_file: /etc/edot/kafka/tls.key
        # Vérification du certificat et du nom d'hôte : TOUJOURS active.
        insecure: false
        insecure_skip_verify: false
        min_version: "1.2"
        # Relecture périodique des fichiers (rotation sans redémarrage).
        reload_interval: 1h
```

Et **aucun** bloc `auth:` (l'ancien bloc `auth.sasl` avec
`username`/`password`/`mechanism` a été supprimé ; `tests/validate-config.sh`
refuse sa réapparition).

Pourquoi c'est suffisant : dans le receiver Kafka, **la présence du bloc `tls`
active TLS**, et `cert_file`/`key_file` font présenter le certificat client.
Sans bloc `auth`, aucun SASL n'est tenté : c'est exactement
`security.protocol=SSL`.

> ⚠️ **Ne mettez jamais** `insecure_skip_verify: true` « pour tester » : vous
> désactiveriez la vérification du broker. Si la vérification échoue, c'est
> qu'il y a un vrai problème à corriger (§A11).

### A7.2 Les autres paramètres à renseigner

```yaml
      brokers:
        - kafka-b1.corp.example:9093
        - kafka-b2.corp.example:9093
        - kafka-b3.corp.example:9093
      ...
      group_id: edot-logs-prod
      ...
      logs:
        topics:
          - otlp-logs-prod
        encoding: otlp_json
```

Règles :

- **Noms DNS, pas d'IP** : le nom configuré doit figurer dans le SAN du
  certificat du broker (sinon `certificate is valid for …, not …`).
- Le **port** est celui du listener **SSL** (mTLS), qui est souvent **différent**
  du port SASL_SSL. Demandez-le à l'équipe Kafka.
- `group_id` : un groupe **dédié** à EDOT, **jamais** celui d'un autre
  consommateur (Logstash…), et **identique** au nom autorisé par l'ACL.
- `initial_offset: latest` (déjà configuré) : au tout premier démarrage, EDOT
  commence aux **nouveaux** messages. Mettez `earliest` uniquement si vous
  voulez indexer tout l'historique encore présent dans le topic.

---

## A8. Ouvrir le réseau vers Kafka

Trois barrières successives doivent laisser passer la connexion :

```
pod EDOT ──[1] NetworkPolicy──▶ nœud OpenShift ──[2] EgressIP/NAT──▶ ──[3] pare-feu──▶ broker
```

### A8.1 Les adresses à autoriser : bootstrap **et** advertised listeners

EDOT contacte d'abord un des `brokers` configurés, reçoit la liste des brokers
du cluster (**advertised listeners**), puis se connecte **directement à chacun
d'eux**. Toutes ces adresses doivent être :

- résolues par le DNS d'OpenShift ;
- autorisées par la NetworkPolicy, le pare-feu, et d'éventuelles ACL par hôte ;
- présentes dans le certificat de **chaque** broker.

Faites la liste avec l'équipe Kafka. Le §A10 permet de la vérifier.

### A8.2 La NetworkPolicy

Fichier : `network-policies/allows/allow-egress-kafka.yaml`. Résolvez les
noms, puis renseignez les CIDR et le port :

```bash
$ for h in kafka-b1.corp.example kafka-b2.corp.example kafka-b3.corp.example; do getent hosts $h; done
10.20.30.11  kafka-b1.corp.example
10.20.30.12  kafka-b2.corp.example
10.20.30.13  kafka-b3.corp.example
```

```yaml
  egress:
    - to:
        - ipBlock:
            cidr: 10.20.30.11/32
        - ipBlock:
            cidr: 10.20.30.12/32
        - ipBlock:
            cidr: 10.20.30.13/32
      ports:
        - protocol: TCP
          port: 9093
```

(Un CIDR plus large, p. ex. `10.20.30.0/28`, est possible s'il couvre
**uniquement** les brokers.) Si les brokers annoncent un **autre port** que le
bootstrap, ajoutez-le dans `ports`.

> Une NetworkPolicy filtre par **IP**. Si les IP des brokers changent, il faut
> mettre à jour ce fichier. Sur OpenShift, un `EgressFirewall` (OVN-Kubernetes)
> permet de filtrer par **nom DNS** — à voir avec l'équipe plateforme.

### A8.3 Pare-feu et IP source

Les pare-feu (et les ACL Kafka restreintes par hôte) voient :

- l'**EgressIP** du namespace `edot`, si l'équipe plateforme en a attribué une
  (`oc get egressip`) — recommandé ;
- sinon l'IP du **nœud** qui héberge le pod — donc potentiellement **tous** les
  workers (`oc get nodes -o wide`).

---

## A9. Adapter les tests du dépôt

> ✅ **Déjà en place dans le dépôt.** Les tests vérifient désormais le mTLS :

| Fichier | Ce qu'il contrôle |
|---|---|
| `tests/validate-config.sh` | clés de Secret `ca.crt`/`tls.crt`/`tls.key` ; `cert_file`/`key_file` présents ; **aucun** bloc `auth` ; volume en `0440` lecture seule ; `elastic-agent otel validate` |
| `tests/values-test.yaml` | valeurs fictives du banc (plus de bloc SASL) |
| `tests/network-diagnostics.sh kafka` | sujet, usage `clientAuth`, correspondance clé/certificat et format PKCS#8 du certificat du Secret ; mTLS sur chaque broker (bootstrap et annoncés) ; ACL — c'est l'automatisation du §A10 |
| `tests/functional/run-tests.py` | banc Docker : Kafka à certificat client obligatoire et ACL sur le DN, chaîne avec intermédiaire, refus attendus, puis filtrage et résilience |

À lancer avant chaque PR :

```bash
$ edot-collector/tests/validate-config.sh           # helm, docker, kubectl, python3
$ edot-collector/tests/functional/run-tests.py all  # banc local, ~20 min
```

---

## A10. Vérifier la connexion Kafka

On vérifie **avant** de démarrer EDOT, depuis un pod de diagnostic placé dans
le namespace `edot`, avec **les mêmes secrets, les mêmes NetworkPolicies et la
même IP de sortie** qu'EDOT. On procède par couches : si une couche échoue,
inutile de tester la suivante.

> **Raccourci** : `edot-collector/tests/network-diagnostics.sh kafka` exécute
> automatiquement les étapes 2 à 9 ci-dessous (pods éphémères, supprimés à la
> fin), une fois les `TODO_*` renseignés. Les étapes manuelles restent utiles
> pour comprendre un échec et creuser un point précis.

```
 1. Secret présent  →  2. DNS  →  3. TCP  →  4. TLS serveur  →  5. TLS mutuel
 →  6. Authentification Kafka + advertised listeners  →  7. chaque broker annoncé
 →  8. ACL topic  →  9. ACL groupe  →  10. EDOT démarré : consommation réelle
```

### A10.1 Étape 1 — Le Secret est bien synchronisé

```bash
$ oc -n edot get externalsecret edot-kafka-credentials
NAME                     STORE   REFRESH INTERVAL   STATUS         READY
edot-kafka-credentials   vault   1h                 SecretSynced   True

# Noms des clés uniquement (jamais les valeurs)
$ oc -n edot get secret edot-kafka-credentials -o go-template='{{range $k,$v := .data}}{{$k}}{{"\n"}}{{end}}'
ca.crt
tls.crt
tls.key

# Le certificat (public) contenu dans le Secret
$ oc -n edot get secret edot-kafka-credentials -o jsonpath='{.data.tls\.crt}' | base64 -d \
    | openssl x509 -noout -subject -enddate -nameopt RFC2253
```

Si `READY` vaut `False` : `oc -n edot describe externalsecret edot-kafka-credentials`
(les événements donnent la cause : chemin Vault, clé absente, droits du rôle
`eso-edot`).

### A10.2 Créer le pod de diagnostic Kafka

Enregistrez ce manifeste dans un fichier temporaire, par exemple
`/tmp/edot-kafkadiag.yaml` (il ne va **pas** dans Git) :

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: edot-kafkadiag
  namespace: edot
  labels:
    # Même label qu'EDOT : le pod est soumis aux MÊMES NetworkPolicies.
    app.kubernetes.io/instance: edot-collector
    edot.diagnostic: "true"
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  activeDeadlineSeconds: 3600          # le pod s'arrête seul au bout d'1 h
  securityContext:
    runAsNonRoot: true
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: diag
      # Image officielle Apache Kafka 4.3.2 (outils kafka-*.sh), épinglée par digest.
      image: apache/kafka@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7
      command: ["sleep", "3600"]
      env:
        - name: LOG_DIR
          value: /tmp
      resources:
        requests: {cpu: 50m, memory: 256Mi}
        limits: {cpu: 500m, memory: 768Mi}
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: ["ALL"]
      volumeMounts:
        - name: tmp
          mountPath: /tmp
        - name: kafka-tls
          mountPath: /etc/edot/kafka
          readOnly: true
  volumes:
    - name: tmp
      emptyDir:
        sizeLimit: 64Mi
    - name: kafka-tls
      secret:
        secretName: edot-kafka-credentials
        defaultMode: 0440
```

Et un second pod basé sur l'**image EDOT** (qui contient `openssl`, `curl`,
`getent`), utile pour les tests réseau et TLS bas niveau. Fichier
`/tmp/edot-netdiag.yaml` :

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: edot-netdiag
  namespace: edot
  labels:
    app.kubernetes.io/instance: edot-collector
    edot.diagnostic: "true"
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  activeDeadlineSeconds: 3600
  securityContext:
    runAsNonRoot: true
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: diag
      image: docker.elastic.co/elastic-agent/elastic-otel-collector@sha256:9caf6481865156cc1ae52992be3b0596719a849501d677e36f38312e8c8ee0f1
      command: ["sleep", "3600"]
      env:
        # Injectée par le Secret, comme pour EDOT : jamais affichée.
        - name: ELASTICSEARCH_API_KEY
          valueFrom:
            secretKeyRef:
              name: edot-elasticsearch-credentials
              key: api-key
      resources:
        requests: {cpu: 50m, memory: 128Mi}
        limits: {cpu: 500m, memory: 512Mi}
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: ["ALL"]
      volumeMounts:
        - name: tmp
          mountPath: /tmp
        - name: kafka-tls
          mountPath: /etc/edot/kafka
          readOnly: true
        - name: es-ca
          mountPath: /etc/edot/elasticsearch
          readOnly: true
  volumes:
    - name: tmp
      emptyDir:
        sizeLimit: 64Mi
    - name: kafka-tls
      secret:
        secretName: edot-kafka-credentials
        defaultMode: 0440
    - name: es-ca
      secret:
        secretName: edot-elasticsearch-credentials
        defaultMode: 0440
        items:
          - key: ca.crt
            path: ca.crt
```

> Si le Secret Elasticsearch n'existe pas encore, retirez temporairement de ce
> manifeste la variable `ELASTICSEARCH_API_KEY` et le volume `es-ca` (sinon le
> pod ne démarre pas).

```bash
$ oc apply -f /tmp/edot-kafkadiag.yaml -f /tmp/edot-netdiag.yaml
$ oc -n edot wait --for=condition=Ready pod/edot-kafkadiag pod/edot-netdiag --timeout=180s
```

### A10.3 Étapes 2 à 5 — DNS, TCP, TLS serveur, TLS mutuel

Entrez dans le pod EDOT de diagnostic :

```bash
$ oc -n edot exec -it edot-netdiag -- bash
```

Puis, pour **chaque** broker de bootstrap :

```bash
diag$ B=kafka-b1.corp.example; P=9093

# 2. DNS : le nom est-il résolu depuis OpenShift ?
diag$ getent ahosts "$B" | awk '{print $1}' | sort -u
10.20.30.11

# 3. TCP : le port est-il joignable (NetworkPolicy + pare-feu) ?
diag$ timeout 5 bash -c "</dev/tcp/$B/$P" && echo "TCP OK" || echo "TCP KO"
TCP OK

# 4 et 5. TLS : vérification du broker ET présentation du certificat client
diag$ timeout 15 openssl s_client -connect "$B:$P" -servername "$B" \
        -CAfile /etc/edot/kafka/ca.crt -verify_hostname "$B" -verify_return_error \
        -cert /etc/edot/kafka/tls.crt -key /etc/edot/kafka/tls.key \
        </dev/null 2>&1 | grep -E "Verify return code|Acceptable client certificate CA names|^C=|^CN=|alert|error" 
```

Ce qu'il faut lire :

| Sortie | Signification |
|---|---|
| `Verify return code: 0 (ok)` | ✅ le certificat du broker est valide **et** porte bien le nom `$B` |
| `Acceptable client certificate CA names` suivi de lignes `CN=…` | le broker **demande** un certificat client et liste les CA qu'il accepte : celle qui a signé votre certificat doit y figurer |
| `verify error:num=20:unable to get local issuer certificate` | ❌ `ca.crt` ne contient pas la bonne CA des brokers (ou il manque un intermédiaire) |
| `verify error:num=62:hostname mismatch` | ❌ le nom `$B` n'est pas dans le SAN du certificat du broker |
| `alert unknown ca`, `alert bad certificate`, `alert certificate required` | ❌ le broker **refuse votre certificat client** (voir §A11) |

> **Subtilité TLS 1.3** : le refus du certificat client peut n'apparaître
> **qu'après** la poignée de main. `openssl s_client` peut donc afficher
> `Verify return code: 0 (ok)` alors que le broker rejettera le client. Le test
> **décisif** est l'étape 6, avec un vrai client Kafka.

Sortez du pod : `exit`.

### A10.4 Étape 6 — Authentification Kafka et liste des advertised listeners

Entrez dans le pod Kafka de diagnostic et préparez la configuration client :

```bash
$ oc -n edot exec -it edot-kafkadiag -- bash

diag$ umask 077
diag$ cat /etc/edot/kafka/tls.key /etc/edot/kafka/tls.crt > /tmp/keystore.pem
diag$ cat > /tmp/client.properties <<'EOF'
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/edot/kafka/ca.crt
ssl.keystore.type=PEM
ssl.keystore.location=/tmp/keystore.pem
ssl.endpoint.identification.algorithm=https
EOF
diag$ BOOT=kafka-b1.corp.example:9093,kafka-b2.corp.example:9093,kafka-b3.corp.example:9093
```

> Ces fichiers ne vivent que dans le `/tmp` du pod (volume `emptyDir`), qui
> disparaît avec lui.

```bash
diag$ timeout 60 /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server "$BOOT" --command-config /tmp/client.properties 2>&1 \
        | grep -E "^[^ ].*\(id: [0-9]+|Exception|ERROR" | sed "s/ -> .*//" | sort -u
kafka-b1.corp.example:9093 (id: 1 rack: null)
kafka-b2.corp.example:9093 (id: 2 rack: null)
kafka-b3.corp.example:9093 (id: 3 rack: null)
```

✅ Si vous obtenez une ligne `(id: N …)` par broker, **le mTLS fonctionne** :
le broker a accepté votre certificat. Ces lignes sont les **advertised
listeners** : ce sont les adresses auxquelles EDOT va réellement se connecter.

❌ En cas d'erreur, voir le tableau du §A11 (`SslAuthenticationException`,
`PKIX path building failed`, `Timed out waiting for a node assignment`…).

### A10.5 Étape 7 — Chaque broker annoncé est joignable

Comparez la liste obtenue avec vos `brokers` et vos NetworkPolicies. **Si un
broker annoncé porte un nom ou un port différent** (p. ex.
`kafka-node-04.internal:9094`), refaites les tests du §A10.3 pour **cette**
adresse depuis `edot-netdiag`. C'est la cause la plus fréquente d'un EDOT qui
« se connecte puis ne consomme rien ».

### A10.6 Étapes 8 et 9 — Les ACL

Toujours dans `edot-kafkadiag` :

```bash
# 8. ACL Describe sur le topic (liste des partitions et de leurs offsets)
diag$ timeout 60 /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server "$BOOT" --command-config /tmp/client.properties \
        --topic otlp-logs-prod
otlp-logs-prod:0:1532045
otlp-logs-prod:1:1529870
otlp-logs-prod:2:1530112
```

✅ Une ligne par partition (`topic:partition:offset`). Notez le **nombre de
partitions** : au-delà de ce nombre, des réplicas EDOT supplémentaires
resteraient inactifs.

❌ `Could not match any topic-partitions with the specified filters` → **Kafka
masque le topic** : soit il n'existe pas, soit le principal n'a **pas** l'ACL
`Describe` (ou le principal est mal écrit). Ce n'est **pas** une erreur
d'autorisation explicite : sans `Describe`, Kafka fait comme si le topic
n'existait pas (constaté sur le banc, Kafka 4.3). Selon l'outil, vous pouvez
aussi voir `TopicAuthorizationException`.

```bash
# 9. ACL sur le consumer group
diag$ timeout 60 /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server "$BOOT" --command-config /tmp/client.properties \
        --describe --group edot-logs-prod
```

| Résultat | Signification |
|---|---|
| `Consumer group 'edot-logs-prod' does not exist.` | ✅ **normal avant le premier démarrage d'EDOT** : l'accès au groupe est autorisé, le groupe n'existe simplement pas encore |
| un tableau `GROUP TOPIC PARTITION CURRENT-OFFSET …` | ✅ le groupe existe déjà (EDOT a déjà tourné) |
| `GroupAuthorizationException` | ❌ ACL sur le groupe absente, ou `group_id` différent du nom autorisé |

> ⚠️ **Ne lancez jamais** `kafka-console-consumer.sh --group edot-logs-prod`
> ni `kafka-consumer-groups.sh --reset-offsets` sur le groupe d'EDOT : vous
> voleriez des partitions à EDOT ou déplaceriez sa position de lecture. Les
> commandes ci-dessus sont **en lecture seule**.

Sortez (`exit`) et **supprimez les pods de diagnostic** quand vous avez fini :

```bash
$ oc -n edot delete pod edot-kafkadiag edot-netdiag --ignore-not-found
```

### A10.7 Étape 10 — EDOT démarré : la consommation réelle

Une fois EDOT déployé (Partie C) :

**a) Les pods sont prêts :**

```bash
$ oc -n edot get pods -l app.kubernetes.io/instance=edot-collector
NAME               READY   STATUS    RESTARTS   AGE
edot-collector-0   1/1     Running   0          3m
edot-collector-1   1/1     Running   0          3m
```

**b) Aucune erreur TLS ou d'autorisation dans les logs** (logs JSON) :

```bash
$ oc -n edot logs edot-collector-0 --since=15m \
    | grep -iE '"level":"(warn|error)"|x509|tls:|AUTHORIZATION_FAILED|certificate' | tail -20
```

Rien à l'écran = bon signe.

**c) Les métriques internes** (le serveur de métriques écoute sur l'IP du pod,
pas sur `localhost`) :

```bash
$ oc -n edot exec edot-collector-0 -- bash -c 'curl -s "http://$MY_POD_IP:8888/metrics"' \
    | grep -E '^otelcol_(kafka_broker_connects|kafka_receiver_messages|kafka_receiver_offset_lag|receiver_accepted_log_records)'
```

| Métrique | Attendu |
|---|---|
| `otelcol_kafka_broker_connects{…outcome="success"…}` | > 0 ; `node_id="seed_0"` = bootstrap, `node_id="1"`, `"2"`… = brokers annoncés |
| `otelcol_kafka_broker_connects{…outcome="failure"…}` | absente, ou qui **n'augmente plus** |
| `otelcol_kafka_receiver_messages` | augmente si des messages arrivent sur le topic |
| `otelcol_receiver_accepted_log_records` | augmente : les logs entrent dans le pipeline |
| `otelcol_kafka_receiver_offset_lag` | faible et stable (retard de lecture par partition) |

**d) Vu par Kafka** : relancez la commande de l'étape 9 (pod de diagnostic) :

```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID                            HOST           CLIENT-ID
edot-logs-prod  otlp-logs-prod  0          1532101         1532104         3    edot-edot-collector-0-6f1c…            /10.20.40.5    edot-edot-collector-0
edot-logs-prod  otlp-logs-prod  1          1529902         1529902         0    edot-edot-collector-1-2b9a…            /10.20.40.5    edot-edot-collector-1
edot-logs-prod  otlp-logs-prod  2          1530150         1530151         1    edot-edot-collector-0-6f1c…            /10.20.40.5    edot-edot-collector-0
```

✅ Chaque partition a un `CONSUMER-ID` (`edot-collector-0` ou `-1`), le `LAG`
reste petit. La colonne `HOST` montre l'**IP source vue par Kafka** (EgressIP ou
IP du nœud) : utile pour les ACL par hôte et les pare-feu.

**e) Dans le tableau de bord Grafana** fourni (`dashboards/edot-collector.json`) :
débit consommé, lag par partition, connexions aux brokers.

---

## A11. Dépannage Kafka

### A11.1 Messages d'erreur et causes

| Message (EDOT = Go, ou outils Kafka = Java) | Cause | Correction |
|---|---|---|
| `x509: certificate signed by unknown authority` / `PKIX path building failed` | `ca.crt` ne contient pas la CA (ou l'intermédiaire) des **brokers** | demander la chaîne complète à l'équipe Kafka, mettre à jour Vault (§A4) |
| `x509: certificate is valid for a, b, not c` / `No name matching c found` | le nom configuré n'est pas dans le SAN du certificat du broker | utiliser le nom présent dans le certificat (jamais une IP si le certificat porte des noms) |
| `remote error: tls: unknown certificate authority` / `Received fatal alert: unknown_ca` | les brokers ne font pas confiance à la CA de **votre** certificat client | équipe Kafka : ajouter la CA client au truststore des brokers ; ou vérifier que `tls.crt` contient bien les intermédiaires |
| `remote error: tls: bad certificate` / `certificate_unknown` | certificat client refusé : EKU `clientAuth` manquant, certificat expiré ou révoqué, chaîne incomplète | contrôles du §A2.4 ; demander la raison exacte dans les logs du broker |
| `failed authentication due to: Failed to process post-handshake messages` (outils Kafka, TLS 1.3) | le broker **refuse le certificat client** après la poignée de main : aucun certificat, CA du client inconnue des brokers, certificat expiré ou sans `clientAuth` (constaté sur le banc pour « sans certificat » et « CA inconnue ») | vérifier le keystore (`ssl.keystore.*`), puis les causes des lignes `unknown_ca` / `bad_certificate` ci-dessous |
| `remote error: tls: certificate required` / `Received fatal alert: certificate_required` | **aucun** certificat client présenté | `cert_file`/`key_file` absents de `values-kafka.yaml`, ou fichiers vides |
| `remote error: tls: expired certificate` | certificat client expiré | renouveler ([annexe 1](#annexe-1--rotation-des-secrets)) |
| `tls: private key does not match public key` | `tls.crt` et `tls.key` ne vont pas ensemble | contrôle n° 2 du §A2.4 |
| `tls: failed to find any PEM data` / `failed to parse private key` | clé chiffrée, mauvais format, ou fichier vide | convertir en PKCS#8 non chiffrée (§A2.4, contrôle n° 4) |
| `open /etc/edot/kafka/tls.key: permission denied` | `defaultMode` du volume à `0400` | `0440` (§A6.2) |
| `CreateContainerConfigError … couldn't find key username` | `KAFKA_USERNAME`/`KAFKA_PASSWORD` encore dans `extraEnvs` | les retirer (§A6.1) |
| `Could not match any topic-partitions` (outils Kafka) ; EDOT connecté mais aucune partition assignée | ACL `Describe` absente : Kafka **masque** le topic | §A3 ; principal exact (§A11.2) |
| `TOPIC_AUTHORIZATION_FAILED` / `TopicAuthorizationException` | ACL topic absente, **principal mal écrit**, mauvais nom de topic, restriction par hôte | §A3 ; demander à l'équipe Kafka le principal exact vu par le broker (§A11.2) |
| `GROUP_AUTHORIZATION_FAILED` / `GroupAuthorizationException` | ACL groupe absente ou `group_id` différent | aligner `group_id` et l'ACL |
| `SASL authentication failed` / `Unexpected Kafka request of type SaslHandshake` | un bloc `auth.sasl` traîne encore (ou le port est celui du listener SASL) | supprimer `auth` (§A7.1), vérifier le port |
| `no such host` | DNS d'OpenShift ne résout pas le nom | zone DNS / forwarders (équipe plateforme) |
| `i/o timeout`, `connection refused`, `Timed out waiting for a node assignment` | NetworkPolicy, pare-feu, EgressIP non autorisée, ou **broker annoncé injoignable** | §A10.3 à §A10.5, §A8 |
| connexion OK au bootstrap puis plus rien | un **advertised listener** n'est pas joignable ou mal nommé | §A10.5 |

### A11.2 Ce que l'équipe Kafka peut regarder de son côté

Côté brokers, deux journaux sont précieux :

- **journal du serveur** : `Failed authentication with /10.20.40.5 (SSL handshake failed)` → le problème est TLS (certificat, CA) ;
- **journal de l'autorisateur** (`kafka-authorizer.log`, si activé) :
  ```
  Principal = User:CN=edot-collector-prod,OU=Observabilite,O=MaSociete,C=FR is Denied operation = READ
  from host = 10.20.40.5 on resource = Topic:LITERAL:otlp-logs-prod
  ```
  → donne le **principal exact** et l'**IP source** vus par Kafka : comparez-les
  caractère par caractère avec les ACL.

---

# Partie B — Elasticsearch

## B1. Comment EDOT écrit dans Elasticsearch

- EDOT envoie des requêtes **`_bulk`** en HTTPS, compressées (`gzip`), par lots
  de 1 à 5 Mo (ou toutes les 5 s).
- Il s'authentifie avec une **API Key** : en-tête
  `Authorization: ApiKey <valeur encodée>`.
- Les documents vont dans le **data stream**
  `logs-<dataset>.otel-<namespace>` (mapping natif OpenTelemetry, `mapping.mode: otel`).
  Le suffixe `.otel` est **ajouté automatiquement** au dataset : dataset `app`
  → `logs-app.otel-prod`.
- Le data stream est **créé automatiquement** au premier document, grâce aux
  templates `logs-*.otel-*` intégrés à Elasticsearch. Ce dépôt ne crée **rien**
  dans Elasticsearch.
- Avant l'envoi, les lots passent par une **file d'attente persistante** sur
  disque (PVC). Si Elasticsearch est indisponible, la file se remplit, puis EDOT
  **arrête de lire Kafka** : rien n'est perdu tant que les messages restent dans
  Kafka.
- La **découverte des nœuds est désactivée** : EDOT ne contacte **que** les URL
  configurées (important derrière un load balancer, une Route OpenShift ou un
  pare-feu).

## B2. Récupérer l'autorité de certification d'Elasticsearch

EDOT vérifie le certificat d'Elasticsearch avec le fichier
`/etc/edot/elasticsearch/ca.crt`, et **uniquement** avec lui : le magasin de
certificats du système n'est **pas** utilisé. Il faut donc y mettre la bonne CA,
**même si** le certificat est émis par une autorité publique.

| Situation | Contenu de `ca.crt` |
|---|---|
| Certificat émis par la PKI d'entreprise | la racine (et les intermédiaires) de la PKI, fournis par l'équipe Elasticsearch |
| Cluster ECK avec certificat **auto-signé d'ECK** | `oc -n <ns-elastic> get secret <cluster>-es-http-certs-public -o jsonpath='{.data.ca\.crt}' \| base64 -d` |
| Certificat public (p. ex. Let's Encrypt, comme le cluster ECK de ce dépôt derrière sa Route `passthrough`) | la **racine publique** correspondante (p. ex. ISRG Root X1 / X2) |
| Reverse proxy / load balancer qui termine TLS | la CA du certificat **du proxy**, pas celle d'Elasticsearch |

Pour voir **quel** émetteur présente le serveur (à titre d'information) :

```bash
$ openssl s_client -connect es-prod.corp.example:9200 -servername es-prod.corp.example \
    -showcerts </dev/null 2>/dev/null | grep -E '^ *[0-9] s:|^ *i:'
```

> Ne vous contentez pas de copier ce que le serveur présente : obtenez la CA
> par un canal de confiance (équipe Elasticsearch, PKI). Sinon, vous feriez
> confiance à n'importe quel serveur placé sur le chemin.

Enregistrez-la dans `~/edot-certs/es-ca.pem` et contrôlez-la :

```bash
$ openssl x509 -in ~/edot-certs/es-ca.pem -noout -subject -enddate
```

## B3. Créer l'API Key

### B3.1 Vérifier la version d'Elasticsearch

Dans Kibana → **Dev Tools** :

```
GET /
```

Regardez `version.number`. La configuration a été validée avec 9.x (voir §0.2,
point 5).

### B3.2 Créer la clé avec les privilèges minimaux

Toujours dans Dev Tools, connecté avec un compte autorisé à créer des API Keys :

```
POST /_security/api_key
{
  "name": "edot-collector-prod",
  "expiration": "180d",
  "role_descriptors": {
    "edot_logs_writer": {
      "cluster": ["monitor"],
      "indices": [
        {
          "names": ["logs-app.otel-prod"],
          "privileges": ["auto_configure", "create_doc"]
        }
      ]
    }
  },
  "metadata": {
    "application": "edot-collector",
    "environment": "prod",
    "owner": "<votre équipe>"
  }
}
```

| Privilège | Pourquoi |
|---|---|
| `monitor` (cluster) | EDOT lit la version d'Elasticsearch au démarrage |
| `auto_configure` (index) | créer le data stream au premier document |
| `create_doc` (index) | écrire des documents (un data stream n'accepte que des créations) |

Points d'attention :

- **`names` doit contenir le nom avec `.otel`** : `logs-app.otel-prod`, pas
  `logs-app-prod`. C'est l'erreur n° 1 (403 sur chaque document).
- Si des applications productrices fixent elles-mêmes leur
  `data_stream.dataset`, utilisez un motif : `"logs-*.otel-prod"`.
- Les privilèges d'une API Key sont **l'intersection** de ceux demandés et de
  ceux du **compte qui la crée** : si ce compte n'a pas `create_doc` sur
  `logs-app.otel-prod`, la clé ne l'aura pas non plus.
- `expiration` : planifiez la rotation **avant** l'échéance ([annexe 1](#annexe-1--rotation-des-secrets)).

Réponse :

```json
{
  "id": "VuaCfGcBCdbkQm-e5aOx",
  "name": "edot-collector-prod",
  "expiration": 1795000000000,
  "api_key": "ui2lp2axTNmsyakw9tvNnw",
  "encoded": "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw=="
}
```

**La valeur à conserver est `encoded`** (c'est `base64(id:api_key)`).
Elle n'est affichée **qu'une seule fois** : copiez-la immédiatement vers Vault
(§B4). Ne la collez ni dans un ticket, ni dans un chat, ni dans Git.

### B3.3 Contrôler la clé (côté Kibana)

```
GET /_security/api_key?name=edot-collector-prod
```

Vérifiez `invalidated: false` et la date d'`expiration`.

## B4. Ranger l'API Key et la CA dans Vault

Le Secret `edot-elasticsearch-credentials` a deux clés : `api-key` et `ca.crt`.
L'ExternalSecret correspondant (`external-secrets/edot-elasticsearch-credentials.externalsecret.yaml`)
est **déjà correct** : rien à y modifier.

Saisie **masquée** de l'API Key (elle n'apparaît ni à l'écran ni dans
l'historique, et sans retour à la ligne final) :

```bash
$ read -rs ES_API_KEY            # collez la valeur "encoded", puis Entrée
$ printf '%s' "$ES_API_KEY" | vault kv put secret/kubernetes/edot/edot-elasticsearch-credentials \
    api-key=- \
    ca.crt=@$HOME/edot-certs/es-ca.pem
$ unset ES_API_KEY
```

(`api-key=-` signifie « lire la valeur sur l'entrée standard ».)

Contrôle sans afficher la clé :

```bash
$ vault kv get -format=json secret/kubernetes/edot/edot-elasticsearch-credentials | jq -r '.data.data | keys[]'
api-key
ca.crt
$ vault kv get -field=api-key secret/kubernetes/edot/edot-elasticsearch-credentials | wc -c
56            # longueur seulement ; pas de retour à la ligne final attendu
```

Puis côté OpenShift :

```bash
$ oc -n edot get externalsecret edot-elasticsearch-credentials
# READY doit être True (ou forcer une resynchronisation immédiate :)
$ oc -n edot annotate externalsecret edot-elasticsearch-credentials force-sync=$(date +%s) --overwrite
```

## B5. Adapter `values-elasticsearch.yaml`

```yaml
      endpoints:
        - https://es-prod.corp.example:9200
```

- **Une seule URL** si Elasticsearch est derrière un load balancer, un reverse
  proxy ou une **Route OpenShift** (p. ex.
  `https://elastic-prod.apps.cluster.example.com:443`). Plusieurs URL (une par
  nœud) sont utilisées à tour de rôle.
- Toujours `https://`, toujours un **nom** présent dans le certificat.
- Ne touchez pas à `discover` (désactivé), `api_key: ${env:ELASTICSEARCH_API_KEY}`
  ni au bloc `tls` (déjà correct : `ca_file: /etc/edot/elasticsearch/ca.crt`).

Puis le data stream par défaut, dans `transform/routing` :

```yaml
            - set(attributes["data_stream.dataset"], "app") where attributes["data_stream.dataset"] == nil
            - set(attributes["data_stream.namespace"], "prod") where attributes["data_stream.namespace"] == nil
```

Règles de nommage : **minuscules**, sans espace, **pas de `-` dans le dataset**
(`app_metier` plutôt que `app-metier`). Résultat : `logs-app.otel-prod`, qui
doit correspondre au `names` de l'API Key (§B3.2).

> **Proxy d'entreprise** : si la sortie vers Elasticsearch doit passer par un
> proxy HTTP(S), ajoutez `HTTPS_PROXY` et `NO_PROXY` dans `extraEnvs`
> (`values.yaml`) et ouvrez la NetworkPolicy vers le **proxy** au lieu
> d'Elasticsearch.

## B6. Ouvrir le réseau vers Elasticsearch

Fichier : `network-policies/allows/allow-egress-elasticsearch.yaml`.

```bash
$ getent hosts es-prod.corp.example
10.20.50.20  es-prod.corp.example
```

```yaml
  egress:
    - to:
        - ipBlock:
            cidr: 10.20.50.20/32
      ports:
        - protocol: TCP
          port: 9200
```

Et, comme pour Kafka, demandez l'ouverture de flux **EgressIP (ou IP des
workers) → Elasticsearch, TCP 9200** (§A8.3).

> Si Elasticsearch est le cluster ECK **de ce même OpenShift**, joint par sa
> Route : l'IP de destination est celle du **routeur OpenShift** (ingress), et
> selon la plateforme une règle `ipBlock` peut ne pas suffire (routeur en
> `hostNetwork`). Validez avec l'équipe plateforme et testez avec le pod de
> diagnostic (§B7.1).

## B7. Vérifier la connexion Elasticsearch

### B7.1 Avant de démarrer EDOT : depuis le pod de diagnostic

Recréez le pod `edot-netdiag` du §A10.2 (avec la variable `ELASTICSEARCH_API_KEY`
et le volume `es-ca`), puis :

```bash
$ oc -n edot exec -it edot-netdiag -- bash

diag$ ES=https://es-prod.corp.example:9200
diag$ H="es-prod.corp.example"; P=9200
diag$ CA=/etc/edot/elasticsearch/ca.crt
```

**1. DNS et TCP :**

```bash
diag$ getent ahosts "$H" | awk '{print $1}' | sort -u
diag$ timeout 5 bash -c "</dev/tcp/$H/$P" && echo "TCP OK" || echo "TCP KO"
```

**2. TLS (certificat et nom) :**

```bash
diag$ timeout 10 openssl s_client -connect "$H:$P" -servername "$H" -CAfile "$CA" \
        -verify_hostname "$H" -verify_return_error </dev/null 2>&1 | grep "Verify return code"
Verify return code: 0 (ok)
```

**3. L'API Key est acceptée, et Elasticsearch répond :**

```bash
diag$ curl -sS --cacert "$CA" -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" "$ES/" \
        | grep -E '"number"|"cluster_name"'
    "cluster_name" : "prod",
      "number" : "9.5.6",
```

> N'utilisez **jamais** `curl -v` ici : il afficherait l'en-tête
> `Authorization` et donc la clé.

**4. Qui suis-je ?**

```bash
diag$ curl -sS --cacert "$CA" -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
        "$ES/_security/_authenticate?pretty" | grep -E '"authentication_type"|"name"'
  "authentication_type" : "api_key",
    "name" : "edot-collector-prod",
```

**5. La clé a-t-elle **exactement** les privilèges nécessaires ?**

```bash
diag$ curl -sS --cacert "$CA" -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
        -H "Content-Type: application/json" -X POST "$ES/_security/user/_has_privileges?pretty" -d '{
          "cluster": ["monitor"],
          "index": [{ "names": ["logs-app.otel-prod"], "privileges": ["create_doc", "auto_configure"] }]
        }' | grep has_all_requested
  "has_all_requested" : true,
```

✅ `true` : la clé peut écrire dans `logs-app.otel-prod`.
❌ `false` : la réponse détaille le privilège manquant (`"create_doc" : false`…).

**6. (Facultatif) Elle ne peut **pas** faire plus** — principe du moindre
privilège :

```bash
diag$ curl -sS -o /dev/null -w '%{http_code}\n' --cacert "$CA" \
        -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" "$ES/_cat/indices"
403
```

Sortez et supprimez le pod : `exit`, puis
`oc -n edot delete pod edot-netdiag`.

> Le script `tests/network-diagnostics.sh elasticsearch` automatise exactement
> ces contrôles.

### B7.2 EDOT démarré : les métriques de l'exporter

```bash
$ oc -n edot exec edot-collector-0 -- bash -c 'curl -s "http://$MY_POD_IP:8888/metrics"' \
    | grep -E '^otelcol_(exporter_sent_log_records|exporter_send_failed_log_records|exporter_queue_size|elasticsearch_docs_processed)|^http_client_request_duration_count'
```

| Métrique | Attendu |
|---|---|
| `otelcol_exporter_sent_log_records{exporter="elasticsearch"}` | augmente régulièrement |
| `otelcol_exporter_send_failed_log_records` | absente ou stable |
| `otelcol_exporter_queue_size` | proche de 0 (la file se vide) |
| `otelcol_elasticsearch_docs_processed{outcome="success"}` | augmente |
| `otelcol_elasticsearch_docs_processed{outcome="failed_client"…}` | absente ; sinon des documents sont **refusés** (voir `http_response_status_code`) |
| `http_client_request_duration_count{http_response_status_code="200"}` | augmente |
| `http_client_request_duration_count{http_response_status_code="401"\|"403"}` | **absente** — sinon export bloqué (§B8) |

> Particularité importante : un **401** ou un **403** de requête est
> **retenté en silence**, **sans message dans les logs d'EDOT**. Ce sont ces
> métriques (et les alertes `EdotElasticsearchAuthRejected`,
> `EdotElasticsearchExportBlocked`) qui le révèlent.

### B7.3 Dans Elasticsearch : les données sont-elles là ?

Dans Kibana → Dev Tools (avec **votre** compte : l'API Key d'EDOT ne peut pas
lire, c'est normal) :

```
GET _data_stream/logs-app.otel-prod
```

✅ Le data stream existe ; `status` vaut `GREEN` (ou `YELLOW` sur un cluster
mono-nœud) ; le champ `template` désigne le template OpenTelemetry intégré
(nom contenant `otel`).

```
GET logs-app.otel-prod/_count
```

✅ Le compteur augmente d'un appel à l'autre.

```
GET logs-app.otel-prod/_search
{
  "size": 3,
  "sort": [{ "@timestamp": "desc" }],
  "_source": ["@timestamp", "severity_text", "body", "resource.attributes.service.name", "data_stream"]
}
```

✅ Les documents les plus récents ont un `@timestamp` de quelques secondes ou
minutes.

**Dans Kibana → Discover** : créez une *data view* `logs-*.otel-*` (champ de
temps `@timestamp`), choisissez « Last 15 minutes » : les logs doivent
apparaître.

## B8. Dépannage Elasticsearch

| Symptôme | Cause | Correction |
|---|---|---|
| `x509: certificate signed by unknown authority` | `ca.crt` ne correspond pas (proxy, ECK, CA publique non fournie) | §B2 |
| `x509: certificate is valid for …, not …` | URL avec un nom absent du certificat | utiliser le nom du certificat |
| `no such host` / `i/o timeout` / `connection refused` | DNS, NetworkPolicy, pare-feu, liste blanche côté Elasticsearch/proxy | §B6, §B7.1 étapes 1-2 |
| **401** (`http_client_request_duration_count{…="401"}`, aucun log) | clé invalide, révoquée, expirée ; valeur `api_key` stockée au lieu de `encoded` ; retour à la ligne en fin de valeur | recréer/re-stocker la clé (§B3, §B4), puis `oc -n edot rollout restart sts/edot-collector` (une variable d'environnement n'est relue qu'au démarrage) |
| **403** au niveau requête | privilège `monitor` manquant, ou refus d'un proxy | `_has_privileges` (§B7.1 étape 5) |
| **403** par document (`docs_processed{http_response_status_code="403"}`) : `unauthorized for API key … on indices [logs-app.otel-prod]` | `names` de la clé ne couvre pas le data stream (souvent : `.otel` oublié) ou dataset fixé par le producteur | corriger `names` (nouvelle clé) ou le routage |
| `… indices:admin/auto_create …` non autorisé | `auto_configure` manquant | nouvelle clé avec `auto_configure` |
| **400** par document (`failed_client`) | document refusé (mapping, conflit de type de champ) | logs EDOT (`error.type`), *failure store* du data stream s'il est activé |
| **413** | taille de requête refusée (souvent un reverse proxy) | relever la limite du proxy (≥ 10 Mo) |
| **429** | Elasticsearch saturé | retenté automatiquement ; si persistant : capacité ES |
| File (`exporter_queue_size`) qui grossit, lag Kafka qui augmente | Elasticsearch lent ou bloqué | comportement normal de protection : corriger la cause côté ES ; surveiller la rétention Kafka |
| Aucune donnée, aucune erreur | topic vide, mauvais topic, `initial_offset: latest` au premier démarrage, logs tous filtrés | `otelcol_receiver_accepted_log_records`, `otelcol_processor_filter_logs_filtered` ; test du §C2 |

---

# Partie C — Mise en service et preuve de bout en bout

## C1. Ordre de déploiement

1. **PR** qui renseigne les `TODO_*` (§A7.2, §A8.2, §B5, §B6), puis validations
   locales :
   ```bash
   $ edot-collector/tests/preflight.sh            # ne doit plus lister aucun TODO_ à renseigner
   $ edot-collector/tests/validate-config.sh
   ```
   Attention : l'étape 5a de `validate-config.sh` **exige** aujourd'hui que la
   configuration de déploiement soit refusée tant qu'il reste des `TODO_*`
   (garde-fou contre une activation accidentelle). Dans la PR qui renseigne
   les dernières valeurs, inversez ce contrôle (la configuration doit alors
   être **acceptée**).
2. **Vault alimenté** (§A4, §B4) et rôle `eso-edot` créé (§2.2).
3. **Synchronisation partielle** : Secrets et NetworkPolicies seulement, sans
   démarrer EDOT (le nom de l'Application dépend de votre déclaration, voir
   [annexe 2](#annexe-2--application-argo-cd)) :
   ```bash
   $ argocd app sync edot-collector \
       --resource external-secrets.io:SecretStore:edot/vault \
       --resource :ServiceAccount:edot/eso-vault \
       --resource external-secrets.io:ExternalSecret:edot/edot-kafka-credentials \
       --resource external-secrets.io:ExternalSecret:edot/edot-elasticsearch-credentials \
       --resource networking.k8s.io:NetworkPolicy:edot/default-deny \
       --resource networking.k8s.io:NetworkPolicy:edot/allow-dns \
       --resource networking.k8s.io:NetworkPolicy:edot/allow-egress-kafka \
       --resource networking.k8s.io:NetworkPolicy:edot/allow-egress-elasticsearch
   ```
4. **Diagnostics** : §A10.1 à §A10.6 (Kafka), §B7.1 (Elasticsearch). **Ne
   passez pas à l'étape 5 tant qu'un contrôle est rouge.**
5. **Synchronisation complète** : `argocd app sync edot-collector`.
6. **Vérifications** : §A10.7, §B7.2, §B7.3, puis **le test de bout en bout**
   (§C2).

## C2. Test de bout en bout avec un message marqué

Le but : publier dans Kafka **un** message reconnaissable, et le retrouver dans
Elasticsearch. C'est la preuve que toute la chaîne fonctionne.

**Qui publie ?** Pas EDOT : son certificat n'a **aucun droit d'écriture** (et
c'est voulu). Utilisez l'identité d'un **producteur** autorisé sur le topic
(application existante, compte de test fourni par l'équipe Kafka), idéalement
sur un **topic et un consumer group de recette** avant la production.

**1. Préparer un message OTLP JSON** (une seule ligne, horodaté maintenant) :

```bash
$ MARK="tuto-$(date +%Y%m%d%H%M%S)"; NOW="$(date +%s%N)"
$ cat > /tmp/otlp-test.json <<EOF
{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"edot-tutoriel-test"}}]},"scopeLogs":[{"scope":{"name":"edot.tutoriel","version":"1.0.0"},"logRecords":[{"timeUnixNano":"$NOW","observedTimeUnixNano":"$NOW","severityNumber":9,"severityText":"INFO","body":{"stringValue":"Message de test du tutoriel EDOT"},"attributes":[{"key":"test.marker","value":{"stringValue":"$MARK"}}]}]}]}]}
EOF
$ echo "$MARK"
tuto-20261119153000
```

Pourquoi ces choix : sévérité `INFO` (les `DEBUG` sont filtrés), un
`service.name` qui n'est pas exclu, aucune route de *healthcheck* : le message
ne sera **pas** supprimé par les filtres de `values-processing.yaml`.

**2. Le publier** avec l'identité du producteur (ACL `Write` + `Describe` sur
le topic : suffisant, y compris pour le mode idempotent par défaut des
producteurs récents — vérifié sur le banc ; avec le certificat d'EDOT, la même
commande échoue par `ClusterAuthorizationException`, ce qui est normal) (sa propre configuration
`producer.properties`, même principe que le §A10.4 avec **son** certificat) :

```bash
$ kafka-console-producer.sh --bootstrap-server kafka-b1.corp.example:9093 \
    --producer.config producer.properties --topic otlp-logs-prod < /tmp/otlp-test.json
```

**3. Le retrouver dans Elasticsearch** (Dev Tools, quelques secondes plus tard —
EDOT regroupe les envois jusqu'à 5 s) :

```
GET logs-app.otel-prod/_search
{
  "query": { "term": { "attributes.test.marker": "tuto-20261119153000" } }
}
```

✅ `hits.total.value` vaut **1** : le message a traversé Kafka → EDOT →
Elasticsearch. Vérifiez au passage `resource.attributes.service.name`,
`severity_text` et `body.text`.

❌ Aucun résultat : suivez le message à la trace avec les métriques
(`otelcol_kafka_receiver_messages` → `otelcol_receiver_accepted_log_records` →
`otelcol_processor_filter_logs_filtered` → `otelcol_exporter_sent_log_records`)
pour savoir **à quelle étape** il s'est arrêté.

## C3. Checklist finale « tout fonctionne »

**Kafka**

- [ ] `ExternalSecret edot-kafka-credentials` : `READY True`, clés `ca.crt`, `tls.crt`, `tls.key`
- [ ] Certificat client : DN exact, EKU `clientAuth`, date d'expiration notée et rappel planifié
- [ ] `openssl s_client` : `Verify return code: 0 (ok)` sur chaque broker (bootstrap **et** annoncés)
- [ ] `kafka-broker-api-versions.sh` : une ligne `(id: N)` par broker
- [ ] `kafka-get-offsets.sh` : partitions du topic listées
- [ ] `kafka-consumer-groups.sh --describe` : chaque partition a un consommateur `edot-collector-N`, lag faible
- [ ] `otelcol_kafka_broker_connects{outcome="failure"}` n'augmente pas

**Elasticsearch**

- [ ] `ExternalSecret edot-elasticsearch-credentials` : `READY True`
- [ ] `_security/_authenticate` : `authentication_type: api_key`
- [ ] `_has_privileges` : `has_all_requested: true`
- [ ] `otelcol_exporter_sent_log_records` augmente ; aucun `401`/`403` dans `http_client_request_duration_count`
- [ ] `GET _data_stream/logs-app.otel-prod` : data stream présent, documents récents

**Bout en bout et exploitation**

- [ ] Message marqué du §C2 retrouvé dans Elasticsearch
- [ ] Pods `Running`, aucun redémarrage, logs sans `warn`/`error` récurrents
- [ ] Tableau de bord Grafana alimenté, alertes `Edot*` chargées dans Prometheus
- [ ] Pods de diagnostic supprimés, copies locales des clés détruites

---

# Annexes

## Annexe 1 — Rotation des secrets

### Certificat client Kafka (avant son expiration)

1. Nouvelle CSR **avec le même DN** (§A2.2) : les ACL restent valables sans
   intervention de l'équipe Kafka.
2. Contrôles du §A2.4, puis `vault kv put` des **trois** clés (§A4).
3. ESO met à jour le Secret dans l'heure (ou immédiatement) :
   ```bash
   $ oc -n edot annotate externalsecret edot-kafka-credentials force-sync=$(date +%s) --overwrite
   ```
4. Kubernetes met à jour les fichiers montés en une à deux minutes ; EDOT les
   relit selon `reload_interval` (1 h). Une connexion TLS déjà établie garde
   l'ancien certificat jusqu'à sa reconnexion. Pour appliquer tout de suite,
   redémarrez les pods un par un :
   ```bash
   $ oc -n edot rollout restart sts/edot-collector
   $ oc -n edot rollout status sts/edot-collector
   ```
5. Vérifiez : §A10.7 (connexions, consommation).

Surveiller l'échéance :

```bash
$ oc -n edot get secret edot-kafka-credentials -o jsonpath='{.data.tls\.crt}' | base64 -d \
    | openssl x509 -noout -enddate -checkend $((30*24*3600)) \
    && echo "valide encore au moins 30 jours" || echo "EXPIRE DANS MOINS DE 30 JOURS"
```

### API Key Elasticsearch

1. Créer une **nouvelle** clé (§B3) — **gardez l'ancienne valide**.
2. Stocker la nouvelle dans Vault (§B4), forcer la synchronisation ESO.
3. `oc -n edot rollout restart sts/edot-collector` (la variable
   d'environnement n'est relue qu'au démarrage).
4. Vérifier (§B7.2), **puis seulement** révoquer l'ancienne :
   ```
   DELETE /_security/api_key
   { "name": "edot-collector-prod-ancienne" }
   ```

## Annexe 2 — Application Argo CD

L'Application est déclarée dans `apps/40-edot-collector-app.yaml`
(namespace `openshift-gitops`, projet `default`, comme les autres Applications
de `apps/`). Elle est lue par `tests/validate-config.sh`, `tests/preflight.sh`
et le banc fonctionnel. Elle n'est **pas** listée dans `apps/kustomization.yaml` :
l'App-of-Apps ne l'enregistre donc pas. Pour l'enregistrer, décommentez la
ligne `# - 40-edot-collector-app.yaml` de `apps/kustomization.yaml` (PR), ou
bien :

```bash
$ oc apply -f apps/40-edot-collector-app.yaml
```

Son contenu (résumé) :

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: edot-collector
  namespace: openshift-gitops
spec:
  project: default
  sources:
    # 1. Chart officiel OpenTelemetry Collector + values du dépôt (ORDRE IMPORTANT)
    - repoURL: https://open-telemetry.github.io/opentelemetry-helm-charts
      chart: opentelemetry-collector
      targetRevision: 0.178.3
      helm:
        releaseName: edot-collector
        valueFiles:
          - $values/edot-collector/values.yaml
          - $values/edot-collector/values-openshift.yaml
          - $values/edot-collector/values-kafka.yaml
          - $values/edot-collector/values-elasticsearch.yaml
          - $values/edot-collector/values-processing.yaml
    # 2. Référence au dépôt Git pour $values
    - repoURL: https://gitlab.com/Numberbis/gitops-openshift-repo.git
      targetRevision: main
      ref: values
    # 3. Répertoire Kustomize compagnon (ExternalSecrets, NetworkPolicies, alertes, dashboard)
    - repoURL: https://gitlab.com/Numberbis/gitops-openshift-repo.git
      targetRevision: main
      path: edot-collector
  destination:
    server: https://kubernetes.default.svc
    namespace: edot
  # Synchronisation MANUELLE pendant la mise en service (README §16).
  syncPolicy:
    syncOptions:
      - CreateNamespace=true
```

> Le dashboard Grafana est créé dans le namespace `monitoring` : l'instance
> Argo CD doit être autorisée à y écrire. Le passage en synchronisation
> automatique se fera dans une PR dédiée, après une période d'observation.

## Annexe 3 — Aide-mémoire

```bash
# État général
oc -n edot get pods,pvc,externalsecrets,networkpolicies

# Logs récents, erreurs seulement
oc -n edot logs edot-collector-0 --since=15m | grep -E '"level":"(warn|error)"'

# Santé du Collector
oc -n edot exec edot-collector-0 -- bash -c 'curl -s "http://$MY_POD_IP:13133/"'

# Métriques clés
oc -n edot exec edot-collector-0 -- bash -c 'curl -s "http://$MY_POD_IP:8888/metrics"' \
  | grep -E '^otelcol_(kafka_broker_connects|kafka_receiver_offset_lag|receiver_accepted_log_records|exporter_sent_log_records|exporter_queue_size)|^http_client_request_duration_count'

# Sujet et expiration du certificat client Kafka
oc -n edot get secret edot-kafka-credentials -o jsonpath='{.data.tls\.crt}' | base64 -d \
  | openssl x509 -noout -subject -enddate -nameopt RFC2253

# Forcer la resynchronisation des secrets
oc -n edot annotate externalsecret --all force-sync=$(date +%s) --overwrite

# Redémarrage progressif
oc -n edot rollout restart sts/edot-collector && oc -n edot rollout status sts/edot-collector

# Arrêt d'urgence de l'ingestion (les messages restent dans Kafka)
oc -n edot scale sts edot-collector --replicas=0
```

## Annexe 4 — Glossaire express

| Terme | Définition |
|---|---|
| **EDOT** | *Elastic Distribution of OpenTelemetry* : la distribution Elastic du Collector OpenTelemetry. |
| **Receiver / processor / exporter** | Les trois étages d'un pipeline OpenTelemetry : entrée (Kafka), traitements (filtres OTTL), sortie (Elasticsearch). |
| **OTLP JSON** | Format JSON standard d'OpenTelemetry ; chaque message Kafka contient un `ExportLogsServiceRequest`. |
| **Offset** | Position de lecture dans une partition Kafka ; EDOT la valide après écriture dans sa file persistante. |
| **Lag** | Nombre de messages publiés mais pas encore lus par le groupe. |
| **EgressIP** | IP source fixe attribuée par OpenShift au trafic sortant d'un namespace. |
| **NetworkPolicy** | Règle de pare-feu interne à Kubernetes, par pod. Ici : tout est interdit par défaut (`default-deny`), sauf les flux listés. |
| **PKCS#8** | Format standard d'une clé privée (`BEGIN PRIVATE KEY`). |
| **RFC 2253** | Format d'écriture d'un DN utilisé par Kafka : `CN=…,OU=…,O=…,C=…`. |
| **Failure store** | Option d'un data stream Elasticsearch qui met de côté les documents refusés au lieu de les perdre. |
