# Jobs Spark & Flink 24h/24 sur OpenShift — GitOps

Documentation complète du déploiement GitOps du **Spark Operator**
(Kubeflow) et du **Flink Kubernetes Operator** (Apache) sur **OpenShift**
via **ArgoCD** (pattern App-of-Apps, sync-waves 30→32), pour exécuter en
continu des jobs Spark et Flink de production — en remplacement d'un
cluster YARN + HDFS traditionnel.

> Philosophie : les jobs eux-mêmes deviennent des **ressources
> Kubernetes versionnées dans Git** (`SparkApplication`,
> `FlinkDeployment`). Déployer, mettre à jour ou scaler un pipeline
> 24/7 = un commit. Les opérateurs se chargent des redémarrages, de la
> haute disponibilité et des upgrades sans perte d'état.

---

## Sommaire

1. [Pourquoi les opérateurs plutôt que YARN](#pourquoi-les-opérateurs-plutôt-que-yarn)
2. [Architecture](#architecture)
3. [Arborescence du dépôt](#arborescence-du-dépôt)
4. [Prérequis](#prérequis)
5. [Installation](#installation)
6. [Vérifier le déploiement](#vérifier-le-déploiement)
7. [Le stockage d'état : S3 remplace HDFS](#le-stockage-détat--s3-remplace-hdfs)
8. [Déployer un pipeline Flink 24/7](#déployer-un-pipeline-flink-247)
9. [Déployer un job Spark 24/7](#déployer-un-job-spark-247)
10. [Cycle de vie GitOps des jobs](#cycle-de-vie-gitops-des-jobs)
11. [Migration depuis le cluster YARN/HDFS existant](#migration-depuis-le-cluster-yarnhdfs-existant)
12. [Sécurité](#sécurité)
13. [Monitoring & alerting](#monitoring--alerting)
14. [Mettre à jour les opérateurs](#mettre-à-jour-les-opérateurs)
15. [Troubleshooting](#troubleshooting)
16. [Désinstaller](#désinstaller)
17. [Références](#références)

---

## Pourquoi les opérateurs plutôt que YARN

Ce dépôt contient deux chemins pour exécuter Spark et Flink :

- **waves 30→32** (ce document) : opérateurs natifs Kubernetes —
  **le chemin recommandé et activé par défaut** ;
- waves 20→23 (`README_Hadoop_Openshift.md`) : cluster HDFS + YARN
  reconstruit dans OpenShift — désactivé (commenté dans
  `apps/kustomization.yaml`), conservé pour un besoin d'iso-production
  YARN stricte.

Pour des jobs qui tournent **24h/24**, la comparaison est nette :

| Critère | YARN dans OpenShift | Opérateurs natifs |
|---|---|---|
| **Haute dispo des jobs** | Jobs perdus au crash du ResourceManager ; la HA YARN exige ZooKeeper + JournalNodes en plus | Flink : HA JobManager native Kubernetes, reprise automatique sur checkpoint. Spark : `restartPolicy: Always` + checkpointing |
| **Upgrade d'un job sans perte d'état** | Procédure manuelle : savepoint, stop, resubmit | **Automatique** : commit Git → savepoint → redéploiement → restauration (`upgradeMode: savepoint`) |
| **Allocation des ressources** | Double ordonnanceur : Kubernetes alloue des pods NodeManager figés, YARN re-découpe dedans → capacité gaspillée | Un seul ordonnanceur : chaque executor / TaskManager est un pod dimensionné juste ; quotas et autoscaling natifs |
| **Sécurité** | Auth Hadoop `simple` sans Kerberos (usurpation triviale), UIs sans login | ServiceAccounts + RBAC par job, PodSecurity `restricted` |
| **Observabilité** | UIs YARN, logs agrégés dans HDFS, hors écosystème | `oc logs` par job, métriques Prometheus (stack monitoring waves 1→3), CRs avec `status` |
| **Charge opérationnelle** | 8+ StatefulSets à administrer (NameNode, DataNodes, RM, NMs…) | 2 opérateurs gérés en GitOps, aucun composant stateful |
| **Cycle de vie GitOps** | La plateforme est GitOps, les jobs restent du `spark-submit` impératif | **Les jobs sont des CRs versionnés** : un pipeline = un YAML dans Git |

Le point décisif pour du 24/7 : avec le Flink Operator, livrer une
nouvelle version d'un pipeline est une opération **déclarative** avec
savepoint automatique. Avec YARN, c'est une procédure manuelle à chaque
release.

---

## Architecture

```
                       openshift-gitops (ArgoCD)
                       root app ── apps/3x-*.yaml
                                 │
     ┌───────────────────────────┼───────────────────────────┐
 wave 30                     wave 31                      wave 32
 streaming-platform          spark-operator               flink-operator
 (kustomize)                 (chart Helm kubeflow)        (chart Helm apache)
     │                           │                            │
     ▼                           ▼                            ▼
 ns data-jobs                ns spark-operator            ns flink-operator
 SA + RBAC flink             controller + webhook         operator + webhook
 NetworkPolicies             surveille `data-jobs`        surveille `data-jobs`
 examples/                       │                            │ (requiert
                                 │                            │  cert-manager)
        ┌────────────────────────┴──────────────┬─────────────┘
        │  CR SparkApplication                  │  CR FlinkDeployment
        ▼                                       ▼
   ┌─────────────────────── ns data-jobs ───────────────────────┐
   │  driver pod ──► executor pods                              │
   │  JobManager pod (HA) ──► TaskManager pods                  │
   └────────────────────────────┬───────────────────────────────┘
                                │ état, checkpoints, savepoints
                                ▼
                    S3 (ODF / MinIO / AWS)
                    [+ HDFS legacy accessible pendant la migration]
```

**Sync waves ArgoCD** : `streaming-platform` (30) →
`spark-operator` (31) → `flink-operator` (32). Le namespace `data-jobs`
doit exister avant les opérateurs : le chart Spark y crée le RBAC des
jobs, et les deux opérateurs sont configurés pour le surveiller.

| Couche | App ArgoCD | Wave | Source | Contenu |
|---|---|---|---|---|
| 1 — Plateforme jobs | `streaming-platform` | 30 | `components/streaming-platform` | namespace `data-jobs`, SA/RBAC `flink`, NetworkPolicies, templates de jobs |
| 2 — Opérateur Spark | `spark-operator` | 31 | chart Helm `spark-operator` 2.1.0 ([kubeflow](https://kubeflow.github.io/spark-operator)) | controller, webhook de mutation, RBAC des jobs |
| 3 — Opérateur Flink | `flink-operator` | 32 | chart Helm `flink-kubernetes-operator` 1.10.0 ([apache](https://archive.apache.org/dist/flink/)) | operator, webhook d'admission (cert-manager) |

Les opérateurs sont installés **directement depuis leurs charts Helm
officiels** par ArgoCD — aucun manifest vendoré. Mettre à jour un
opérateur = bump de `targetRevision` dans l'app correspondante.

### Choix de conception

- **Un namespace unique `data-jobs`** pour tous les workloads : les
  opérateurs vivent ailleurs (`spark-operator`, `flink-operator`).
  Pour isoler des équipes, dupliquer le namespace et l'ajouter aux
  `jobNamespaces` / `watchNamespaces` des apps 31/32.
- **PodSecurity `restricted` / SCC `restricted-v2`** partout : tout
  tourne en non-root avec UID arbitraire. Les incompatibilités connues
  des charts sont neutralisées dans les values (cf.
  [Troubleshooting](#troubleshooting)).
- **NetworkPolicies deny-all** avec ouvertures explicites — même
  philosophie que les stacks Elastic et monitoring du dépôt.
- **`ServerSideApply=true`** sur les apps : la CRD `FlinkDeployment`
  dépasse la limite des 256 KiB de l'annotation
  `last-applied-configuration` (même contrainte que le CR Elasticsearch,
  wave 12).

---

## Arborescence du dépôt

```
.
├── README_Flink_Spark.md                    # 📖 Ce document
│
├── apps/
│   ├── 30-streaming-platform-app.yaml       # Wave 30 — fondation
│   ├── 31-spark-operator-app.yaml           # Wave 31 — chart Helm + values
│   └── 32-flink-operator-app.yaml           # Wave 32 — chart Helm + values
│
└── components/
    └── streaming-platform/
        ├── kustomization.yaml               # + vos jobs de prod (jobs/)
        ├── namespace.yaml                   # ns data-jobs (restricted)
        ├── rbac-flink.yaml                  # SA + Role + RoleBinding flink
        ├── networkpolicy.yaml               # deny-all + ouvertures
        └── examples/                        # 📋 Templates NON synchronisés
            ├── flink-deployment-24x7.yaml   #    pipeline Flink type prod
            └── spark-application-24x7.yaml  #    job Spark Streaming type prod
```

---

## Prérequis

| Composant | Détail |
|---|---|
| **OpenShift** | 4.12+ (testé avec le socle du dépôt) |
| **OpenShift GitOps** | déployé via `bootstrap/` (cf. `README.md`) |
| **cert-manager** | `bootstrap/06-cert-manager-subscription.yaml` — requis par le webhook du Flink Operator |
| **Object storage S3** | ODF/NooBaa, MinIO ou S3 externe — état, checkpoints, savepoints (cf. [section dédiée](#le-stockage-détat--s3-remplace-hdfs)) |
| **Registry d'images** | pour les images custom des jobs (jar embarqué) |
| **Secrets S3** | `spark-s3-credentials` et `flink-s3-credentials` dans `data-jobs`, créés **hors Git** (idéalement External Secrets + le ClusterSecretStore Vault du composant `elastic-platform`) |

---

## Installation

### 1. Adapter les placeholders

Deux endroits à personnaliser avant le premier sync :

- **`components/streaming-platform/networkpolicy.yaml`** — policy
  `allow-egress-data-sources` : remplacer les CIDR placeholders par vos
  vraies cibles (endpoint S3, brokers Kafka, cluster HDFS legacy pendant
  la migration).
- **`apps/31-*` / `apps/32-*`** : vérifier que `repoURL` du dépôt Git
  (app 30) pointe sur votre fork, comme pour les autres apps du dépôt.

### 2. Commit & push

```bash
git add apps/30-streaming-platform-app.yaml \
        apps/31-spark-operator-app.yaml \
        apps/32-flink-operator-app.yaml \
        apps/kustomization.yaml \
        components/streaming-platform \
        README_Flink_Spark.md
git commit -m "feat: opérateurs Spark/Flink pour jobs streaming 24/7 (waves 30-32)"
git push
```

ArgoCD (root app) détecte les 3 nouvelles applications et synchronise
dans l'ordre 30 → 31 → 32.

### 3. Créer les Secrets S3 (hors Git)

```bash
oc -n data-jobs create secret generic flink-s3-credentials \
    --from-literal=AWS_ACCESS_KEY_ID=... \
    --from-literal=AWS_SECRET_ACCESS_KEY=...
oc -n data-jobs create secret generic spark-s3-credentials \
    --from-literal=AWS_ACCESS_KEY_ID=... \
    --from-literal=AWS_SECRET_ACCESS_KEY=...
```

> En production, préférer un `ExternalSecret` versionné dans
> `components/streaming-platform/` : le ClusterSecretStore Vault existe
> déjà (composant `elastic-platform`, wave 11).

---

## Vérifier le déploiement

```bash
# Les 3 applications Synced/Healthy
oc -n openshift-gitops get applications | grep -E 'streaming|spark|flink'

# Opérateurs Running
oc -n spark-operator get pods     # controller + webhook
oc -n flink-operator get pods     # flink-kubernetes-operator

# CRDs installées
oc get crd | grep -E 'sparkoperator.k8s.io|flink.apache.org'

# RBAC des jobs en place dans data-jobs
oc -n data-jobs get sa            # → flink + spark-operator-spark
```

---

## Le stockage d'état : S3 remplace HDFS

Sur Kubernetes, tout ce que HDFS fournissait aux jobs (checkpoints,
savepoints, état, jars) est mieux servi par un **object storage S3** :

| Usage | HDFS (avant) | S3 (après) |
|---|---|---|
| Checkpoints Flink | `hdfs:///flink/checkpoints` | `s3://flink-state/checkpoints/<job>` |
| Savepoints Flink | `hdfs:///flink/savepoints` | `s3://flink-state/savepoints/<job>` |
| Métadonnées HA Flink | ZooKeeper | `s3://flink-state/ha/<job>` + ConfigMaps k8s |
| Checkpoints Spark Streaming | `hdfs:///spark/checkpoints` | `s3a://spark-state/checkpoints/<job>` |
| Données métier | `hdfs:///data/...` | `s3a://...` (ou HDFS legacy pendant la migration) |

Options par ordre de préférence :

1. **OpenShift Data Foundation (ODF/NooBaa)** : S3 interne au cluster,
   buckets provisionnés par `ObjectBucketClaim` — donc GitOps-ables
   dans `components/streaming-platform/` :

   ```yaml
   apiVersion: objectbucket.io/v1alpha1
   kind: ObjectBucketClaim
   metadata:
     name: flink-state
     namespace: data-jobs
   spec:
     generateBucketName: flink-state
     storageClassName: openshift-storage.noobaa.io
   ```

2. **MinIO** (léger, à déployer dans le cluster) ou **S3 externe**
   existant.

Les buckets minimum : `flink-state`, `spark-state` (+ vos buckets de
données). L'endpoint S3 se configure dans chaque CR de job
(placeholder `https://s3.openshift-storage.svc:443` dans les templates).

---

## Déployer un pipeline Flink 24/7

Template complet : `components/streaming-platform/examples/flink-deployment-24x7.yaml`.

### 1. L'image du job

L'image officielle Flink + votre jar dans `usrlib` ; le connecteur S3
est embarqué dans l'image officielle et s'active par variable
d'environnement (pas de jar à ajouter) :

```dockerfile
FROM flink:1.20-scala_2.12-java17
COPY target/mon-pipeline.jar /opt/flink/usrlib/mon-pipeline.jar
# Si UID arbitraire OpenShift pose problème (cf. Troubleshooting) :
RUN chmod -R g=u /opt/flink
```

### 2. Le CR `FlinkDeployment` — les réglages qui comptent pour du 24/7

```yaml
spec:
  flinkConfiguration:
    # État : RocksDB incrémental + checkpoints S3 toutes les 60 s
    state.backend.type: rocksdb
    state.backend.incremental: "true"
    execution.checkpointing.interval: 60 s
    execution.checkpointing.mode: EXACTLY_ONCE
    state.checkpoints.dir: s3://flink-state/checkpoints/mon-pipeline
    state.savepoints.dir: s3://flink-state/savepoints/mon-pipeline
    # HA : le JobManager survit à la perte de son pod/nœud
    high-availability.type: kubernetes
    high-availability.storageDir: s3://flink-state/ha/mon-pipeline
  job:
    state: running
    # ★ LE réglage clé : tout changement commité passe par
    #   savepoint → stop → redeploy → restore. Zéro perte d'état.
    upgradeMode: savepoint
```

| Réglage | Rôle pour le 24/7 |
|---|---|
| `high-availability.type: kubernetes` | leader election + métadonnées dans des ConfigMaps : reprise automatique sans ZooKeeper |
| `upgradeMode: savepoint` | upgrades **sans perte d'état** pilotés par Git (`last-state` : plus rapide, reprend du dernier checkpoint) |
| `execution.checkpointing.*` | granularité de la reprise sur incident |
| `restart-strategy.type: exponential-delay` | absorbe les incidents transitoires sans intervention |
| `serviceAccount: flink` | SA + RBAC posés par la wave 30 |

### 3. Passage en prod GitOps

```bash
mkdir -p components/streaming-platform/jobs
cp components/streaming-platform/examples/flink-deployment-24x7.yaml \
   components/streaming-platform/jobs/mon-pipeline.yaml
# … adapter image, buckets, parallélisme …
# puis référencer dans components/streaming-platform/kustomization.yaml :
#   - jobs/mon-pipeline.yaml
git add . && git commit -m "feat(jobs): pipeline mon-pipeline" && git push
```

### 4. Suivi

```bash
oc -n data-jobs get flinkdeployments
oc -n data-jobs describe flinkdeployment mon-pipeline-flink   # status, savepoints
oc -n data-jobs logs deploy/mon-pipeline-flink -f             # JobManager
# UI Flink du job :
oc -n data-jobs port-forward svc/mon-pipeline-flink-rest 8081
```

---

## Déployer un job Spark 24/7

Template complet : `components/streaming-platform/examples/spark-application-24x7.yaml`.

### 1. L'image du job

⚠ L'image officielle Spark **n'embarque pas** le connecteur S3 :

```dockerfile
FROM apache/spark:3.5.4
# hadoop-aws aligné sur le Hadoop embarqué par Spark 3.5.x (3.3.4)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
COPY target/mon-job.jar /opt/spark/app/mon-job.jar
```

### 2. Le CR `SparkApplication` — les réglages qui comptent pour du 24/7

```yaml
spec:
  # ★ Relance automatique permanente
  restartPolicy:
    type: Always
    onFailureRetryInterval: 15
    onSubmissionFailureRetryInterval: 30
  sparkConf:
    # Reprise exacte (offsets Kafka, agrégations) via le checkpointing
    # Structured Streaming — dans S3, plus dans HDFS
    spark.sql.streaming.checkpointLocation: s3a://spark-state/checkpoints/mon-job
  driver:
    serviceAccount: spark-operator-spark   # créé par le chart (wave 31)
```

Le driver tourne en pod et crée lui-même ses executors : un executor
perdu (éviction, nœud down) est recréé, le job continue en dégradé au
lieu de tomber. Si le driver meurt, `restartPolicy: Always` relance
l'application qui reprend sur son checkpoint.

### 3. Passage en prod GitOps & suivi

Même workflow que Flink (copier dans `jobs/`, référencer, commit).

```bash
oc -n data-jobs get sparkapplications
oc -n data-jobs describe sparkapplication mon-job-streaming-spark
oc -n data-jobs logs -l spark-role=driver -f
```

---

## Cycle de vie GitOps des jobs

Toutes les opérations du quotidien sont des commits :

| Opération | Geste | Ce qui se passe |
|---|---|---|
| **Livrer une version** | changer le tag `image:` du CR | Flink : savepoint → redéploiement → restore. Spark : restart sur checkpoint |
| **Scaler** | `job.parallelism` (Flink) / `executor.instances` (Spark) | redéploiement contrôlé (savepoint côté Flink) |
| **Suspendre** | `job.state: suspended` (Flink) | arrêt **avec savepoint** ; `running` pour reprendre |
| **Rollback** | `git revert` | même mécanique qu'une livraison |
| **Changer la config** | éditer `flinkConfiguration` / `sparkConf` | idem livraison |

> **Autoscaling Flink** (optionnel, précieux en 24/7) : l'opérateur
> embarque un autoscaler qui ajuste le parallélisme à la charge réelle :
>
> ```yaml
> flinkConfiguration:
>   job.autoscaler.enabled: "true"
>   job.autoscaler.stabilization.interval: 1m
>   job.autoscaler.metrics.window: 5m
> ```

---

## Migration depuis le cluster YARN/HDFS existant

Les deux plateformes coexistent : chaque job migre **indépendamment**.

### Phase 1 — Socle

- Waves 30→32 synchronisées, Secrets S3 en place ;
- buckets créés (`flink-state`, `spark-state`) ;
- NetworkPolicy `allow-egress-data-sources` ouverte vers le **HDFS
  legacy** (NameNode 8020, DataNodes 9866) : les jobs migrés peuvent
  continuer à lire/écrire leurs données historiques.

### Phase 2 — Adapter chaque job (souvent minime)

| | Avant (YARN) | Après (opérateur) |
|---|---|---|
| Soumission | `spark-submit --master yarn` / `flink run -t yarn-*` | CR dans Git |
| État Flink | `hdfs:///flink/...` + ZooKeeper | `s3://flink-state/...` + HA kubernetes |
| Checkpoints Spark | `hdfs:///spark/...` | `s3a://spark-state/...` |
| Données métier | `hdfs://namenode:8020/data/...` | inchangé dans un premier temps |

### Phase 3 — Bascule job par job, sans perte d'état

```bash
# 1. Arrêter le job YARN AVEC savepoint (exemple Flink)
flink stop --savepointPath s3://flink-state/savepoints/mon-pipeline <jobId>

# 2. Démarrer le CR en pointant sur ce savepoint :
#    spec.job.initialSavepointPath: s3://flink-state/savepoints/mon-pipeline/savepoint-...
#    → reprise exacte de l'état, y compris les offsets Kafka.
```

Côté Spark : copier le répertoire de checkpoint vers S3 avant bascule
(`hadoop distcp hdfs://... s3a://...`), puis pointer
`spark.sql.streaming.checkpointLocation` dessus.

### Phase 4 — Décommissionner

Quand plus aucun job ne lit le HDFS legacy : migrer les données
résiduelles (`distcp`), retirer les ouvertures réseau, éteindre l'ancien
cluster.

---

## Sécurité

- **SCC `restricted-v2` / PodSecurity `restricted`** : aucun container
  privilégié, UID arbitraires. Le chart Flink fixait `runAsUser: 9999`,
  neutralisé par `podSecurityContext: null` dans
  `apps/32-flink-operator-app.yaml`.
- **RBAC minimal par moteur** : SA `spark-operator-spark` (créé par le
  chart) et SA `flink` (wave 30) limités à leur namespace — pods,
  services, ConfigMaps (HA), deployments.
- **NetworkPolicies deny-all** sur `data-jobs`, avec uniquement :
  - trafic intra-namespace (driver↔executors, JM↔TM — ports dynamiques),
  - egress **kube-apiserver** (6443) : vital, les drivers/JobManagers
    créent leurs pods eux-mêmes,
  - egress DNS,
  - ingress opérateurs → jobs (REST JobManager 8081, UI driver 4040),
  - ingress Prometheus UWM (scraping),
  - egress data sources (S3, Kafka, HDFS legacy) — **placeholders à
    adapter**.
- **Secrets hors Git** : credentials S3 via External Secrets/Vault, ou
  créés à la main. Jamais de credential dans un CR.

---

## Monitoring & alerting

Les deux opérateurs exposent des métriques Prometheus, et l'User
Workload Monitoring est déjà actif (wave 1) :

1. Poser des `ServiceMonitor`/`PodMonitor` dans
   `components/streaming-platform/` (la NetworkPolicy
   `allow-monitoring-scrape` est déjà en place) ;
2. Ajouter des dashboards via le composant `grafana-instance` (wave 3) ;
3. Reporter Prometheus natif Flink par job :

   ```yaml
   flinkConfiguration:
     metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
     metrics.reporter.prom.port: "9249"
   ```

**Alertes minimum vital pour du 24/7** :

| Alerte | Signal |
|---|---|
| Pipeline arrêté | `FlinkDeployment.status.jobStatus.state != RUNNING` / `SparkApplication.status.applicationState.state == FAILED` |
| **Checkpoints qui ne passent plus** | ancienneté du dernier checkpoint réussi (`lastCheckpointTime`) — un pipeline « vert » qui ne checkpointe plus est un incident **silencieux** : en cas de crash, la reprise se fera sur un état ancien |
| Backpressure / lag | métriques `busyTimeMsPerSecond`, lag consommateur Kafka |

---

## Mettre à jour les opérateurs

1. Lire les release notes ([spark-operator](https://github.com/kubeflow/spark-operator/releases),
   [flink-kubernetes-operator](https://flink.apache.org/downloads/)) ;
2. Bump `targetRevision` dans `apps/31-*` ou `apps/32-*` — pour Flink,
   changer aussi le numéro de version **dans `repoURL`** (un répertoire
   de miroir Apache par version) ;
3. Commit : ArgoCD met à jour l'opérateur **et ses CRDs**
   (`ServerSideApply=true`). Les jobs en cours ne sont pas interrompus.

⚠ Le schéma des values du chart Spark a changé entre les majeures 1.x
et 2.x : vérifier les values inline de l'app à chaque changement de
majeure.

---

## Troubleshooting

### Pod de l'opérateur Flink rejeté (`unable to validate against any SCC`)

Le chart fixe `runAsUser: 9999`, hors plage UID du namespace. Déjà
neutralisé (`podSecurityContext: null` dans les values de l'app 32) —
vérifier que la value n'a pas sauté lors d'un bump de chart.

### Pods d'un job Flink : `permission denied` sur `/opt/flink/conf`

L'UID arbitraire OpenShift n'a pas les droits attendus par
l'entrypoint. Rebuild de l'image du job avec `RUN chmod -R g=u /opt/flink`.

### App `flink-operator` OutOfSync en boucle

- CRD trop grosse : `ServerSideApply=true` obligatoire (déjà posé) ;
- diff résiduel sur le `caBundle` du webhook (injecté par cert-manager) :
  ajouter un `ignoreDifferences` sur ce champ dans l'app 32.

### `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem` (Spark)

L'image du job n'embarque pas `hadoop-aws` + `aws-java-sdk-bundle`
(cf. [Dockerfile](#déployer-un-job-spark-247)). Aligner `hadoop-aws` sur
la version de Hadoop embarquée par Spark (3.3.4 pour Spark 3.5.x).

### Executors Spark jamais créés

Le driver n'atteint pas l'API server → vérifier la NetworkPolicy
`allow-egress-apiserver` (6443) et que le driver utilise le SA
`spark-operator-spark`.

### Job Flink bloqué en `RECONCILING` après un upgrade

Le savepoint a échoué (S3 inaccessible, credentials expirés) :

```bash
oc -n data-jobs describe flinkdeployment <nom>       # events
oc -n flink-operator logs deploy/flink-kubernetes-operator
```

Issue de secours : passer temporairement `upgradeMode: last-state`
(reprise sur le dernier checkpoint au lieu d'un savepoint frais).

### Webhook Flink : `certificate not found`

cert-manager absent ou pas prêt → vérifier
`bootstrap/06-cert-manager-subscription.yaml` et les pods
`cert-manager`. Le webhook peut aussi être désactivé
(`webhook.create: false` dans les values) au prix de la validation
d'admission.

---

## Désinstaller

```bash
# 1. Supprimer les jobs AVEC savepoint final si l'état doit survivre :
#    (job.state: suspended, attendre le savepoint, puis delete)
oc -n data-jobs delete flinkdeployments,sparkapplications --all

# 2. Retirer les 3 apps de apps/kustomization.yaml, commit — ArgoCD prune
#    les opérateurs et la plateforme.

# 3. Les buckets S3 (état, savepoints) ne sont PAS touchés : les garder
#    permet de redéployer plus tard avec initialSavepointPath.
```

---

## Références

- **Flink Kubernetes Operator** :
  <https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/>
- **Spark Operator (Kubeflow)** :
  <https://www.kubeflow.org/docs/components/spark-operator/>
- **Flink HA Kubernetes** :
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/ha/kubernetes_ha/>
- **Spark Structured Streaming — checkpointing** :
  <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing>
- **ODF ObjectBucketClaim** :
  <https://docs.redhat.com/en/documentation/red_hat_openshift_data_foundation/>
- **ArgoCD + Helm** :
  <https://argo-cd.readthedocs.io/en/stable/user-guide/helm/>
- **Alternative YARN dans OpenShift** (désactivée) :
  `README_Hadoop_Openshift.md`
