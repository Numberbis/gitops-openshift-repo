# Stack Hadoop (HDFS + YARN) sur OpenShift — GitOps

> **⚠ Stack désactivée par défaut** (apps commentées dans
> `apps/kustomization.yaml`). Pour des jobs Spark/Flink 24h/24, le chemin
> **recommandé** est la stack streaming à base d'opérateurs natifs
> Kubernetes (waves 30→32) — voir `README_Flink_Spark.md`.
> Cette stack YARN reste disponible pour un besoin d'iso-production
> Hadoop (migration lift-and-shift, jobs fortement couplés à YARN).

Déploiement GitOps d'un cluster **HDFS** et d'un cluster **YARN** sur
OpenShift via ArgoCD, permettant d'exécuter des jobs **Spark** et **Flink**
sur YARN. Même pattern App-of-Apps / sync-waves que les stacks monitoring
(waves 1→3) et Elastic (waves 9→14) de ce dépôt.

> **Positionnement** : ce déploiement reproduit un cluster Hadoop
> « classique » (les executors Spark et TaskManagers Flink tournent dans
> des containers **YARN**, pas dans des pods Kubernetes). C'est le bon
> choix pour migrer des jobs YARN existants ou pour un environnement
> iso-prod on-premise. Si vous partez de zéro sans dépendance YARN,
> évaluez plutôt **Spark Operator** et **Flink Kubernetes Operator**, qui
> ordonnancent nativement sur Kubernetes.

---

## Architecture

```
                        openshift-gitops (ArgoCD)
                        root app ── apps/2x-*.yaml
                                        │
        ┌───────────────┬───────────────┼───────────────┐
   wave 20         wave 21          wave 22         wave 23
hadoop-platform     hdfs             yarn         hadoop-client
   │                 │                │                │
   ▼                 ▼                ▼                ▼
namespace        NameNode ×1     ResourceMgr ×1    hadoop-edge
hadoop-config    (STS + PVC)     (STS)             (Deployment)
NetworkPolicies  DataNode ×3     NodeManager ×3    examples/
                 (STS + PVC)     (STS + emptyDir)  (jobs à la main)
                 Route UI 9870   Route UI 8088

   Jobs :  spark-submit --master yarn ──► RM 8032 ──► containers
           flink run -t yarn-*          (AM + executors/TM sur les NM)
   Data :  hdfs://hdfs-namenode-0.hdfs-namenode.hadoop.svc:8020
```

| Couche | App ArgoCD | Wave | Contenu |
|---|---|---|---|
| 1 — Plateforme | `hadoop-platform` | 20 | namespace, ConfigMap `hadoop-config`, NetworkPolicies |
| 2 — Stockage | `hdfs` | 21 | NameNode, DataNodes, Route UI (`prune: false`) |
| 3 — Calcul | `yarn` | 22 | ResourceManager, NodeManagers, Route UI |
| 4 — Client | `hadoop-client` | 23 | pod edge + exemples de jobs |

**Dimensionnement par défaut** (à adapter) : 3 DataNodes × 100 Gi
(réplication 2 → ~150 Gi utiles), 3 NodeManagers × 6 Gi / 4 vcores
→ **18 Gi / 12 vcores** de capacité YARN.

---

## Choix de conception

- **Image unique** `apache/hadoop:3.4.1` (officielle Apache) pour tous les
  daemons ; les commandes (`hdfs namenode`, `yarn nodemanager`, …) sont
  passées en `command:` explicite, sans magie d'entrypoint.
- **Configuration centralisée** : une seule ConfigMap `hadoop-config`
  montée en `HADOOP_CONF_DIR=/etc/hadoop/conf` partout (daemons + clients
  Spark/Flink). Un changement de config = un commit.
- **SCC `restricted-v2`** : tout tourne en non-root avec UID arbitraire.
  L'env `HADOOP_USER_NAME=hadoop` donne une identité Hadoop stable aux
  process (auth `simple` — voir Limites).
- **StatefulSets + services headless** : HDFS/YARN exigent des hostnames
  stables ; `dfs.namenode.datanode.registration.ip-hostname-check=false`
  gère l'enregistrement par IP de pod.
- **Non-HA assumé** : 1 NameNode, 1 ResourceManager. Les PVC survivent à
  la replanification des pods ; voir « Évolution vers la HA » pour le
  chemin JournalNodes + ZooKeeper.

### Limites connues (à lire avant la prod)

1. **Pas de Kerberos** : auth Hadoop `simple`, n'importe quel pod
   autorisé par les NetworkPolicies peut se faire passer pour n'importe
   quel user. Isolation = NetworkPolicies (namespace `hadoop` deny-all,
   clients whitelistés par label). Pour du multi-tenant réel : Kerberos
   (lourd) ou passer aux opérateurs Spark/Flink natifs Kubernetes.
2. **UIs sans authentification** : les Routes NameNode/ResourceManager
   sont en TLS edge mais sans login. Sur un cluster exposé, mettre un
   `oauth-proxy` devant ou supprimer les Routes.
3. **Architecture CPU** : vérifier que les tags `apache/hadoop:3.4.1` et
   `apache/spark:3.5.4` existent pour l'architecture de vos nœuds
   (`oc get nodes -o wide`). Sur ARM64, rebuilder les images si le
   manifest multi-arch n'est pas publié :
   `docker buildx build --platform linux/arm64 …`.
4. **Java des NodeManagers** : les containers YARN (drivers/executors
   Spark, JobManager/TaskManager Flink) utilisent la JVM de l'image
   `apache/hadoop`. Spark 3.5 accepte Java 8/11/17 ; **Flink ≥ 1.18 exige
   Java 11+** — si l'image Hadoop est en Java 8, builder une variante
   avec un JRE 11/17 (cf. § Flink).

---

## Installation

Le bootstrap général du dépôt (GitOps operator, root app) est décrit dans
`README.md`. Si le root app est déjà déployé, il suffit de :

```bash
git add apps/2*.yaml components/hadoop-platform components/hdfs \
        components/yarn components/hadoop-client README_Hadoop_Openshift.md
git commit -m "feat: stack Hadoop HDFS+YARN (waves 20-23)"
git push
```

ArgoCD synchronise dans l'ordre 20 → 23. Vérification :

```bash
oc -n openshift-gitops get applications | grep -E 'hadoop|hdfs|yarn'
oc -n hadoop get pods
# Attendu :
#   hdfs-namenode-0           1/1 Running
#   hdfs-datanode-{0,1,2}     1/1 Running
#   yarn-resourcemanager-0    1/1 Running
#   yarn-nodemanager-{0,1,2}  1/1 Running
#   hadoop-edge-…             1/1 Running

# Santé HDFS : 3 DataNodes enregistrés, 0 blocs manquants
oc -n hadoop exec deploy/hadoop-edge -- hdfs dfsadmin -report

# Santé YARN : 3 NodeManagers RUNNING
oc -n hadoop exec deploy/hadoop-edge -- yarn node -list

# UIs web
oc -n hadoop get route
```

### Initialisation one-shot (après le premier sync)

```bash
oc -n hadoop exec deploy/hadoop-edge -- hdfs dfs -mkdir -p /user/hadoop /tmp/logs
```

`/user/hadoop` sert de staging à Spark, `/tmp/logs` à l'agrégation des
logs YARN.

---

## Lancer un job Spark sur YARN

### Via le Job Kubernetes d'exemple

```bash
oc apply -f components/hadoop-client/examples/spark-pi-yarn-job.yaml
oc -n hadoop logs -f job/spark-pi-yarn
# ... final status: SUCCEEDED
```

Le pod ne fait que le `spark-submit` : le driver et les 2 executors
tournent dans des containers YARN sur les NodeManagers. Suivre dans l'UI
ResourceManager (Route `yarn-resourcemanager-ui`).

### Depuis vos propres pods/pipelines

Trois ingrédients suffisent, quel que soit l'outillage (Airflow,
Tekton, CronJob…) :

1. une image contenant Spark (`apache/spark:3.5.4` ou la vôtre),
2. la ConfigMap `hadoop-config` montée + `HADOOP_CONF_DIR=/etc/hadoop/conf`
   et `YARN_CONF_DIR=/etc/hadoop/conf`,
3. `HADOOP_USER_NAME=hadoop`.

```bash
spark-submit --master yarn --deploy-mode cluster \
  --num-executors 2 --executor-memory 1g \
  --class com.exemple.MonJob hdfs:///apps/mon-job.jar
```

Depuis un **autre namespace** : poser le label
`oc label namespace mon-ns hadoop-client=true` (NetworkPolicy
`allow-external-clients`).

> Astuce perfo : uploader une fois les jars Spark dans HDFS et pointer
> `spark.yarn.archive=hdfs:///apps/spark/spark-libs.zip` pour éviter le
> re-upload à chaque soumission.

## Lancer un job Flink sur YARN

L'image officielle `flink` ne contient pas les libs client Hadoop : il
faut une petite image custom (et un JRE 11+ côté NodeManagers, cf.
Limites) :

```dockerfile
# flink-yarn-client/Dockerfile
FROM flink:1.20-scala_2.12-java17
ENV HADOOP_VERSION=3.4.1
RUN curl -fsSL https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
      | tar -xz -C /opt \
 && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/etc/hadoop/conf \
    HADOOP_CLASSPATH="/opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/yarn:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*"
```

Puis, dans un pod basé sur cette image (mêmes montages/envs que Spark) :

```bash
# Mode application : JobManager + TaskManagers dans des containers YARN
flink run-application -t yarn-application \
  -Djobmanager.memory.process.size=1024m \
  -Dtaskmanager.memory.process.size=2048m \
  ./examples/streaming/WordCount.jar

# Ou session YARN long-running puis soumissions multiples
yarn-session.sh -d -jm 1024m -tm 2048m
flink run -t yarn-session -Dyarn.application.id=<appId> mon-job.jar
```

---

## Exploitation

### Scaler le stockage (DataNodes)

```bash
# GitOps : éditer replicas dans components/hdfs/datanode.yaml, commit.
```

Au-delà de 3 DataNodes, passer `dfs.replication` à 3 dans
`components/hadoop-platform/configmap-hadoop.yaml`. **Scale-down** :
décommissionner proprement AVANT de réduire les replicas (sinon perte de
blocs) — HDFS ne re-répliquera automatiquement que si les autres
DataNodes ont l'espace nécessaire.

### Scaler le calcul (NodeManagers)

Éditer `replicas` dans `components/yarn/nodemanager.yaml` : chaque
replica ajoute 6 Gi / 4 vcores au pool YARN. Le scale-down est sans
risque de perte de données (YARN relance les containers ailleurs, les
jobs Spark/Flink tolèrent la perte d'executors).

### Changer la config Hadoop

Éditer la ConfigMap (`configmap-hadoop.yaml`), commit. ⚠ Les pods ne
rechargent pas la config à chaud :

```bash
oc -n hadoop rollout restart statefulset/yarn-nodemanager statefulset/yarn-resourcemanager
# et si hdfs-site/core-site ont changé :
oc -n hadoop rollout restart statefulset/hdfs-datanode statefulset/hdfs-namenode
```

### Évolution vers la HA

Ordre des chantiers, chacun étant un commit dans `components/` :

1. ZooKeeper ×3 (ou réutiliser un ensemble existant),
2. JournalNodes ×3 (StatefulSet + PVC),
3. NameNode ×2 avec `dfs.nameservices` + ZKFC (failover automatique),
4. ResourceManager ×2 avec `yarn.resourcemanager.ha.enabled=true`
   (l'état RM peut être stocké dans ZooKeeper).

---

## Troubleshooting

### Les DataNodes ne s'enregistrent pas (`Datanode denied communication`)

Vérifier que `dfs.namenode.datanode.registration.ip-hostname-check` vaut
bien `false` dans la ConfigMap montée :

```bash
oc -n hadoop exec hdfs-datanode-0 -- grep -A1 ip-hostname-check /etc/hadoop/conf/hdfs-site.xml
```

### NameNode en `CrashLoopBackOff` au premier démarrage

Presque toujours un fsimage absent (init container `format-namenode` a
échoué) ou un PVC en lecture seule :

```bash
oc -n hadoop logs hdfs-namenode-0 -c format-namenode
oc -n hadoop get pvc
```

### `Permission denied` sur /hadoop/dfs/…

Le CSI driver de la StorageClass ne supporte pas `fsGroup`. Vérifier le
groupe effectif : `oc -n hadoop exec hdfs-namenode-0 -- id`. Solutions :
StorageClass avec `fsType` explicite, ou CSIDriver avec
`fsGroupPolicy: File`.

### Job Spark bloqué en `ACCEPTED`

Le cluster n'a pas la ressource demandée : comparer la demande
(`--executor-memory` × `--num-executors` + driver) à la capacité
(`yarn node -list -showDetails`). Sur petit cluster, réduire
`--num-executors` ou augmenter les replicas NodeManager.

### NodeManager marqué `UNHEALTHY`

Le disk-health-checker a dépassé le seuil sur `/hadoop/yarn/local`
(emptyDir plein — shuffle volumineux). Augmenter `sizeLimit` du volume
`yarn-local`, ou monter un PVC à la place.

### `There is no such user …` / erreurs d'identité

L'env `HADOOP_USER_NAME=hadoop` manque sur le container concerné (elle
doit être posée sur TOUS les containers qui parlent au cluster, y compris
les init containers et vos pods clients).

---

## Désinstaller

```bash
# 1. Retirer les 4 apps de apps/kustomization.yaml, commit — ArgoCD prune
#    hadoop-platform / yarn / hadoop-client. L'app hdfs est en
#    prune:false : la supprimer explicitement.
oc -n openshift-gitops delete application hdfs

# 2. Les PVC (données HDFS !) ne sont PAS supprimés par ArgoCD :
oc -n hadoop delete pvc -l app.kubernetes.io/part-of=hadoop   # ⚠ destructif
oc delete namespace hadoop
```
