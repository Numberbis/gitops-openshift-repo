# Elastic Stack sur OpenShift — Compromis hybride **ECK + GitOps**

> Document d'architecture et de référence opérationnelle pour déployer la
> **Suite Elastic** (Elasticsearch, Kibana, APM, Beats, Fleet, Logstash) sur
> **OpenShift**, en s'inspirant des pratiques des équipes **SRE matures**
> (Elastic Cloud, équipes plateforme grands comptes, opérateurs régulés).
>
> Ce document **complète** le `README.md` principal du dépôt qui couvre la
> stack Prometheus/Alertmanager/Grafana. Ici, on parle d'une stack **Elastic**
> autonome, gérée selon le même paradigme GitOps (ArgoCD + Kustomize) mais
> avec un opérateur tiers (**ECK**, Elastic Cloud on Kubernetes).

---

## Sommaire

1. [Le dilemme : pur Operator vs pur GitOps](#le-dilemme--pur-operator-vs-pur-gitops)
2. [Le compromis hybride (synthèse)](#le-compromis-hybride-synthèse)
3. [Architecture cible](#architecture-cible)
4. [Modèle de séparation des responsabilités (4 couches)](#modèle-de-séparation-des-responsabilités-4-couches)
5. [Couche 1 — Operator ECK via GitOps](#couche-1--operator-eck-via-gitops)
6. [Couche 2 — Topologie cluster (CR Elasticsearch)](#couche-2--topologie-cluster-cr-elasticsearch)
7. [Couche 3 — Configuration stack (StackConfigPolicy)](#couche-3--configuration-stack-stackconfigpolicy)
8. [Couche 4 — Workload (Beats, APM, Fleet, Logstash)](#couche-4--workload-beats-apm-fleet-logstash)
9. [Secrets et authentification](#secrets-et-authentification)
10. [Certificats TLS](#certificats-tls)
11. [Stockage et snapshots](#stockage-et-snapshots)
12. [Spécificités OpenShift](#spécificités-openshift)
13. [Multi-environnements et multi-clusters (ApplicationSet)](#multi-environnements-et-multi-clusters-applicationset)
14. [Stratégie de mise à jour](#stratégie-de-mise-à-jour)
15. [Stack Monitoring (méta-monitoring)](#stack-monitoring-méta-monitoring)
16. [Day-2 operations / runbooks](#day-2-operations--runbooks)
17. [Anti-patterns à éviter](#anti-patterns-à-éviter)
18. [Arborescence cible dans ce dépôt](#arborescence-cible-dans-ce-dépôt)
19. [Bootstrap pas à pas](#bootstrap-pas-à-pas)
20. [Références](#références)

---

## Le dilemme : pur Operator vs pur GitOps

Quand on déploie Elastic sur Kubernetes, on tombe rapidement sur deux écoles
opposées qui se renvoient la balle depuis ~2020 :

### École A — « Tout Operator » (ECK pilote tout)

L'opérateur ECK gère le cycle de vie complet : création des StatefulSets,
rolling upgrade ordonné, renouvellement des certificats internes, protection
du quorum master, ajout/suppression de nœuds avec rebalance, etc.

- **Forces** : robustesse, sait éviter les *split-brain*, gère les transitions
  de version Elasticsearch (qui sont des opérations délicates).
- **Faiblesses** : tout ce qui concerne **le contenu** d'Elasticsearch
  (politiques ILM, templates d'index, repos de snapshots, rôles, mappings de
  rôles SAML, watchers, transforms…) **n'est pas un CR Kubernetes**. C'est de
  l'API REST Elasticsearch. Donc on retombe vite dans des scripts `curl`
  post-déploiement ou des outils externes (`elasticsearch-cli`, Terraform
  provider Elasticstack, Ansible…) — **non GitOps**.

### École B — « Tout GitOps brut » (manifests Helm/Kustomize, pas d'opérateur)

On déploie Elasticsearch comme un StatefulSet « à la main », tout dans Git.

- **Forces** : tout est déclaratif, audit trivial, *no magic*.
- **Faiblesses** : on **réimplémente** ce que fait ECK (gestion des certs,
  upgrade, scaling), généralement mal. Les *upgrades majeurs* (7→8, 8→9)
  deviennent des opérations manuelles risquées. On perd l'orchestration de
  *graceful shutdown* et la protection contre les *unsafe operations*.

### La réalité observée chez les équipes matures

Aucune équipe SRE Elastic mature n'utilise A **ou** B en pur. Toutes basculent
vers un **modèle hybride** où :

- L'opérateur **ECK** gère les opérations qui demandent de l'**intelligence
  d'orchestration** (cycle de vie des nœuds, quorum, certs internes,
  upgrades).
- **GitOps** gère **toute la déclaration** — y compris ce qui était
  historiquement « API only » — grâce à la CRD **`StackConfigPolicy`**
  (introduite dans ECK 2.6, stabilisée à partir de 2.11).
- Les **secrets** sortent du Git via **External Secrets Operator** ou
  **Sealed Secrets**.
- Les **dépendances externes** (object storage pour snapshots, IdP pour SAML)
  sont **référencées** depuis Git mais **provisionnées hors-stack**
  (Terraform / opérateurs cloud / OpenShift Data Foundation).

C'est ce compromis que ce document détaille.

---

## Le compromis hybride (synthèse)

| Domaine | Qui gère ? | Source de vérité |
|---|---|---|
| Installation de l'opérateur ECK | OLM (OperatorHub OpenShift) **ou** Helm chart, déclaré dans Git | Git |
| Topologie cluster (NodeSets, ressources, PV) | CR `Elasticsearch` dans Git, **réconcilié** par ECK | Git |
| Cycle de vie nœuds (rolling, upgrade, quorum) | **ECK** (logique impérative) | Code de l'opérateur |
| Certificats TLS internes (transport + http) | **ECK** (auto-renouvellement) | ECK |
| Certificat TLS externe (Route OpenShift) | **cert-manager** + `Certificate` dans Git | Git |
| ILM, ISM, snapshot lifecycle (SLM), index templates | **`StackConfigPolicy`** dans Git, appliqué par ECK | Git |
| Rôles, role mappings, file realm users | **`StackConfigPolicy`** + `secureSettings` | Git (refs) + Vault |
| Mots de passe, kibana encryption keys, S3 keys | **External Secrets Operator** → Vault / AWS SM / Azure KV | Vault (référencé en Git) |
| Snapshots (repo + politique) | Repo S3/ODF dans `StackConfigPolicy`, secrets dans Vault | Git + Vault |
| Beats / APM Server / Fleet / Logstash | CR ECK dédiés dans Git | Git |
| Dashboards Kibana, lens, saved searches | **`StackConfigPolicy`** (champ `kibana.config`) ou **NDJSON** committé + Job d'import | Git |
| Stack Monitoring (métriques de l'Elastic lui-même) | Cluster monitoring **dédié** (séparé), shippé via Metricbeat ou Elastic Agent | Git |
| Promotion d'environnement (dev → staging → prod) | **ApplicationSet** ArgoCD + overlays Kustomize | Git |

### La règle d'or

> **« Tout ce qui peut être un CR Kubernetes l'est. Tout ce qui ne peut pas
> l'être est encapsulé dans une `StackConfigPolicy`. Tout ce qui contient un
> secret sort du dépôt via un opérateur de secrets. »**

Avec cette règle, **un `git revert` suffit** à rollback n'importe quoi —
une topologie, un changement d'ILM, un nouveau rôle, une nouvelle dashboard —
et le diff Git **est** l'audit trail.

---

## Architecture cible

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           OpenShift Cluster (prod)                      │
│                                                                         │
│   ┌───────────────────────┐     reconcile     ┌───────────────────────┐ │
│   │ openshift-gitops      │ ────────────────► │ elastic-system        │ │
│   │ ArgoCD ApplicationSet │                   │  ECK Operator         │ │
│   └────────┬──────────────┘                   └───────────┬───────────┘ │
│            │ sync waves                                   │ owns        │
│            ▼                                              ▼             │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                      elastic namespace                           │  │
│   │                                                                  │  │
│   │   ┌─────────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │  │
│   │   │ Elasticsearch│ │  Kibana  │  │   APM    │  │ Elastic Agent│  │  │
│   │   │  (CR ECK)   │  │ (CR ECK) │  │ (CR ECK) │  │   / Fleet    │  │  │
│   │   └──────┬──────┘  └────┬─────┘  └─────┬────┘  └──────┬───────┘  │  │
│   │          │              │              │              │          │  │
│   │   ┌──────▼──────────────▼──────────────▼──────────────▼──────┐   │  │
│   │   │  StackConfigPolicy (ILM, SLM, templates, rôles, dashboards)│ │  │
│   │   └────────────────────────────────────────────────────────────┘ │  │
│   │                                                                  │  │
│   │   ┌──────────────────────┐    ┌────────────────────────────┐    │  │
│   │   │ ExternalSecrets      │◄───│ ClusterSecretStore (Vault) │    │  │
│   │   │  → Secrets K8s natifs│    └────────────────────────────┘    │  │
│   │   └──────────────────────┘                                      │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                       │                                 │
│                                       │ snapshots                       │
│                                       ▼                                 │
│                          ┌────────────────────────┐                     │
│                          │  S3 / ODF / MinIO      │                     │
│                          └────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────────────┘
            ▲                                            │
            │ ship metrics + logs                        │
            ▼                                            ▼
┌─────────────────────────────────────────┐  ┌─────────────────────────┐
│  Cluster « observability »              │  │  Vault / AWS SM / Azure │
│  Stack Elastic dédié au méta-monitoring │  │  source des secrets     │
└─────────────────────────────────────────┘  └─────────────────────────┘
```

**Points-clés** :

1. **Un opérateur**, **plusieurs CR**. ECK est installé une fois et observe
   tous les CR de tous les namespaces autorisés.
2. **Un namespace = un cluster Elastic logique**. On évite de mélanger
   plusieurs Elasticsearch dans le même namespace, sauf cas de
   *multi-tenancy* contrôlée.
3. **Stack Monitoring séparé**. La règle d'or : on ne monitore **jamais** un
   cluster Elastic avec lui-même. On déploie un **mini-cluster Elastic
   dédié** sur un cluster OpenShift d'observabilité, qui reçoit les métriques
   et logs des clusters de prod.

---

## Modèle de séparation des responsabilités (4 couches)

Les équipes matures structurent leur dépôt et leurs Apps ArgoCD selon
**4 couches**, déployées dans cet ordre via **sync waves** :

| Wave | Couche | Contenu | Fréquence de changement |
|---|---|---|---|
| 1 | **Plateforme** | ECK Operator, External Secrets Operator, cert-manager, StorageClass, ClusterIssuer | Trimestrielle (upgrade opérateurs) |
| 2 | **Topologie** | CR `Elasticsearch`, `Kibana` (NodeSets, PVC, anti-affinity, resources) | Mensuelle (capacity planning) |
| 3 | **Configuration stack** | `StackConfigPolicy` (ILM, SLM, templates, roles, mappings, dashboards) | Hebdomadaire (nouveaux indices, nouvelles politiques) |
| 4 | **Ingestion / consommation** | Beats, APM, Fleet, Logstash, Elastic Agent, Saved Objects métier | Quotidienne |

Cette séparation est **fondamentale** : elle permet à chaque couche d'évoluer
à son rythme sans risque de régression sur les couches en dessous, et elle
correspond aux **frontières d'équipes** (plateforme vs SRE Elastic vs équipes
applicatives).

---

## Couche 1 — Operator ECK via GitOps

Deux options selon le contexte OpenShift :

### Option A — OLM (recommandée sur OpenShift)

Subscription gérée par GitOps, c'est la voie supportée Red Hat :

```yaml
# components/eck-operator/subscription.yaml
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: elastic-cloud-eck
  namespace: openshift-operators
spec:
  channel: stable
  installPlanApproval: Manual    # ⚠ Manual en prod, jamais Automatic
  name: elastic-cloud-eck
  source: certified-operators
  startingCSV: elastic-cloud-eck.v2.16.0
```

- `installPlanApproval: Manual` est le standard SRE : on **veut** voir et
  approuver chaque upgrade de l'opérateur (les bugs introduits par un opérateur
  cassent tout le cluster Elastic). On approuve manuellement après lecture du
  changelog ECK.
- `startingCSV` est **épinglé**. Pas de `latest`, jamais.
- Catalogue `certified-operators` car c'est l'opérateur signé Elastic /
  Red Hat.

### Option B — Helm chart

Si vous n'utilisez pas OLM (ex : OKD lite, cluster non-Red Hat), passez par
le chart officiel Elastic packagé par ArgoCD :

```yaml
# apps/eck-operator-app.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: eck-operator
  namespace: openshift-gitops
  annotations:
    argocd.argoproj.io/sync-wave: "1"
spec:
  source:
    repoURL: https://helm.elastic.co
    chart: eck-operator
    targetRevision: 2.16.0       # épinglé
    helm:
      values: |
        installCRDs: true
        managedNamespaces: ["elastic"]
        resources:
          limits: {cpu: 1, memory: 1Gi}
  destination:
    server: https://kubernetes.default.svc
    namespace: elastic-system
  syncPolicy:
    automated: { prune: true, selfHeal: true }
    syncOptions: [ServerSideApply=true, CreateNamespace=true]
```

> **`ServerSideApply=true`** est **obligatoire** pour les CRD volumineux
> d'ECK (le `kubectl apply` client-side dépasse la limite des 256 KiB
> d'annotation `last-applied-configuration`).

### Restreindre la portée

`managedNamespaces` (Helm) ou `WATCH_NAMESPACE` (OLM via Subscription
config) doit lister **explicitement** les namespaces où vivent vos CR
Elastic. Sinon l'opérateur watch tout le cluster — dans un grand OpenShift,
c'est inutilement lourd et c'est une surface d'attaque.

---

## Couche 2 — Topologie cluster (CR Elasticsearch)

Voici le **squelette de référence** pour un cluster de production :
3 masters dédiés, hot/warm tiers, anti-affinity stricte.

```yaml
# components/elasticsearch/elasticsearch.yaml
apiVersion: elasticsearch.k8s.elastic.co/v1
kind: Elasticsearch
metadata:
  name: prod
  namespace: elastic
spec:
  version: 8.16.1                # épinglé, suit l'opérateur
  updateStrategy:
    changeBudget:
      maxSurge: 1
      maxUnavailable: 1          # protège le quorum

  # Auth : on désactive l'auth basic interne au profit d'OIDC OpenShift
  auth:
    fileRealm:
      - secretName: es-internal-users     # secret géré par ESO

  http:
    tls:
      certificate:
        secretName: es-http-tls           # cert-manager → cert externe
    service:
      spec:
        type: ClusterIP                   # on expose via Route OpenShift

  nodeSets:
    # ── Masters dédiés ─────────────────────────────────────────────
    - name: master
      count: 3
      config:
        node.roles: ["master"]
        xpack.ml.enabled: false
      podTemplate:
        spec:
          # OpenShift : pas de runAsUser fixe, on laisse le SCC restricted-v2
          securityContext:
            fsGroup: 1000
          affinity:
            podAntiAffinity:
              requiredDuringSchedulingIgnoredDuringExecution:
                - labelSelector:
                    matchLabels:
                      elasticsearch.k8s.elastic.co/cluster-name: prod
                      elasticsearch.k8s.elastic.co/node-master: "true"
                  topologyKey: kubernetes.io/hostname
          containers:
            - name: elasticsearch
              resources:
                requests: {cpu: 500m, memory: 2Gi}
                limits:   {memory: 2Gi}    # ⚠ pas de limit CPU, voir plus bas
              env:
                - name: ES_JAVA_OPTS
                  value: "-Xms1g -Xmx1g"   # 50 % de la mem, max 31 Gi
      volumeClaimTemplates:
        - metadata: { name: elasticsearch-data }
          spec:
            accessModes: [ReadWriteOnce]
            resources: { requests: { storage: 10Gi } }
            storageClassName: ocs-storagecluster-ceph-rbd

    # ── Data hot ───────────────────────────────────────────────────
    - name: data-hot
      count: 3
      config:
        node.roles: ["data_hot", "data_content", "ingest"]
      podTemplate:
        spec:
          # Les data nodes sur des nœuds OpenShift dédiés (taint/toleration)
          tolerations:
            - key: workload
              value: elastic-data
              effect: NoSchedule
          nodeSelector:
            node-role.kubernetes.io/elastic-data: ""
          affinity:
            podAntiAffinity:
              requiredDuringSchedulingIgnoredDuringExecution:
                - labelSelector:
                    matchExpressions:
                      - { key: elasticsearch.k8s.elastic.co/node-data_hot, operator: In, values: ["true"] }
                  topologyKey: topology.kubernetes.io/zone   # spread inter-zones
          containers:
            - name: elasticsearch
              resources:
                requests: {cpu: 4,    memory: 32Gi}
                limits:   {memory: 32Gi}
              env:
                - name: ES_JAVA_OPTS
                  value: "-Xms16g -Xmx16g"
      volumeClaimTemplates:
        - metadata: { name: elasticsearch-data }
          spec:
            accessModes: [ReadWriteOnce]
            resources: { requests: { storage: 1Ti } }
            storageClassName: ocs-storagecluster-ceph-rbd-ssd

    # ── Data warm ──────────────────────────────────────────────────
    - name: data-warm
      count: 2
      config:
        node.roles: ["data_warm"]
      # ... resources + PVC sur du HDD ou stockage moins cher

    # ── (Optionnel) Frozen tier sur object storage ─────────────────
    - name: data-frozen
      count: 2
      config:
        node.roles: ["data_frozen"]
        # Searchable snapshots → cache local seulement
      volumeClaimTemplates:
        - metadata: { name: elasticsearch-data }
          spec:
            resources: { requests: { storage: 200Gi } }   # cache, pas data

    # ── Coordinating / ML (optionnel) ──────────────────────────────
    - name: coordinating
      count: 2
      config:
        node.roles: []                    # rôle vide = coordinating only
```

### Points sensibles SRE

- **3 masters dédiés** non négociable en prod. Jamais 2, jamais 1, jamais
  collocaté avec data sur clusters > 50 GB.
- **Anti-affinity hard** (`required…`) sur les masters par hostname,
  **et** sur les data par zone. Sans ça, une perte de nœud OpenShift coupe le
  quorum.
- **`maxUnavailable: 1`** : ECK fait un *graceful drain* nœud par nœud.
- **Pas de `limits.cpu`** sur les data nodes : c'est la
  [recommandation Elastic depuis 2022](https://www.elastic.co/blog/managing-cpu-resources-for-elasticsearch-on-kubernetes).
  Le throttling CFS Linux dégrade dramatiquement les latences de search. On
  garde `requests.cpu` pour le scheduling, pas de limit.
- **JVM heap** : 50 % de la mémoire du conteneur, **plafonné à 31 GiB**
  (au-delà on perd le compressed-oops). Le reste sert au file system cache,
  vital pour les performances de search.
- **`storage`** : prévoyez **3× la taille des données primaires** (réplicas
  + overhead segments + headroom merge). Et utilisez un StorageClass
  `WaitForFirstConsumer` pour respecter l'anti-affinity.
- **`vm.max_map_count = 262144`** : nécessaire pour Elasticsearch. Sur
  OpenShift, soit vous utilisez un `MachineConfig` pour le mettre au niveau
  des nœuds (recommandé), soit vous laissez ECK déployer un `initContainer`
  privilégié (contraire à la philosophie OpenShift).

---

## Couche 3 — Configuration stack (StackConfigPolicy)

C'est **la pièce centrale** du compromis hybride. Avant ECK 2.6, tout ce qui
suit demandait des appels API REST post-déploiement. Aujourd'hui, c'est
**du YAML versionné**.

```yaml
# components/elasticsearch/stack-config.yaml
apiVersion: stackconfigpolicy.k8s.elastic.co/v1alpha1
kind: StackConfigPolicy
metadata:
  name: prod-config
  namespace: elastic
spec:
  resourceSelector:
    matchLabels:
      env: prod
  secureSettings:
    - secretName: es-secure-settings        # géré par ESO (S3 keys, etc.)

  elasticsearch:
    clusterSettings:
      indices.lifecycle.history_index_enabled: true
      action.destructive_requires_name: true   # garde-fou anti delete *

    snapshotRepositories:
      backup-s3:
        type: s3
        settings:
          bucket: elastic-prod-snapshots
          base_path: prod-cluster
          region: eu-west-3
          # access_key et secret_key sont dans secureSettings → keystore

    snapshotLifecyclePolicies:
      daily-snapshots:
        schedule: "0 30 1 * * ?"             # 01:30 UTC chaque jour
        name: "<daily-snap-{now/d}>"
        repository: backup-s3
        config:
          indices: ["*"]
          include_global_state: true
        retention:
          expire_after: "30d"
          min_count: 7
          max_count: 60

    indexLifecyclePolicies:
      logs-ilm:
        phases:
          hot:
            min_age: "0ms"
            actions:
              rollover: { max_primary_shard_size: "50gb", max_age: "1d" }
              set_priority: { priority: 100 }
          warm:
            min_age: "7d"
            actions:
              shrink: { number_of_shards: 1 }
              forcemerge: { max_num_segments: 1 }
              allocate: { include: { _tier_preference: "data_warm" } }
              set_priority: { priority: 50 }
          cold:
            min_age: "30d"
            actions:
              searchable_snapshot: { snapshot_repository: backup-s3 }
          delete:
            min_age: "365d"
            actions:
              delete: {}

    indexTemplates:
      composableIndexTemplates:
        logs-template:
          index_patterns: ["logs-*"]
          priority: 500
          template:
            settings:
              index.lifecycle.name: logs-ilm
              index.lifecycle.rollover_alias: logs
              number_of_shards: 3
              number_of_replicas: 1
              codec: best_compression

    roleMappings:
      sre-team:
        roles: ["superuser"]
        rules:
          field: { "groups": "sre-elastic" }   # claim depuis OIDC
        enabled: true

  kibana:
    config:
      xpack.fleet.agentPolicies:
        - name: default-policy
          # ...
      xpack.encryptedSavedObjects.encryptionKey: "${KBN_ENCRYPTION_KEY}"
```

### Pourquoi c'est le pivot du compromis

- **Tout est dans Git**. ILM, SLM, templates, rôles, mappings SAML : un
  changement = un commit = une PR = une revue.
- **ECK applique l'idempotence**. Si une politique est modifiée hors-bande
  (par un humain via Kibana), l'opérateur la **réécrit** au prochain
  reconcile. C'est l'équivalent du `selfHeal: true` d'ArgoCD, mais au niveau
  Elasticsearch.
- **Les secrets restent ailleurs**. Le champ `secureSettings` pointe vers un
  Secret Kubernetes natif (lui-même produit par External Secrets), qui est
  injecté dans le **keystore Elasticsearch**, jamais en clair dans un CR.

### Limites connues à 2026

- `StackConfigPolicy` ne gère pas **encore** les `transforms`, les
  `enrich policies` complexes, ni l'ensemble des `watchers`. Pour ces cas,
  on retombe sur des **Jobs d'init** (cf. plus bas).
- Les **dashboards Kibana** ne se déclarent pas tous via `kibana.config`. Pour
  les dashboards complexes, on commit le NDJSON exporté et on lance un Job
  qui appelle `POST /api/saved_objects/_import`. C'est la principale fuite du
  modèle déclaratif aujourd'hui — assumée.

---

## Couche 4 — Workload (Beats, APM, Fleet, Logstash)

Tous des CR ECK natifs, déployés en wave 4.

```yaml
# components/elastic-agent/fleet-server.yaml
apiVersion: agent.k8s.elastic.co/v1alpha1
kind: Agent
metadata:
  name: fleet-server
  namespace: elastic
spec:
  version: 8.16.1
  kibanaRef: { name: prod-kibana }
  elasticsearchRefs:
    - { name: prod }
  mode: fleet
  fleetServerEnabled: true
  deployment:
    replicas: 2
    podTemplate:
      spec:
        affinity:
          podAntiAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              - labelSelector: { matchLabels: { common.k8s.elastic.co/type: agent } }
                topologyKey: kubernetes.io/hostname
```

```yaml
# components/elastic-agent/agent-daemonset.yaml
apiVersion: agent.k8s.elastic.co/v1alpha1
kind: Agent
metadata:
  name: node-agent
  namespace: elastic
spec:
  version: 8.16.1
  kibanaRef: { name: prod-kibana }
  fleetServerRef: { name: fleet-server }
  mode: fleet
  daemonSet:
    podTemplate:
      spec:
        serviceAccountName: elastic-agent
        # SCC OpenShift : Agent a besoin de hostNetwork + hostPath
        # → SCC privileged dédié, jamais le défaut "privileged" générique
```

> **OpenShift + Elastic Agent** : Agent collecte des métriques nœud
> (cgroups, kubelet) et a besoin d'un SCC qui autorise `hostPath` et
> `hostNetwork`. Créez un SCC dédié `elastic-agent-scc` (clone de
> `node-exporter`), liez-le au ServiceAccount `elastic-agent` via un
> `RoleBinding`. Ne réutilisez **jamais** `privileged`.

---

## Secrets et authentification

### Architecture cible

```
        ┌──────────────┐
        │    Vault     │
        │   (HashiCorp │
        │    ou KV     │
        │    Azure/AWS)│
        └──────┬───────┘
               │  pull (token / IRSA / Workload Identity)
               ▼
   ┌───────────────────────┐
   │ External Secrets Op.  │  (ClusterSecretStore)
   └──────────┬────────────┘
              │ materialize
              ▼
   ┌───────────────────────┐       referenced by
   │ Secret natif K8s      │ ◄────────────────────  CR ECK / SCP
   └───────────────────────┘
```

### Conventions

| Secret | Source Vault | Consommateur | Type |
|---|---|---|---|
| `es-internal-users` | `secret/elastic/prod/users` | `Elasticsearch.spec.auth.fileRealm` | `kubernetes.io/basic-auth` formaté file_realm |
| `es-secure-settings` | `secret/elastic/prod/keystore` | `StackConfigPolicy.secureSettings` | Opaque (clés S3, smtp, …) |
| `es-http-tls` | issued by cert-manager | `Elasticsearch.spec.http.tls` | `kubernetes.io/tls` |
| `kibana-encryption-keys` | `secret/elastic/prod/kibana` | `Kibana.spec.config` (envFrom) | Opaque |
| `fleet-enrollment-token` | généré par Kibana au bootstrap | `Agent` (mode fleet) | Opaque |

### Auth utilisateur — OIDC OpenShift

L'authentification utilisateur passe **toujours** par OIDC en prod. L'idée
est de **fédérer Kibana avec l'OAuth OpenShift** lui-même (qui peut être
backé par Keycloak, AAD, Okta…) :

```yaml
# extrait Kibana.spec.config
xpack.security.authc.providers:
  oidc.openshift:
    order: 0
    realm: oidc-openshift
    description: "Sign in with OpenShift"
elasticsearch.requestHeadersWhitelist: ["authorization", "content-type"]
```

```yaml
# extrait StackConfigPolicy.elasticsearch.config
xpack.security.authc.realms.oidc.oidc-openshift:
  order: 2
  rp.client_id: "elastic-prod"
  rp.response_type: code
  op.issuer: "https://oauth-openshift.apps.cluster.example.com"
  op.authorization_endpoint: "${op.issuer}/oauth/authorize"
  op.token_endpoint: "${op.issuer}/oauth/token"
  claims.principal: preferred_username
  claims.groups: groups
```

Les `roleMappings` (cf. couche 3) traduisent ensuite les groupes OIDC en
rôles Elastic. L'utilisateur `elastic` superuser n'est **jamais** utilisé
en interactif — réservé au break-glass.

---

## Certificats TLS

Trois niveaux de certificats coexistent :

1. **Transport (inter-nœuds)** — géré 100 % par ECK avec sa propre CA. On
   n'y touche jamais.
2. **HTTP (cluster interne)** — par défaut ECK génère aussi. **OK pour les
   appels internes** (Kibana → ES, Beats → ES via service ClusterIP).
3. **HTTP (exposition externe via Route)** — c'est là qu'on prend la main :
   on déclare un `Certificate` cert-manager (Let's Encrypt ou CA d'entreprise)
   et on l'injecte dans `Elasticsearch.spec.http.tls.certificate.secretName`.

```yaml
# components/elasticsearch/certificate.yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: es-http-tls
  namespace: elastic
spec:
  secretName: es-http-tls
  issuerRef:
    name: letsencrypt-prod
    kind: ClusterIssuer
  dnsNames:
    - elastic-prod.apps.cluster.example.com
  duration: 2160h        # 90 j
  renewBefore: 360h      # 15 j
```

L'opérateur ECK détecte le Secret, le monte dans les pods, et **gère le
rolling restart** quand cert-manager le renouvelle.

---

## Stockage et snapshots

### Choix de StorageClass

| Tier | StorageClass typique sur OpenShift | IOPS / latence cible |
|---|---|---|
| Master | `ocs-storagecluster-ceph-rbd` (10 Gi, peu sollicité) | Standard |
| Hot | NVMe local ou `ocs-storagecluster-ceph-rbd-ssd` | > 5000 IOPS, < 5 ms |
| Warm | RBD HDD ou stockage classique | > 1000 IOPS |
| Frozen | **Pas de PV de données** — searchable snapshots ; juste un cache local | N/A |

**Avoid** : NFS (latences imprévisibles, locks), GlusterFS, et tout
StorageClass qui ne garantit pas le `fsync` durable.

### Snapshots — l'option de référence

L'objet storage **doit** être hors du cluster Kubernetes :

- **AWS** : S3 + IRSA pour l'authentification.
- **Azure** : Azure Blob + Workload Identity.
- **OpenShift on-prem** : ODF MultiCloud Object Gateway (MCG) ou MinIO
  externe.

Le repo s'enregistre via `StackConfigPolicy.snapshotRepositories`, et la
politique de rétention via `snapshotLifecyclePolicies`. **Tout dans Git**,
clés dans Vault.

### Test de restauration

> **Une sauvegarde non testée n'est pas une sauvegarde.**

Schedulez **mensuellement** un test de restauration sur un cluster
*scratch* (peut être un cluster ECK lancé à la demande). Un Job CronJob qui :

1. Provisionne un cluster ECK temporaire.
2. Enregistre le repo S3 en mode `readonly: true`.
3. Restaure une politique d'index récente.
4. Vérifie le compte de documents et la cohérence.
5. Détruit le cluster.

Ce CronJob est lui-même dans Git, ses logs partent dans le Stack Monitoring.

---

## Spécificités OpenShift

| Sujet | Comportement par défaut | Action SRE |
|---|---|---|
| **SCC** | OpenShift impose `restricted-v2` | ECK est compatible depuis 2.5. Vérifier `runAsNonRoot: true`, pas de `runAsUser` fixe. |
| **`vm.max_map_count`** | 65530 par défaut, insuffisant | `MachineConfig` qui set `262144` au niveau du nœud, **pas** d'init container privilégié. |
| **`fs.file-max`** | OK par défaut | Aucune action. |
| **Routes vs Ingress** | OpenShift préfère `Route` | `Route` avec `passthrough` TLS pour préserver le cert ECK. |
| **Namespaces réservés** | `openshift-*` | Ne **jamais** déployer Elastic dans ces namespaces. Utiliser `elastic` ou `elastic-{env}`. |
| **Image registry** | `registry.redhat.io` ou `docker.elastic.co` | Mirror dans le registry interne OpenShift via ImageContentSourcePolicy en environnement air-gap. |
| **Network policies** | OpenShift SDN/OVN-Kubernetes | Une `NetworkPolicy` par namespace : ingress restreint à Kibana + Fleet + Beats, egress restreint à S3 + IdP. |
| **PriorityClass** | Pas de défaut | Créer `elastic-master-priority` (high) et `elastic-data-priority` (medium) pour protéger Elastic en cas de pression mémoire. |
| **PodDisruptionBudget** | ECK le crée automatiquement | Ne pas l'override sauf compréhension fine. |

---

## Multi-environnements et multi-clusters (ApplicationSet)

Pour gérer dev / staging / prod (et N clusters), on utilise un
**ApplicationSet** ArgoCD avec un *cluster generator* + *list generator*
combinés :

```yaml
# apps/elastic-applicationset.yaml
apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata:
  name: elastic-stack
  namespace: openshift-gitops
spec:
  generators:
    - matrix:
        generators:
          - clusters:
              selector:
                matchLabels:
                  elastic-target: "true"
          - list:
              elements:
                - tier: dev
                - tier: staging
                - tier: prod
  template:
    metadata:
      name: 'elastic-{{tier}}-{{name}}'
    spec:
      project: elastic
      source:
        repoURL: https://github.com/<org>/gitops-openshift-repo.git
        targetRevision: main
        path: 'overlays/{{tier}}'
      destination:
        server: '{{server}}'
        namespace: 'elastic'
      syncPolicy:
        automated: { prune: true, selfHeal: true }
        syncOptions: [ServerSideApply=true]
```

Avec une arborescence Kustomize :

```
overlays/
├── base/                     # CR génériques sans valeurs spécifiques
├── dev/
│   ├── kustomization.yaml    # patch : 1 master + 1 data, ressources réduites
│   └── stackconfig-patch.yaml
├── staging/
│   ├── kustomization.yaml    # patch : 3 masters + 2 data
│   └── stackconfig-patch.yaml
└── prod/
    ├── kustomization.yaml    # base, full topo
    └── stackconfig-patch.yaml
```

**Promotion** = merge d'une PR de `dev` vers `staging` puis `prod`. Le diff
Kustomize entre overlays montre **exactement** ce qui change entre envs.

---

## Stratégie de mise à jour

### Upgrade ECK Operator

1. Lire le **CHANGELOG ECK** intégralement (les *breaking changes* CRD
   arrivent).
2. Sur **dev**, bumper le `startingCSV` (OLM) ou la `targetRevision`
   (Helm) → laisser ArgoCD synchroniser → approuver l'InstallPlan
   manuellement.
3. **Attendre 24 h**, observer le Stack Monitoring.
4. Promouvoir staging, attendre 72 h, promouvoir prod.

### Upgrade Elasticsearch (mineure : 8.16.1 → 8.16.2)

- Bump `version:` dans le CR `Elasticsearch`. ECK fait le rolling upgrade
  tout seul, **un nœud à la fois**, en respectant le quorum et en attendant
  *green* avant de passer au suivant.
- Surveiller la métrique `elasticsearch_cluster_health_status` pendant
  l'opération.

### Upgrade Elasticsearch (majeure : 8.x → 9.x)

- **Lire la doc « Upgrade Elasticsearch »** d'Elastic (déprécations,
  reindex obligatoire ?).
- Snapshot complet **avant** de bumper.
- Sur dev : test du upgrade avec un dataset représentatif.
- Sur prod : fenêtre de maintenance, `maxUnavailable: 1`, surveiller chaque
  nœud.
- Plan de rollback documenté : le rollback ES 9 → 8 **n'est pas supporté
  in-place**. Le rollback = restore du snapshot pré-upgrade sur un cluster
  8.x neuf. Cette contrainte conditionne la fenêtre acceptable d'indispo.

---

## Stack Monitoring (méta-monitoring)

**Règle absolue** : on ne monitore pas un cluster Elastic avec lui-même.

Pourquoi : si le cluster est en panne, on **perd la visibilité** au moment
où on en a besoin. C'est l'équivalent de stocker les logs Kubernetes sur
le cluster qu'on diagnostique.

### Architecture

```
  ┌──────────────────┐    metrics + logs     ┌──────────────────────┐
  │ cluster prod 1   │ ────────────────────► │  cluster monitoring  │
  │  ES + Kibana     │                       │   ES dédié           │
  └──────────────────┘                       │   (1 master, 2 data) │
  ┌──────────────────┐                       │   Kibana             │
  │ cluster prod 2   │ ────────────────────► │                      │
  └──────────────────┘                       └──────────────────────┘
```

### Implémentation

Sur chaque cluster prod, l'`Elasticsearch` CR référence un cluster
externe pour shipper ses propres métriques :

```yaml
spec:
  monitoring:
    metrics:
      elasticsearchRefs:
        - name: monitoring
          namespace: elastic-monitoring
          serviceName: monitoring-es-http
          # secret de credentials managé par ESO
    logs:
      elasticsearchRefs:
        - name: monitoring
          namespace: elastic-monitoring
```

ECK déploie automatiquement un Metricbeat et un Filebeat sidecar.

---

## Day-2 operations / runbooks

À documenter dans `docs/runbooks/` (un fichier par procédure) — exemples
des runbooks d'une équipe SRE Elastic mature :

| Runbook | Trigger | Action |
|---|---|---|
| `cluster-red.md` | Alerte `ClusterStatusRed` | Identifier shards unassigned, vérifier disk watermark, lancer `_cluster/reroute` si besoin |
| `disk-watermark-high.md` | `ElasticsearchDiskUsage > 85%` | Étendre PVC ou supprimer indices oldest via ILM force-delete |
| `master-quorum-lost.md` | Pas de master élu | Vérifier les 3 master pods, network policies, DNS, **ne pas** supprimer le PVC d'un master |
| `restore-from-snapshot.md` | DR | Procédure complète depuis l'objet storage |
| `rotate-encryption-keys.md` | Compliance trimestrielle | Rotation des clés Kibana saved objects |
| `add-node.md` | Capacity planning | Bump du `count` dans le NodeSet, monitor du rebalance |
| `remove-node.md` | Décommissioning | Retirer du NodeSet, ECK fait l'allocation away avant arrêt |
| `eck-operator-stuck.md` | CR en `Reconciling` infini | Vérifier les events, regarder les logs operator |

Ces runbooks sont **versionnés dans le même dépôt** que les manifests :
quand on modifie le déploiement, on touche aussi le runbook si besoin.

---

## Anti-patterns à éviter

À lister explicitement dans la doc d'équipe pour qu'aucun nouveau ne les
reproduise :

1. **`installPlanApproval: Automatic`** sur l'opérateur ECK en prod. Un bug
   d'ECK = tout le cluster Elastic gelé.
2. **`master + data` collocaté** sur un cluster > 50 GB. Le master OOM
   pendant un GC long et tout tombe.
3. **Mots de passe en clair dans Git**, même « pour démarrer ». Ça reste
   pour toujours dans l'historique Git.
4. **`limits.cpu`** sur les data nodes. Throttling CFS = latences search
   x 10.
5. **NFS en backend de PV.** Locks NFS + Elasticsearch = corruption.
6. **Pas d'anti-affinity.** Un seul nœud OpenShift qui tombe coupe le
   quorum.
7. **Reindex / scripts ad-hoc en console Kibana** sur prod. Tout passe par
   un PR sur le `StackConfigPolicy` ou un Job versionné.
8. **Cluster Elastic qui se monitore lui-même.** Cf. plus haut.
9. **Snapshots vers un PV K8s.** Les snapshots doivent être hors-cluster,
   point.
10. **Pas de plan de rollback sur upgrade majeur.** ES major n'est pas
    réversible in-place.
11. **`replicas: 0`** sur un nœud master pour « débugger ». Vous venez de
    perdre le quorum.
12. **Désactiver xpack.security en prod**, même temporairement. C'est un
    aller simple.

---

## Arborescence cible dans ce dépôt

À ajouter en sus de l'existant :

```
.
├── README.md                                         # stack Prom/Graf existante
├── README_Elastic_Openshift.md                       # (ce fichier)
│
├── bootstrap/
│   ├── 01-gitops-operator.yaml                       # déjà là
│   ├── 02-argocd-rbac.yaml                           # déjà là
│   ├── 03-root-app.yaml                              # à étendre pour Elastic
│   ├── 04-eck-operator-subscription.yaml             # nouveau
│   ├── 05-external-secrets-subscription.yaml        # nouveau
│   └── 06-cert-manager-subscription.yaml             # nouveau
│
├── apps/
│   ├── kustomization.yaml
│   ├── 01-cluster-monitoring-app.yaml                # déjà là
│   ├── 02-grafana-operator-app.yaml                  # déjà là
│   ├── 03-grafana-instance-app.yaml                  # déjà là
│   ├── 10-eck-operator-app.yaml                      # nouveau (wave 1)
│   ├── 11-elastic-platform-app.yaml                  # nouveau (wave 1) — ESO, certs
│   ├── 12-elasticsearch-app.yaml                     # nouveau (wave 2)
│   ├── 13-stackconfig-app.yaml                       # nouveau (wave 3)
│   └── 14-elastic-agents-app.yaml                    # nouveau (wave 4)
│
└── components/
    ├── cluster-monitoring/                           # déjà là
    ├── grafana-operator/                             # déjà là
    ├── grafana-instance/                             # déjà là
    │
    ├── eck-operator/                                 # nouveau
    │   ├── kustomization.yaml
    │   └── subscription.yaml
    │
    ├── elastic-platform/                             # nouveau
    │   ├── kustomization.yaml
    │   ├── namespace.yaml                            # ns "elastic"
    │   ├── networkpolicy.yaml
    │   ├── cluster-secret-store.yaml                 # ESO → Vault
    │   ├── cluster-issuer.yaml                       # cert-manager
    │   └── scc-elastic-agent.yaml                    # SCC custom Agent
    │
    ├── elasticsearch/                                # nouveau
    │   ├── kustomization.yaml
    │   ├── elasticsearch.yaml                        # CR Elasticsearch
    │   ├── kibana.yaml                               # CR Kibana
    │   ├── certificate.yaml                          # cert-manager
    │   ├── route-elasticsearch.yaml
    │   ├── route-kibana.yaml
    │   └── external-secrets/
    │       ├── es-internal-users.yaml
    │       ├── es-secure-settings.yaml
    │       └── kibana-encryption-keys.yaml
    │
    ├── stackconfig/                                  # nouveau
    │   ├── kustomization.yaml
    │   └── stack-config-policy.yaml                  # ILM, SLM, templates, roles
    │
    └── elastic-agents/                               # nouveau
        ├── kustomization.yaml
        ├── fleet-server.yaml
        ├── agent-daemonset.yaml
        └── apm-server.yaml

overlays/                                             # nouveau
├── base/                                             # symlinks vers components/
├── dev/
│   ├── kustomization.yaml
│   ├── elasticsearch-patch.yaml                      # 1 master, 1 data
│   └── stackconfig-patch.yaml                        # ILM compressé
├── staging/
│   ├── kustomization.yaml
│   └── elasticsearch-patch.yaml
└── prod/
    └── kustomization.yaml                            # base sans patch
```

---

## Bootstrap pas à pas

> **Pré-requis** : avoir déjà appliqué `bootstrap/01-gitops-operator.yaml`
> et `bootstrap/02-argocd-rbac.yaml` du `README.md` principal.

### 1. Provisionner les dépendances hors-stack

Avant tout déploiement Elastic :

- **Bucket S3 pour snapshots** (Terraform / console cloud).
- **Vault** initialisé, KV `secret/elastic/` créé, policy ArgoCD/ESO en
  place.
- **DNS** : enregistrements `elastic-prod.apps.cluster.example.com` et
  `kibana-prod.apps.cluster.example.com`.
- **MachineConfig** `vm.max_map_count = 262144` appliqué et nœuds
  redémarrés.

### 2. Installer les opérateurs de plateforme

```bash
oc apply -f bootstrap/04-eck-operator-subscription.yaml
oc apply -f bootstrap/05-external-secrets-subscription.yaml
oc apply -f bootstrap/06-cert-manager-subscription.yaml

# Approuver les InstallPlans manuellement :
oc -n openshift-operators get installplan
oc -n openshift-operators patch installplan <name> \
   --type merge -p '{"spec":{"approved":true}}'
```

### 3. Étendre l'App racine

Ajouter au `bootstrap/03-root-app.yaml` (ou créer une App ArgoCD
parallèle) une référence vers `apps/` qui contient désormais aussi les
Apps Elastic. Les **sync waves** (annotation
`argocd.argoproj.io/sync-wave`) garantissent l'ordre :

- wave **1** : `eck-operator-app`, `elastic-platform-app`
- wave **2** : `elasticsearch-app` (CR ES + Kibana)
- wave **3** : `stackconfig-app` (StackConfigPolicy)
- wave **4** : `elastic-agents-app` (Fleet, APM, Beats)

### 4. Pousser, observer

```bash
git add . && git commit -m "feat: Elastic stack hybrid (ECK+GitOps)" && git push

# Watch
oc -n openshift-gitops get applications -w
oc -n elastic get elasticsearch,kibana,stackconfigpolicy,agent
oc -n elastic get pods
```

### 5. Récupérer les accès

```bash
# Mot de passe superuser "elastic" (break-glass uniquement)
oc -n elastic get secret prod-es-elastic-user \
   -o go-template='{{.data.elastic | base64decode}}{{"\n"}}'

# URL Kibana
oc -n elastic get route kibana-route \
   -o jsonpath='https://{.spec.host}{"\n"}'
```

L'authentification interactive doit ensuite **immédiatement** passer par
OIDC (cf. *Auth utilisateur*). Le superuser `elastic` est mis en coffre
break-glass.

---

## Références

### Officielles Elastic

- ECK documentation :
  <https://www.elastic.co/guide/en/cloud-on-k8s/current/index.html>
- StackConfigPolicy reference :
  <https://www.elastic.co/guide/en/cloud-on-k8s/current/k8s-stack-config-policy.html>
- ECK on OpenShift :
  <https://www.elastic.co/guide/en/cloud-on-k8s/current/k8s-openshift.html>
- Sizing Elasticsearch on Kubernetes (CPU limits) :
  <https://www.elastic.co/blog/managing-cpu-resources-for-elasticsearch-on-kubernetes>

### OpenShift / Red Hat

- OpenShift GitOps :
  <https://docs.openshift.com/gitops/latest/understanding_openshift_gitops/about-redhat-openshift-gitops.html>
- Security Context Constraints :
  <https://docs.openshift.com/container-platform/latest/authentication/managing-security-context-constraints.html>
- MachineConfig pour `sysctl` :
  <https://docs.openshift.com/container-platform/latest/post_installation_configuration/machine-configuration-tasks.html>

### Outillage GitOps

- ArgoCD ApplicationSet :
  <https://argo-cd.readthedocs.io/en/stable/operator-manual/applicationset/>
- Sync waves :
  <https://argo-cd.readthedocs.io/en/stable/user-guide/sync-waves/>
- External Secrets Operator :
  <https://external-secrets.io/>
- cert-manager :
  <https://cert-manager.io/docs/>

### Lectures recommandées

- *Elasticsearch in Production* — Elastic engineering blog series.
- *Kubernetes Operators* (O'Reilly) — Dobies & Wood, chapitre sur les
  *stateful operators*.
- *The Site Reliability Workbook* — chapitre 12 *Introducing Non-Abstract
  Large System Design* pour la philosophie de séparation Ops/Dev.

---

## Licence

Document publié sous la même licence que le dépôt parent (MIT-style).
Les composants déployés (ECK, Elasticsearch, Kibana…) restent soumis à
leurs licences respectives — **attention notamment à Elastic License v2 vs
SSPL** selon les fonctionnalités utilisées (xpack.security est gratuit
depuis 7.11, mais certaines features ML restent sous licence commerciale).
