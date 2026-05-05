# gitops-openshift-repo

Dépôt **GitOps** prêt à l'emploi pour déployer une stack d'observabilité complète
**Prometheus / Alertmanager / Grafana** sur **OpenShift** via **ArgoCD**
(OpenShift GitOps Operator), en utilisant le pattern **App-of-Apps** et
**Kustomize**.

> Philosophie : on s'appuie sur la stack monitoring **native d'OpenShift**
> (Cluster Monitoring Operator) pour Prometheus + Alertmanager + Thanos, et on
> ajoute **Grafana** via le **Grafana Operator** (community), avec une
> datasource pré-câblée sur Thanos Querier pour bénéficier d'une vue unifiée
> cluster + user-workload.

---

## Sommaire

1. [Architecture](#architecture)
2. [Arborescence du dépôt](#arborescence-du-dépôt)
3. [Prérequis](#prérequis)
4. [Installation pas à pas](#installation-pas-à-pas)
5. [Personnalisation](#personnalisation)
6. [Vérifier le déploiement](#vérifier-le-déploiement)
7. [Configurer Alertmanager](#configurer-alertmanager)
8. [Ajouter vos propres dashboards / datasources](#ajouter-vos-propres-dashboards--datasources)
9. [Mettre à jour la stack](#mettre-à-jour-la-stack)
10. [Désinstaller](#désinstaller)
11. [Troubleshooting](#troubleshooting)

---

## Architecture

```
                            ┌────────────────────────────────────────┐
                            │             OpenShift Cluster          │
                            │                                        │
   ┌───────────┐  push      │  ┌──────────────────────────────────┐  │
   │ Dev/Ops   │ ─────────► │  │  openshift-gitops (ArgoCD)       │  │
   │  Git      │            │  │  ─ root-monitoring-stack         │  │
   └───────────┘            │  │     ├─ cluster-monitoring  (1)   │  │
                            │  │     ├─ grafana-operator    (2)   │  │
                            │  │     └─ grafana-instance    (3)   │  │
                            │  └─────────────────┬────────────────┘  │
                            │                    │ sync              │
                            │   ┌────────────────▼─────────────┐     │
                            │   │ openshift-monitoring          │    │
                            │   │ ┌─────────┐ ┌──────────────┐  │    │
                            │   │ │ Prom    │ │ Alertmanager │  │    │
                            │   │ └────┬────┘ └──────────────┘  │    │
                            │   │      │  Thanos Querier        │    │
                            │   └──────┼─────────▲──────────────┘    │
                            │          │         │                   │
                            │   ┌──────▼─────────┴──────────┐        │
                            │   │ openshift-user-workload-… │        │
                            │   │   Prometheus (UWM)        │        │
                            │   └───────────────────────────┘        │
                            │                    ▲                   │
                            │                    │ scraping          │
                            │   ┌────────────────┴───────┐           │
                            │   │ Vos applications       │           │
                            │   └────────────────────────┘           │
                            │                                        │
                            │   ┌────────────────────────────────┐   │
                            │   │ grafana                        │   │
                            │   │  Grafana ──► Thanos Querier    │   │
                            │   │  (datasource auto-configurée)  │   │
                            │   └────────────────────────────────┘   │
                            └────────────────────────────────────────┘
```

**Sync waves ArgoCD** : `cluster-monitoring` (wave 1) → `grafana-operator`
(wave 2) → `grafana-instance` (wave 3). Cet ordonnancement garantit que les
CRDs Grafana sont disponibles avant de créer les CRs `Grafana`,
`GrafanaDatasource`, `GrafanaDashboard`.

---

## Arborescence du dépôt

```
.
├── README.md
├── bootstrap/                              # ⚙ À appliquer manuellement la 1ère fois
│   ├── 01-gitops-operator.yaml             # Installation OpenShift GitOps Operator
│   ├── 02-argocd-rbac.yaml                 # cluster-admin pour le SA d'ArgoCD
│   └── 03-root-app.yaml                    # Application racine (App-of-Apps)
│
├── apps/                                   # 📦 Applications ArgoCD (synchronisées par root)
│   ├── kustomization.yaml
│   ├── 01-cluster-monitoring-app.yaml
│   ├── 02-grafana-operator-app.yaml
│   └── 03-grafana-instance-app.yaml
│
└── components/                             # 🧱 Manifests réels appliqués sur le cluster
    ├── cluster-monitoring/
    │   ├── kustomization.yaml
    │   ├── cluster-monitoring-config.yaml          # Active UWM, retention Prom
    │   └── user-workload-monitoring-config.yaml    # Config Prom user-workload
    │
    ├── grafana-operator/
    │   ├── kustomization.yaml
    │   ├── namespace.yaml
    │   ├── operatorgroup.yaml
    │   └── subscription.yaml                       # Canal v5, community-operators
    │
    └── grafana-instance/
        ├── kustomization.yaml
        ├── serviceaccount.yaml                     # SA + Secret token
        ├── clusterrolebinding.yaml                 # Bind cluster-monitoring-view
        ├── grafana.yaml                            # CR Grafana + Route
        ├── grafana-datasource.yaml                 # Thanos Querier datasource
        ├── grafana-dashboard-cluster.yaml          # Dashboard ID 7249
        └── grafana-dashboard-node-exporter.yaml    # Dashboard ID 1860
```

---

## Prérequis

| Composant | Version minimale | Commentaire |
|---|---|---|
| **OpenShift** | 4.12+ | Testé sur 4.14 / 4.15 |
| **CLI `oc`** | identique au cluster | <https://docs.openshift.com/container-platform/latest/cli_reference/openshift_cli/getting-started-cli.html> |
| **Droits** | `cluster-admin` | Requis pour installer les Operators |
| **Catalogues** | `redhat-operators` et `community-operators` activés | Activés par défaut sur OpenShift |
| **Stockage** *(optionnel)* | StorageClass disponible | Nécessaire si vous activez la persistance Prom/Alertmanager |

> **Hors-cluster** : un éditeur YAML, un compte Git pour forker ce repo.

---

## Installation pas à pas

### 1. Forker / cloner le dépôt

```bash
git clone https://gitlab.com/Numberbis/gitops-openshift-repo.git
cd gitops-openshift-repo
```

### 2. Pointer ArgoCD vers **votre** fork

Trois fichiers contiennent la `repoURL` qu'ArgoCD utilisera comme source de
vérité. **Remplacez-la dans les trois** par l'URL de votre fork :

```bash
# Exemple : remplacement automatique
NEW_REPO="https://github.com/<votre-org>/gitops-openshift-repo.git"
sed -i "s|https://gitlab.com/Numberbis/gitops-openshift-repo.git|${NEW_REPO}|g" \
    bootstrap/03-root-app.yaml \
    apps/01-cluster-monitoring-app.yaml \
    apps/02-grafana-operator-app.yaml \
    apps/03-grafana-instance-app.yaml

git commit -am "chore: point ArgoCD apps to my fork"
git push
```

### 3. Se connecter à OpenShift

```bash
oc login --server=https://api.<votre-cluster>:6443 -u <admin>
oc whoami        # doit retourner un user cluster-admin
```

### 4. Bootstrap : installer OpenShift GitOps puis l'App racine

L'ordre est important : on installe d'abord l'opérateur GitOps, on attend que
les CRDs `Application` soient créées, puis on applique le RBAC et l'App racine.

```bash
# 4.a — Installation de l'opérateur OpenShift GitOps
oc apply -f bootstrap/01-gitops-operator.yaml

# 4.b — Attendre que l'opérateur déploie l'instance ArgoCD par défaut
#      (ServiceAccount openshift-gitops-argocd-application-controller)
oc -n openshift-gitops wait \
    --for=condition=Available deployment/openshift-gitops-server \
    --timeout=300s

# 4.c — Donner les droits cluster-admin au SA d'ArgoCD
oc apply -f bootstrap/02-argocd-rbac.yaml

# 4.d — Appliquer l'Application racine (App-of-Apps)
oc apply -f bootstrap/03-root-app.yaml
```

### 5. Suivre la synchronisation

```bash
# Watcher la création des Applications enfants
oc -n openshift-gitops get applications -w

# Récupérer l'URL de la console ArgoCD
oc -n openshift-gitops get route openshift-gitops-server \
   -o jsonpath='https://{.spec.host}{"\n"}'

# Récupérer le mot de passe admin initial (compte: admin)
oc -n openshift-gitops get secret openshift-gitops-cluster \
   -o jsonpath='{.data.admin\.password}' | base64 -d ; echo
```

À l'issue de la synchronisation (3 à 5 minutes selon le cluster), vous devez
voir 4 applications **`Synced` / `Healthy`** :

```
NAME                       SYNC STATUS   HEALTH STATUS
root-monitoring-stack      Synced        Healthy
cluster-monitoring         Synced        Healthy
grafana-operator           Synced        Healthy
grafana-instance           Synced        Healthy
```

---

## Personnalisation

### Activer la persistance Prometheus & Alertmanager

Éditez `components/cluster-monitoring/cluster-monitoring-config.yaml` et
décommentez les blocs `volumeClaimTemplate`. Adaptez `storageClassName` à
votre cluster :

```bash
oc get sc        # liste des StorageClass disponibles
```

Commitez, ArgoCD applique automatiquement.

### Changer le mot de passe admin Grafana

Le mot de passe est en clair dans `components/grafana-instance/grafana.yaml`
(uniquement pour démarrer rapidement). **En production**, utilisez un Secret :

```yaml
# 1. Créez un Secret hors GitOps (ou via Sealed Secrets / External Secrets)
oc -n grafana create secret generic grafana-admin \
    --from-literal=GF_SECURITY_ADMIN_USER=admin \
    --from-literal=GF_SECURITY_ADMIN_PASSWORD='un-mdp-fort'

# 2. Dans grafana.yaml, remplacez le bloc spec.config.security par :
#    deployment:
#      spec:
#        template:
#          spec:
#            containers:
#              - name: grafana
#                envFrom:
#                  - secretRef:
#                      name: grafana-admin
```

### Restreindre les droits du SA ArgoCD

`bootstrap/02-argocd-rbac.yaml` accorde `cluster-admin` pour la simplicité.
Si votre politique de sécurité l'interdit, créez un `ClusterRole` dédié
limité à : `monitoring.coreos.com`, `operators.coreos.com`,
`grafana.integreatly.org`, `route.openshift.io`, et aux ConfigMaps/Secrets
des namespaces concernés.

---

## Vérifier le déploiement

```bash
# Cluster monitoring (Prom + Alertmanager + Thanos) doit être Up
oc -n openshift-monitoring get pods

# User Workload Monitoring doit avoir ses pods
oc -n openshift-user-workload-monitoring get pods

# Grafana Operator
oc -n grafana get pods -l control-plane=controller-manager

# Instance Grafana
oc -n grafana get grafana,grafanadatasource,grafanadashboard

# URL de la console Grafana (Route auto-créée par l'opérateur)
oc -n grafana get route grafana-route \
    -o jsonpath='https://{.spec.host}{"\n"}'
```

Connectez-vous avec `admin` / `ChangeMe123!` (ou ce que vous avez paramétré).
La datasource **prometheus-thanos** doit apparaître comme datasource par
défaut, et les deux dashboards doivent afficher des métriques.

---

## Configurer Alertmanager

L'Alertmanager d'OpenShift se configure via le Secret
`alertmanager-main` du namespace `openshift-monitoring`. Il **n'est
volontairement pas géré dans ce dépôt** : un fichier YAML versionné
écraserait le routing par défaut d'OpenShift et masquerait des alertes
critiques.

Pour ajouter votre routing (Slack, e-mail, PagerDuty…), passez par la console
OpenShift (**Administration → Cluster Settings → Configuration →
Alertmanager**) ou par CLI :

```bash
oc -n openshift-monitoring extract secret/alertmanager-main \
    --to=- --keys=alertmanager.yaml > alertmanager.yaml
# … éditer alertmanager.yaml …
oc -n openshift-monitoring create secret generic alertmanager-main \
    --from-file=alertmanager.yaml --dry-run=client -o yaml \
    | oc apply -f -
```

> Si vous souhaitez gérer Alertmanager en GitOps, utilisez **Sealed Secrets**
> ou **External Secrets** pour ne pas committer de credentials, et ajoutez
> le Secret chiffré dans `components/cluster-monitoring/`.

---

## Ajouter vos propres dashboards / datasources

### Dashboard depuis grafana.com

Créez `components/grafana-instance/grafana-dashboard-mon-truc.yaml` :

```yaml
apiVersion: grafana.integreatly.org/v1beta1
kind: GrafanaDashboard
metadata:
  name: mon-dashboard
  namespace: grafana
spec:
  instanceSelector:
    matchLabels:
      dashboards: grafana
  datasources:
    - inputName: "DS_PROMETHEUS"
      datasourceName: "prometheus-thanos"
  grafanaCom:
    id: 12345        # ID public sur grafana.com/dashboards
```

Puis ajoutez-le à `components/grafana-instance/kustomization.yaml`.

### Dashboard depuis JSON inline ou URL

```yaml
spec:
  instanceSelector:
    matchLabels:
      dashboards: grafana
  url: "https://raw.githubusercontent.com/.../mon-dashboard.json"
  # OU :
  json: |
    { "title": "Mon dashboard", "panels": [ ... ] }
```

### Datasource supplémentaire (Loki, Tempo, …)

Mêmes principes : créer un `GrafanaDatasource` puis l'ajouter à la
kustomization du composant.

---

## Mettre à jour la stack

Tout est déclaratif et versionné. Pour mettre à jour :

1. Modifier le YAML pertinent (par ex. `channel: v5` → `channel: v6` pour le
   Grafana Operator).
2. `git commit && git push`.
3. ArgoCD synchronise dans la minute (ou cliquer **Sync** dans la console).

Pour les composants **OpenShift** (Prometheus, Alertmanager, Thanos), c'est le
**Cluster Version Operator** qui gère leurs versions, lié à la version du
cluster lui-même (`oc adm upgrade`).

---

## Désinstaller

```bash
# 1. Supprimer l'application racine — la pruning d'ArgoCD nettoie les apps enfants
oc -n openshift-gitops delete application root-monitoring-stack

# 2. Supprimer les CRs résiduels (par sécurité)
oc -n grafana delete grafana --all
oc -n grafana delete grafanadatasource --all
oc -n grafana delete grafanadashboard --all

# 3. Désinstaller le Grafana Operator
oc -n grafana delete subscription grafana-operator
oc delete namespace grafana

# 4. (Optionnel) Désinstaller OpenShift GitOps
oc delete -f bootstrap/02-argocd-rbac.yaml
oc delete -f bootstrap/01-gitops-operator.yaml

# 5. Réinitialiser la configuration cluster monitoring
oc -n openshift-monitoring delete configmap cluster-monitoring-config
oc -n openshift-user-workload-monitoring delete configmap user-workload-monitoring-config
```

> ⚠ La suppression des ConfigMaps désactive le User Workload Monitoring et
> ramène la rétention Prometheus aux valeurs par défaut.

---

## Troubleshooting

### `Application root-monitoring-stack` reste en `OutOfSync`

```bash
oc -n openshift-gitops describe application root-monitoring-stack
```

Causes fréquentes :

- **`repoURL` faux** dans les manifests d'apps → vérifiez que vous avez bien
  remplacé l'URL pour pointer sur votre fork (cf. étape 2).
- **Repo privé** → ajoutez un Secret de credentials dans `openshift-gitops`
  (cf. doc OpenShift GitOps : *Connecting to a private Git repository*).

### Grafana ne démarre pas

```bash
oc -n grafana get pods
oc -n grafana logs deploy/grafana-deployment
```

- **CrashLoopBackOff** : presque toujours un mot de passe admin invalide ou
  un Secret référencé manquant.
- **CRD missing** : l'application `grafana-operator` n'a pas fini de
  synchroniser. Attendez ou forcez `Sync` sur cette application avant
  `grafana-instance`. Les sync-waves devraient gérer ça automatiquement.

### Datasource Grafana en erreur "401 Unauthorized"

Le token du SA est manquant ou invalide :

```bash
oc -n grafana get secret grafana-sa-token -o jsonpath='{.data.token}' | base64 -d
```

S'il est vide, supprimez le Secret pour qu'il soit regénéré, puis re-déployez
la datasource :

```bash
oc -n grafana delete secret grafana-sa-token
oc -n openshift-gitops patch application grafana-instance \
    -p '{"operation":{"sync":{}}}' --type merge
```

### "no metrics" dans les dashboards

- Vérifier que Thanos répond :
  ```bash
  oc -n grafana exec deploy/grafana-deployment -- \
      curl -sk -H "Authorization: Bearer $(oc -n grafana get secret grafana-sa-token -o jsonpath='{.data.token}' | base64 -d)" \
      https://thanos-querier.openshift-monitoring.svc:9091/api/v1/query?query=up | head
  ```
- Vérifier que `cluster-monitoring-view` est bien bindé :
  ```bash
  oc get clusterrolebinding grafana-cluster-monitoring-view -o yaml
  ```

### Alertmanager : "no receivers configured"

Comportement par défaut d'OpenShift. Configurez un receiver
(cf. [Configurer Alertmanager](#configurer-alertmanager)).

---

## Références

- **OpenShift Monitoring**:
  <https://docs.openshift.com/container-platform/latest/monitoring/monitoring-overview.html>
- **OpenShift GitOps**:
  <https://docs.openshift.com/gitops/latest/understanding_openshift_gitops/about-redhat-openshift-gitops.html>
- **Grafana Operator (v5)**:
  <https://grafana-operator.github.io/grafana-operator/>
- **ArgoCD App-of-Apps**:
  <https://argo-cd.readthedocs.io/en/stable/operator-manual/cluster-bootstrapping/>
- **Sync Waves**:
  <https://argo-cd.readthedocs.io/en/stable/user-guide/sync-waves/>

---

## Licence

Ce dépôt est fourni à titre d'exemple et peut être réutilisé librement
(MIT-style). Les composants déployés (Grafana, Prometheus, etc.) restent
soumis à leurs licences respectives.
