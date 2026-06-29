# Runbook — Étape 2 : Prometheus, datasource Grafana, et routing d'alertes

**Objectif** : alimenter le Grafana OIDC de l'étape 1 en métriques, afficher des courbes réelles, puis router des alertes. À la fin : un dashboard affiche des données live, et une alerte de test part vers un receiver.

**Pré-requis** : étape 1 terminée (Grafana + OIDC validé dans le namespace `monitoring`, CR `Grafana` portant le label `dashboards: "grafana"`).

**Décision structurante** : ce runbook couvre **deux voies** pour Prometheus. Choisis-en une avant de commencer (§0), puis suis le chemin correspondant. Les sections marquées **[UWM]** et **[STACK]** sont mutuellement exclusives ; les sections non marquées sont communes.

---

## 0. Choisir la voie Prometheus

| Critère | Voie A — User Workload Monitoring (UWM) | Voie B — Stack Prometheus dédiée |
|---|---|---|
| Mise en place | Activation d'un flag, opérateur déjà présent | Installer le Prometheus Operator + CRs |
| Configs custom (relabeling lourd, fédération, receivers exotiques) | Limité au modèle déclaratif OCP | Liberté totale, proche de ton existant Docker |
| RBAC / scoping | Géré par la plateforme | À ta charge (ServiceAccount, ClusterRole) |
| Rétention / HA / Thanos | Intégrés | À configurer toi-même |
| Datasource Grafana | Thanos Querier + token (plus de friction) | `prometheus-operated` en direct (simple) |
| Maintenance | Portée par Red Hat | Portée par toi |

**Règle de décision** : si tes `prometheus.yml`/`alertmanager.yml` actuels sont du scrape + alerting assez standard → **UWM** (moins de dette). Si tu as des configs très spécifiques à préserver telles quelles → **stack dédiée**.

> Le reste du runbook est sectionné. Saute les blocs marqués de la voie que tu n'as pas choisie.

---

# VOIE A — User Workload Monitoring

## A.1 Activer UWM **[UWM]**

UWM s'active par un ConfigMap dans `openshift-monitoring`. **Attention** : c'est une ressource cluster-wide, tu as besoin des droits correspondants (sinon, demande à un cluster-admin).

Vérifie d'abord s'il existe déjà :

```bash
oc -n openshift-monitoring get configmap cluster-monitoring-config -o yaml 2>/dev/null
```

S'il n'existe pas, crée-le :

```yaml
# a1-enable-uwm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-monitoring-config
  namespace: openshift-monitoring
data:
  config.yaml: |
    enableUserWorkload: true
```

```bash
oc apply -f a1-enable-uwm.yaml
```

S'il **existe déjà**, ne l'écrase pas : édite-le et ajoute `enableUserWorkload: true` dans le `config.yaml` existant (`oc -n openshift-monitoring edit configmap cluster-monitoring-config`).

**Vérifier que la stack UWM démarre** (namespace créé automatiquement) :

```bash
oc -n openshift-user-workload-monitoring get pods -w
```

Tu dois voir apparaître `prometheus-user-workload-*`, `thanos-ruler-user-workload-*`, `prometheus-operator-*`. Attends qu'ils soient `Running`.

## A.2 (Optionnel) Régler rétention et ressources UWM **[UWM]**

Config utilisateur de la stack UWM via un ConfigMap dédié dans `openshift-user-workload-monitoring` :

```yaml
# a2-uwm-config.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: user-workload-monitoring-config
  namespace: openshift-user-workload-monitoring
data:
  config.yaml: |
    prometheus:
      retention: 15d
      volumeClaimTemplate:
        spec:
          resources:
            requests:
              storage: 40Gi
    # alertmanager dédié à UWM (voir §A.5, option 2)
    # alertmanager:
    #   enabled: true
```

```bash
oc apply -f a2-uwm-config.yaml
```

## A.3 Exposer une cible à scraper (ServiceMonitor) **[UWM]**

UWM ne scrape que ce que tu déclares via `ServiceMonitor`/`PodMonitor` dans **ton** namespace. Pré-requis : ta charge expose un endpoint `/metrics` via un `Service` portant un label.

Exemple pour un workload qui expose un port nommé `metrics` :

```yaml
# a3-servicemonitor.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: mon-app
  namespace: monitoring
  labels:
    team: platform
spec:
  selector:
    matchLabels:
      app: mon-app          # doit matcher les labels du Service cible
  endpoints:
    - port: metrics         # nom du port dans le Service, pas le numéro
      interval: 30s
      path: /metrics
  namespaceSelector:
    matchNames:
      - monitoring
```

```bash
oc apply -f a3-servicemonitor.yaml
```

**Vérifier que la cible est prise en compte** : dans la console OCP, **Observe → Targets** (filtre sur ton namespace), ou via l'API Thanos une fois la datasource branchée (§A.4). La cible doit passer `UP`.

> Si la cible n'apparaît pas : le `selector` du ServiceMonitor ne matche pas les labels du `Service`, ou le port n'est pas **nommé** côté Service (UWM exige un port nommé, pas un numéro).

## A.4 Datasource Grafana vers Thanos Querier **[UWM]**

C'est le point de divergence majeur avec la voie B. Grafana n'interroge pas directement le Prometheus UWM : il passe par le **Thanos Querier** de la plateforme, qui exige une **authentification par token** et impose le **RBAC**.

### A.4.1 ServiceAccount + droit de lecture des métriques

```bash
# ServiceAccount dédié à la lecture
oc create sa grafana-reader -n monitoring

# Droit de lire les métriques (cluster-monitoring-view = lecture cross-namespace)
oc adm policy add-cluster-role-to-user cluster-monitoring-view \
  -z grafana-reader -n monitoring
```

### A.4.2 Token longue durée pour ce ServiceAccount

Sur OCP récent, les tokens de SA ne sont plus générés automatiquement en Secret. On crée un Secret de type token, que le contrôleur peuple :

```yaml
# a4-sa-token.yaml
apiVersion: v1
kind: Secret
metadata:
  name: grafana-reader-token
  namespace: monitoring
  annotations:
    kubernetes.io/service-account.name: grafana-reader
type: kubernetes.io/service-account-token
```

```bash
oc apply -f a4-sa-token.yaml
# Vérifie que le token est peuplé
oc -n monitoring get secret grafana-reader-token -o jsonpath='{.data.token}' | wc -c   # > 0
```

> Alternative courte (token à durée de vie limitée, pratique pour tester mais pas pour de la prod GitOps) : `oc create token grafana-reader -n monitoring --duration=8760h`.

### A.4.3 Le CR GrafanaDatasource

On injecte le token via `valuesFrom` (le placeholder `${...}` est résolu par l'opérateur depuis le Secret), pour ne jamais l'écrire en clair :

```yaml
# a4-datasource-uwm.yaml
apiVersion: grafana.integreatly.org/v1beta1
kind: GrafanaDatasource
metadata:
  name: prometheus-uwm
  namespace: monitoring
spec:
  instanceSelector:
    matchLabels:
      dashboards: "grafana"        # doit matcher le label du CR Grafana (étape 1)
  valuesFrom:
    - targetPath: "secureJsonData.httpHeaderValue1"
      valueFrom:
        secretKeyRef:
          name: grafana-reader-token
          key: token
  datasource:
    name: Prometheus
    type: prometheus
    access: proxy
    url: "https://thanos-querier.openshift-monitoring.svc.cluster.local:9091"
    isDefault: true
    jsonData:
      httpHeaderName1: "Authorization"
      timeInterval: "30s"
      tlsSkipVerify: true          # voir note ci-dessous pour la version propre avec CA
    secureJsonData:
      httpHeaderValue1: "Bearer ${token}"
```

```bash
oc apply -f a4-datasource-uwm.yaml
oc get grafanadatasource prometheus-uwm -n monitoring
```

> **`tlsSkipVerify: true`** est acceptable pour valider la chaîne, mais en cible propre tu montes le CA du service (`service-ca.crt`) et tu passes `tlsAuthWithCACert`. Le CA de service OpenShift est disponible via un ConfigMap annoté `service.beta.openshift.io/inject-cabundle: "true"`. À durcir une fois la jonction validée.

Passe directement à **§3 (dashboard)**, commune aux deux voies.

---

# VOIE B — Stack Prometheus dédiée

## B.1 Installer le Prometheus Operator (communautaire) **[STACK]**

Même logique OLM que pour le grafana-operator (étape 1). Vérifie le channel disponible puis souscris :

```bash
oc get packagemanifests | grep -i prometheus
```

```yaml
# b1-subscription.yaml
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: prometheus
  namespace: monitoring
spec:
  channel: beta                    # remplace par le channel remonté ci-dessus
  name: prometheus
  source: community-operators
  sourceNamespace: openshift-marketplace
  installPlanApproval: Automatic
```

```bash
oc apply -f b1-subscription.yaml
oc get csv -n monitoring | grep -i prometheus     # attends Succeeded
oc get crd | grep monitoring.coreos.com           # prometheuses, alertmanagers, servicemonitors...
```

> L'`OperatorGroup` du namespace existe déjà (créé à l'étape 1). N'en crée pas un second : un seul OperatorGroup par namespace est autorisé.

## B.2 RBAC pour le Prometheus **[STACK]**

Le Prometheus a besoin d'un ServiceAccount autorisé à découvrir et scraper les cibles.

```yaml
# b2-rbac.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: prometheus
  namespace: monitoring
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: prometheus-monitoring
rules:
  - apiGroups: [""]
    resources: ["services", "endpoints", "pods", "nodes", "nodes/metrics"]
    verbs: ["get", "list", "watch"]
  - nonResourceURLs: ["/metrics"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: prometheus-monitoring
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: prometheus-monitoring
subjects:
  - kind: ServiceAccount
    name: prometheus
    namespace: monitoring
```

```bash
oc apply -f b2-rbac.yaml
```

## B.3 Déployer le CR Prometheus **[STACK]**

```yaml
# b3-prometheus.yaml
apiVersion: monitoring.coreos.com/v1
kind: Prometheus
metadata:
  name: main
  namespace: monitoring
spec:
  serviceAccountName: prometheus
  replicas: 1
  retention: 15d
  # sélectionne les ServiceMonitor/Rules portant ces labels (ici : tous ceux du ns)
  serviceMonitorSelector: {}
  podMonitorSelector: {}
  ruleSelector: {}
  serviceMonitorNamespaceSelector: {}
  securityContext: {}              # laisse OpenShift injecter le SCC restricted
  storage:
    volumeClaimTemplate:
      spec:
        resources:
          requests:
            storage: 40Gi
  alerting:                        # branche le Prometheus sur l'Alertmanager (§B.5)
    alertmanagers:
      - namespace: monitoring
        name: alertmanager-operated
        port: web
```

```bash
oc apply -f b3-prometheus.yaml
oc get prometheus -n monitoring
oc get pods -n monitoring -l app.kubernetes.io/name=prometheus -w
```

L'opérateur crée un Service `prometheus-operated` (headless). C'est l'URL que Grafana utilisera.

```bash
oc get svc -n monitoring prometheus-operated
```

> `serviceMonitorSelector: {}` = « prends tous les ServiceMonitor du namespace ». Si tu préfères un filtrage par label, mets `matchLabels` et étiquette tes ServiceMonitor en conséquence.

## B.4 Exposer une cible (ServiceMonitor) **[STACK]**

Identique à §A.3 dans la forme, mais c'est ton Prometheus dédié qui scrape. Adapte le `selector` aux labels de ton Service :

```yaml
# b4-servicemonitor.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: mon-app
  namespace: monitoring
spec:
  selector:
    matchLabels:
      app: mon-app
  endpoints:
    - port: metrics
      interval: 30s
      path: /metrics
```

```bash
oc apply -f b4-servicemonitor.yaml
# Cible visible dans l'UI Prometheus (port-forward) :
oc -n monitoring port-forward svc/prometheus-operated 9090:9090
# puis http://localhost:9090/targets
```

## B.5 Datasource Grafana vers le Prometheus dédié **[STACK]**

Beaucoup plus simple que la voie UWM : pas de token, accès direct au Service interne.

```yaml
# b5-datasource-stack.yaml
apiVersion: grafana.integreatly.org/v1beta1
kind: GrafanaDatasource
metadata:
  name: prometheus
  namespace: monitoring
spec:
  instanceSelector:
    matchLabels:
      dashboards: "grafana"
  datasource:
    name: Prometheus
    type: prometheus
    access: proxy
    url: "http://prometheus-operated.monitoring.svc.cluster.local:9090"
    isDefault: true
    jsonData:
      timeInterval: "30s"
```

```bash
oc apply -f b5-datasource-stack.yaml
oc get grafanadatasource prometheus -n monitoring
```

---

# COMMUN AUX DEUX VOIES

## 3. Vérifier la datasource côté Grafana

```bash
# Le status du CR doit refléter une synchro réussie
oc get grafanadatasource -n monitoring -o wide
oc describe grafanadatasource -n monitoring <nom> | sed -n '/Status/,$p'
```

Puis dans l'UI Grafana (login OIDC de l'étape 1) : **Connections → Data sources → Prometheus → Test**. Le test doit être vert.

**Si le test échoue :**

| Symptôme | Voie | Cause probable |
|---|---|---|
| Datasource absente de l'UI | A+B | `instanceSelector` ne matche pas le label du CR Grafana |
| `401 / 403` | A (UWM) | token absent/expiré, ou `cluster-monitoring-view` non lié au SA |
| `x509 / certificate` | A (UWM) | CA Thanos non fournie → `tlsSkipVerify` ou monter le service-ca |
| `connection refused` | B (stack) | URL ou port erronés, ou pod Prometheus pas `Running` |
| Test vert mais 0 série | A+B | aucune cible `UP` → revoir le ServiceMonitor (§A.3 / §B.4) |

## 4. Ajouter un dashboard

### 4.1 Dashboard de validation (depuis grafana.com)

Pour confirmer la chaîne complète, un dashboard tout fait (ex. Node Exporter Full, id 1860) :

```yaml
# 4-dashboard-validation.yaml
apiVersion: grafana.integreatly.org/v1beta1
kind: GrafanaDashboard
metadata:
  name: node-exporter
  namespace: monitoring
spec:
  instanceSelector:
    matchLabels:
      dashboards: "grafana"
  datasources:
    - inputName: "DS_PROMETHEUS"
      datasourceName: "Prometheus"     # doit matcher le name de la datasource
  grafanaCom:
    id: 1860
```

```bash
oc apply -f 4-dashboard-validation.yaml
oc get grafanadashboard -n monitoring
```

Ouvre Grafana → ce dashboard doit afficher des **courbes réelles**. C'est le critère de validation de la jonction Prometheus ↔ Grafana.

### 4.2 Tes dashboards existants (migration depuis Docker)

Tes dashboards actuels passent en `GrafanaDashboard` via l'une de ces sources :

- `spec.json` : JSON du dashboard inline dans le CR (versionnable, le plus GitOps).
- `spec.url` : pointe un export JSON hébergé (raw Git).
- `spec.configMapRef` : JSON stocké dans un ConfigMap.

```yaml
spec:
  instanceSelector:
    matchLabels:
      dashboards: "grafana"
  json: >
    { ... ton JSON de dashboard ... }
```

> Le `datasources` mapping (§4.1) sert à réécrire le nom de datasource embarqué dans le JSON exporté (souvent `${DS_PROMETHEUS}`) vers ta datasource réelle. Sans ça, un dashboard importé peut pointer une datasource inexistante et n'afficher aucune donnée.

## 5. Alertmanager + PrometheusRule

L'approche diffère selon la voie. Dans les deux cas, une `PrometheusRule` définit les règles d'alerte ; ce qui change, c'est **qui** les évalue et **quel** Alertmanager les route.

### 5.1 Définir une règle d'alerte (commun)

```yaml
# 5-prometheusrule.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: exemple-regles
  namespace: monitoring
  labels:
    role: alert-rules            # utile si tu filtres par ruleSelector
spec:
  groups:
    - name: exemple.rules
      rules:
        - alert: TargetDown
          expr: up == 0
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Cible {{ $labels.instance }} injoignable"
            description: "La cible {{ $labels.job }} est DOWN depuis 5 minutes."
```

```bash
oc apply -f 5-prometheusrule.yaml
oc get prometheusrule -n monitoring
```

### 5.2 Routing — Voie A (UWM) **[UWM]**

Deux options :

**Option 1 — réutiliser l'Alertmanager de plateforme** (le plus simple). Il faut autoriser les `AlertmanagerConfig` venant des namespaces utilisateurs, via le `cluster-monitoring-config` :

```yaml
# dans openshift-monitoring/cluster-monitoring-config, config.yaml :
    enableUserWorkload: true
    alertmanagerMain:
      enableUserAlertmanagerConfig: true
```

**Option 2 — Alertmanager dédié à UWM** : active-le dans `user-workload-monitoring-config` (§A.2, bloc `alertmanager: enabled: true`).

Dans les deux cas, le routing par namespace se déclare avec un `AlertmanagerConfig` :

```yaml
# 5-amconfig-uwm.yaml
apiVersion: monitoring.coreos.com/v1beta1
kind: AlertmanagerConfig
metadata:
  name: routing-equipe
  namespace: monitoring
spec:
  route:
    receiver: email-equipe
    groupBy: ["alertname"]
    groupWait: 30s
    groupInterval: 5m
    repeatInterval: 4h
  receivers:
    - name: email-equipe
      emailConfigs:
        - to: "ops@example.com"
          from: "alertmanager@example.com"
          smarthost: "smtp.example.com:587"
          authUsername: "alertmanager@example.com"
          authPassword:
            name: smtp-secret
            key: password
```

```bash
oc create secret generic smtp-secret --from-literal=password='xxx' -n monitoring
oc apply -f 5-amconfig-uwm.yaml
```

### 5.3 Routing — Voie B (stack dédiée) **[STACK]**

Tu déploies ton propre Alertmanager via le CR, puis sa config.

```yaml
# 5-alertmanager-stack.yaml
apiVersion: monitoring.coreos.com/v1
kind: Alertmanager
metadata:
  name: main
  namespace: monitoring
spec:
  replicas: 1
  alertmanagerConfigSelector:        # ramasse les AlertmanagerConfig du ns
    matchLabels:
      alertmanager: main
  securityContext: {}
```

```bash
oc apply -f 5-alertmanager-stack.yaml
oc get alertmanager -n monitoring
oc get svc -n monitoring alertmanager-operated      # ciblé par §B.3 spec.alerting
```

Le routing se fait soit par un `AlertmanagerConfig` (même forme qu'en §5.2, avec le label `alertmanager: main` pour matcher le selector ci-dessus), soit par un Secret `alertmanager-main` contenant un `alertmanager.yaml` complet si tu veux réutiliser ta config Docker telle quelle :

```bash
oc create secret generic alertmanager-main \
  --from-file=alertmanager.yaml=./alertmanager.yaml -n monitoring
```

> Le Secret doit s'appeler `alertmanager-<nom du CR>` (ici `alertmanager-main`) pour que l'opérateur le monte automatiquement.

### 5.4 Vérifier le routing (commun)

```bash
# L'alerte de test : coupe une cible ou crée un up==0 artificiel, puis :
oc -n monitoring port-forward svc/alertmanager-operated 9093:9093   # ou l'AM plateforme
# http://localhost:9093 → l'alerte TargetDown doit apparaître puis partir vers le receiver
```

Côté Grafana, tu peux aussi visualiser l'état des alertes via **Alerting → Alert rules** si tu importes les règles, mais la source de vérité du routing reste l'Alertmanager.

---

## 6. Critères de sortie de l'étape 2

✅ Prometheus (UWM ou dédié) tourne et scrape au moins une cible `UP`.
✅ La datasource Grafana teste au vert depuis l'UI (login OIDC).
✅ Le dashboard de validation (id 1860) affiche des courbes réelles.
✅ Une `PrometheusRule` est évaluée et déclenche une alerte de test.
✅ L'alerte arrive bien à l'Alertmanager et part vers le receiver configuré.
✅ Aucun secret en clair (token SA, mot de passe SMTP) dans les manifests destinés à Git.

---

## 7. Récapitulatif des manifests (bascule ArgoCD)

| Fichier | Voie | Va dans Git ? |
|---|---|---|
| `a1-enable-uwm.yaml` | A | Oui (mais ressource cluster-wide, attention au scope ArgoCD) |
| `a2-uwm-config.yaml` | A | Oui |
| `a3-servicemonitor.yaml` | A | Oui |
| `a4-sa-token.yaml` + RBAC | A | RBAC oui ; **token via ESO/Sealed Secrets** |
| `a4-datasource-uwm.yaml` | A | Oui |
| `b1-subscription.yaml` | B | Oui |
| `b2-rbac.yaml` | B | Oui |
| `b3-prometheus.yaml` | B | Oui |
| `b4-servicemonitor.yaml` | B | Oui |
| `b5-datasource-stack.yaml` | B | Oui |
| `4-dashboard-*.yaml` | A+B | Oui |
| `5-prometheusrule.yaml` | A+B | Oui |
| `5-amconfig-*.yaml` / Alertmanager CR | A+B | Oui |
| Secret SMTP / token SA | A+B | **Non en clair** — ESO / Sealed Secrets / SOPS |

> Point d'attention ArgoCD spécifique à la voie A : le `cluster-monitoring-config` vit dans `openshift-monitoring`, hors de ton namespace applicatif. Si tu le gères en GitOps, scope bien l'Application Argo pour ne pas entrer en conflit avec la gestion de plateforme, et préfère un patch ciblé plutôt qu'un remplacement complet du ConfigMap.
