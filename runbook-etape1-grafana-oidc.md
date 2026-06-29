# Runbook — Étape 1 : Grafana sur OpenShift avec authentification OIDC

**Objectif** : déployer une instance Grafana propre, gérée par le grafana-operator v5, et valider le login OIDC de bout en bout. À la fin de ce runbook, un utilisateur se connecte via l'IdP et arrive dans Grafana avec le bon rôle.

**Périmètre** : namespace → opérateur → Secret → CR Grafana → enregistrement IdP → login validé. Pas de datasource ni de dashboard ici (étape ultérieure, une fois Prometheus en place).

**Hypothèses de travail** (à adapter à ton contexte) :

| Variable | Valeur d'exemple |
|---|---|
| Namespace | `monitoring` |
| Hostname Grafana | `grafana.apps.mon-cluster.example.com` |
| IdP | Keycloak (realm `infra`) |
| Client ID OIDC | `grafana` |
| Groupe admin | `grafana-admins` |

---

## 0. Prérequis

- `oc` installé et session ouverte : `oc login ...`
- Droits suffisants pour créer un projet, un `OperatorGroup` et une `Subscription` (typiquement `self-provisioner` + admin du namespace cible).
- Accès au catalogue `community-operators` (vérifié en 2).
- Côté IdP : pouvoir créer/éditer un client OIDC et ses mappers (claim `groups` notamment).
- Le hostname Grafana doit être résolvable par le wildcard de routes du cluster (`*.apps.mon-cluster.example.com`). Vérifie le domaine d'apps :

```bash
oc get ingresses.config/cluster -o jsonpath='{.spec.domain}{"\n"}'
```

> **Pourquoi le hostname est critique dès le départ** : `root_url` doit matcher *exactement* le host de la Route, car Grafana en dérive le `redirect_uri` envoyé à l'IdP. Toute divergence = `redirect_uri_mismatch` au premier login. On fixe donc le host explicitement (étape 4) plutôt que de laisser l'opérateur en générer un.

---

## 1. Créer le namespace

```bash
oc new-project monitoring
# ou, si le projet existe déjà :
oc project monitoring
```

Vérification :

```bash
oc project -q          # doit afficher "monitoring"
```

---

## 2. Installer le grafana-operator via OLM

Sur OpenShift, on privilégie OLM (`Subscription`) plutôt que Helm : intégration native, mises à jour gérées, et c'est l'objet que tu versionneras ensuite pour ArgoCD.

### 2.1 Identifier le bon channel

Ne fige jamais un channel « au hasard » : liste ce que publie ton catalogue.

```bash
# Channels disponibles
oc get packagemanifest grafana-operator -n openshift-marketplace \
  -o jsonpath='{range .status.channels[*]}{.name}{"\n"}{end}'

# Channel par défaut recommandé par le paquet
oc get packagemanifest grafana-operator -n openshift-marketplace \
  -o jsonpath='{.status.defaultChannel}{"\n"}'
```

Note le channel retourné (souvent `v5`) et reporte-le dans la `Subscription` ci-dessous.

> Si la commande ne retourne rien : le paquet n'est pas dans `community-operators`. Vérifie les CatalogSources disponibles avec `oc get catalogsource -n openshift-marketplace` et la présence du paquet avec `oc get packagemanifest -n openshift-marketplace | grep -i grafana`.

### 2.2 OperatorGroup + Subscription

```yaml
# 01-operatorgroup.yaml
apiVersion: operators.coreos.com/v1
kind: OperatorGroup
metadata:
  name: grafana-operator-group
  namespace: monitoring
spec:
  targetNamespaces:
    - monitoring          # opérateur scopé à ce seul namespace
---
# 02-subscription.yaml
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: grafana-operator
  namespace: monitoring
spec:
  channel: v5                       # <-- valeur remontée en 2.1
  name: grafana-operator
  source: community-operators
  sourceNamespace: openshift-marketplace
  installPlanApproval: Automatic    # Manual si tu veux valider chaque montée de version
```

```bash
oc apply -f 01-operatorgroup.yaml -f 02-subscription.yaml
```

### 2.3 Vérifier l'installation

Attends que la CSV passe en `Succeeded` :

```bash
oc get csv -n monitoring -w
```

```bash
# Le contrôleur doit être Available 1/1
oc get deploy -n monitoring grafana-operator-controller-manager

# Les CRDs doivent être enregistrées
oc get crd | grep grafana.integreatly.org
```

Tu dois voir au minimum `grafanas`, `grafanadatasources`, `grafanadashboards`, `grafanafolders`.

**Si la CSV ne progresse pas** (`Pending`, `InstallReady`, `Failed`) :

```bash
oc get installplan -n monitoring
oc describe subscription grafana-operator -n monitoring | sed -n '/Conditions/,$p'
oc describe csv -n monitoring <nom-csv>
```

Les causes classiques : channel inexistant (faute de frappe en 2.1), `installPlanApproval: Manual` avec un InstallPlan non approuvé (`oc patch installplan <nom> -n monitoring --type merge -p '{"spec":{"approved":true}}'`), ou OperatorGroup en double dans le namespace (un seul autorisé).

---

## 3. Créer le Secret OIDC (avant le CR Grafana)

On crée le Secret **en premier** pour qu'il existe quand le pod Grafana démarre, sinon le pod reste bloqué en `ContainerCreating`.

```bash
oc create secret generic grafana-oidc \
  --from-literal=client_secret='LE-SECRET-DE-TON-IDP' \
  -n monitoring
```

Vérification (sans révéler la valeur) :

```bash
oc get secret grafana-oidc -n monitoring \
  -o jsonpath='{.data.client_secret}' | base64 -d | wc -c   # > 0
```

> **Principe à garder** : non sensible → `spec.config` du CR ; sensible → Secret + variable d'environnement. Grafana mappe automatiquement toute variable `GF_<SECTION>_<CLE>` sur l'option `.ini` correspondante, donc `GF_AUTH_GENERIC_OAUTH_CLIENT_SECRET` surcharge `[auth.generic_oauth] client_secret` sans jamais l'écrire en clair dans le CR.

---

## 4. Déployer le CR Grafana

C'est le cœur de l'étape. Le CR porte trois choses : le host fixe de la Route, la config serveur (`root_url`), et le bloc OIDC.

```yaml
# 03-grafana.yaml
apiVersion: grafana.integreatly.org/v1beta1
kind: Grafana
metadata:
  name: grafana
  namespace: monitoring
  labels:
    dashboards: "grafana"        # ancre pour les futurs GrafanaDatasource/Dashboard
spec:
  route:
    spec:
      host: grafana.apps.mon-cluster.example.com
      tls:
        termination: edge        # TLS terminé au routeur ; HTTP en interne vers le pod
      to:
        kind: Service
        name: grafana-service
  config:
    server:
      root_url: "https://grafana.apps.mon-cluster.example.com"
      # root_url DOIT matcher le host de la Route ci-dessus
    log:
      mode: "console"
      level: "info"              # passe à "debug" temporairement pour diagnostiquer l'OIDC
    auth:
      # désactive le login local une fois l'OIDC validé (optionnel, voir §note sécurité)
      disable_login_form: "false"
    auth.generic_oauth:
      enabled: "true"
      name: "OIDC"
      allow_sign_up: "true"
      client_id: "grafana"
      # client_secret : injecté par variable d'env, PAS ici
      scopes: "openid profile email groups"
      auth_url:  "https://idp.example.com/realms/infra/protocol/openid-connect/auth"
      token_url: "https://idp.example.com/realms/infra/protocol/openid-connect/token"
      api_url:   "https://idp.example.com/realms/infra/protocol/openid-connect/userinfo"
      # mapping des attributs utilisateur depuis les claims
      login_attribute_path: "preferred_username"
      name_attribute_path:  "name"
      email_attribute_path: "email"
      # mapping de rôle via JMESPath sur le claim groups
      role_attribute_path: "contains(groups[*], 'grafana-admins') && 'Admin' || contains(groups[*], 'grafana-editors') && 'Editor' || 'Viewer'"
      role_attribute_strict: "false"
      use_pkce: "true"
  deployment:
    spec:
      template:
        spec:
          containers:
            - name: grafana
              env:
                - name: GF_AUTH_GENERIC_OAUTH_CLIENT_SECRET
                  valueFrom:
                    secretKeyRef:
                      name: grafana-oidc
                      key: client_secret
```

```bash
oc apply -f 03-grafana.yaml
```

### 4.1 Détail des paramètres OIDC qui comptent

| Paramètre | Rôle | Piège |
|---|---|---|
| `root_url` | Base de calcul du `redirect_uri` | Doit matcher le host de la Route au caractère près |
| `scopes` | Scopes demandés à l'IdP | `groups` doit être présent **et** émis par l'IdP (mapper côté IdP) |
| `role_attribute_path` | JMESPath qui calcule le rôle Grafana | S'évalue sur les claims ; si le claim `groups` est absent → tout le monde en `Viewer` |
| `role_attribute_strict` | `true` = refuse le login si aucun rôle ne matche | Pratique pour bloquer les non-autorisés, mais peut verrouiller tout le monde en cas de mauvais path |
| `login_attribute_path` | Quel claim devient le username | Évite les usernames type UUID si tu mappes sur `sub` |
| `use_pkce` | Active PKCE (recommandé) | L'IdP doit l'autoriser pour le client |
| `allow_sign_up` | Crée l'utilisateur au 1er login | `false` = seuls les utilisateurs déjà provisionnés entrent |

> **`role_attribute_path` — la syntaxe JMESPath** : l'expression `contains(groups[*], 'grafana-admins') && 'Admin' || 'Viewer'` se lit « si le tableau `groups` contient `grafana-admins`, retourne `Admin`, sinon `Viewer` ». Tu peux chaîner plusieurs niveaux comme dans l'exemple (Admin → Editor → Viewer). Teste tes expressions JMESPath à froid sur le JSON réel du token (voir §6.3) avant de soupçonner Grafana.

### 4.2 Vérifier le déploiement

```bash
# Le CR est-il réconcilié ?
oc get grafana grafana -n monitoring

# Le pod doit être Running 1/1
oc get pods -n monitoring -l app=grafana -w

# La Route doit porter le host attendu
oc get route -n monitoring -o custom-columns=NAME:.metadata.name,HOST:.spec.host,TLS:.spec.tls.termination

# Service créé par l'opérateur
oc get svc -n monitoring -l app=grafana
```

Logs d'init du provider OAuth :

```bash
oc logs -n monitoring deploy/grafana -c grafana | grep -iE "oauth|generic_oauth|init"
```

Tu dois voir l'enregistrement du provider `generic_oauth` sans erreur.

**Si le pod reste en `ContainerCreating`** : presque toujours un `secretKeyRef` qui pointe sur un Secret ou une clé absente.

```bash
oc describe pod -n monitoring -l app=grafana | sed -n '/Events/,$p'
```

**Si le pod `CrashLoopBackOff`** : config invalide. Regarde les dernières lignes de log :

```bash
oc logs -n monitoring deploy/grafana -c grafana --previous | tail -30
```

---

## 5. Enregistrer le redirect URI côté IdP

L'oubli n°1. Avant tout test de login, déclare dans ton client OIDC le redirect autorisé :

```
https://grafana.apps.mon-cluster.example.com/login/generic_oauth
```

Côté Keycloak (client `grafana`) :

- **Valid redirect URIs** : `https://grafana.apps.mon-cluster.example.com/login/generic_oauth`
- **Web origins** : `https://grafana.apps.mon-cluster.example.com` (ou `+`)
- **Client authentication** : ON (client confidentiel → c'est ce client secret qui est dans ton Secret K8s)
- **Mapper `groups`** : crée un mapper « Group Membership », claim name `groups`, *Add to userinfo* + *Add to ID token* + *Add to access token*. Décoche « Full group path » si tu veux des noms courts (`grafana-admins` et non `/grafana-admins`) — sinon adapte ton `role_attribute_path` en conséquence.

> Si tes groupes sortent en chemin complet (`/grafana-admins`), soit tu actives le path court dans le mapper, soit tu écris `contains(groups[*], '/grafana-admins')`. C'est une cause fréquente de « tout le monde en Viewer ».

---

## 6. Tester et valider le login OIDC

### 6.1 Test fonctionnel

1. Ouvre `https://grafana.apps.mon-cluster.example.com` dans un navigateur (idéalement en navigation privée pour repartir d'une session propre).
2. Clique sur **Sign in with OIDC**.
3. Authentifie-toi sur l'IdP.
4. Tu dois être redirigé dans Grafana, connecté.

### 6.2 Valider le rôle

- En haut à droite → profil. Vérifie le rôle de l'org (Admin / Editor / Viewer) selon ton appartenance de groupe.
- Côté serveur, en suivant les logs pendant le login :

```bash
oc logs -n monitoring deploy/grafana -c grafana -f | grep -iE "oauth|login|role|failed"
```

### 6.3 Inspecter les claims réellement reçus

Si le rôle est faux, le problème est presque toujours côté claims, pas côté Grafana. Vérifie ce que l'IdP émet vraiment :

```bash
# Récupère un token via le flux password (debug uniquement, si le client l'autorise)
curl -s -X POST \
  "https://idp.example.com/realms/infra/protocol/openid-connect/token" \
  -d "grant_type=password" \
  -d "client_id=grafana" \
  -d "client_secret=LE-SECRET" \
  -d "username=ton-user" \
  -d "password=ton-mdp" \
  -d "scope=openid profile email groups" | jq -r '.access_token' \
  | cut -d. -f2 | base64 -d 2>/dev/null | jq .
```

Regarde si le claim `groups` est présent et sous quelle forme (noms courts vs chemins). C'est la source de vérité pour écrire ton `role_attribute_path`.

### 6.4 Matrice de dépannage

| Symptôme | Cause probable | Action |
|---|---|---|
| `redirect_uri_mismatch` / `invalid redirect` | `root_url` ≠ host Route, ou redirect non enregistré à l'IdP | Aligner `root_url` sur le host ; ajouter `/login/generic_oauth` côté IdP |
| Login OK mais tout le monde en `Viewer` | claim `groups` absent ou format inattendu | Mapper `groups` côté IdP ; ajuster le JMESPath (chemin court vs `/path`) |
| `login.OAuthLogin failed to look up user` | claims d'identité manquants | Vérifier `login_attribute_path` / `email_attribute_path` |
| Login refusé alors que le groupe est bon | `role_attribute_strict: true` + path qui ne matche pas | Mettre `strict` à `false` le temps de fiabiliser le path |
| Erreur TLS vers l'IdP | CA de l'IdP non reconnue par le pod | Injecter le CA bundle (ConfigMap CA + montage), **éviter** `tls_skip_verify_insecure` en prod |
| Boucle de redirection | `web origins` / cookies / `root_url` en `http` au lieu de `https` | Vérifier `root_url` en https et la terminaison `edge` de la Route |

---

## Note sécurité — durcissement après validation

Une fois le login OIDC fiable, tu peux resserrer :

- `disable_login_form: "true"` dans `[auth]` pour supprimer le login local (admin/admin) et forcer le passage par l'IdP. **Garde d'abord un accès de secours** (port-forward + admin local) le temps d'être sûr.
- `auto_login: "true"` dans `[auth.generic_oauth]` si tu veux rediriger directement vers l'IdP sans page intermédiaire.
- `role_attribute_strict: "true"` pour refuser quiconque n'a pas de rôle mappé.
- `allow_assign_grafana_admin: "true"` (+ `Grafana Admin` dans le JMESPath) si tu veux gérer le rôle super-admin serveur via les groupes IdP.
- `allow_sign_up: "false"` si tu provisionnes les comptes en amont plutôt qu'à la volée.

---

## Critère de sortie de l'étape 1

✅ La CSV de l'opérateur est `Succeeded` et le contrôleur tourne.
✅ Le pod Grafana est `Running 1/1`, la Route répond en HTTPS sur le host attendu.
✅ Un utilisateur membre de `grafana-admins` se connecte via l'IdP et arrive en **Admin**.
✅ Un utilisateur lambda se connecte et arrive en **Viewer** (ou est refusé si `strict`).
✅ Aucun secret en clair dans les manifests (`client_secret` uniquement dans le Secret K8s).

À ce stade, l'ensemble `01-operatorgroup.yaml` → `03-grafana.yaml` (Secret externalisé à part) constitue le socle que tu basculeras tel quel dans Git pour ArgoCD.

---

## Manifests — récapitulatif des fichiers

| Fichier | Contenu | Va dans Git (ArgoCD) ? |
|---|---|---|
| `01-operatorgroup.yaml` | OperatorGroup | Oui |
| `02-subscription.yaml` | Subscription OLM | Oui |
| `03-grafana.yaml` | CR Grafana (config + OIDC + route) | Oui |
| Secret `grafana-oidc` | client_secret OIDC | **Non en clair** — via ESO / Sealed Secrets / SOPS |

> Prochaine étape (hors périmètre de ce runbook) : déploiement de Prometheus (User Workload Monitoring ou stack dédiée), puis `GrafanaDatasource` + `GrafanaDashboard` pour afficher des courbes, et enfin Alertmanager + `PrometheusRule` pour le routing d'alertes.
