# Compte rendu de validation — edot-integration-test

Validations réalisées le **2026-11-21** sur la branche `feature/develop_EDOT_ETL`.
Tout ce qui est marqué ✅ a été **exécuté** ; ce qui ne l'a pas été figure au §6.

Cluster utilisé : cluster **Kubernetes kubeadm** du dépôt (v1.35.8, 4 nœuds,
Cilium, containerd), contexte `kubernetes-admin@kubernetes`, commandes lancées
sur master-01 via Ansible (`config-k8s_nodes`), **dans le seul namespace
`edot-integration-test`**. Aucun cluster OpenShift n'était accessible : les
contraintes OpenShift ont été reproduites sous Docker (§3).

## 1. Analyse préalable du cluster (lecture seule)

| Point | Constat | Conséquence |
|---|---|---|
| Plateforme | Kubernetes kubeadm, pas d'API `security.openshift.io` | overlay `kubernetes` (UID 10001) ; overlay `openshift` fourni |
| EDOT existant | Application `edot-collector` non enregistrée, namespace `edot` vide | instance de test distincte ; production non touchée |
| Operators | ni Strimzi ni ECK ; CRD présentes : ServiceMonitor, PrometheusRule, ExternalSecret | manifests autonomes, aucun Operator installé |
| StorageClasses | `longhorn` (défaut, 3 réplicas), `longhorn-static`, `local-path` | `local-path` : pas de triple réplication |
| Ressources | masters : 71–87 % de CPU demandé ; `worker-01` : 24 CPU / 256 Gio quasi libre | requests faibles (≈ 0,45 CPU, 1,75 Gio au total) |
| `vm.max_map_count` | 1048576 sur les 4 nœuds | sans objet de toute façon (`allow_mmap: false`) |
| Supervision | kube-prometheus-stack (`release: kube-prometheus-stack`, tous namespaces), metrics-server | réutilisés, rien de déployé |
| Quotas / LimitRange | uniquement `labs-terminals` | aucune contrainte |
| Kyverno | aucune ClusterPolicy active dans le cluster | manifests conformes aux policies du dépôt malgré tout (non-root, probes, ressources, pas de `latest`, pas de hostPath) |

## 2. Validations statiques

| Validation | Résultat |
|---|---|
| `kubectl kustomize` overlays `kubernetes`, `openshift`, `*/producer` | ✅ 20 objets + 3 (producteur), tous namespacés, aucun objet cluster-wide |
| `helm lint` du chart 0.178.3 avec les 7 fichiers de values de test | ✅ |
| `helm template` : StatefulSet, ConfigMap, Service, ServiceAccount, ServiceMonitor ; ordre du pipeline de production conservé ; labels client présents | ✅ |
| `elastic-agent otel validate` (EDOT 9.5.6) sur la configuration rendue | ✅ acceptée ; une clé inconnue du receiver Kafka est bien rejetée |
| Constat : l'exporter Elasticsearch **accepte** une clé inconnue (`usr:`) sans erreur | ⚠️ `user`/`password` prouvés uniquement par le test réel (§4) |
| `scripts/argocd-projects-check.py` (38 Applications) | ✅ |
| `trivy config` (image locale, hors ligne) | ✅ aucun constat sur la sécurité des pods ; KSV-0109/01010 = faux positifs heuristiques (mots « password » du script de setup, clé `…_MS`) ; KSV-0125 = registres officiels hors liste par défaut (comme pour EDOT) |
| `python3 -m py_compile`, `bash -n` sur tous les scripts | ✅ |

## 3. Compatibilité OpenShift (simulée sous Docker)

Conditions de `restricted-v2` : UID arbitraire `1000680000`, GID 0, groupe
supplémentaire fsGroup, rootfs en **lecture seule**, `--cap-drop ALL`,
`no-new-privileges`, volumes de données au groupe fsGroup.

| Image | Résultat |
|---|---|
| `apache/kafka:4.3.2` | ✅ formatage KRaft puis démarrage (`Kafka Server started`) ; redémarrage : `already formatted` ; 290 Mio RSS sous limite 1 Gio. Le wrapper Docker de l'image (écriture dans `/opt/kafka/config`) est contourné |
| `elasticsearch:9.5.6` | ✅ `green`, 1 nœud, 1,4 Gio sous limite 2 Gio ; anonyme → 401 ; `config/` copié en volume par l'initContainer. Avertissement `jansi` dû au `tmpfs` Docker `noexec` (sans objet avec un `emptyDir`) |
| EDOT 9.5.6 | validé précédemment dans ces conditions (`infra/edot-collector/VALIDATION.md`) |

## 4. Tests sur le cluster

| # | Vérification | Résultat |
|---|---|---|
| 1 | Images officielles tirées depuis les registres (Kafka 239 Mo en 4 min 55 s, Elasticsearch 917 Mo en 14 min 42 s) | ✅ (registre lent : timeouts de `deploy.sh` portés à 15 min) |
| 2 | Admission sous Pod Security **`restricted` en enforce** (UID 10001, rootfs RO, aucune capability) | ✅ tous les pods admis, 0 redémarrage |
| 3 | PVC `local-path` liés et inscriptibles (fsGroup) : `data-kafka-0` 5 Gi, `data-elasticsearch-0` 5 Gi, `queue-edot-collector-test-0` 1 Gi | ✅ |
| 4 | Kafka en KRaft (broker+controller, `metadata.version 4.3-IV0`), aucun ZooKeeper | ✅ |
| 5 | Kafka joignable **par son Service** depuis un autre pod (`kafka-broker-api-versions.sh`, advertised listener) | ✅ |
| 6 | Topic `otlp-logs-test` : 3 partitions, RF 1, rétention 6 h ; Job idempotent relancé 3 fois | ✅ |
| 7 | Elasticsearch single-node, `green`, 0 shard non alloué (template 0 réplica) | ✅ |
| 8 | Elasticsearch joignable par son Service (Job de setup) ; rôle et utilisateur EDOT créés | ✅ |
| 9 | EDOT : receiver Kafka en PLAINTEXT, membre du groupe `edot-integration-test`, 3 partitions assignées | ✅ |
| 10 | Filtres OTTL (DEBUG, `/health`, `excluded-service`) et suppressions (`user.email`, `client.ip`, `debug.payload`, `body.user.email`) | ✅ |
| 11 | Export Elasticsearch (`user`/`password`, HTTP) en mapping natif OTel, data stream `logs-edotintegration.otel-test` | ✅ |
| 12 | `scripts/run-tests.sh` : **11/11** (3 exécutions sur environnements successifs) | ✅ |
| 13 | Garde-fou « non vérifiable » : 1ʳᵉ exécution, lag mal interprété → 3 tests en **échec** (pas de faux succès), puis correction du calcul du lag | ✅ |
| 14 | `scripts/produce.sh --events info,nested,debug --count 5 --interval 0.5` : 16 messages → 11 documents (5 info, 5 nested, 1 témoin, 0 debug) | ✅ |
| 15 | NetworkPolicies : pod sans label → Kafka et ES **bloqués** ; pod `kafka-client` → Kafka joignable, ES bloqué | ✅ |
| 16 | `cleanup.sh` (défaut) : ne restent que 3 PVC + Secret ; redéploiement : données conservées (11 documents retrouvés) | ✅ |
| 17 | `cleanup.sh --delete-namespace --yes` : namespace, PVC, Secret supprimés, **0 PV résiduel** ; redéploiement à neuf puis 11/11 | ✅ |
| 18 | Prometheus : cible EDOT `up=1` ; CPU/mémoire des 3 pods disponibles (cAdvisor) ; compteurs de filtres = 2 par filtre après 2 campagnes | ✅ |
| 19 | Namespace `edot` (production) : toujours vide ; seule release Helm EDOT : `edot-collector-test` | ✅ |

### Consommation mesurée (au repos, après les tests)

| Pod | CPU | Mémoire | Limite mémoire |
|---|---|---|---|
| `kafka-0` | 23–62m | 326–351 Mio | 1 Gio |
| `elasticsearch-0` | 24–182m | 1,40–1,43 Gio | 2 Gio |
| `edot-collector-test-0` | 4m | 110 Mio | 512 Mio |

Les limites demandées suffisent : il n'a pas été nécessaire de les augmenter.

## 5. Constats et corrections intégrées

- **Lag Kafka** : une partition vide affiche `CURRENT-OFFSET -` / `LAG -` ; elle est
  désormais comptée à 0 si `LOG-END-OFFSET` vaut 0, et « inconnue » sinon.
- **Téléchargement d'images** : les Jobs, le producteur et le pod client des tests
  ont une affinité préférentielle vers le nœud du composant (image déjà présente,
  pas de second téléchargement de 900 Mo). Vérifié : chaque Job a été planifié
  sur le nœud de son composant.
- **`local-path` sans quota ni `kubelet_volume_stats_*`** : `df` affiche le disque
  de l'hôte ; `check-health.sh` mesure donc les données par `du`.
- **Métriques EDOT et port-forward** : le port 8888 écoute sur l'IP du pod ; les
  compteurs se lisent dans Prometheus, pas par port-forward (retiré du script).
- **Nom de métrique avec Prometheus 3** : le compteur de filtres est stocké sous
  `otelcol_processor_filter_logs.filtered` (point). ⚠️ **Hors périmètre, non
  modifié** : les 4 panneaux du dashboard de production
  `infra/edot-collector/dashboards/edot-collector.json` qui utilisent
  `otelcol_processor_filter_logs_filtered` resteront vides sur ce Prometheus.

## 6. Non exécuté (et pourquoi)

| Test | Raison | Comment le faire |
|---|---|---|
| Déploiement sur un vrai cluster OpenShift (SCC réelle, StorageClass, supervision UWM) | aucun cluster OpenShift accessible | `PLATFORM=openshift scripts/deploy.sh` (détection automatique) ; contraintes simulées au §3 |
| Déploiement par l'Application Argo CD | elle suit `main` (branche non fusionnée) et n'a pas été enregistrée ; `deploy.sh` utilisé à la place | après fusion : README §5.4 |
| Policies Kyverno du dépôt (`infra/kyverno-policies/validate.sh`) et job CI `trivy-config` complet | CLI `kyverno` absente, téléchargement de binaires refusé | s'exécuteront à la PR ; manifests conçus pour les respecter |
| Agrandissement d'un PVC | `local-path` ne le permet pas | README §10 |
| Charge prolongée / remplissage du disque | hors objectif (environnement de validation fonctionnelle) | `scripts/produce.sh --count …` |
