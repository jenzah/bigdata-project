# Facebook et le trafic référent des réseaux sociaux (2015–2025)

**Auteurs** : VALONY Anne Laure — ZAHORA Jennifer

**Prérequis** : compte Onyxia (SSP Cloud) ; service `vscode-pyspark` (Spark 3.5, Python ≥ 3.10) lancé avec le rôle Kubernetes *admin* ; `aws` et `kubectl` (fournis par le service) ; `pip install psutil` (mesure mémoire, facultatif).

**Jeu de données** : StatCounter Global Stats, *Social Media Stats* par pays (tous appareils), 132 CSV mensuels de 2015-01 à 2025-12, versionnés tels quels dans `src/data/raw/`. Source : <https://gs.statcounter.com/social-media-stats>. Licence : CC BY-SA 3.0, © [StatCounter](https://gs.statcounter.com/). Date de téléchargement : 21/11/2026. Taille : 2.96 Mo. Les valeurs sont des parts du trafic web référé par chaque réseau, et non des nombres d'utilisateurs.

**Problématique** : entre 2015 et 2025, Facebook a-t-il perdu sa domination du trafic web référent issu des réseaux sociaux, et dans quels pays ce recul a-t-il été le plus marqué ? Nous comparons, pour chaque pays, la part annuelle moyenne de Facebook entre 2015 et 2025. Nous relevons aussi la plateforme en tête chaque année.

**Chemins S3** (`BUCKET=$KUBERNETES_NAMESPACE`, schémas dans [`src/CONTRACT.md`](src/CONTRACT.md))

| Brut | Traité |
|---|---|
| `s3://$BUCKET/bronze/statcounter/` (132 CSV) | `silver/social_share/`, `gold/facebook_decline/`, `gold/facebook_decline_summary/`, `gold/facebook_decline_csv/`, `scale_test/{x1,x100}/` |

**Reproduction** (depuis un terminal du service ; chaque étape écrase sa sortie, donc on peut la relancer)

```bash
# 0. Environnement
git clone https://github.com/jenzah/bigdata-project.git && cd bigdata-project
export BUCKET="$KUBERNETES_NAMESPACE"
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile 'default' || echo "https://$AWS_S3_ENDPOINT")
S3="--profile default --endpoint-url $S3_ENDPOINT_URL"

# 1. Ingestion bronze (Job Kubernetes : GitHub -> S3)
bash k8s/create-config.sh                       # ConfigMap s3-config + Secret s3-credentials
kubectl apply -f k8s/job-upload-bronze.yaml
kubectl wait --for=condition=complete job/upload-bronze --timeout=600s
kubectl logs job/upload-bronze -c download; kubectl logs job/upload-bronze -c upload
aws s3 ls s3://$BUCKET/bronze/statcounter/ $S3 --summarize --human-readable | tail -2   # 132 objets
kubectl delete job upload-bronze; kubectl delete configmap s3-config; kubectl delete secret s3-credentials

# 2. Bronze -> Silver (Spark)          -> logs et compteurs qualité dans src/logs/silver_metrics.json
python src/spark/silver.py
# 3. Silver -> Gold (Spark)            -> src/data/gold/facebook_decline.csv + src/logs/gold_metrics.json
python src/spark/gold.py
# 4. Benchmark x1 / x100, puis cohérence avec la sortie gold dbt
python src/spark/make_scale_data.py --factor 100 --out /tmp/scale_x100
aws s3 cp /tmp/scale_x100 s3://$BUCKET/scale_test/x100/bronze/ --recursive $S3 --only-show-errors
python src/spark/benchmark.py --runs 3 --notes "vscode-pyspark <N> CPU / <M> Go"   # -> src/benchmark/results_spark.csv
python src/spark/compare_gold.py --dbt <chemin/gold_dbt.csv> --tol 0.01             # -> src/benchmark/consistency_check.json
```

**Relance / reprise** : on peut relancer n'importe quelle étape seule, à condition que l'étape précédente ait été faite (Spark écrit en mode `overwrite`). Après une relance de l'étape 2, relancer aussi l'étape 3. Pour relancer l'ingestion, supprimer d'abord le Job (`kubectl delete job upload-bronze`), puis recommencer l'étape 1. Si les identifiants temporaires Onyxia ont expiré (`ExpiredToken`, `403`), il faut ouvrir un nouveau terminal ou redémarrer le service, puis relancer `create-config.sh`. Les chemins Spark se surchargent avec `--input` et `--output` (voir `--help`). Version dbt (binôme) : `src/dbt/`, à compléter.

**Vérification indépendante** : la part de Facebook en 2025 pour 3 à 5 pays (`src/data/gold/facebook_decline.csv`) a été comparée à la vue *Yearly* du site StatCounter (captures dans `evidence/`).
