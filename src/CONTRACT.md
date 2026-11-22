# Contrat de données — Spark et dbt

Ce document fixe les schémas, les règles de nettoyage et les chemins S3. Les deux implémentations du pipeline (Spark : `src/spark/`, dbt sur DuckDB) doivent les respecter, pour que leurs sorties gold soient comparables avec `src/spark/compare_gold.py`.

## 1. Chemins S3 (`BUCKET = $KUBERNETES_NAMESPACE`)

| Couche | Chemin | Format |
|---|---|---|
| Bronze (brut, inchangé) | `s3://$BUCKET/bronze/statcounter/*.csv` | 132 CSV mensuels |
| Silver | `s3://$BUCKET/silver/social_share/` | Parquet, partitionné par `year` |
| Gold (pays) | `s3://$BUCKET/gold/facebook_decline/` | Parquet |
| Gold (résumé) | `s3://$BUCKET/gold/facebook_decline_summary/` | Parquet, 1 ligne |
| Gold (copie CSV) | `s3://$BUCKET/gold/facebook_decline_csv/{countries,summary}/` | CSV |
| Copie gold du dépôt | `src/data/gold/facebook_decline.csv` et `facebook_decline_summary.csv` | CSV |
| Benchmark | `s3://$BUCKET/scale_test/x1/…` et `s3://$BUCKET/scale_test/x100/{bronze,silver,gold,gold_summary}/` | CSV / Parquet |

Les sorties dbt peuvent aller sous un autre préfixe (par exemple `…/gold_dbt/`), mais avec **les mêmes colonnes**.

## 2. Format brut (bronze)

- Un fichier par mois : `Statcounter-Social-Media-Country-desktop-mobile-tablet-console-YYYY-MM.csv`, de 2015-01 à 2025-12 (132 fichiers).
- Le format est « transposé » : la ligne d'en-tête vaut `"Social Media","Andorra","United Arab Emirates",…`, avec une colonne par pays (environ 240). Ensuite, il y a **une ligne par plateforme**, par exemple `"Facebook",94.18,82.29,…`. Les valeurs sont des pourcentages décimaux avec un point, sans le signe `%`.
- L'ensemble des plateformes change selon les fichiers.
- **L'année et le mois n'existent que dans le nom du fichier.** On les extrait avec l'expression régulière `(\d{4})-(\d{2})\.csv$`.
- Le CSV est découpé sur `,`, car les noms StatCounter ne contiennent pas de virgule. Un champ entre guillemets mal fermé est compté (`dq_broken_quoted_fields`).

## 3. Schéma silver — `social_share`

Il y a une ligne par `(country, year, month, platform)`, et cette clé est unique.

| Colonne | Type | Description |
|---|---|---|
| `country` | string | Nom de pays StatCounter, standardisé |
| `year` | int | Année tirée du nom de fichier (colonne de partition) |
| `month` | int | Mois 1–12, tiré du nom de fichier |
| `platform` | string | Plateforme standardisée |
| `share_pct` | double | Part du trafic web référent, de 0 à 100, arrondie à 4 décimales |
| `source_file` | string | Nom du fichier bronze d'origine (traçabilité) |

## 4. Définition de la part annuelle

Pour un pays *c*, une année *y* et une plateforme *p* :

```
n_months(c, y)          = nombre de mois distincts où c est présent en silver l'année y
yearly_share(c, y, p)   = SUM(share_pct sur les mois de y) / n_months(c, y)
```

C'est la **moyenne des mois disponibles**. Une plateforme absente d'un mois où le pays est observé compte pour 0. Un pays observé dans l'année mais sans ligne Facebook a `fb_share = 0`.

## 5. Règles de nettoyage (dans cet ordre)

1. **Nettoyage des champs** : on ignore les lignes vides. Dans chaque champ, on retire le BOM UTF-8 (marqueur invisible en début de fichier), les guillemets et les espaces superflus. Exemple : `"Facebook"` devient `Facebook`.
2. **Dépivotage (format large → format long)** : dans le fichier brut, chaque pays est une colonne. On transforme chaque ligne de plateforme en une ligne par pays, en associant la n-ième valeur au n-ième pays de l'en-tête **du même fichier**. On obtient une ligne par pays × plateforme × mois. Les lignes dont le nombre de valeurs diffère du nombre de pays de l'en-tête sont comptées (`dq_rows_wrong_cell_count`).
3. **Conversion numérique** : `share_pct = TRY_CAST(valeur AS DOUBLE)`, après avoir retiré un éventuel `%`. Les valeurs vides ou non numériques sont supprimées et comptées.
4. **Contrôle des bornes** : une part doit être comprise entre 0 et 100. Les valeurs hors de cet intervalle sont supprimées et comptées.
5. **Standardisation des pays** : on supprime les valeurs nulles ou vides et les entrées qui ne sont pas des pays (`Unknown`, `Europe`, `Asia/Pacific Region`, `Anonymous Proxy`, `Satellite Provider`, `European Union`, `Other`, `(not set)`). Un pays renommé pendant la période garde son nom de 2015 (`North Macedonia`→`Macedonia`, `Eswatini`→`Swaziland`, `Czechia`→`Czech Republic`, `Türkiye`/`Turkiye`→`Turkey`, `Cabo Verde`→`Cape Verde`), pour ne pas être compté comme deux pays.
6. **Standardisation des plateformes** (sans tenir compte de la casse) : `Twitter`, `X`, `Twitter/X`, `X/Twitter` et `X (Twitter)` deviennent **`X (Twitter)`**, pour ne pas couper son historique en deux. L'orthographe de `Facebook`, `YouTube`, `Instagram`, `Pinterest`, `LinkedIn`, `Reddit` (pour `reddit`), `Tumblr`, `StumbleUpon`, `Google+`, `VKontakte` (pour `VK`) et `Other` est uniformisée. Les autres noms sont conservés tels quels.
7. **Dédoublonnage** : une ligne présente deux fois à l'identique (même pays, année, mois, plateforme brute et valeur) n'est gardée qu'une fois. Si une même clé a des valeurs différentes, on garde leur moyenne et le conflit est compté.
8. **Fusion des alias** : si un même fichier contient à la fois `Twitter` et `X`, leurs parts sont additionnées, puisqu'il s'agit de la même plateforme après la règle 6.
9. **Suppression des pays-mois sans trafic** : quand toutes les plateformes d'un pays valent 0 sur un mois, StatCounter n'a mesuré aucun trafic. Ces lignes sont supprimées, car ce ne sont pas de vraies observations et elles feraient baisser les moyennes.
10. **Contrôles signalés, sans correction** : on compte les pays-mois dont la somme des parts s'écarte de 100 de plus de 1 point (écart dû aux arrondis). On liste aussi les pays absents certaines années ou certains mois.

## 6. Schéma gold — `facebook_decline`

Le gold contient une ligne par pays **présent en 2015 ET en 2025**. Les autres pays sont exclus, et leur liste est enregistrée dans `src/logs/gold_metrics.json`.

| Colonne | Type | Règle |
|---|---|---|
| `country` | string | |
| `fb_share_2015` | double | `ROUND(yearly_share(c, 2015, 'Facebook'), 2)` |
| `fb_share_2025` | double | `ROUND(yearly_share(c, 2025, 'Facebook'), 2)` |
| `delta_pts` | double | `ROUND(fb_2025 − fb_2015, 2)`, calculé sur les valeurs **non arrondies** |
| `top_platform_2015` | string | Plateforme de part annuelle maximale, **`Other` exclu**. En cas d'égalité, ordre alphabétique |
| `top_platform_2025` | string | Idem |
| `lost_first_place` | boolean | `top_platform_2015 = 'Facebook' AND top_platform_2025 <> 'Facebook'` |
| `rank_by_decline` | int | `ROW_NUMBER() OVER (ORDER BY delta_non_arrondi ASC, country ASC)` : le rang 1 correspond à la plus forte baisse |
| `n_months_2015` | int | Mois observés en 2015 |
| `n_months_2025` | int | Mois observés en 2025 |
| `low_coverage` | boolean | `n_months_2015 < 6 OR n_months_2025 < 6` |

Les 8 premières colonnes sont celles qui sont exigées. Les 3 dernières servent à signaler les cas peu fiables. Dans les CSV, les booléens s'écrivent `true` ou `false`.

## 7. Schéma gold — `facebook_decline_summary` (1 ligne)

| Colonne | Type | Règle |
|---|---|---|
| `n_countries` | int | Lignes du gold |
| `n_low_coverage` | int | Pays avec `low_coverage = true` |
| `n_fb_first_2015` | int | Pays où Facebook est n°1 en 2015 |
| `n_fb_first_2025` | int | Pays où Facebook est n°1 en 2025 |
| `n_lost_first_place` | int | Pays avec `lost_first_place = true` |
| `n_declined` | int | Pays avec `delta_pts < 0` |
| `avg_delta_pts` | double | Moyenne simple (non pondérée) de `delta_pts`, arrondie à 2 décimales |
| `median_delta_pts` | double | Médiane exacte de `delta_pts`, arrondie à 2 décimales |
| `n_countries_reliable` | int | Pays avec `low_coverage = false` |
| `avg_delta_pts_reliable` | double | Moyenne de `delta_pts` sur ces pays |

## 8. Format commun des résultats de benchmark

Fichiers : `src/benchmark/results_spark.csv` (Spark) et `src/benchmark/results_dbt.csv` (dbt). Ils ont les mêmes colonnes :

```
engine,dataset,scale_factor,stage,run,wall_seconds,peak_rss_mb,rows_out,input_mb,started_at_utc,host_cpus,host,notes
```

- `engine` : `spark` ou `dbt_duckdb`.
- `dataset` : `x1` (données réelles) ou `x100` (copie ×100 produite par `src/spark/make_scale_data.py`).
- `stage` : `bronze_to_silver`, `silver_to_gold`, `end_to_end` (silver + gold) et, si besoin, `session_start` (démarrage du moteur).
- `wall_seconds` : temps horloge. `peak_rss_mb` : mémoire résidente maximale du processus (vide si elle n'est pas mesurée).
- Au moins 3 exécutions par jeu et par étape. On compare les **médianes**, sur un service Onyxia de même taille (à indiquer dans `notes`).

## 9. Contrôle de cohérence Spark ↔ dbt

`python src/spark/compare_gold.py --dbt <gold_dbt.csv> --tol 0.01` vérifie :
- le même nombre de lignes et le même ensemble de pays ;
- des colonnes numériques égales à ±0,01 point près ;
- des colonnes texte, booléennes et de rang strictement égales.

Le résultat est écrit dans `src/benchmark/consistency_check.json`.
