"""Bronze -> Silver: merge the 132 monthly StatCounter CSVs into one long, clean Parquet table.

Raw layout (one file per month, see src/CONTRACT.md):
    "Social Media","Andorra","United Arab Emirates",...      <- header: one column per country
    "Facebook",94.18,82.29,...                                <- one row per platform
Year and month exist only in the file name (...-YYYY-MM.csv).

Output: one row per (country, year, month, platform) with share_pct, partitioned by year.

Usage:
    python src/spark/silver.py                       # default S3 paths from $BUCKET
    python src/spark/silver.py --input s3a://.../bronze/statcounter/ --output s3a://.../silver/social_share/
"""

import argparse
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_session import default_paths, get_logger, get_spark, save_metrics

log = get_logger("silver")

HEADER_FIRST_CELL = "Social Media"
# StatCounter names contain no commas, so a plain split is safe and linear. (A quote-aware
# look-ahead regex overflows the Java stack on the 100x lines of ~150 KB.) Any quoted field that
# would have been cut by a comma is counted in dq_broken_quoted_fields.
CSV_SPLIT_REGEX = ","
FILE_DATE_REGEX = r"(\\d{4})-(\\d{2})\\.csv$"

# Platform names are standardised case-insensitively; unknown names are kept as-is (trimmed).
PLATFORM_ALIASES = {
    "twitter": "X (Twitter)",
    "x": "X (Twitter)",
    "x (twitter)": "X (Twitter)",
    "twitter/x": "X (Twitter)",
    "x/twitter": "X (Twitter)",
    "facebook": "Facebook",
    "youtube": "YouTube",
    "instagram": "Instagram",
    "pinterest": "Pinterest",
    "linkedin": "LinkedIn",
    "reddit": "Reddit",
    "tumblr": "Tumblr",
    "vkontakte": "VKontakte",
    "vk": "VKontakte",
    "stumbleupon": "StumbleUpon",
    "google+": "Google+",
    "other": "Other",
}

# Safety net in case StatCounter renames a country during the period (canonical = 2015 name).
COUNTRY_ALIASES = {
    "North Macedonia": "Macedonia",
    "Eswatini": "Swaziland",
    "Czechia": "Czech Republic",
    "Türkiye": "Turkey",
    "Turkiye": "Turkey",
    "Cabo Verde": "Cape Verde",
}

# Entries that are not countries (StatCounter sometimes reports them).
NON_COUNTRIES = ["Unknown", "Europe", "Asia/Pacific Region", "Anonymous Proxy", "Satellite Provider",
                 "European Union", "Other", "(not set)"]

SUM_TOLERANCE_PTS = 1.0


def _map_expr(mapping: dict, col: F.Column, default: F.Column) -> F.Column:
    """Return mapping[col] if present, else default (built as a literal Spark map)."""
    m = F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])
    return F.coalesce(m[col], default)


def read_bronze(spark: SparkSession, input_path: str) -> DataFrame:
    """Read every CSV line as text, keeping the source file name; split into clean fields."""
    # Read the folder with a file filter (a "*.csv" glob in the path makes Spark print a
    # misleading FileNotFoundException warning while it looks for streaming metadata).
    lines = (
        spark.read.option("pathGlobFilter", "*.csv").text(input_path.rstrip("/") + "/")
        .withColumn("source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
        .withColumn("line", F.regexp_replace("value", "^\ufeff", ""))  # strip UTF-8 BOM
        .filter(F.trim("line") != "")
    )
    raw_fields = F.split("line", CSV_SPLIT_REGEX)
    # A field with an unbalanced quote means a comma inside a quoted name.
    broken = F.size(F.filter(raw_fields, lambda x: F.trim(x).rlike(r'^"[^"]*$|^[^"]+"$')))
    fields = F.transform(raw_fields, lambda x: F.trim(F.regexp_replace(F.trim(x), r'^"|"$', "")))
    return lines.select("source_file", fields.alias("fields"), broken.alias("n_broken"))


def build_silver(spark: SparkSession, input_path: str, output_path: str) -> dict:
    m: dict = {"input_path": input_path, "output_path": output_path}
    t0 = time.perf_counter()

    lines = read_bronze(spark, input_path).cache()
    m["raw_files"] = lines.select("source_file").distinct().count()
    m["raw_lines_non_empty"] = lines.count()
    m["dq_broken_quoted_fields"] = lines.agg(F.sum("n_broken")).first()[0] or 0
    log.info("Read %d files, %d non-empty lines", m["raw_files"], m["raw_lines_non_empty"])

    is_header = F.col("fields")[0] == HEADER_FIRST_CELL
    headers = lines.filter(is_header).select(
        "source_file", F.slice("fields", 2, 100000).alias("countries")
    )
    data = lines.filter(~is_header).select(
        "source_file",
        F.col("fields")[0].alias("raw_platform"),
        F.slice("fields", 2, 100000).alias("raw_values"),
    )
    m["files_with_header"] = headers.count()
    m["files_without_header"] = m["raw_files"] - m["files_with_header"]
    m["raw_platform_rows"] = data.count()

    joined = data.join(headers, "source_file", "inner")
    m["dq_rows_wrong_cell_count"] = joined.filter(F.size("raw_values") != F.size("countries")).count()

    # Wide -> long: zip the header countries with the row values, one output row per cell.
    long_raw = (
        joined.select("source_file", "raw_platform", F.explode(F.arrays_zip("countries", "raw_values")).alias("c"))
        .select(
            "source_file",
            "raw_platform",
            F.col("c.countries").alias("raw_country"),
            F.col("c.raw_values").alias("raw_value"),
        )
        # try_cast: Spark 4 runs in ANSI mode, where casting "" (no regex match) would raise an error.
        .withColumn("year", F.expr(f"try_cast(regexp_extract(source_file, '{FILE_DATE_REGEX}', 1) AS INT)"))
        .withColumn("month", F.expr(f"try_cast(regexp_extract(source_file, '{FILE_DATE_REGEX}', 2) AS INT)"))
    )
    long_raw = long_raw.withColumn(
        "share_pct",
        F.expr("try_cast(regexp_replace(replace(raw_value, '%', ''), ',', '.') AS DOUBLE)"),
    )
    long_raw = long_raw.withColumn(
        "country", _map_expr(COUNTRY_ALIASES, F.col("raw_country"), F.col("raw_country"))
    ).withColumn(
        "platform", _map_expr(PLATFORM_ALIASES, F.lower(F.col("raw_platform")), F.col("raw_platform"))
    )

    # ---- Data quality checks: all counted in ONE pass over the long table ----------------------
    # (the "__sNN" suffix only exists in the scaled benchmark copy; it is ignored here)
    base_country = F.regexp_replace("country", r"__s\d{2}$", "")
    flags = {
        "dq_unparsed_file_date": F.col("year").isNull() | F.col("month").isNull(),
        "dq_null_or_empty_value": F.col("raw_value").isNull() | (F.col("raw_value") == ""),
        "dq_non_numeric_value": (F.col("raw_value") != "") & F.col("share_pct").isNull(),
        "dq_out_of_range_0_100": (F.col("share_pct") < 0) | (F.col("share_pct") > 100),
        "dq_null_or_unknown_country": F.col("country").isNull() | (F.col("country") == "")
        | base_country.isin(NON_COUNTRIES),
        "dq_country_renamed": F.col("country") != F.col("raw_country"),
        "dq_platform_renamed": F.col("platform") != F.col("raw_platform"),
    }
    valid = (
        F.col("year").isNotNull() & F.col("month").isNotNull() & F.col("share_pct").isNotNull()
        & F.col("share_pct").between(0, 100) & F.col("country").isNotNull() & (F.col("country") != "")
        & ~base_country.isin(NON_COUNTRIES)
    )
    dq = long_raw.agg(
        F.count("*").alias("long_cells_total"),
        F.sum(valid.cast("int")).alias("valid_cells"),
        *[F.sum(F.coalesce(c, F.lit(False)).cast("int")).alias(k) for k, c in flags.items()],
    ).first().asDict()
    m.update({k: int(v or 0) for k, v in dq.items()})
    log.info("Wide -> long: %d cells (country x platform x month)", m["long_cells_total"])
    m["platform_renames"] = {
        r["raw_platform"]: r["platform"]
        for r in data.select("raw_platform").distinct()
        .withColumn("platform", _map_expr(PLATFORM_ALIASES, F.lower(F.col("raw_platform")), F.col("raw_platform")))
        .filter(F.col("platform") != F.col("raw_platform")).collect()
    }

    clean = long_raw.filter(valid)

    # Exact duplicates (same file content twice, or a repeated platform row) are dropped;
    # the same raw key with different values keeps the mean (conflict counted).
    key_raw = ["country", "year", "month", "raw_platform"]
    by_raw_key = (
        clean.dropDuplicates(key_raw + ["share_pct"])
        .groupBy(key_raw)
        .agg(
            F.avg("share_pct").alias("share_pct"),
            F.count("*").alias("n"),
            F.first("platform").alias("platform"),
            F.first("source_file").alias("source_file"),
        )
        .cache()
    )
    d = by_raw_key.agg(F.sum("n"), F.sum((F.col("n") > 1).cast("int"))).first()
    m["dq_exact_duplicates_removed"] = m["valid_cells"] - int(d[0] or 0)
    m["dq_conflicting_duplicate_keys"] = int(d[1] or 0)

    # Aliases (e.g. Twitter + X in the same month) are summed into one standardised platform.
    key = ["country", "year", "month", "platform"]
    clean = (
        by_raw_key.groupBy(key)
        .agg(
            F.round(F.sum("share_pct"), 4).alias("share_pct"),
            F.first("source_file").alias("source_file"),
            F.count("*").alias("n_alias"),
        )
        .cache()
    )
    m["dq_alias_rows_merged"] = int(clean.agg(F.sum(F.col("n_alias") - 1)).first()[0] or 0)
    clean = clean.drop("n_alias")

    # Country-months with no traffic at all (every platform = 0) carry no information: drop them.
    cm = clean.groupBy("country", "year", "month").agg(F.sum("share_pct").alias("total")).cache()
    zero_cm = cm.filter(F.col("total") == 0)
    m["dq_all_zero_country_months"] = zero_cm.count()
    m["dq_all_zero_rows_removed"] = clean.join(zero_cm, ["country", "year", "month"], "left_semi").count()
    m["dq_all_zero_examples"] = [
        f'{r["country"]} {r["year"]}-{r["month"]:02d}'
        for r in zero_cm.orderBy("year", "month", "country").limit(10).collect()
    ]
    clean_all = clean
    clean = clean.join(zero_cm, ["country", "year", "month"], "left_anti").cache()

    # Shares should sum to ~100 per country and month (flag only: rounding and hidden platforms).
    cm_ok = cm.filter(F.col("total") > 0)
    m["country_months"] = cm_ok.count()
    off = cm_ok.filter(F.abs(F.col("total") - 100) > SUM_TOLERANCE_PTS)
    m["dq_sum_not_100_country_months"] = off.count()
    m["dq_sum_tolerance_pts"] = SUM_TOLERANCE_PTS
    stats = cm_ok.agg(F.min("total"), F.max("total")).first()
    m["sum_min"], m["sum_max"] = stats[0], stats[1]
    m["dq_sum_not_100_examples"] = [
        f'{r["country"]} {r["year"]}-{r["month"]:02d}: {r["total"]:.2f}'
        for r in off.orderBy(F.abs(F.col("total") - 100).desc()).limit(10).collect()
    ]

    # Coverage: countries missing in some years / months.
    years_all = [r[0] for r in clean.select("year").distinct().orderBy("year").collect()]
    cov = clean.groupBy("country").agg(
        F.countDistinct("year").alias("n_years"),
        F.countDistinct("year", "month").alias("n_months"),
    )
    n_months_all = clean.select("year", "month").distinct().count()
    m["years"] = years_all
    m["months_total"] = n_months_all
    m["countries"] = cov.count()
    missing_years = cov.filter(F.col("n_years") < len(years_all))
    m["dq_countries_missing_some_years"] = missing_years.count()
    m["dq_countries_missing_some_years_list"] = [
        f'{r["country"]} ({r["n_years"]}/{len(years_all)} years)'
        for r in missing_years.orderBy("n_years", "country").collect()
    ]
    m["dq_countries_missing_some_months"] = cov.filter(F.col("n_months") < n_months_all).count()

    m["platforms_by_year"] = {
        str(r["year"]): sorted(r["p"])
        for r in clean.groupBy("year").agg(F.collect_set("platform").alias("p")).collect()
    }

    silver = clean.select(
        F.col("country").cast("string"),
        F.col("year").cast("int"),
        F.col("month").cast("int"),
        F.col("platform").cast("string"),
        F.col("share_pct").cast("double"),
        F.col("source_file").cast("string"),
    )
    silver.repartition("year").write.mode("overwrite").partitionBy("year").parquet(output_path)

    m["silver_rows"] = spark.read.parquet(output_path).count()
    m["seconds"] = round(time.perf_counter() - t0, 2)
    for df in (lines, by_raw_key, clean_all, cm, clean):
        df.unpersist()

    for k, v in m.items():
        if not isinstance(v, (list, dict)):
            log.info("%-36s %s", k, v)
    log.info("Countries missing some years: %s", m["dq_countries_missing_some_years_list"][:15])
    log.info("Sum != 100 examples: %s", m["dq_sum_not_100_examples"][:5])
    log.info("All-zero examples: %s", m["dq_all_zero_examples"])
    log.info("Platforms by year: %s", m["platforms_by_year"])
    return m


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", help="bronze folder with the monthly CSVs")
    p.add_argument("--output", help="silver Parquet folder (overwritten)")
    p.add_argument("--metrics-name", default="silver_metrics", help="file name in src/logs/")
    p.add_argument("--local", action="store_true", help="no S3 config (local paths, for tests)")
    a = p.parse_args()

    if not (a.input and a.output):
        d = default_paths()
        a.input, a.output = a.input or d["bronze"], a.output or d["silver"]
    spark = get_spark("silver", s3=not a.local)
    metrics = build_silver(spark, a.input, a.output)
    log.info("Metrics written to %s", save_metrics(a.metrics_name, metrics))
    spark.stop()


if __name__ == "__main__":
    main()