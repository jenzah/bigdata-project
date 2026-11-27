"""Silver -> Gold: Facebook's referral share per country, 2015 vs 2025.

Yearly share (see src/CONTRACT.md):
    yearly_share(country, year, platform) = sum of monthly share_pct / number of months in which
    the country is observed that year (a platform absent from a month counts as 0).

Outputs:
    gold/facebook_decline/          one row per country (Parquet)
    gold/facebook_decline_summary/  one global summary row (Parquet)
    gold/facebook_decline_csv/      CSV copy of both (single files)
    src/data/gold/*.csv             local copies committed to the repo (unless --no-local-csv)

Usage:
    python src/spark/gold.py
    python src/spark/gold.py --input s3a://.../silver/social_share/ --output s3a://.../gold/facebook_decline/
"""

import argparse
import csv
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark_session import REPO_ROOT, default_paths, get_logger, get_spark, save_metrics

log = get_logger("gold")

LOW_COVERAGE_MONTHS = 6  # fewer observed months than this in either year -> low_coverage = true


def _fmt(v):
    """CSV formatting shared with dbt: booleans as true/false, nulls as empty."""
    if isinstance(v, bool):
        return "true" if v else "false"
    return "" if v is None else v


def yearly_shares(silver: DataFrame) -> tuple[DataFrame, DataFrame]:
    months = silver.groupBy("country", "year").agg(
        F.countDistinct("month").alias("n_months")
    )
    yearly = (
        silver.groupBy("country", "year", "platform")
        .agg(F.sum("share_pct").alias("share_sum"))
        .join(months, ["country", "year"])
        .withColumn("yearly_share", F.col("share_sum") / F.col("n_months"))
        .drop("share_sum")
    )
    return yearly, months


def top_platform(yearly: DataFrame) -> DataFrame:
    """Platform with the highest yearly share ("Other" excluded; ties broken alphabetically)."""
    w = Window.partitionBy("country", "year").orderBy(F.col("yearly_share").desc(), F.col("platform").asc())
    return (
        yearly.filter(F.col("platform") != "Other")
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .select("country", "year", F.col("platform").alias("top_platform"))
    )


def build_gold(spark: SparkSession, input_path: str, output_path: str, summary_path: str,
               csv_path: str | None, start: int, end: int, local_csv: bool) -> dict:
    t0 = time.perf_counter()
    m: dict = {"input_path": input_path, "output_path": output_path, "start_year": start, "end_year": end}

    silver = spark.read.parquet(input_path).filter(F.col("year").isin(start, end)).cache()
    m["silver_rows_used"] = silver.count()

    yearly, months = yearly_shares(silver)
    yearly = yearly.cache()
    m["yearly_rows"] = yearly.count()
    tops = top_platform(yearly)

    fb = yearly.filter(F.col("platform") == "Facebook").select("country", "year", F.col("yearly_share").alias("fb"))
    per_year = (
        months.join(fb, ["country", "year"], "left")
        .join(tops, ["country", "year"], "left")
        .withColumn("fb", F.coalesce("fb", F.lit(0.0)))  # observed but no Facebook row -> 0
    )

    def side(y: int) -> DataFrame:
        return per_year.filter(F.col("year") == y).select(
            "country",
            F.col("fb").alias(f"fb_raw_{y}"),
            F.col("top_platform").alias(f"top_platform_{y}"),
            F.col("n_months").alias(f"n_months_{y}"),
        )

    s, e = side(start), side(end)
    all_countries = s.select("country").union(e.select("country")).distinct()
    m["countries_in_either_year"] = all_countries.count()
    both = s.join(e, "country", "inner")
    m["countries_in_both_years"] = both.count()
    excluded = all_countries.join(both, "country", "left_anti")
    m["excluded_not_in_both_years"] = excluded.count()
    m["excluded_list"] = sorted(r[0] for r in excluded.collect())

    delta_raw = F.col(f"fb_raw_{end}") - F.col(f"fb_raw_{start}")
    rank_w = Window.orderBy(delta_raw.asc(), F.col("country").asc())  # rank 1 = largest decline
    gold = both.select(
        F.col("country"),
        F.round(f"fb_raw_{start}", 2).alias(f"fb_share_{start}"),
        F.round(f"fb_raw_{end}", 2).alias(f"fb_share_{end}"),
        F.round(delta_raw, 2).alias("delta_pts"),
        F.col(f"top_platform_{start}"),
        F.col(f"top_platform_{end}"),
        ((F.col(f"top_platform_{start}") == "Facebook") & (F.col(f"top_platform_{end}") != "Facebook"))
        .alias("lost_first_place"),
        F.row_number().over(rank_w).cast("int").alias("rank_by_decline"),
        F.col(f"n_months_{start}").cast("int"),
        F.col(f"n_months_{end}").cast("int"),
        ((F.col(f"n_months_{start}") < LOW_COVERAGE_MONTHS) | (F.col(f"n_months_{end}") < LOW_COVERAGE_MONTHS))
        .alias("low_coverage"),
    ).orderBy("rank_by_decline").cache()

    gold.write.mode("overwrite").parquet(output_path)

    reliable = gold.filter(~F.col("low_coverage"))
    summary = gold.agg(
        F.count("*").cast("int").alias("n_countries"),
        F.sum(F.col("low_coverage").cast("int")).cast("int").alias("n_low_coverage"),
        F.sum((F.col(f"top_platform_{start}") == "Facebook").cast("int")).cast("int").alias(f"n_fb_first_{start}"),
        F.sum((F.col(f"top_platform_{end}") == "Facebook").cast("int")).cast("int").alias(f"n_fb_first_{end}"),
        F.sum(F.col("lost_first_place").cast("int")).cast("int").alias("n_lost_first_place"),
        F.sum((F.col("delta_pts") < 0).cast("int")).cast("int").alias("n_declined"),
        F.round(F.avg("delta_pts"), 2).alias("avg_delta_pts"),
        F.round(F.expr("percentile(delta_pts, 0.5)"), 2).alias("median_delta_pts"),
    ).crossJoin(
        reliable.agg(
            F.count("*").cast("int").alias("n_countries_reliable"),
            F.round(F.avg("delta_pts"), 2).alias("avg_delta_pts_reliable"),
        )
    )
    summary.write.mode("overwrite").parquet(summary_path)

    if csv_path:
        gold.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_path.rstrip("/") + "/countries")
        summary.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_path.rstrip("/") + "/summary")

    m["gold_rows"] = gold.count()
    summary_row = summary.first().asDict()
    m["summary"] = summary_row
    m["top10_decline"] = [
        f'{r["country"]}: {r[f"fb_share_{start}"]} -> {r[f"fb_share_{end}"]} ({r["delta_pts"]:+.2f} pts), '
        f'{r[f"top_platform_{start}"]} -> {r[f"top_platform_{end}"]}'
        for r in gold.filter(~F.col("low_coverage")).orderBy("rank_by_decline").limit(10).collect()
    ]
    m["lost_first_place_list"] = [
        f'{r["country"]} ({r[f"top_platform_{end}"]})'
        for r in gold.filter("lost_first_place").orderBy("country").collect()
    ]

    if local_csv:
        out_dir = REPO_ROOT / "src" / "data" / "gold"
        out_dir.mkdir(parents=True, exist_ok=True)
        rows = gold.collect()
        with open(out_dir / "facebook_decline.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(gold.columns)
            w.writerows([[_fmt(r[c]) for c in gold.columns] for r in rows])
        with open(out_dir / "facebook_decline_summary.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(summary_row.keys())
            w.writerow([_fmt(v) for v in summary_row.values()])
        m["local_csv"] = str(out_dir)

    m["seconds"] = round(time.perf_counter() - t0, 2)
    silver.unpersist()
    yearly.unpersist()
    gold.unpersist()

    for k in ["silver_rows_used", "yearly_rows", "countries_in_either_year", "countries_in_both_years",
              "excluded_not_in_both_years", "gold_rows", "seconds"]:
        log.info("%-30s %s", k, m[k])
    log.info("Excluded (not in both years): %s", m["excluded_list"])
    log.info("Summary: %s", summary_row)
    log.info("Top 10 declines (low coverage excluded):\n  %s", "\n  ".join(m["top10_decline"]))
    return m


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", help="silver Parquet folder")
    p.add_argument("--output", help="gold Parquet folder (overwritten)")
    p.add_argument("--summary-output", help="gold summary Parquet folder")
    p.add_argument("--csv-output", help="S3 folder for the CSV copy ('' to skip)")
    p.add_argument("--start-year", type=int, default=2015)
    p.add_argument("--end-year", type=int, default=2025)
    p.add_argument("--no-local-csv", action="store_true", help="do not write src/data/gold/*.csv")
    p.add_argument("--metrics-name", default="gold_metrics")
    p.add_argument("--local", action="store_true", help="no S3 config (local paths, for tests)")
    a = p.parse_args()

    if not a.local:
        d = default_paths()
        a.input = a.input or d["silver"]
        a.output = a.output or d["gold"]
        a.summary_output = a.summary_output or d["gold_summary"]
        a.csv_output = d["gold_csv"] if a.csv_output is None else a.csv_output
    spark = get_spark("gold", s3=not a.local)
    metrics = build_gold(spark, a.input, a.output, a.summary_output, a.csv_output or None,
                         a.start_year, a.end_year, local_csv=not a.no_local_csv)
    log.info("Metrics written to %s", save_metrics(a.metrics_name, metrics))
    spark.stop()


if __name__ == "__main__":
    main()
