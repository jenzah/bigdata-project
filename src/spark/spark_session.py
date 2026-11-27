"""Shared helpers: SparkSession with S3A (Onyxia) config, default paths, logging, metrics."""

import glob
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = REPO_ROOT / "src" / "logs"


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)


def bucket() -> str:
    b = os.environ.get("BUCKET") or os.environ.get("KUBERNETES_NAMESPACE")
    if not b:
        raise RuntimeError("Set BUCKET (export BUCKET=\"$KUBERNETES_NAMESPACE\").")
    return b


def default_paths() -> dict:
    """Default S3A paths of the medallion layers (see src/CONTRACT.md)."""
    root = f"s3a://{bucket()}"
    return {
        "bronze": f"{root}/bronze/statcounter/",
        "silver": f"{root}/silver/social_share/",
        "gold": f"{root}/gold/facebook_decline/",
        "gold_summary": f"{root}/gold/facebook_decline_summary/",
        "gold_csv": f"{root}/gold/facebook_decline_csv/",
        "scale_root": f"{root}/scale_test/",
    }


def _hadoop_aws_package() -> str | None:
    """Return a hadoop-aws Maven coordinate if the jar is missing from $SPARK_HOME/jars."""
    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home:
        import pyspark

        spark_home = os.path.dirname(pyspark.__file__)
    jars = glob.glob(os.path.join(spark_home, "jars", "*.jar"))
    if any(os.path.basename(j).startswith("hadoop-aws") for j in jars):
        return None
    version = "3.3.4"  # matches the Hadoop bundled with Spark 3.5
    for j in jars:
        m = re.match(r"hadoop-client-api-(\d+\.\d+\.\d+)\.jar", os.path.basename(j)) or re.match(
            r"hadoop-common-(\d+\.\d+\.\d+)\.jar", os.path.basename(j)
        )
        if m:
            version = m.group(1)
            break
    return f"org.apache.hadoop:hadoop-aws:{version}"


def get_spark(app_name: str, s3: bool = True) -> SparkSession:
    """Build (or reuse) a local SparkSession configured for Onyxia S3 via S3A."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        # Small data: 200 shuffle partitions would mostly be overhead.
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if s3:
        endpoint = os.environ.get("AWS_S3_ENDPOINT")
        if not endpoint:
            raise RuntimeError("AWS_S3_ENDPOINT is not set (are you in an Onyxia service?).")
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", f"https://{endpoint}")
            .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
        package = _hadoop_aws_package()
        if package:
            builder = builder.config("spark.jars.packages", package)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def save_metrics(name: str, metrics: dict) -> Path:
    """Write step metrics (row and data quality counts) to src/logs/<name>.json for the report."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    metrics = {"written_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), **metrics}
    path = LOG_DIR / f"{name}.json"
    path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return path
