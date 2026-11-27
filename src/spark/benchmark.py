"""Time the Spark pipeline (each stage + end to end) on the real data and on a scaled copy.

Datasets (inputs must exist, see README):
    x1   : s3a://$BUCKET/bronze/statcounter/          -> outputs under s3a://$BUCKET/scale_test/x1/
    x100 : s3a://$BUCKET/scale_test/x100/bronze/      -> outputs under s3a://$BUCKET/scale_test/x100/
Production silver/gold are never overwritten by the benchmark.

Each run records wall-clock seconds and peak RSS (Python driver + Spark JVM, sampled every 0.2 s)
for the stages: session_start, bronze_to_silver, silver_to_gold, end_to_end (= silver + gold).
Rows are appended to src/benchmark/results_spark.csv using the shared format of src/CONTRACT.md,
so the dbt/DuckDB timings can be appended to the same kind of file and compared directly.

Usage:
    python src/spark/benchmark.py                       # x1 and x100, 3 runs each
    python src/spark/benchmark.py --datasets x1 --runs 5
"""

import argparse
import csv
import os
import platform
import statistics
import threading
import time
from datetime import datetime, timezone

from gold import build_gold
from silver import build_silver
from spark_session import REPO_ROOT, bucket, get_logger, get_spark, save_metrics

log = get_logger("benchmark")

RESULTS = REPO_ROOT / "src" / "benchmark" / "results_spark.csv"
FIELDS = ["engine", "dataset", "scale_factor", "stage", "run", "wall_seconds", "peak_rss_mb",
          "rows_out", "input_mb", "started_at_utc", "host_cpus", "host", "notes"]

try:
    import psutil
except ImportError:  # memory is then reported as empty
    psutil = None


class PeakMemory:
    """Sample the RSS of the driver process tree (Python + JVM child, or JVM parent under spark-submit)."""

    def __init__(self, interval: float = 0.2):
        self.interval, self.peak, self._stop = interval, 0.0, threading.Event()
        self._root = None
        if psutil:
            me = psutil.Process(os.getpid())
            parent = me.parent()
            self._root = parent if parent and "java" in parent.name().lower() else me

    def _rss(self) -> float:
        procs = [self._root] + self._root.children(recursive=True)
        total = 0
        for p in procs:
            try:
                total += p.memory_info().rss
            except psutil.Error:
                pass
        return total / 1024**2

    def _loop(self):
        while not self._stop.is_set():
            self.peak = max(self.peak, self._rss())
            time.sleep(self.interval)

    def __enter__(self):
        if self._root:
            self.peak = self._rss()
            self._t = threading.Thread(target=self._loop, daemon=True)
            self._t.start()
        return self

    def __exit__(self, *exc):
        if self._root:
            self._stop.set()
            self._t.join()
            self.peak = max(self.peak, self._rss())

    @property
    def mb(self):
        return round(self.peak, 1) if self._root else ""


def input_size_mb(spark, path: str) -> float:
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    return round(fs.getContentSummary(p).getLength() / 1e6, 2)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--datasets", default="x1,x100", help="comma list among x1,x100")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--notes", default="", help="free text stored with each row (e.g. service size)")
    p.add_argument("--root", help="override s3a://$BUCKET (e.g. file:///tmp/bench for a local test)")
    a = p.parse_args()

    root = a.root or f"s3a://{bucket()}"
    use_s3 = root.startswith("s3a://")
    datasets = {
        "x1": (1, f"{root}/bronze/statcounter/", f"{root}/scale_test/x1"),
        "x100": (100, f"{root}/scale_test/x100/bronze/", f"{root}/scale_test/x100"),
    }
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    new_file = not RESULTS.exists()
    rows = []

    with open(RESULTS, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if new_file:
            writer.writeheader()

        def record(ds, factor, stage, run, secs, mem, rows_out, in_mb, started):
            row = {"engine": "spark", "dataset": ds, "scale_factor": factor, "stage": stage, "run": run,
                   "wall_seconds": round(secs, 3), "peak_rss_mb": mem, "rows_out": rows_out,
                   "input_mb": in_mb, "started_at_utc": started, "host_cpus": os.cpu_count(),
                   "host": platform.node(), "notes": a.notes}
            writer.writerow(row)
            f.flush()
            rows.append(row)
            log.info("%-5s run %d %-17s %8.2f s  peak %s MB  rows %s", ds, run, stage, secs, mem, rows_out)

        for ds in a.datasets.split(","):
            factor, bronze, out = datasets[ds]
            for run in range(1, a.runs + 1):
                started = datetime.now(timezone.utc).isoformat(timespec="seconds")
                with PeakMemory() as mem:
                    t = time.perf_counter()
                    spark = get_spark(f"benchmark-{ds}-{run}", s3=use_s3)
                    t_start = time.perf_counter() - t
                record(ds, factor, "session_start", run, t_start, mem.mb, "", "", started)
                in_mb = input_size_mb(spark, bronze)

                with PeakMemory() as mem_s:
                    t = time.perf_counter()
                    ms = build_silver(spark, bronze, f"{out}/silver/")
                    t_silver = time.perf_counter() - t
                record(ds, factor, "bronze_to_silver", run, t_silver, mem_s.mb, ms["silver_rows"], in_mb, started)

                with PeakMemory() as mem_g:
                    t = time.perf_counter()
                    mg = build_gold(spark, f"{out}/silver/", f"{out}/gold/", f"{out}/gold_summary/",
                                    None, 2015, 2025, local_csv=False)
                    t_gold = time.perf_counter() - t
                record(ds, factor, "silver_to_gold", run, t_gold, mem_g.mb, mg["gold_rows"], "", started)

                peak = max(x for x in [mem_s.peak, mem_g.peak]) if psutil else ""
                record(ds, factor, "end_to_end", run, t_silver + t_gold,
                       round(peak, 1) if psutil else "", mg["gold_rows"], in_mb, started)
                if run == 1:
                    save_metrics(f"benchmark_{ds}_silver_metrics", ms)
                    save_metrics(f"benchmark_{ds}_gold_metrics", mg)
                spark.stop()

    log.info("Median wall-clock seconds (this invocation):")
    for ds in a.datasets.split(","):
        for stage in ["session_start", "bronze_to_silver", "silver_to_gold", "end_to_end"]:
            vals = [r["wall_seconds"] for r in rows if r["dataset"] == ds and r["stage"] == stage]
            if vals:
                log.info("  %-5s %-17s median %8.2f s  (min %.2f, max %.2f, n=%d)",
                         ds, stage, statistics.median(vals), min(vals), max(vals), len(vals))
    log.info("Results appended to %s", RESULTS)


if __name__ == "__main__":
    main()
