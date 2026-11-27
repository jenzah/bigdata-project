"""Consistency check: compare the Spark gold CSV with the dbt gold CSV (standard library only).

Checks row count, country sets, and every contract column (numbers within --tol, text/bool exact).
Writes src/benchmark/consistency_check.json and exits with code 1 if anything differs.

Usage:
    python src/spark/compare_gold.py --spark src/data/gold/facebook_decline.csv \
        --dbt path/to/dbt_facebook_decline.csv --tol 0.01
"""

import argparse
import csv
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
KEY = "country"
NUMERIC = ["fb_share_2015", "fb_share_2025", "delta_pts"]
TEXT = ["top_platform_2015", "top_platform_2025"]
BOOL = ["lost_first_place"]
INT = ["rank_by_decline"]


def load(path: str) -> dict:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return {r[KEY].strip(): {k.strip().lower(): (v or "").strip() for k, v in r.items()} for r in csv.DictReader(f)}


def to_bool(v: str) -> bool:
    return v.lower() in {"true", "1", "t", "yes"}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--spark", default=str(REPO_ROOT / "src/data/gold/facebook_decline.csv"))
    p.add_argument("--dbt", required=True)
    p.add_argument("--tol", type=float, default=0.01, help="absolute tolerance in percentage points")
    a = p.parse_args()

    s, d = load(a.spark), load(a.dbt)
    common = sorted(set(s) & set(d))
    report = {
        "rows_spark": len(s), "rows_dbt": len(d), "rows_common": len(common),
        "only_in_spark": sorted(set(s) - set(d)), "only_in_dbt": sorted(set(d) - set(s)),
        "tolerance": a.tol, "mismatches": {}, "max_abs_diff": {},
    }
    for col in NUMERIC + TEXT + BOOL + INT:
        bad, max_diff = [], 0.0
        for c in common:
            vs, vd = s[c].get(col, ""), d[c].get(col, "")
            if col in NUMERIC:
                diff = abs(float(vs) - float(vd))
                max_diff = max(max_diff, diff)
                ok = diff <= a.tol + 1e-9
            elif col in BOOL:
                ok = to_bool(vs) == to_bool(vd)
            elif col in INT:
                ok = int(float(vs)) == int(float(vd))
            else:
                ok = vs == vd
            if not ok:
                bad.append(f"{c}: spark={vs} dbt={vd}")
        report["mismatches"][col] = bad
        if col in NUMERIC:
            report["max_abs_diff"][col] = round(max_diff, 6)

    n_bad = sum(len(v) for v in report["mismatches"].values())
    report["status"] = "OK" if (n_bad == 0 and not report["only_in_spark"] and not report["only_in_dbt"]) else "DIFF"

    out = REPO_ROOT / "src" / "benchmark" / "consistency_check.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Spark rows: {len(s)} | dbt rows: {len(d)} | common: {len(common)}")
    print(f"Only in Spark: {report['only_in_spark'][:10]} | only in dbt: {report['only_in_dbt'][:10]}")
    for col, bad in report["mismatches"].items():
        extra = f" (max |diff| {report['max_abs_diff'][col]})" if col in NUMERIC else ""
        print(f"  {col:20s} mismatches: {len(bad)}{extra}  {bad[:3]}")
    print(f"STATUS: {report['status']}  -> {out}")
    sys.exit(0 if report["status"] == "OK" else 1)


if __name__ == "__main__":
    main()
