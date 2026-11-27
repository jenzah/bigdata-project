"""Build an N-times larger copy of the raw data for the benchmark (standard library only).

Each monthly file keeps its name and layout, but every country column is repeated N times with a
modified key: "France", "France__s01", ..., "France__s99". After the wide -> long reshape this gives
N times more silver rows (and N times more gold countries), with identical values.

Usage:
    python src/spark/make_scale_data.py --factor 100 --src src/data/raw --out /tmp/scale_x100
    aws s3 cp /tmp/scale_x100 "s3://$BUCKET/scale_test/x100/bronze/" --recursive --profile default \
        --endpoint-url "$S3_ENDPOINT_URL"
"""

import argparse
import csv
from pathlib import Path


def scale_file(src: Path, dst: Path, factor: int) -> None:
    with open(src, newline="", encoding="utf-8-sig") as f:
        rows = [r for r in csv.reader(f) if r]
    header, data = rows[0], rows[1:]
    countries = header[1:]
    new_header = [header[0]] + [c if k == 0 else f"{c}__s{k:02d}" for k in range(factor) for c in countries]
    with open(dst, "w", encoding="utf-8", newline="") as f:
        f.write(",".join(f'"{c}"' for c in new_header) + "\n")
        for r in data:
            f.write(f'"{r[0]}",' + ",".join(r[1:] * factor) + "\n")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--factor", type=int, default=100)
    p.add_argument("--src", default="src/data/raw")
    p.add_argument("--out", default="/tmp/scale_x100")
    a = p.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    files = sorted(Path(a.src).glob("*.csv"))
    for src in files:
        scale_file(src, out / src.name, a.factor)
    size = sum(f.stat().st_size for f in out.glob("*.csv"))
    print(f"{len(files)} files x{a.factor} written to {out} ({size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
