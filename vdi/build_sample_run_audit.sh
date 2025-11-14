#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

export MAP="$BATCH_SEL/srr_to_sample_map.csv"
export R2F="$BATCH_SEL/run_to_files.csv"

export OUT="$BATCH_SEL/sample_run_audit.csv"

python3 - << 'PY'
import csv, os

map_file = os.environ["MAP"]
r2f_file = os.environ["R2F"]
out_file = os.environ["OUT"]

r2f = {}
with open(r2f_file, newline='') as f:
    for ra, f1, f2, plat, lay in csv.reader(f):
        r2f[ra.strip()] = (f1.strip(), f2.strip(), plat.strip(), lay.strip())

out = []
with open(map_file, newline='') as f:
    for ra, sample, _type in csv.reader(f):
        ra = ra.strip()
        sample = sample.strip()
        if ra in r2f:
            f1, f2, plat, lay = r2f[ra]
            out.append([sample, ra, f1, f2, plat, lay])

with open(out_file, 'w', newline='') as g:
    csv.writer(g).writerows(out)

print(f"rows: {len(out)}")
PY

echo "Wrote $OUT"
