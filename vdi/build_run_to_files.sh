#!/usr/bin/env bash
set -euo pipefail

# Batch/env (resolves ROOT and batch-scoped dirs)
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

# Step D – Build run_to_files.csv from nf-core/fetchngs output
export INP="$BATCH_STAGING/results_fetchngs/samplesheet/samplesheet.csv"
export OUT="$BATCH_SEL/run_to_files.csv"

python3 - << 'PY'
import csv, sys, os
inp = os.environ["INP"]
out = os.environ["OUT"]
with open(inp, newline='') as f, open(out, 'w', newline='') as g:
    r = csv.DictReader(f)
    w = csv.writer(g)
    for row in r:
        w.writerow([
            row["run_accession"],
            row["fastq_1"],
            row["fastq_2"],
            row["instrument_platform"],
            row["library_layout"]
        ])
print("wrote", out, file=sys.stderr)
PY
