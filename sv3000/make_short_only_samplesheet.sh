#!/usr/bin/env bash
# scripts/make_short_only_samplesheet.sh
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

IN="$BATCH_SEL/samplesheet.tsv"
OUT="$BATCH_SEL/samplesheet.short.tsv"
mkdir -p "$BATCH_SEL"

# Preserve header; on data rows, force LongFastQ (column 4) to NA
awk -F'\t' 'BEGIN{OFS="\t"} NR==1{print; next} { $4="NA"; print }' "$IN" > "$OUT"

echo "[ok] wrote SHORT-only samplesheet: $OUT"
