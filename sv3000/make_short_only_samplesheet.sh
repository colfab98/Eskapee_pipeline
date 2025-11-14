#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

IN="$BATCH_SEL/samplesheet.tsv"
OUT="$BATCH_SEL/samplesheet.short.tsv"
mkdir -p "$BATCH_SEL"

awk -F'\t' 'BEGIN{OFS="\t"} NR==1{print; next} { $4="NA"; print }' "$IN" > "$OUT"

echo "[ok] wrote SHORT-only samplesheet: $OUT"
