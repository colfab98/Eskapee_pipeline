#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${BATCH_ID:?set BATCH_ID}"

ASSETS="$HOME/.nextflow/assets/nf-core/bacass"
CONF="$ROOT/custom.config"
OUTDIR="$ROOT/batches_being_processed/${BATCH_ID}.SHORT"

WORK="$ROOT/work/${BATCH_ID}.SHORT"
NXF_CONDA_CACHEDIR="${NXF_CONDA_CACHEDIR:-$ROOT/work/global_conda_cache}"
export NXF_CONDA_CACHEDIR

LOGS="$ROOT/logs/$BATCH_ID/SHORT"
mkdir -p "$LOGS" "$OUTDIR" "$ROOT/logs"

SHEET="$ROOT/selections/$BATCH_ID/samplesheet.short.tsv"

if [[ -f "$ROOT/env.offline.sh" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT/env.offline.sh"
else
  export NXF_OFFLINE=true
fi

NF_CMD=(
  nextflow run "$ASSETS"
  -with-conda
  -c "$CONF"
  -work-dir "$WORK"
  --input "$SHEET"
  --outdir "$OUTDIR"
  -with-report   "$LOGS/bacass.report.html"
  -with-trace    "$LOGS/bacass.trace.tsv"
  -with-timeline "$LOGS/bacass.timeline.html"
  -with-dag      "$LOGS/bacass.dag.svg"
  -resume
)

echo "==[ $(date -Iseconds) ]== Launching bacass SHORT (v8-parity offline)"
printf 'CMD: %q ' "${NF_CMD[@]}"; echo
exec "${NF_CMD[@]}"
