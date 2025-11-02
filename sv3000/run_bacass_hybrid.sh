#!/usr/bin/env bash
# Runs HYBRID assembly on sv3000 with strict v8-parity for caching & offline.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${BATCH_ID:?set BATCH_ID}"

# v8-parity: local cached assets + offline flags
ASSETS="$HOME/.nextflow/assets/nf-core/bacass"
CONF="$ROOT/custom.config"

# v9-necessary: split outputs into .HYBRID
OUTDIR="$ROOT/batches_being_processed/${BATCH_ID}.HYBRID"

# v8-parity: unified work dir per batch; same cache dir
WORK="$ROOT/work/${BATCH_ID}.HYBRID"
NXF_CONDA_CACHEDIR="${NXF_CONDA_CACHEDIR:-$ROOT/work/global_conda_cache}"
export NXF_CONDA_CACHEDIR

# v8-parity: logging artifacts (separate subdir to avoid HYBRID/SHORT collisions)
LOGS="$ROOT/logs/$BATCH_ID/HYBRID"
mkdir -p "$LOGS" "$OUTDIR" "$ROOT/logs"

# Inputs
SHEET="$ROOT/selections/$BATCH_ID/samplesheet.tsv"

# v8-parity: offline env, prefer env.offline.sh
if [[ -f "$ROOT/env.offline.sh" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT/env.offline.sh"
else
  export NXF_OFFLINE=true
fi

# v8-parity (+ permanent fix): add --assembly_type hybrid
NF_CMD=(
  nextflow run "$ASSETS"
  -with-conda
  -c "$CONF"
  -work-dir "$WORK"
  --input "$SHEET"
  --outdir "$OUTDIR"
  --assembly_type hybrid
  -with-report   "$LOGS/bacass.report.html"
  -with-trace    "$LOGS/bacass.trace.tsv"
  -with-timeline "$LOGS/bacass.timeline.html"
  -with-dag      "$LOGS/bacass.dag.svg"
  -resume
)

echo "==[ $(date -Iseconds) ]== Launching bacass HYBRID (v8-parity offline)"
printf 'CMD: %q ' "${NF_CMD[@]}"; echo
exec "${NF_CMD[@]}"
