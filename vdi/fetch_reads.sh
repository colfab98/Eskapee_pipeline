#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

export NXF_CONDA_CACHEDIR="$ROOT/work/global_conda_cache"
export NXF_MAMBA=true

nextflow run nf-core/fetchngs -r 1.12.0 -work-dir "$BATCH_WORK" \
  --input "$BATCH_SEL/sra_ids_for_download.csv" \
  --outdir "$BATCH_STAGING/results_fetchngs" \
  --max_memory '3.GB' --max_cpus 2 --max_time '24.h' \
  -profile conda | tee "$BATCH_LOG/fetchngs.run.log"
