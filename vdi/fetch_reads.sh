#!/usr/bin/env bash
set -euo pipefail

# v8-parity: derive all paths/env from _env.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

# v8-parity: keep Nextflow conda cache + mamba toggle
export NXF_CONDA_CACHEDIR="$ROOT/work/global_conda_cache"
export NXF_MAMBA=true

# v8-parity: same work dir, outdir, resources, profile, and NO --download_method override
nextflow run nf-core/fetchngs -r 1.12.0 -work-dir "$BATCH_WORK" \
  --input "$BATCH_SEL/sra_ids_for_download.csv" \
  --outdir "$BATCH_STAGING/results_fetchngs" \
  --max_memory '3.GB' --max_cpus 2 --max_time '24.h' \
  -profile conda | tee "$BATCH_LOG/fetchngs.run.log"
