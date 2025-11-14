#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

LOG="$BATCH_LOG/verify_stage.log"
ASSETS="$HOME/.nextflow/assets/nf-core/bacass"
CACHE="$ROOT/work/global_conda_cache"

SHEET="$BATCH_SEL/samplesheet.tsv"
CONF="$ROOT/custom.config"
MERGED="$BATCH_STAGING/merged_fastqs"

mkdir -p "$BATCH_LOG"
echo "==[ Stage Verification: $(date -Iseconds) ]==" | tee "$LOG"

for p in "$SHEET" "$CONF" "$MERGED" "$ASSETS"; do
  [[ -e "$p" ]] || { echo "ERROR: missing $p" | tee -a "$LOG"; exit 1; }
done
echo "OK: core paths exist." | tee -a "$LOG"

LINES=$(wc -l < "$SHEET" | tr -d ' ')
ROWS=$((LINES-1))
echo "INFO: samplesheet rows (excluding header): $ROWS" | tee -a "$LOG"

ERR=0
while IFS= read -r f; do
  [[ -z "$f" || "$f" == "NA" ]] && continue
  if [[ ! -s "$f" ]]; then
    echo "ERROR: FASTQ path missing/empty: $f" | tee -a "$LOG"
    ERR=1
  fi
done < <(awk -F'\t' 'NR>1{print $2"\n"$3"\n"$4}' "$SHEET" | tr -d '\r')
[[ "$ERR" -eq 0 ]] || { echo "FAILED: one or more FASTQ paths are missing." | tee -a "$LOG"; exit 1; }
echo "OK: FASTQs OK" | tee -a "$LOG"

if [[ ! -d "$ASSETS/modules/local/unicycler" ]]; then
  echo "WARNING: local unicycler module not found under bacass assets." | tee -a "$LOG"
elif grep -qE 'unicycler=0\.5\.1' "$ASSETS/modules/local/unicycler/environment.yml"; then
  echo "OK: unicycler pinned to 0.5.1 in local module env." | tee -a "$LOG"
else
  echo "INFO: unicycler pin 0.5.1 not detected (may be intentional)." | tee -a "$LOG"
fi

[[ -d "$CACHE" ]] || { echo "ERROR: global conda cache not found at $CACHE" | tee -a "$LOG"; exit 1; }
du -sh "$CACHE" | tee -a "$LOG"

command -v nextflow >/dev/null || { echo "ERROR: nextflow not in PATH" | tee -a "$LOG"; exit 1; }
nextflow -version | tee -a "$LOG"
if command -v conda >/dev/null 2>&1; then conda --version | tee -a "$LOG" || true
else echo "INFO: conda not in PATH (Nextflow may still use mamba/conda via module)." | tee -a "$LOG"; fi

if [[ ! -x "$ROOT/envs/label_env/bin/minigraph" ]]; then
  echo "ERROR: minigraph not found at $ROOT/envs/label_env/bin/minigraph" | tee -a "$LOG"
  exit 1
else
  "$ROOT/envs/label_env/bin/minigraph" --version | tee -a "$LOG" || true
fi

cat > "$ROOT/env.offline.sh" <<EENV
export NXF_CONDA_CACHEDIR="$ROOT/work/global_conda_cache"
export NXF_OFFLINE=true
EENV
chmod +x "$ROOT/env.offline.sh"
echo "OK: wrote $ROOT/env.offline.sh" | tee -a "$LOG"

sha256sum "$SHEET" "$CONF" > "$BATCH_LOG/checksums.stage.sha256"
echo "OK: checksums recorded to $BATCH_LOG/checksums.stage.sha256" | tee -a "$LOG"

echo "==[ Stage Verification: COMPLETE ]==" | tee -a "$LOG"
