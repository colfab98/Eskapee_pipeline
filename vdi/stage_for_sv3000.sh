#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

PROJECT="$(basename "$ROOT")"
REMOTE="sv3000:~/$PROJECT"
RSYNC="rsync -a --info=progress2"

CONFIG="$ROOT/custom.config"
SAMPLESHEET="$BATCH_SEL/samplesheet.tsv"
FASTQS_DIR="$BATCH_STAGING/merged_fastqs"
CACHE="$ROOT/work/global_conda_cache"
BACASS="$HOME/.nextflow/assets/nf-core/bacass"

echo "[i] Project     : $PROJECT"
echo "[i] Remote      : $REMOTE"
echo "[i] Batch       : $BATCH_ID"
echo "[i] Samplesheet : $SAMPLESHEET"
echo "[i] FASTQs dir  : $FASTQS_DIR"
echo "[i] Conda cache : $CACHE"
echo "[i] Bacass path : $BACASS"
echo "[i] Config      : $CONFIG"

test -s "$SAMPLESHEET" || { echo "[e] missing samplesheet: $SAMPLESHEET"; exit 2; }
test -s "$CONFIG"      || { echo "[e] missing custom.config at $CONFIG"; exit 2; }
test -d "$FASTQS_DIR"  || { echo "[e] missing FASTQs dir: $FASTQS_DIR"; exit 2; }
test -d "$CACHE"       || { echo "[e] missing conda cache: $CACHE"; exit 2; }
test -d "$BACASS"      || { echo "[e] missing bacass at: $BACASS"; exit 2; }

echo "[i] samplesheet rows:"
awk -F'\t' 'NR>1{n++} END{print (n+0)}' "$SAMPLESHEET"

echo "[i] verifying FASTQ paths from samplesheet"
awk -F'\t' 'NR>1{print $2"\n"$3"\n"$4}' "$SAMPLESHEET" | grep -v '^NA$' \
  | xargs -I{} bash -lc 'test -s "{}" || { echo "[e] MISSING: {}"; exit 3; }'
echo "[i] FASTQs OK"

echo "[i] cache size:"
du -sh "$CACHE" || true

ssh "${REMOTE%:*}" "mkdir -p ~/$PROJECT/selections/$BATCH_ID ~/$PROJECT/staging/$BATCH_ID ~/$PROJECT/work ~/$PROJECT/envs ~/$PROJECT/logs ~/.nextflow/assets/nf-core"

echo "[i] rsync: samplesheet + config"
$RSYNC "$SAMPLESHEET" "$REMOTE/selections/$BATCH_ID/"
$RSYNC "$CONFIG"      "$REMOTE/"

echo "[i] rsync: selection manifest"
$RSYNC "$ROOT/selections/$BATCH_ID/eskapee5_selection.tsv" \
      "$REMOTE/selections/$BATCH_ID/"

echo "[i] rsync: truth indices"
ssh "${REMOTE%:*}" "mkdir -p ~/$PROJECT/truth/indices"
$RSYNC "$ROOT/truth/indices/" "$REMOTE/truth/indices/"

echo "[i] rsync: merged FASTQs"
$RSYNC "$FASTQS_DIR" "$REMOTE/staging/$BATCH_ID/"

echo "[i] rsync: conda cache -> project-local work/global_conda_cache on remote"
$RSYNC "$CACHE" "$REMOTE/work/"

if [ -d "$ROOT/work/global_conda_envs" ]; then
  echo "[i] rsync: prebuilt envs (global_conda_envs)"
  $RSYNC "$ROOT/work/global_conda_envs/" "$REMOTE/work/global_conda_envs/"
fi

echo "[i] rsync: exact bacass checkout"
$RSYNC "$BACASS" "${REMOTE%:*}:~/.nextflow/assets/nf-core/"

echo "[i] rsync: batch logs (if present)"
[ -d "$BATCH_LOG" ] && $RSYNC "$BATCH_LOG" "$REMOTE/logs/"

echo "[i] rsync: label env"
$RSYNC "$ROOT/envs/label_env/" "$REMOTE/envs/label_env/"

# require minigraph on remote (fails early if missing)
ssh "${REMOTE%:*}" "test -x ~/$PROJECT/envs/label_env/bin/minigraph" \
  || { echo "[e] minigraph missing on remote at ~/$PROJECT/envs/label_env/bin/minigraph"; exit 2; }

echo "[i] staging complete -> $REMOTE"
echo "[i] on sv3000, run:"
echo "    export NXF_CONDA_CACHEDIR=~/$PROJECT/work/global_conda_cache"
echo "    BATCH_ID=$BATCH_ID nextflow run nf-core/bacass -r 2.3.1 \\"
echo "      -c ~/$PROJECT/custom.config -with-conda \\"
echo "      -work-dir ~/$PROJECT/work/$BATCH_ID \\"
echo "      --input ~/$PROJECT/selections/$BATCH_ID/samplesheet.tsv \\"
echo "      --outdir ~/$PROJECT/batches_being_processed/$BATCH_ID"
