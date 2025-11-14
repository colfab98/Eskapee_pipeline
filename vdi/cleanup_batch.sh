#!/usr/bin/env bash

set -euo pipefail

: "${BATCH_ID:?set BATCH_ID=your_batch_id}"
REMOTE="${REMOTE:-fcolanto@sv3000}"

VDI_BASE="${VDI_BASE:-$PWD}"
PROJ="$(basename "$VDI_BASE")"
MODE="${BATCH_ID%%_*}"
if [[ "$BATCH_ID" =~ ^batch_ ]]; then
    MODE="test" # Assume old 'batch_' is 'test'
elif [[ "$MODE" != "train" && "$MODE" != "test" ]]; then
    # The validation check below will catch this, but good to be explicit
    MODE="test" # Default might be safer
fi

DEFAULT_DEST_ROOT="$HOME/assemblies_${MODE}"
DEST_ROOT="${DEST_ROOT:-$DEFAULT_DEST_ROOT}"
DEST_DIR="$DEST_ROOT/$BATCH_ID"

if [[ ! "$BATCH_ID" =~ ^(batch|train|test)_[0-9]+$ ]]; then
  echo "[e] BATCH_ID must look like 'batch_###', 'train_###', or 'test_###' (got: $BATCH_ID)" >&2
  exit 2
fi

_run_local() {
  if [[ -n "${DRY_RUN:-}" ]]; then
    echo "[DRY_RUN][local] $*"
  else
    "$@"
  fi
}

echo "[i] Project: $PROJ"
echo "[i] Batch:   $BATCH_ID"
echo "[i] Remote:  $REMOTE"

if [[ ! -f "$DEST_DIR/_OK.received" ]]; then
  echo "[e] VDI receipt marker missing: $DEST_DIR/_OK.received" >&2
  exit 1
fi

if ! ssh -o BatchMode=yes "$REMOTE" "test -f \"\$HOME/$PROJ/batches_being_processed/${BATCH_ID}.SHORT/_OK.export_set\" || test -f \"\$HOME/$PROJ/batches_being_processed/$BATCH_ID/_OK.export_set\""; then
  echo "[e] sv3000 export marker missing: ~/$PROJ/batches_being_processed/${BATCH_ID}.SHORT/_OK.export_set" >&2
  echo "    (also checked legacy: ~/$PROJ/batches_being_processed/$BATCH_ID/_OK.export_set)" >&2
  exit 1
fi

if [[ -z "${FORCE:-}" ]] && ssh "$REMOTE" "pgrep -af '[n]extflow' | grep -E '\\b$BATCH_ID(\\.SHORT|\\.HYBRID)?\\b|/work/.*/$BATCH_ID(\\.SHORT|\\.HYBRID)?\\b' >/dev/null 2>&1"; then
  echo "[e] Detected Nextflow process for $BATCH_ID on $REMOTE; aborting for safety." >&2
  echo "[i] If you've verified nothing is running, re-run with FORCE=1 to bypass this guard." >&2
  exit 1
fi

echo "[i] Cleaning remote (sv3000) work & staging for $BATCH_ID ..."
ssh "$REMOTE" "PROJ='$PROJ' BATCH_ID='$BATCH_ID' DRY_RUN='${DRY_RUN:-}' bash -s" <<'EOF'
set -euo pipefail

PROJ="${PROJ:?}"
BATCH_ID="${BATCH_ID:?}"
BASE="$HOME/$PROJ"

BATCH_DIR_SHORT="$BASE/batches_being_processed/${BATCH_ID}.SHORT"
BATCH_DIR_HYB="$BASE/batches_being_processed/${BATCH_ID}.HYBRID"
LEGACY_DIR="$BASE/batches_being_processed/$BATCH_ID"

WORK_SHORT="$BASE/work/${BATCH_ID}.SHORT"
WORK_HYB="$BASE/work/${BATCH_ID}.HYBRID"
WORK_LEGACY="$BASE/work/$BATCH_ID"

RUN() {
  if [[ -n "${DRY_RUN:-}" ]]; then
    echo "[DRY_RUN][remote] $*"
  else
    "$@"
  fi
}

echo "[remote][i] BASE=$BASE"
du -sh "$BASE/work" "$BASE/staging" "$BATCH_DIR_SHORT" "$BATCH_DIR_HYB" "$LEGACY_DIR" 2>/dev/null || true

RUN rm -rf "$WORK_SHORT" "$WORK_HYB" "$WORK_LEGACY"

RUN rm -rf "$BASE/staging/$BATCH_ID"

RUN rm -f "$BASE/labeling/tmp/${BATCH_ID}."*
RUN rm -f "$BASE/work/edge_support_tmp/${BATCH_ID}."*.{paf,gaf} \
           "$BASE/work/edge_support_tmp/${BATCH_ID}."*.assembly.gfa 2>/dev/null || true

[[ -d "$BATCH_DIR_SHORT" ]] && RUN touch "$BATCH_DIR_SHORT/_CLEANED" || true
[[ -d "$BATCH_DIR_HYB"   ]] && RUN touch "$BATCH_DIR_HYB/_CLEANED"   || true
[[ -d "$LEGACY_DIR"      ]] && RUN touch "$LEGACY_DIR/_CLEANED"      || true

du -sh "$BASE/work" "$BASE/staging" "$BATCH_DIR_SHORT" "$BATCH_DIR_HYB" "$LEGACY_DIR" 2>/dev/null || true

if [[ -z "${DRY_RUN:-}" ]]; then
  echo "[remote][i] Top leftovers >500M under $BASE (post-clean):"
  find "$BASE" -type f -size +500M -printf "%s\t%p\n" \
    | numfmt --to=iec --field=1 \
    | sort -hr | head -n 20 || true
fi
EOF

echo "[i] Cleaning local (VDI) work & staging for $BATCH_ID ..."
_run_local rm -rf "$VDI_BASE/staging/$BATCH_ID"
_run_local rm -rf "$VDI_BASE/work/$BATCH_ID"

if [[ -z "${DRY_RUN:-}" ]]; then
  _run_local touch "$DEST_DIR/_DONE"
fi

echo "[OK] Cleaned batch $BATCH_ID"
if [[ -n "${DRY_RUN:-}" ]]; then
  echo "[i] DRY_RUN was enabled; no files were deleted."
fi
