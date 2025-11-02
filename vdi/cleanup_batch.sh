#!/usr/bin/env bash
#
# cleanup_batch.sh — VDI0044 → cleans heavy intermediates on sv3000 for a finished batch.
#
# What it does
#   - On sv3000: removes per-batch Nextflow work dirs and per-batch staging.
#                Keeps finals/logs under batches_being_processed/${BATCH_ID}.SHORT and .HYBRID.
#   - On VDI:    removes per-batch work and staging.
#   - Writes markers: _CLEANED (remote, for each of SHORT/HYBRID that exists) and _DONE (local).
#
# Requirements (safety gates)
#   - Local (VDI):  ~/assemblies/$BATCH_ID/_OK.received   must exist
#   - Remote (sv3000): require export marker for SHORT:
#         ~/<proj>/batches_being_processed/${BATCH_ID}.SHORT/_OK.export_set
#     (accepts legacy ~/<proj>/batches_being_processed/$BATCH_ID/_OK.export_set too)
#
# Env vars
#   BATCH_ID   (required) e.g., batch_001
#   REMOTE     (optional) default: fcolanto@sv3000
#   VDI_BASE   (optional) default: $PWD (your VDI project root; run from repo root)
#   DEST_ROOT  (optional) default: $HOME/assemblies
#   DRY_RUN    (optional) if set (e.g. 1), only prints what would be deleted
#   FORCE      (optional) if set (e.g. 1), bypass Nextflow-process guard (use with care)
#
# Usage
#   BATCH_ID=batch_001 ./cleanup_batch.sh
#   DRY_RUN=1 BATCH_ID=batch_001 ./cleanup_batch.sh
#   FORCE=1  BATCH_ID=batch_001 ./cleanup_batch.sh
#

set -euo pipefail

: "${BATCH_ID:?set BATCH_ID=your_batch_id}"
REMOTE="${REMOTE:-fcolanto@sv3000}"

VDI_BASE="${VDI_BASE:-$PWD}"
PROJ="$(basename "$VDI_BASE")"
# Determine MODE from BATCH_ID (handle legacy batch_*)
MODE="${BATCH_ID%%_*}"
if [[ "$BATCH_ID" =~ ^batch_ ]]; then
    MODE="test" # Assume old 'batch_' is 'test'
elif [[ "$MODE" != "train" && "$MODE" != "test" ]]; then
    # The validation check below will catch this, but good to be explicit
    MODE="test" # Default might be safer
fi

# Set DEST_ROOT based on MODE, allowing override via environment
DEFAULT_DEST_ROOT="$HOME/assemblies_${MODE}"
DEST_ROOT="${DEST_ROOT:-$DEFAULT_DEST_ROOT}"
DEST_DIR="$DEST_ROOT/$BATCH_ID"

# Optional sanity guard for batch naming
if [[ ! "$BATCH_ID" =~ ^(batch|train|test)_[0-9]+$ ]]; then
  echo "[e] BATCH_ID must look like 'batch_###', 'train_###', or 'test_###' (got: $BATCH_ID)" >&2
  exit 2
fi

# DRY-RUN helper
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

# ---------------------- Preconditions ----------------------
# Local receipt marker
if [[ ! -f "$DEST_DIR/_OK.received" ]]; then
  echo "[e] VDI receipt marker missing: $DEST_DIR/_OK.received" >&2
  exit 1
fi

# Remote export marker (prefer SHORT; accept legacy)
if ! ssh -o BatchMode=yes "$REMOTE" "test -f \"\$HOME/$PROJ/batches_being_processed/${BATCH_ID}.SHORT/_OK.export_set\" || test -f \"\$HOME/$PROJ/batches_being_processed/$BATCH_ID/_OK.export_set\""; then
  echo "[e] sv3000 export marker missing: ~/$PROJ/batches_being_processed/${BATCH_ID}.SHORT/_OK.export_set" >&2
  echo "    (also checked legacy: ~/$PROJ/batches_being_processed/$BATCH_ID/_OK.export_set)" >&2
  exit 1
fi

# Guard: abort if Nextflow seems to be running this batch remotely (unless FORCE)
if [[ -z "${FORCE:-}" ]] && ssh "$REMOTE" "pgrep -af '[n]extflow' | grep -E '\\b$BATCH_ID(\\.SHORT|\\.HYBRID)?\\b|/work/.*/$BATCH_ID(\\.SHORT|\\.HYBRID)?\\b' >/dev/null 2>&1"; then
  echo "[e] Detected Nextflow process for $BATCH_ID on $REMOTE; aborting for safety." >&2
  echo "[i] If you've verified nothing is running, re-run with FORCE=1 to bypass this guard." >&2
  exit 1
fi

# ---------------------- Remote cleanup on sv3000 ----------------------
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

# Remove per-batch Nextflow work (SHORT/HYBRID + legacy)
RUN rm -rf "$WORK_SHORT" "$WORK_HYB" "$WORK_LEGACY"

# Remove per-batch staging (merged FASTQs and other large inputs)
RUN rm -rf "$BASE/staging/$BATCH_ID"

# Remove labeling temp files for this batch only
RUN rm -f "$BASE/labeling/tmp/${BATCH_ID}."*

# Remove edge-read intermediates for this batch (large intermediates)
RUN rm -f "$BASE/work/edge_support_tmp/${BATCH_ID}."*.{paf,gaf} \
           "$BASE/work/edge_support_tmp/${BATCH_ID}."*.assembly.gfa 2>/dev/null || true

# Mark cleaned on batch dirs that exist
[[ -d "$BATCH_DIR_SHORT" ]] && RUN touch "$BATCH_DIR_SHORT/_CLEANED" || true
[[ -d "$BATCH_DIR_HYB"   ]] && RUN touch "$BATCH_DIR_HYB/_CLEANED"   || true
[[ -d "$LEGACY_DIR"      ]] && RUN touch "$LEGACY_DIR/_CLEANED"      || true

du -sh "$BASE/work" "$BASE/staging" "$BATCH_DIR_SHORT" "$BATCH_DIR_HYB" "$LEGACY_DIR" 2>/dev/null || true

# Optional: list any >500M leftovers still on the project tree
if [[ -z "${DRY_RUN:-}" ]]; then
  echo "[remote][i] Top leftovers >500M under $BASE (post-clean):"
  find "$BASE" -type f -size +500M -printf "%s\t%p\n" \
    | numfmt --to=iec --field=1 \
    | sort -hr | head -n 20 || true
fi
EOF

# ---------------------- Local cleanup on VDI ----------------------
echo "[i] Cleaning local (VDI) work & staging for $BATCH_ID ..."
_run_local rm -rf "$VDI_BASE/staging/$BATCH_ID"
_run_local rm -rf "$VDI_BASE/work/$BATCH_ID"

# Final local marker
if [[ -z "${DRY_RUN:-}" ]]; then
  _run_local touch "$DEST_DIR/_DONE"
fi

echo "[OK] Cleaned batch $BATCH_ID"
if [[ -n "${DRY_RUN:-}" ]]; then
  echo "[i] DRY_RUN was enabled; no files were deleted."
fi
