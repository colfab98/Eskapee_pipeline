#!/usr/bin/env bash
set -euo pipefail

# -------------------------------------------------------------------
# Resolve repo root & batch context
# -------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Batch ID (set by caller or default)
BATCH_ID="${BATCH_ID:-dev}"

# Batch-scoped dirs (created once here)
BATCH_SEL="$ROOT/selections/$BATCH_ID"
BATCH_LOG="$ROOT/logs/$BATCH_ID"
BATCH_STAGING="$ROOT/staging/$BATCH_ID"
BATCH_WORK="$ROOT/work/$BATCH_ID"
mkdir -p "$BATCH_SEL" "$BATCH_LOG" "$BATCH_STAGING" "$BATCH_WORK"

# -------------------------------------------------------------------
# NEW: References & environment (aligns with staged truth bundle)
# -------------------------------------------------------------------
# Root directory holding closed genomes, PLSDB, and PhiX snapshots.
REF_ROOT="${REF_ROOT:-$ROOT/truth}"
REF_CLOSED="${REF_CLOSED:-$REF_ROOT/ClosedGenomes}"   # e.g., fasta/ + mmi/
REF_PLSDB="${REF_PLSDB:-$REF_ROOT/PLSDB}"             # e.g., fasta/ + mmi/
REF_PHIX="${REF_PHIX:-$REF_ROOT/PhiX}"                # e.g., phiX174.fasta + mmi/

# Optional: project-local conda cache and Java 17 (matches your runbook).
export NXF_CONDA_CACHEDIR="${NXF_CONDA_CACHEDIR:-$ROOT/.conda_cache}"
export JAVA_HOME="${JAVA_HOME:-$ROOT/jdk17}"

# -------------------------------------------------------------------
# NEW: Per-sample path helpers (keeps _9 tidy & consistent)
# -------------------------------------------------------------------
sample_root()       { echo "$BATCH_WORK/$1"; }
sample_state()      { echo "$(sample_root "$1")/_state"; }
sample_logs()       { echo "$BATCH_LOG/$1"; }
sample_maps()       { echo "$(sample_root "$1")/maps"; }
sample_hybrid()     { echo "$(sample_root "$1")/hybrid"; }
sample_short()      { echo "$(sample_root "$1")/short_unicycler"; }

mk_sample_dirs() {
  local s="$1"
  mkdir -p \
    "$(sample_state "$s")" \
    "$(sample_logs "$s")" \
    "$(sample_maps "$s")" \
    "$(sample_hybrid "$s")" \
    "$(sample_short "$s")"
}

# -------------------------------------------------------------------
# NEW: Canonical state-file names for per-sample DAG
# -------------------------------------------------------------------
H1_DONE=".h1_hybrid_done"                 # Unicycler hybrid finished
S1_DONE=".s1_short_unicycler_done"        # Unicycler short-only finished
H2_DONE=".h2_len_circ_labels_done"        # Hybrid length/circularity labeling done
H3_DONE=".h3_refine_hybrid_done"          # Hybrid unlabeled refinement (refs) done
T1_DONE=".t1_transfer_labels_done"        # Short→Hybrid mapping & label transfer done
G1_DONE=".g1_shortgraph_pruned_done"      # Short graph <100bp prune+bypass done
F1_DONE=".f1_features_done"               # Feature extraction done
Q1_DONE=".q1_qc_done"                     # QC (unlabeled fraction) done

# -------------------------------------------------------------------
# NEW: Small logging helpers (used by _9 and sub-scripts)
# -------------------------------------------------------------------
log_info()  { echo "[INFO]  $(date -Is) $*"; }
log_warn()  { echo "[WARN]  $(date -Is) $*" >&2; }
log_error() { echo "[ERROR] $(date -Is) $*" >&2; }

# End of file
