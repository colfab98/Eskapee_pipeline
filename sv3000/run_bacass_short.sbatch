#!/bin/bash -l
#SBATCH -J bacass_short
## #SBATCH -A YOUR_ACCOUNT
## #SBATCH -p YOUR_PARTITION
#SBATCH --cpus-per-task=20
#SBATCH --mem=2G
#SBATCH --time=12:00:00
#SBATCH -o logs/slurm-%x-%j.out
#SBATCH -e logs/slurm-%x-%j.err
#SBATCH -D .
#SBATCH --export=ALL

set -euo pipefail

ROOT="${SLURM_SUBMIT_DIR:-$PWD}"
cd "$ROOT"
mkdir -p "$ROOT/logs"

: "${BATCH_ID:?Set BATCH_ID when submitting, e.g.
  mkdir -p logs; BATCH_ID=batch_001 sbatch scripts/run_bacass_short.sbatch}"

echo "==[ $(date -Iseconds) ]== SLURM head job on ${SLURM_NODELIST:-unknown}"
echo "Host: $(hostname)"
echo "Workdir: $(pwd)"
echo "Batch: $BATCH_ID"

has_cmd(){ command -v "$1" >/dev/null 2>&1; }
prepend_path(){ PATH="$1:$PATH"; export PATH; }

ensure_nextflow(){
  if has_cmd nextflow; then return 0; fi
  if has_cmd module; then
    module purge >/dev/null 2>&1 || true
    module load nextflow >/dev/null 2>&1 || true
    if has_cmd nextflow; then return 0; fi
  fi
  if [[ -n "${NXF_BIN:-}" && -x "${NXF_BIN}" ]]; then
    prepend_path "$(dirname "$NXF_BIN")"
    if has_cmd nextflow; then return 0; fi
  fi
  for cand in "$HOME/bin/nextflow" "$HOME/.local/bin/nextflow" \
              "$ROOT/bin/nextflow" "$ROOT/.tools/nextflow/nextflow"
  do
    [[ -x "$cand" ]] && { prepend_path "$(dirname "$cand")"; break; }
  done
  has_cmd nextflow || { echo "ERROR: nextflow not found in PATH." >&2; exit 127; }
}

ensure_java(){
  if has_cmd java; then return 0; fi
  for jh in "${JAVA_HOME:-}" "${NXF_JAVA_HOME:-}"; do
    if [[ -n "$jh" && -x "$jh/bin/java" ]]; then
      export JAVA_HOME="$jh"; prepend_path "$JAVA_HOME/bin"; break
    fi
  done
  if has_cmd java; then return 0; fi
  if has_cmd module; then
    module load java >/dev/null 2>&1 || \
    module load openjdk >/dev/null 2>&1 || \
    module load java/11 >/dev/null 2>&1 || \
    module load java/17 >/dev/null 2>&1 || true
    has_cmd java && return 0
  fi
  if ! has_cmd conda; then
    [[ -r "$HOME/miniconda3/etc/profile.d/conda.sh" ]] && source "$HOME/miniconda3/etc/profile.d/conda.sh"
    [[ -r "$HOME/anaconda3/etc/profile.d/conda.sh"  ]] && source "$HOME/anaconda3/etc/profile.d/conda.sh"
    [[ -r "$HOME/.bashrc" ]] && source "$HOME/.bashrc"
  fi
  if has_cmd conda; then
    [[ -n "${NXF_ENV:-}" ]] && conda activate "$NXF_ENV" 2>/dev/null || true
    has_cmd java && return 0
    if conda info --envs | awk '{print $1}' | grep -qx "nextflow"; then
      conda activate nextflow 2>/dev/null || true
      has_cmd java && return 0
    fi
    if [[ -n "${CONDA_PREFIX:-}" && -x "$CONDA_PREFIX/bin/java" ]]; then
      prepend_path "$CONDA_PREFIX/bin"; has_cmd java && return 0
    fi
  fi
  echo "ERROR: Java not found. Set JAVA_HOME/NXF_JAVA_HOME or load a module." >&2
  exit 127
}

ensure_nextflow
ensure_java

echo "Using nextflow: $(command -v nextflow)"
echo "Java: $(java -version 2>&1 | head -n1)"

if [[ ! -s "$ROOT/selections/$BATCH_ID/samplesheet.short.tsv" ]]; then
  [[ -x "$ROOT/scripts/make_short_only_samplesheet.sh" ]] || {
    echo "[e] missing $ROOT/scripts/make_short_only_samplesheet.sh" >&2; exit 4; }
  BATCH_ID="$BATCH_ID" "$ROOT/scripts/make_short_only_samplesheet.sh"
fi

exec "$ROOT/scripts/run_bacass_short.sh"
