#!/bin/bash -l
#SBATCH -J bacass_hybrid
## #SBATCH -A YOUR_ACCOUNT
## #SBATCH -p YOUR_PARTITION
#SBATCH --cpus-per-task=20
#SBATCH --mem=2G
#SBATCH --time=8:00:00
#SBATCH -o logs/slurm-%x-%j.out
#SBATCH -e logs/slurm-%x-%j.err
#SBATCH -D .
#SBATCH --export=ALL

set -euo pipefail

ROOT="${SLURM_SUBMIT_DIR:-$PWD}"
cd "$ROOT"
mkdir -p "$ROOT/logs"

: "${BATCH_ID:?Set BATCH_ID when submitting, e.g.
  mkdir -p logs; BATCH_ID=batch_001 sbatch scripts/run_bacass_hybrid.sbatch}"

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
  if ! has_cmd nextflow; then
    echo "ERROR: nextflow not found in PATH." >&2
    echo "Hint: module load nextflow, or set NXF_BIN=/abs/path/to/nextflow." >&2
    exit 127
  fi
}

ensure_java(){
  if has_cmd java; then return 0; fi
  for jh in "${JAVA_HOME:-}" "${NXF_JAVA_HOME:-}"; do
    if [[ -n "$jh" && -x "$jh/bin/java" ]]; then
      export JAVA_HOME="$jh"
      prepend_path "$JAVA_HOME/bin"
      break
    fi
  done
  if has_cmd java; then return 0; fi
  if has_cmd module; then
    module load java >/dev/null 2>&1 || \
    module load openjdk >/dev/null 2>&1 || \
    module load java/11 >/dev/null 2>&1 || \
    module load java/17 >/dev/null 2>&1 || true
    if has_cmd java; then return 0; fi
  fi
  if ! has_cmd conda; then
    [[ -r "$HOME/miniconda3/etc/profile.d/conda.sh" ]] && source "$HOME/miniconda3/etc/profile.d/conda.sh"
    [[ -r "$HOME/miniforge3/etc/profile.d/conda.sh"  ]] && source "$HOME/miniforge3/etc/profile.d/conda.sh"
    [[ -r "$HOME/.bashrc" ]] && source "$HOME/.bashrc"
  fi
  if has_cmd conda; then
    if [[ -n "${NXF_ENV:-}" ]]; then
      conda activate "$NXF_ENV" 2>/dev/null || true
      if has_cmd java; then return 0; fi
    fi
    if conda info --envs | awk '{print $1}' | grep -qx "nextflow"; then
      conda activate nextflow 2>/dev/null || true
      if has_cmd java; then return 0; fi
    fi
    if [[ -n "${CONDA_PREFIX:-}" && -x "$CONDA_PREFIX/bin/java" ]]; then
      prepend_path "$CONDA_PREFIX/bin"
      if has_cmd java; then return 0; fi
    fi
  fi
  if ! has_cmd java; then
    echo "ERROR: Java not found. Nextflow requires a JVM." >&2
    echo "Fix via module load java, set JAVA_HOME/NXF_JAVA_HOME, or use a conda env with openjdk." >&2
    exit 127
  fi
}

ensure_nextflow
ensure_java

echo "Using nextflow: $(command -v nextflow)"
echo "Java: $(java -version 2>&1 | head -n1)"

exec "$ROOT/scripts/run_bacass_hybrid.sh"
