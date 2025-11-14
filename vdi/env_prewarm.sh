#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BACASS="$HOME/.nextflow/assets/nf-core/bacass"
NF_DIR="$ROOT/work/_env_prewarm"
NF="$NF_DIR/env_prewarm.nf"

declare -A ENVS=(
  ["$BACASS/modules/nf-core/fastp/environment.yml"]="FASTP_ENV"
  ["$BACASS/modules/nf-core/nanoplot/environment.yml"]="NANOPLOT_ENV"
  ["$BACASS/modules/nf-core/gunzip/environment.yml"]="GUNZIP_ENV"
  ["$BACASS/modules/local/unicycler/environment.yml"]="UNICYCLER_ENV"
  ["$BACASS/modules/nf-core/quast/environment.yml"]="QUAST_ENV"
  ["$BACASS/modules/nf-core/prokka/environment.yml"]="PROKKA_ENV"
  ["$BACASS/modules/nf-core/porechop/porechop/environment.yml"]="PORECHOP_ENV"
)

declare -A SPECS=(
  ["bioconda::multiqc=1.19"]="MULTIQC_SPEC"
)

rm -f "$NF"
mkdir -p "$NF_DIR"

{
  echo "workflow {"
  for p in "${!ENVS[@]}"; do
    [[ -s "$p" ]] && echo "  ${ENVS[$p]}()"
  done
  for spec in "${!SPECS[@]}"; do
    echo "  ${SPECS[$spec]}()"
  done
  echo "}"

  for p in "${!ENVS[@]}"; do
    tag="${ENVS[$p]}"
    if [[ -s "$p" ]]; then
      cat <<EOF
process $tag {
  tag "$tag"
  conda "$p"
  """
  echo ${tag}_READY
  """
}
EOF
    fi
  done

  for spec in "${!SPECS[@]}"; do
    tag="${SPECS[$spec]}"
    cat <<EOF
process $tag {
  tag "$tag"
  conda '$spec'
  """
  echo ${tag}_READY
  """
}
EOF
  done
} > "$NF"

mkdir -p "$ROOT/logs"
echo "[i] Will prewarm these envs:"
grep -E '^process ' "$NF" | awk '{print " - " $2}'

JAVA_ENV="$ROOT/work/global_conda_envs/java17"
echo "[prewarm] ensuring Java at: $JAVA_ENV"
if [[ -x "$JAVA_ENV/bin/java" ]]; then
  echo "[prewarm] java already present:"
  "$JAVA_ENV/bin/java" -version 2>&1 | head -n1
else
  mkdir -p "$(dirname "$JAVA_ENV")"
  if command -v mamba >/dev/null 2>&1; then
    mamba create -y -p "$JAVA_ENV" -c conda-forge openjdk=17
  else
    conda create -y -p "$JAVA_ENV" -c conda-forge openjdk=17
  fi
  echo "[prewarm] java created:"
  "$JAVA_ENV/bin/java" -version 2>&1 | head -n1
fi

export NXF_CONDA_CACHEDIR="$ROOT/work/global_conda_cache"
export NXF_MAMBA=true
export NXF_MAMBA_CMD_OPTS="-y"
export CONDA_ALWAYS_YES=true

cd "$ROOT"
nextflow run "$NF" -with-conda \
  -with-report logs/env_prewarm.report.html \
  -with-trace  logs/env_prewarm.trace.tsv

echo "[i] Env prewarm complete. Cache size:"
du -sh "$NXF_CONDA_CACHEDIR" || true

rm -f "$NF"
echo "[i] Removed $NF"
