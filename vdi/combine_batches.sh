#!/usr/bin/env bash

set -Eeuo pipefail
trap 'echo "Error on line $LINENO: $BASH_COMMAND" >&2' ERR

shopt -u failglob
shopt -s nullglob

if [[ $# -ne 1 || ( "$1" != "train" && "$1" != "test" ) ]]; then
  echo "Usage: $0 [train|test]" >&2
  exit 1
fi

MODE=$1
SRC_ROOT="${HOME}/assemblies_${MODE}"
FINAL_DATASET_DIR="${HOME}/plasgraph2-datasets_new"
OUT_DIR_BASENAME="eskapee-${MODE}"
OUT_DIR="${FINAL_DATASET_DIR}/${OUT_DIR_BASENAME}"
OUT_MANIFEST="${FINAL_DATASET_DIR}/${OUT_DIR_BASENAME}.csv"

echo "Mode set to: ${MODE}"
echo "   - Source Data:           ${SRC_ROOT}"
echo "   - New Dataset Directory: ${FINAL_DATASET_DIR}"
echo "   - Output Sub-directory:  ${OUT_DIR}"
echo "   - Output Manifest:       ${OUT_MANIFEST}"

mkdir -p "${FINAL_DATASET_DIR}"
rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}"
: > "${OUT_MANIFEST}"   

[[ -d "${SRC_ROOT}" ]] || { echo "Error: Source directory not found at ${SRC_ROOT}" >&2; exit 1; }

batches=( "${SRC_ROOT}"/* )
if ((${#batches[@]} == 0)); then
  echo "No '${MODE}_*' directories found under ${SRC_ROOT}"
  echo "   Tip: ls -d ${SRC_ROOT}/${MODE}_*"
  exit 0
fi
echo "Found ${#batches[@]} batch dirs."

sample_count=0

for batch_dir in "${batches[@]}"; do
  [[ -d "${batch_dir}" ]] || continue
  batch_name="${batch_dir##*/}"
  echo "Processing ${batch_name}..."

  SET_DIR="${batch_dir}/set"
  if [[ ! -d "${SET_DIR}" ]]; then
    echo "   - Skipping ${batch_name}: 'set' directory not found."
    continue
  fi

  gfa_files=( "${SET_DIR}"/*.assembly.gfa.gz )
  if ((${#gfa_files[@]} == 0)); then
    echo "   - No '*.assembly.gfa.gz' files in ${SET_DIR}"
    continue
  fi

  mapfile -t gfa_sorted < <(printf '%s\n' "${gfa_files[@]}" | LC_ALL=C sort)

  for src_gfa in "${gfa_sorted[@]}"; do
    sample_id="$(basename "${src_gfa}" .assembly.gfa.gz)"
    src_label="${SET_DIR}/${sample_id}.gfa.csv"
    src_edge="${SET_DIR}/${sample_id}.edge_reads.csv"

    # Check required companions
    if [[ ! -r "${src_label}" ]]; then
      echo "   - Skipping '${sample_id}': missing/unreadable '${src_label}'."
      continue
    fi
    if [[ ! -r "${src_edge}" ]]; then
      echo "   - Skipping '${sample_id}': missing/unreadable '${src_edge}'."
      continue
    fi

    cp -- "${src_gfa}" "${src_label}" "${src_edge}" "${OUT_DIR}/"

    printf '%s,%s,%s,%s\n' \
      "${OUT_DIR_BASENAME}/$(basename "${src_gfa}")" \
      "${OUT_DIR_BASENAME}/$(basename "${src_label}")" \
      "${OUT_DIR_BASENAME}/$(basename "${src_edge}")" \
      "${sample_id}" >> "${OUT_MANIFEST}"

    ((++sample_count))
    echo "   - ✔️  ${sample_id}"
  done
done

echo ""
echo "Success! Consolidated ${sample_count} samples."
echo "   - Files copied into:     ${OUT_DIR}"
echo "   - Manifest (no header):  ${OUT_MANIFEST}"
