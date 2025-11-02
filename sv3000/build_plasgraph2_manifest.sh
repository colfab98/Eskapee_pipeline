#!/usr/bin/env bash
# Build v8-shaped manifest from v9 artifacts (PRUNED SHORT as the GFA of record)
# Header: gfa_gz,gfa_csv,edge_csv,sample_id

set -u  # (no -e / pipefail; we handle errors explicitly)

BASE="${BASE:-$PWD}"
: "${BATCH_ID:?set BATCH_ID=your_batch_id}"

SHORT_DIR="$BASE/batches_being_processed/${BATCH_ID}.SHORT"
U_SHORT="$SHORT_DIR/Unicycler"
SEL="$BASE/selections/${BATCH_ID}"

COMBINED_DIR="$SEL/short_pruned_features"         # preferred features (combined CSV)
NODES_DIR="$SEL/features/nodes"                   # fallback features (TSV -> CSV)
EDGE_DIR="$SEL/short_pruned_edge_support"         # preferred edge support

OUT="$SHORT_DIR/plasgraph2_manifest.csv"
TMPDIR="$SHORT_DIR/tmp_manifest_csv"

[[ -d "$U_SHORT" ]] || { echo "[e] Unicycler SHORT dir not found: $U_SHORT" >&2; exit 1; }
mkdir -p "$TMPDIR"
: > "$OUT"
echo "gfa_gz,gfa_csv,edge_csv,sample_id" > "$OUT"

tsv_to_csv() {  # TSV -> CSV preserving header
  local in="$1" out="$2"
  awk -v OFS="," 'BEGIN{FS="\t"} { $1=$1; print $0 }' "$in" > "$out"
}

added=0
shopt -s nullglob
for gfa in "$U_SHORT"/*.assembly.pruned.gfa.gz; do
  sample="${gfa##*/}"; sample="${sample%%.assembly.pruned.gfa.gz}"

  # -------- features (gfa_csv) --------
  gfa_csv=""
  if [[ -s "$COMBINED_DIR/${sample}.csv" ]]; then
    gfa_csv="$COMBINED_DIR/${sample}.csv"
  elif [[ -s "$NODES_DIR/${sample}.node_features.tsv" ]]; then
    gfa_csv="$TMPDIR/${sample}.gfa.csv"
    tsv_to_csv "$NODES_DIR/${sample}.node_features.tsv" "$gfa_csv" || {
      echo "[w] TSV->CSV failed for $sample; skipping" >&2; continue; }
  else
    echo "[w] no features for $sample; skipping" >&2
    continue
  fi

  # -------- edge support (edge_csv) --------
  edge_csv=""
  if [[ -s "$EDGE_DIR/${sample}.edge_reads.csv" ]]; then
    edge_csv="$EDGE_DIR/${sample}.edge_reads.csv"
  elif [[ -s "$U_SHORT/${sample}.edge_reads.csv" ]]; then
    edge_csv="$U_SHORT/${sample}.edge_reads.csv"
  else
    edge_csv=""  # allowed (v8 tolerated missing edge CSV)
  fi

  echo "$gfa,$gfa_csv,$edge_csv,$sample" >> "$OUT"
  ((added++))
done

if (( added == 0 )); then
  echo "[e] no rows written (check pruned GFAs/features locations)" >&2
  exit 2
fi

echo "[OK] Wrote $OUT (rows: $added)"
