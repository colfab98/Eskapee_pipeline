#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

CSV="${1:-$ROOT/all_samples.csv}"
N="${2:-5}"
MODE="${3:-test}"; [[ "$MODE" =~ ^(test|train)$ ]] || { echo "Usage: $0 <all_samples.csv> <N> [test|train]"; exit 1; }
DATASET="eskapee-${MODE}"

OUT_DIR="$BATCH_SEL"
LOG_DIR="$BATCH_LOG"
mkdir -p "$OUT_DIR" "$LOG_DIR"

out_sel="$OUT_DIR/eskapee5_selection.tsv"
out_sra="$OUT_DIR/sra_ids_for_download.csv"
log="$LOG_DIR/pick_eskapee5.log"

if [[ -s "$out_sel" ]]; then
  echo "[pick] $out_sel already exists; skipping pick." | tee -a "$log"
  exit 0
fi

tmp_csv="$(mktemp)"
tr -d '\r' < "$CSV" > "$tmp_csv"

tmp_excl="$(mktemp)"
if [[ -d "$ROOT/selections" ]]; then
  # First field of tsv; skip comment lines beginning with '#'
  find "$ROOT/selections" -mindepth 2 -maxdepth 2 -name 'eskapee5_selection.tsv' -print0 2>/dev/null \
  | xargs -0 -r awk -F'\t' '($0!~/^#/ && NF>=1 && $1!=""){print $1}' \
  | sort -u > "$tmp_excl"
else
  : > "$tmp_excl"
fi

echo "[i] cwd: $(pwd)" | tee "$log"
echo "[i] input CSV: $CSV" | tee -a "$log"
echo "[i] excluding already-assigned samples: $(wc -l < "$tmp_excl") our_id(s)" | tee -a "$log"
echo "[i] selecting first $N samples (short and/or long) from dataset=${DATASET}" | tee -a "$log"

tmp_sel="$(mktemp)"
awk -F, -v N="$N" -v EXCL="$tmp_excl" -v DS="$DATASET" '
  BEGIN{
    OFS="\t"
    # load exclude set (our_id)
    while ((getline line < EXCL) > 0) { excl[line]=1 }
    close(EXCL)
  }
  NR==1{
    for(i=1;i<=NF;i++){
      gsub(/^ *"|" *$/,"",$i); key=$i; gsub(/ /,"",key); h[key]=i
    }
    print "# picked from " DS " (short and/or long):"
    print "# columns: our_id\tsample_id\tspecies\tshort_reads\tlong_reads"
    next
  }
  {
    our_id=$(h["our_id"])
    sample_id=$(h["sample_id"])
    species=$(h["species"])
    dataset=$(h["dataset"])
    sr=$(h["short_reads"])
    lr=$(h["long_reads"])
    gsub(/^ *"|" *$/,"",our_id); gsub(/^ *"|" *$/,"",sample_id)
    gsub(/^ *"|" *$/,"",species); gsub(/^ *"|" *$/,"",dataset)
    gsub(/^ *"|" *$/,"",sr);     gsub(/^ *"|" *$/,"",lr)

    if (dataset==DS && ((sr!="" && sr!="NA") || (lr!="" && lr!="NA")) && excl[our_id]!=1){
      print our_id, sample_id, species, sr, lr
      c++; if (c==N) exit
    }
  }
' "$tmp_csv" | tee "$tmp_sel" >> "$log"

mv "$tmp_sel" "$out_sel"
echo "[ok] wrote $out_sel" | tee -a "$log"

awk -F'\t' '
  $0 ~ /^#/ { next }
  NF>=5 {
    sr=$4; lr=$5;
    gsub(/[;,]/," ",sr); gsub(/[;,]/," ",lr);
    print sr; print lr;
  }
' "$out_sel" \
| tr " " "\n" | sed '/^$/d' | sed '/^NA$/d' \
| awk '!seen[$0]++' > "$out_sra"

valid=$(awk 'toupper($0) ~ /^(SRR|ERR|DRR)[0-9]+$/ {c++} END{print (c+0)}' "$out_sra")
if [[ "$valid" -eq 0 ]]; then
  echo "[pick][error] No SRR/ERR/DRR accessions found for picked rows. Check all_samples.csv columns and values." | tee -a "$log"
  echo "[pick][hint] Expected columns: our_id, sample_id, species, dataset, short_reads, long_reads" | tee -a "$log"
  rm -f "$out_sel" "$out_sra"
  exit 1
fi

{
  echo ""
  echo "# ---- SUMMARY ----"
  echo "selected rows: $(grep -vc '^#' "$out_sel")"
  echo "SRA IDs written to: $out_sra  (count: $(wc -l < "$out_sra"))"
  echo "timestamp: $(date -Is)"
} >> "$log"

echo "[ok] wrote $out_sra" | tee -a "$log"
