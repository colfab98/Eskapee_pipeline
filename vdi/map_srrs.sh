#!/usr/bin/env bash
set -euo pipefail

# Batch/env (resolves ROOT and batch-scoped dirs)
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

# Step C – Map SRRs to Sample IDs (per PDF workflow)
CSV="$ROOT/all_samples.csv"
SRR_LIST="$BATCH_SEL/sra_ids_for_download.csv"
MODE="${MODE:-test}"
TARGET_DATASET="eskapee-${MODE}"

# Batch-scoped outputs
dataset_rows="$BATCH_SEL/dataset_rows_all.csv"
srr_list_norm="$BATCH_SEL/srr_downloaded.list"
map_out="$BATCH_SEL/srr_to_sample_map.csv"

awk -F, -v DS="$TARGET_DATASET" 'NR==1{for(i=1;i<=NF;i++)h[$i]=i; print; next} $h["dataset"]==DS' "$CSV" > "$dataset_rows"

# 2. Normalize SRR list from sra_ids_for_download.csv
tr -d '\r' < "$SRR_LIST" \
  | tr ' ;,\t' '\n' \
  | grep -E '^[SED]RR[0-9]+' \
  | sort -u > "$srr_list_norm"

# 3. Map SRR → our_id,type, restricted to SRRs actually in the list
awk -F, 'NR==1{
  for(i=1;i<=NF;i++)h[$i]=i; next
}{
  sid=$h["our_id"];
  n=split($h["short_reads"], s, /[ ;,]+/);
  for(i=1;i<=n;i++) if (s[i] ~ /^[SED]RR[0-9]+$/) print s[i] "," sid ",short";
  m=split($h["long_reads"], l, /[ ;,]+/);
  for(i=1;i<=m;i++) if (l[i] ~ /^[SED]RR[0-9]+$/) print l[i] "," sid ",long";
}' "$dataset_rows" \
| sort -t, -k1,1 \
| join -t, -1 1 -2 1 -o 1.1,1.2,1.3 - <(awk '{print $1}' "$srr_list_norm" | sort) \
> "$map_out"

echo "Wrote:"
echo " - $dataset_rows"
echo " - $srr_list_norm"
echo " - $map_out"
