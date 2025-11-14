#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

sed -i 's/\r$//' "$BATCH_SEL/sample_run_audit.csv"

awk -F, '
BEGIN { OFS="\t" }
{
  s=$1; f1=$3; f2=$4; p=$5; l=$6
  gsub(/^[ \t\r\n]+|[ \t\r\n]+$/, "", p)
  gsub(/^[ \t\r\n]+|[ \t\r\n]+$/, "", l)
  p=toupper(p); l=toupper(l)
  if (p=="ILLUMINA" && l=="PAIRED") {
    print s, "R1", f1
    print s, "R2", f2
  } else {
    print s, "LONG", f1
  }
}' "$BATCH_SEL/sample_run_audit.csv" > "$BATCH_SEL/filelists.tsv"

cut -f1 "$BATCH_SEL/filelists.tsv" | sort -u > "$BATCH_SEL/samples.list"

work="$BATCH_STAGING/merged_fastqs"
mkdir -p "$work"

echo -e "ID\tR1\tR2\tLongFastQ\tFast5\tGenomeSize" > "$BATCH_SEL/samplesheet.tsv"

while read -r s; do
  mapfile -t R1s < <(awk -v s="$s" -F'\t' '$1==s && $2=="R1" { print $3 }' "$BATCH_SEL/filelists.tsv")
  mapfile -t R2s < <(awk -v s="$s" -F'\t' '$1==s && $2=="R2" { print $3 }' "$BATCH_SEL/filelists.tsv")
  mapfile -t LNs < <(awk -v s="$s" -F'\t' '$1==s && $2=="LONG" { print $3 }' "$BATCH_SEL/filelists.tsv")

  R1="NA"; R2="NA"; LONG="NA"
  ((${#R1s[@]})) && { R1="$work/${s}_R1.fastq.gz"; cat "${R1s[@]}" > "$R1"; }
  ((${#R2s[@]})) && { R2="$work/${s}_R2.fastq.gz"; cat "${R2s[@]}" > "$R2"; }
  ((${#LNs[@]})) && { LONG="$work/${s}_long.fastq.gz"; cat "${LNs[@]}" > "$LONG"; }

  echo -e "${s}\t${R1}\t${R2}\t${LONG}\tNA\tNA" >> "$BATCH_SEL/samplesheet.tsv"
done < "$BATCH_SEL/samples.list"

echo "Checking that all merged FASTQs exist..."
awk -F'\t' 'NR>1 { print $2"\n"$3"\n"$4 }' "$BATCH_SEL/samplesheet.tsv" | grep -v '^NA$' \
  | xargs -I{} bash -c 'test -s "{}" || echo "MISSING: {}"'

echo "Wrote samplesheet: $BATCH_SEL/samplesheet.tsv"
echo "Merged FASTQs in: $work/"
