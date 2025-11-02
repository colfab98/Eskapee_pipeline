#!/usr/bin/env bash
# scripts/truth_from_hybrid.sh
# PURPOSE (v9-necessary): Build HYBRID-based truth per sample:
#   rule stage (len/circular) → homology refinement (closed-genome vs PLSDB, 80/20, length guards) + optional PhiX drop.
# Outputs under selections/<BATCH_ID>/labels_hybrid/

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

HYB_OUT="$ROOT/batches_being_processed/${BATCH_ID}.HYBRID"
TRUTH_DIR="$ROOT/selections/$BATCH_ID/labels_hybrid"
LOG="$BATCH_LOG/truth_from_hybrid.log"
mkdir -p "$TRUTH_DIR" "$BATCH_LOG"

# Indices
CHR_IDX="$ROOT/truth/indices/chromosomes.mmi"
PLS_MMI_SINGLE="$ROOT/truth/indices/plsdb.mmi"
PLS_ARGS1="$ROOT/truth/indices/plsdb.args"
PLS_ARGS2="$ROOT/truth/indices/plasmids.args"
PHI_IDX="$ROOT/truth/indices/phix.mmi"

# Tool
MINIMAP="$ROOT/envs/label_env/bin/minimap2"
if [[ ! -x "$MINIMAP" ]]; then
  if command -v minimap2 >/dev/null 2>&1; then MINIMAP="$(command -v minimap2)"; else
    echo "[e] minimap2 not found" | tee -a "$LOG"; exit 127
  fi
fi

# Thresholds
CHR_MIN=1000000
PLAS_CIRC_MAX=1000000
CHR_LEN_GUARD=100000
PLAS_LEN_GUARD=1000
COV_HI=0.80
COV_LO=0.20
PHIX_GATE=0.80

echo "==[ $(date -Iseconds) ]== HYBRID truth build (BATCH=$BATCH_ID)" | tee "$LOG"
echo "[i] HYB_OUT=$HYB_OUT" | tee -a "$LOG"

# Find HYBRID scaffolds (Unicycler)
mapfile -t FASTAS < <(find "$HYB_OUT/Unicycler" -maxdepth 1 -type f \
  \( -name '*.scaffolds.fa' -o -name '*.scaffolds.fa.gz' -o -name '*.scaffolds.fasta' -o -name '*.scaffolds.fasta.gz' \) \
  | LC_ALL=C sort)
if ((${#FASTAS[@]}==0)); then
  echo "[e] No HYBRID scaffolds found under $HYB_OUT/Unicycler" | tee -a "$LOG"
  exit 2
fi

# Resolve PLSDB indices (single or shards)
resolve_pls_indices() {
  if [[ -s "$PLS_MMI_SINGLE" ]]; then echo "$PLS_MMI_SINGLE"; return 0; fi
  if [[ -s "$PLS_ARGS1" ]]; then tr ' \t\n' '\n' < "$PLS_ARGS1" | sed '/^$/d'; return 0; fi
  if [[ -s "$PLS_ARGS2" ]]; then tr ' \t\n' '\n' < "$PLS_ARGS2" | sed '/^$/d'; return 0; fi
  shopt -s nullglob
  local shards=("$ROOT"/truth/indices/plsdb.part.*.mmi "$ROOT"/truth/indices/plasmids.part.*.mmi)
  shopt -u nullglob
  ((${#shards[@]}>0)) && printf '%s\n' "${shards[@]}" && return 0
  return 1
}

# PAF → coverage helper
paf2cov_py="$(mktemp)"
cat > "$paf2cov_py" <<'PY'
import sys
def merge(iv):
    if not iv: return 0
    iv.sort()
    s,e=iv[0]; tot=0
    for a,b in iv[1:]:
        if a<=e: e=max(e,b)
        else: tot+=e-s; s,e=a,b
    tot+=e-s
    return tot
cov={}; ql={}
for ln in sys.stdin:
    if not ln.strip() or ln[0]=='#': continue
    f=ln.rstrip('\n').split('\t')
    q=f[0]; L=int(f[1]); qs=int(f[2]); qe=int(f[3])
    ql.setdefault(q,L)
    s,e=(qs,qe) if qs<=qe else (qe,qs)
    cov.setdefault(q,[]).append((s,e))
for q in ql:
    covered=merge(cov.get(q,[]))
    frac=0.0 if ql[q]==0 else min(1.0, covered/ql[q])
    print(f"{q}\t{frac:.6f}\t{ql[q]}")
PY
trap 'rm -f "$paf2cov_py"' EXIT

MASTER="$TRUTH_DIR/hybrid_truth_master.tsv"
echo -e "sample\tcontig\tlength\tcircular\tlabel\treason\tchrom_cov\tplasmid_cov\tphix_cov" > "$MASTER"

# Per-sample
for fasta in "${FASTAS[@]}"; do
  base="$(basename "$fasta")"
  sample="${base%%.scaffolds.*}"

  tmpdir="$(mktemp -d)"
  if [[ "$fasta" =~ \.gz$ ]]; then
    uf="$tmpdir/${sample}.fasta"; gzip -cd "$fasta" > "$uf"
  else
    uf="$fasta"
  fi

  out_tsv="$TRUTH_DIR/${sample}.hybrid_truth.tsv"

  # (1) contig lengths + circular tag
  python3 - "$uf" > "$tmpdir/contigs.tsv" <<'PY'
import sys, re
f=sys.argv[1]
name=None; L=0; circ=False
def flush(n,L,c):
    if n is None: return
    print(f"{n}\t{L}\t{1 if c else 0}")
with open(f) as fh:
    name=None; L=0; circ=False
    for ln in fh:
        if ln.startswith('>'):
            flush(name,L,circ)
            h=ln[1:].strip()
            name=h.split()[0]
            circ=bool(re.search(r'circular\s*=\s*true', h, re.I))
            L=0
        else:
            L += len(ln.strip())
    flush(name,L,circ)
PY

  # (2) rule stage
  awk -v CHR_MIN="$CHR_MIN" -v PLAS_CIRC_MAX="$PLAS_CIRC_MAX" 'BEGIN{OFS="\t"}{
    contig=$1; L=$2+0; circ=($3+0); lab="unlabeled"; why="init";
    if (L>CHR_MIN) { lab="chromosome"; why="len>1M" }
    else if (circ==1 && L<PLAS_CIRC_MAX) { lab="plasmid"; why="circ&<1M" }
    print contig, L, circ, lab, why
  }' "$tmpdir/contigs.tsv" > "$tmpdir/labels_init.tsv"

  # (3) homology refinement coverage
  chrom_cov="$tmpdir/chr.cov.tsv"; plas_cov="$tmpdir/pls.cov.tsv"; phix_cov="$tmpdir/phix.cov.tsv"
  : > "$chrom_cov"; : > "$plas_cov"; : > "$phix_cov"

  if [[ -s "$CHR_IDX" ]]; then
    "$MINIMAP" -c -x asm5 -t 8 "$CHR_IDX" "$uf" 2>>"$LOG" | python3 "$paf2cov_py" > "$chrom_cov" || true
  else
    echo "[i] chromosome index missing; skipping chrom refinement for $sample" | tee -a "$LOG"
  fi

  if pls_list=$(resolve_pls_indices); then
    paf_cat="$tmpdir/pls.all.paf"; : > "$paf_cat"
    while IFS= read -r idx; do
      [[ -z "$idx" ]] && continue
      [[ -s "$idx" ]] || { echo "[w] missing shard index: $idx" | tee -a "$LOG"; continue; }
      "$MINIMAP" -c -x asm5 -t 8 "$idx" "$uf" 2>>"$LOG" >> "$paf_cat" || true
    done <<< "$pls_list"
    [[ -s "$paf_cat" ]] && python3 "$paf2cov_py" < "$paf_cat" > "$plas_cov" || true
  else
    echo "[i] PLSDB index/args missing; skipping plasmid refinement for $sample" | tee -a "$LOG"
  fi

  if [[ -s "$PHI_IDX" ]]; then
    "$MINIMAP" -c -x asm5 -t 4 "$PHI_IDX" "$uf" 2>>"$LOG" | python3 "$paf2cov_py" > "$phix_cov" || true
  fi

  # Join coverage to initial labels
  awk 'BEGIN{OFS="\t"}
    FNR==NR { chr[$1]=$2; next }
    FILENAME==ARGV[2]{ pls[$1]=$2; next }
    FILENAME==ARGV[3]{ phi[$1]=$2; next }
    { print $1,$2,$3,$4,$5,(($1 in chr)?chr[$1]:0), (($1 in pls)?pls[$1]:0), (($1 in phi)?phi[$1]:0) }
  ' "$chrom_cov" "$plas_cov" "$phix_cov" "$tmpdir/labels_init.tsv" > "$tmpdir/joined.tsv"

  # (4) final decision
  {
    echo -e "contig\tL\tcirc\tinit\treason\tchr\tpls\tphix"
    cat "$tmpdir/joined.tsv"
  } > "$tmpdir/joined.with_header.tsv"

  awk -v COV_HI="$COV_HI" -v COV_LO="$COV_LO" -v CHR_LEN_GUARD="$CHR_LEN_GUARD" -v PLAS_LEN_GUARD="$PLAS_LEN_GUARD" -v PHIX_GATE="$PHIX_GATE" 'BEGIN{OFS="\t"}
    NR==1{next}
    {
      contig=$1; L=$2+0; circ=$3+0; init=$4; reason=$5; c=$6+0; p=$7+0; x=$8+0;
      label=init; why=reason;
      if (x>=PHIX_GATE){ label="drop"; why="phix"; print contig, L, circ, label, why, c, p, x; next }
      if (init=="unlabeled"){
        if (L>=CHR_LEN_GUARD && c>=COV_HI && p<COV_LO){ label="chromosome"; why="homology_chr" }
        else if (L>=PLAS_LEN_GUARD && p>=COV_HI && c<COV_LO){ label="plasmid"; why="homology_pls" }
        else { label="unlabeled"; why="homology_none" }
      } else {
        if (init=="chromosome" && p>=COV_HI && c<COV_LO){ label="unlabeled"; why="contradict_revert" }
        if (init=="plasmid"    && c>=COV_HI && p<COV_LO){ label="unlabeled"; why="contradict_revert" }
      }
      print contig, L, circ, label, why, c, p, x
    }
  ' "$tmpdir/joined.with_header.tsv" > "$tmpdir/final.tsv"

  # (5) write per-sample (exclude drops)
  {
    echo -e "contig\tlength\tcircular\tlabel\treason\tchrom_cov\tplasmid_cov\tphix_cov"
    awk '$4!="drop"{print $1"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6"\t"$7"\t"$8}' "$tmpdir/final.tsv"
  } > "$out_tsv"

  # Append to master
  awk -v s="$sample" 'NR>1{print s"\t"$0}' "$out_tsv" >> "$MASTER"

  echo "[ok] $sample -> $(awk 'END{print NR-1}' "$out_tsv") contigs labeled (truth)" | tee -a "$LOG"

  rm -rf "$tmpdir"
done

echo "[ok] HYBRID truth complete:"
echo " - per-sample: $TRUTH_DIR/<sample>.hybrid_truth.tsv"
echo " - batch master: $MASTER"
