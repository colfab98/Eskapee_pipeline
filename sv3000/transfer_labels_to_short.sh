!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"

HYB_UNI="$ROOT/batches_being_processed/${BATCH_ID}.HYBRID/Unicycler"
SHO_UNI="$ROOT/batches_being_processed/${BATCH_ID}.SHORT/Unicycler"
TRUTH_DIR="$ROOT/selections/$BATCH_ID/labels_hybrid"     
OUT_DIR="$ROOT/selections/$BATCH_ID/short_labels"
LOG="$BATCH_LOG/transfer_labels_to_short.log"
mkdir -p "$OUT_DIR" "$BATCH_LOG"

MINIMAP="$ROOT/envs/label_env/bin/minimap2"
if [[ ! -x "$MINIMAP" ]]; then
  if command -v minimap2 >/dev/null 2>&1; then MINIMAP="$(command -v minimap2)"
  else
    echo "[e] minimap2 not found at $ROOT/envs/label_env/bin/minimap2 or in PATH" | tee -a "$LOG"
    exit 127
  fi
fi

MIN_SUPPORT_DOM="${MIN_SUPPORT_DOM:-0.50}"  
MAX_OTHER_DOM="${MAX_OTHER_DOM:-0.20}"      
AMBIG_MIN="${AMBIG_MIN:-0.30}"              
AMBIG_DELTA="${AMBIG_DELTA:-0.10}"          
echo "==[ $(date -Iseconds) ]== Transfer HYBRID → SHORT (BATCH=$BATCH_ID)" | tee "$LOG"

list_stems() {
  local d="$1"
  find "$d" -maxdepth 1 -type f \
    \( -name '*.scaffolds.fa' -o -name '*.scaffolds.fa.gz' -o -name '*.scaffolds.fasta' -o -name '*.scaffolds.fasta.gz' \) \
    -printf '%f\n' 2>/dev/null \
    | sed -E 's/\.scaffolds\.(fa|fasta)(\.gz)?$//' \
    | LC_ALL=C sort -u
}

if [[ ! -d "$HYB_UNI" || ! -d "$SHO_UNI" ]]; then
  echo "[e] Unicycler dirs missing: $HYB_UNI or $SHO_UNI" | tee -a "$LOG"
  exit 2
fi

mapfile -t HYB_S <<EOF
$(list_stems "$HYB_UNI")
EOF
mapfile -t SHO_S <<EOF
$(list_stems "$SHO_UNI")
EOF

mapfile -t SAMPLES < <(printf "%s\n" "${HYB_S[@]}" "${SHO_S[@]}" \
  | sort | uniq -d)

if ((${#SAMPLES[@]}==0)); then
  echo "[e] No overlapping samples between HYBRID and SHORT Unicycler outputs." | tee -a "$LOG"
  exit 2
fi

pick_scaffolds() {
  local d="$1" s="$2"
  local f
  for f in \
    "$d/$s.scaffolds.fa.gz" \
    "$d/$s.scaffolds.fasta.gz" \
    "$d/$s.scaffolds.fa" \
    "$d/$s.scaffolds.fasta"
  do
    [[ -s "$f" ]] && { echo "$f"; return 0; }
  done
  return 1
}

paf2labelcov_py="$(mktemp)"
cat > "$paf2labelcov_py" <<'PY'
import sys, collections, gzip, io, os

truth_tsv, paf, short_fa = sys.argv[1], sys.argv[2], sys.argv[3]

lbl = {}
with open(truth_tsv) as f:
    hdr = f.readline().rstrip('\n').split('\t')
    idx = {k:i for i,k in enumerate(hdr)}
    for line in f:
        if not line.strip(): continue
        p = line.rstrip('\n').split('\t')
        lbl[p[idx['contig']]] = p[idx['label']]

def merge(iv):
    iv.sort()
    out=[]
    for s,e in iv:
        if not out or s>out[-1][1]:
            out.append([s,e])
        else:
            if e>out[-1][1]: out[-1][1]=e
    return out

def open_text_maybe_gz(path):
    if path.endswith('.gz'):
        return io.TextIOWrapper(gzip.open(path, 'rb'))
    return open(path, 'rt')

qlen = {}
with open_text_maybe_gz(short_fa) as f:
    name = None
    L = 0
    for line in f:
        if line.startswith('>'):
            if name is not None:
                qlen[name] = L
            name = line[1:].strip().split()[0]
            L = 0
        else:
            L += len(line.strip())
    if name is not None:
        qlen[name] = L

iv_by_q_lbl = collections.defaultdict(lambda: {'chromosome':[], 'plasmid':[], 'unlabeled':[]})

if os.path.exists(paf) and os.path.getsize(paf) > 0:
    with open(paf) as f:
        for line in f:
            if not line.strip() or line.startswith('#'): continue
            p = line.rstrip('\n').split('\t')
            q, ql = p[0], int(p[1])
            qs, qe = int(p[2]), int(p[3])
            t = p[5]
            # ensure we have a length even if the FASTA didn't have it (shouldn't happen)
            if q not in qlen:
                qlen[q] = ql
            s, e = (qs, qe) if qs <= qe else (qe, qs)
            lab = lbl.get(t, 'unlabeled')
            lab = lab if lab in ('chromosome','plasmid') else 'unlabeled'
            iv_by_q_lbl[q][lab].append((s, e))

print("short_contig\tshort_len\tcov_chr\tcov_plasmid\tcov_unlabeled")
for q in qlen:
    L = qlen[q]
    cov = {}
    for lab in ('chromosome','plasmid','unlabeled'):
        merged = merge(iv_by_q_lbl[q][lab])
        cov_bp = sum(e-s for s,e in merged)
        cov[lab] = 0.0 if L==0 else min(1.0, cov_bp / L)
    print(f"{q}\t{L}\t{cov['chromosome']:.6f}\t{cov['plasmid']:.6f}\t{cov['unlabeled']:.6f}")
PY
trap 'rm -f "$paf2labelcov_py"' EXIT

MASTER="$OUT_DIR/short_labels_master.tsv"
echo -e "sample\tshort_contig\tlength\tlabel\treason\tcov_chr\tcov_plasmid\tcov_unlabeled" > "$MASTER"

for s in "${SAMPLES[@]}"; do
  sho_fa="$(pick_scaffolds "$SHO_UNI" "$s" || true)"
  hyb_fa="$(pick_scaffolds "$HYB_UNI" "$s" || true)"
  truth="$TRUTH_DIR/${s}.hybrid_truth.tsv"

  if [[ -z "${sho_fa:-}" || ! -s "$sho_fa" ]]; then echo "[w] missing SHORT scaffolds for $s — skipping" | tee -a "$LOG"; continue; fi
  if [[ -z "${hyb_fa:-}" || ! -s "$hyb_fa" ]]; then echo "[w] missing HYBRID scaffolds for $s — skipping" | tee -a "$LOG"; continue; fi
  if [[ ! -s "$truth" ]]; then echo "[w] missing HYBRID truth for $s — skipping" | tee -a "$LOG"; continue; fi

  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' RETURN

  paf="$tmpdir/short_vs_hybrid.paf"
  "$MINIMAP" -c -x asm5 -t "${SLURM_CPUS_PER_TASK:-8}" "$hyb_fa" "$sho_fa" > "$paf" 2>>"$LOG"

  cov_tsv="$tmpdir/label_cov.tsv"
  python3 "$paf2labelcov_py" "$truth" "$paf" "$sho_fa" > "$cov_tsv"

  out_tsv="$OUT_DIR/${s}.short_labels.tsv"
  awk -v MS="$MIN_SUPPORT_DOM" -v MO="$MAX_OTHER_DOM" -v AM="$AMBIG_MIN" -v AD="$AMBIG_DELTA" 'BEGIN{OFS="\t"}
    NR==1{next}
    {
      sc=$1; L=$2+0; c=$3+0; p=$4+0; u=$5+0;
      label="unlabeled"; why="no_support";
      if (c>=AM && p>=AM && ((c>p?c-p:p-c) <= AD)) {
        label="ambiguous"; why="both_close";
      } else if (c>=MS && p<MO) {
        label="chromosome"; why="dom_chr";
      } else if (p>=MS && c<MO) {
        label="plasmid"; why="dom_pls";
      } else if (c>=AM && p<AM && u<AM) {
        label="chromosome"; why="chr_only";
      } else if (p>=AM && c<AM && u<AM) {
        label="plasmid"; why="pls_only";
      } else {
        label="unlabeled"; why="mixed_or_low";
      }
      print sc, L, label, why, c, p, u;
    }' "$cov_tsv" > "$out_tsv"

  awk -v s="$s" 'BEGIN{OFS="\t"} NR>1{ print s, $1, $2, $3, $4, $5, $6, $7 }' "$out_tsv" >> "$MASTER"
  echo "[ok] $s -> labeled $(($(wc -l < "$out_tsv")-1)) SHORT contigs" | tee -a "$LOG"

  rm -rf "$tmpdir"
done

echo "[ok] Transfer complete:"
echo " - per-sample: $OUT_DIR/<sample>.short_labels.tsv"
echo " - batch master: $MASTER"
