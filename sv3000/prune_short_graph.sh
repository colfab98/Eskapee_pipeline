#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_env.sh"  # defines ROOT, BATCH_ID, BATCH_LOG
: "${BATCH_ID:?set BATCH_ID=your_batch_id}"

MIN_LEN="${MIN_LEN:-100}"      
SAMPLE_ID="${SAMPLE_ID:-}"     

U_SHORT="$ROOT/batches_being_processed/${BATCH_ID}.SHORT/Unicycler"
TMP_ROOT="${SLURM_TMPDIR:-$ROOT/work/prune_short_tmp}"
LOG_DIR="$BATCH_LOG"
mkdir -p "$TMP_ROOT" "$LOG_DIR"

LOG="$LOG_DIR/prune_short_graph.${BATCH_ID}.log"
log(){ echo "[$(date -Iseconds)] $*" | tee -a "$LOG"; }

[[ -d "$U_SHORT" ]] || { echo "[e] Unicycler SHORT dir not found: $U_SHORT" | tee -a "$LOG" >&2; exit 2; }

mapfile -t GFAS < <(find "$U_SHORT" -maxdepth 1 -type f \( -name '*.assembly.gfa' -o -name '*.assembly.gfa.gz' \) | LC_ALL=C sort)
((${#GFAS[@]})) || { echo "[e] no *.assembly.gfa[.gz] found under $U_SHORT" | tee -a "$LOG" >&2; exit 2; }

declare -a WORK
if [[ -n "$SAMPLE_ID" ]]; then
  if   [[ -f "$U_SHORT/${SAMPLE_ID}.assembly.gfa.gz" ]]; then WORK=("$U_SHORT/${SAMPLE_ID}.assembly.gfa.gz")
  elif [[ -f "$U_SHORT/${SAMPLE_ID}.assembly.gfa"    ]]; then WORK=("$U_SHORT/${SAMPLE_ID}.assembly.gfa")
  else
    echo "[e] SAMPLE_ID=$SAMPLE_ID not found under $U_SHORT" | tee -a "$LOG" >&2; exit 3
  fi
else
  WORK=("${GFAS[@]}")
fi

log "batch=$BATCH_ID  samples=${#WORK[@]}  MIN_LEN=$MIN_LEN"
mkdir -p "$TMP_ROOT"

cleanup_paths=()
trap 'for p in "${cleanup_paths[@]}"; do [[ -e "$p" ]] && rm -rf "$p" || true; done' EXIT

process_one() {
  local gfa_path="$1"
  local sample; sample="$(basename "$gfa_path")"; sample="${sample%%.assembly.gfa*}"
  local out_gz="$U_SHORT/${sample}.assembly.pruned.gfa.gz"

  if [[ -s "$out_gz" ]]; then
    log "skip (exists): $(basename "$out_gz")"
    return 0
  fi

  local tmpdir; tmpdir="$(mktemp -d -p "$TMP_ROOT" "prune_${BATCH_ID}_${sample}.XXXX")"
  cleanup_paths+=("$tmpdir")
  local gfa="$tmpdir/${sample}.assembly.gfa"

  # normalize input to plain .gfa
  if [[ "$gfa_path" =~ \.gz$ ]]; then
    gzip -cd "$gfa_path" > "$gfa"
  else
    cp -f "$gfa_path" "$gfa"
  fi

  MIN_LEN="$MIN_LEN" OUT="$out_gz" python3 - "$gfa" << 'PY'
import sys, os, gzip
from collections import defaultdict

gfa_path = sys.argv[1]
out_gz = os.environ["OUT"]
MIN_LEN = int(os.environ.get("MIN_LEN","100"))

hdr = []
seg_raw = {}     # id -> original S line (preserve)
seg_len = {}     # id -> length (from seq or LN tag)
keep_seg = {}    # id -> bool
edges = []       # (u,uo,v,vo,cigar,raw)

def seg_length(fields):
    # fields: ["S", name, seq, (tags...)]
    seq = fields[2]
    if seq != "*" and len(seq) > 0:
      return len(seq)
    for t in fields[3:]:
      if t.startswith("LN:i:"):
        try:
          return int(t.split(":")[2])
        except Exception:
          pass
    return 0

with open(gfa_path, "rt") as fh:
  for ln in fh:
    if not ln.strip(): continue
    t = ln.rstrip("\n").split("\t")
    if t[0] == "H":
      hdr.append(ln.rstrip("\n"))
    elif t[0] == "S":
      L = seg_length(t)
      sid = t[1]
      seg_raw[sid] = ln.rstrip("\n")
      seg_len[sid] = L
      keep_seg[sid] = (L >= MIN_LEN)
    elif t[0] == "L" and len(t) >= 6:
      edges.append((t[1], t[2], t[3], t[4], t[5], ln.rstrip("\n")))

neighbors = defaultdict(set)
for u,uo,v,vo,cg,raw in edges:
  neighbors[u].add(v)
  neighbors[v].add(u)

pruned  = {s for s,k in keep_seg.items() if not k}
survive = {s for s,k in keep_seg.items() if k}

bypass = set()  # undirected key (a,b) with a<b
for x in pruned:
  Ns = sorted(n for n in neighbors.get(x, []) if n in survive)
  for i in range(len(Ns)):
    for j in range(i+1, len(Ns)):
      a,b = Ns[i], Ns[j]
      if a==b: continue
      bypass.add((a,b) if a<b else (b,a))

kept_edges = []
edge_set = set()  # undirected keys to avoid duplicates
for u,uo,v,vo,cg,raw in edges:
  if u in pruned or v in pruned: continue
  kept_edges.append((u,uo,v,vo,cg,raw))
  edge_set.add((u,v) if u<v else (v,u))

for a,b in sorted(bypass):
  if (a,b) in edge_set: continue
  kept_edges.append((a,"+",b,"+","0M", f"L\t{a}\t+\t{b}\t+\t0M"))

tmp_out = out_gz + ".tmp"
with gzip.open(tmp_out, "wt") as out:
  for h in hdr:
    out.write(h + "\n")
  for sid, keep in keep_seg.items():
    if keep:
      out.write(seg_raw[sid] + "\n")
  for _,_,_,_,_, raw in kept_edges:
    out.write(raw + "\n")

os.replace(tmp_out, out_gz)
removed = len(pruned)
added   = len(bypass)
print(f"[ok] pruned -> {os.path.basename(out_gz)}  removed={removed}  bypass_added={added}")
PY

  log "[ok] $sample pruned -> $(basename "$out_gz")"
}

for g in "${WORK[@]}"; do
  process_one "$g"
done

log "[OK] prune_short_graph complete for $BATCH_ID (samples: ${#WORK[@]})."
