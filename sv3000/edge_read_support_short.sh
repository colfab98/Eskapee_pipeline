#!/usr/bin/env bash
#SBATCH --job-name=edge_read_support_short
#SBATCH --cpus-per-task=8
#SBATCH --mem=12G
#SBATCH --time=04:00:00
#SBATCH -o slurm/%x-%A_%a.out

set -euo pipefail

BASE="${BASE:-$PWD}"
: "${BATCH_ID:?set BATCH_ID=your_batch_id}"

MINIGRAPH="${MINIGRAPH:-$BASE/envs/label_env/bin/minigraph}"
MG_PRESET_SHORT="${MG_PRESET_SHORT:-sr}"       
MG_PRESET_LONG="${MG_PRESET_LONG:-lr}"        
GAF_COUNT_MODE="${GAF_COUNT_MODE:-all_edges}"  

THREADS="${SLURM_CPUS_PER_TASK:-8}"
END_MAX="${END_MAX:-1000}"                   
MAPQ_MIN="${MAPQ_MIN:-20}"
USE_LOCAL_SCRATCH="${USE_LOCAL_SCRATCH:-1}"

U="$BASE/batches_being_processed/${BATCH_ID}.SHORT/Unicycler"
STAGED="$BASE/staging/$BATCH_ID/merged_fastqs"
TMP="${SLURM_TMPDIR:-$BASE/work/edge_support_tmp}"
mkdir -p "$TMP" "$BASE/slurm"

[[ -x "$MINIGRAPH" ]] || { echo "[e] minigraph not found at $MINIGRAPH"; exit 1; }
[[ -d "$U"      ]] || { echo "[e] Unicycler dir not found: $U"; exit 1; }
[[ -d "$STAGED" ]] || { echo "[e] merged_fastqs dir not found: $STAGED"; exit 1; }

mapfile -t PRUNED < <(find "$U" -maxdepth 1 -type f -name '*.assembly.pruned.gfa.gz' | LC_ALL=C sort)
((${#PRUNED[@]})) || { echo "[e] no *.assembly.pruned.gfa.gz under $U (run prune first)"; exit 2; }

declare -a WORK_SAMPLES
if [[ -n "${SAMPLE_ID:-}" ]]; then
  if [[ -s "$U/${SAMPLE_ID}.assembly.pruned.gfa.gz" ]]; then
    WORK_SAMPLES=("$SAMPLE_ID"); MODE="single"
  else
    echo "[e] SAMPLE_ID=$SAMPLE_ID missing pruned GFA"; exit 3
  fi
elif [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
  idx="${SLURM_ARRAY_TASK_ID}"
  if (( idx < 0 || idx >= ${#PRUNED[@]} )); then
    echo "[e] SLURM_ARRAY_TASK_ID=$idx out of range (0..$(( ${#PRUNED[@]}-1 )))"; exit 4
  fi
  s="$(basename "${PRUNED[$idx]}")"; s="${s%%.assembly.pruned.gfa.gz}"
  WORK_SAMPLES=("$s"); MODE="array[$idx]"
else
  for g in "${PRUNED[@]}"; do s="$(basename "$g")"; s="${s%%.assembly.pruned.gfa.gz}"; WORK_SAMPLES+=("$s"); done
  MODE="sequential"
fi

echo "[i] batch=$BATCH_ID  mode=$MODE  samples=${#WORK_SAMPLES[@]}  threads=$THREADS  tmp=$TMP  presets(short=$MG_PRESET_SHORT,long=$MG_PRESET_LONG)"

stage_to_tmp() {
  local src="$1"
  [[ -s "$src" ]] || { echo ""; return 0; }
  if [[ "$USE_LOCAL_SCRATCH" -eq 1 && -n "${SLURM_TMPDIR:-}" ]]; then
    local dst="$TMP/$(basename "$src")"
    if [[ ! -s "$dst" ]]; then cp -f "$src" "$dst"; fi
    echo "$dst"
  else
    echo "$src"
  fi
}

sanitize_gfa() {
  local in="$1" out="$2"
  if [[ "$in" =~ \.gz$ ]]; then gzip -cd "$in" > "$out.tmp"; else cp -f "$in" "$out.tmp"; fi
  awk -F'\t' '
    BEGIN{ OFS="\t" }
    FNR==NR {
      if ($1=="S") {
        good=0
        if ($3!="*" && length($3)>0) good=1
        else for (i=4;i<=NF;i++) if ($i ~ /^LN:i:/){ split($i,a,":"); if (a[3]+0>0) good=1 }
        if (good) S[$2]=1
      }
      next
    }
    { if ($1=="H"){print;next}
      if ($1=="S"){ if (S[$2]) print; next}
      if ($1=="L"){ if (S[$2] && S[$4]) print; next}
    }
  ' "$out.tmp" "$out.tmp" > "$out"
  rm -f "$out.tmp"
}

process_sample() {
  local sample="$1"
  echo "== $BATCH_ID :: $sample =="

  local gfa_gz="$U/${sample}.assembly.pruned.gfa.gz"
  [[ -s "$gfa_gz" ]] || { echo "[e] missing pruned GFA: $gfa_gz"; return 0; }

  local out_csv="$U/${sample}.edge_reads.csv"
  if [[ -s "$out_csv" && "${OVERWRITE:-0}" -ne 1 ]]; then
    echo "skip (exists): $out_csv (set OVERWRITE=1 to recompute)"
    return 0
  fi

  local gfa_path="$TMP/${BATCH_ID}.${sample}.assembly.pruned.sanitized.gfa"
  sanitize_gfa "$gfa_gz" "$gfa_path"

  local R1_SRC="$STAGED/${sample}_R1.fastq.gz"
  local R2_SRC="$STAGED/${sample}_R2.fastq.gz"
  local LON_SRC="$STAGED/${sample}_long.fastq.gz"

  local R1="$(stage_to_tmp "$R1_SRC")"
  local R2="$(stage_to_tmp "$R2_SRC")"
  local LON="$(stage_to_tmp "$LON_SRC")"

  local s1_gaf="$TMP/${BATCH_ID}.${sample}.R1.gaf"
  local s2_gaf="$TMP/${BATCH_ID}.${sample}.R2.gaf"
  local l_gaf="$TMP/${BATCH_ID}.${sample}.long.gaf"
  rm -f "$s1_gaf" "$s2_gaf" "$l_gaf"

  if [[ -s "$R1" && -s "$R2" ]]; then
    echo "[i] graph-align short reads -> $s1_gaf ; $s2_gaf"
    "$MINIGRAPH" -x "$MG_PRESET_SHORT" -t "$THREADS" "$gfa_path" "$R1" > "$s1_gaf"
    "$MINIGRAPH" -x "$MG_PRESET_SHORT" -t "$THREADS" "$gfa_path" "$R2" > "$s2_gaf"
  else
    echo "[i] no paired short reads for $sample"
  fi

  # Map long reads to the graph
  if [[ -s "$LON" ]]; then
    echo "[i] graph-align long reads -> $l_gaf"
    "$MINIGRAPH" -x "$MG_PRESET_LONG" -t "$THREADS" "$gfa_path" "$LON" > "$l_gaf"
  else
    echo "[i] no long reads for $sample"
  fi

  END_MAX="$END_MAX" MAPQ_MIN="$MAPQ_MIN" GAF_COUNT_MODE="$GAF_COUNT_MODE" \
  python3 - "$gfa_path" "$out_csv" "$s1_gaf" "$s2_gaf" "$l_gaf" << 'PY'
import sys, csv, re, os
from collections import defaultdict

gfa_path, out_csv, s1_gaf, s2_gaf, l_gaf = sys.argv[1:6]
END_MAX   = int(os.environ.get("END_MAX", "1000"))
MAPQ_MIN  = int(os.environ.get("MAPQ_MIN", "20"))
MODE      = os.environ.get("GAF_COUNT_MODE", "all_edges")

edges = set()
with open(gfa_path, "rt") as fh:
  for ln in fh:
    if ln and ln[0] == "L":
      f = ln.rstrip("\n").split("\t")
      a, b = f[1], f[3]
      if a != b:
        u, v = (a, b) if a < b else (b, a)
        edges.add((u, v))

def nodes_from_tpath(tpath: str):
  # Support paths like ">11,<5,>20" OR "11+,5-,20+"
  return [s.lstrip('><').rstrip('+-') for s in re.split(r'[,\s]+', tpath) if s]

def parse_gaf_best(path):
  best = {}
  if not (path and os.path.exists(path) and os.path.getsize(path) > 0):
    return best
  with open(path) as fh:
    for ln in fh:
      if not ln.strip(): continue
      f = ln.rstrip("\n").split("\t")
      if len(f) < 12: continue
      try:
        q   = f[0]
        tpath = f[5]
        tlen  = int(f[6]); ts = int(f[7]); te = int(f[8])
        alen  = int(f[10]); mapq = int(f[11])
      except ValueError:
        continue
      if mapq < MAPQ_MIN: continue
      rec = dict(nodes=nodes_from_tpath(tpath), tlen=tlen, ts=ts, te=te, alen=alen, mapq=mapq)
      if (q not in best) or (alen > best[q]["alen"]):
        best[q] = rec
  return best

def parse_gaf_all(path):
  """qname -> list of recs (MAPQ>=MAPQ_MIN) in file order."""
  allr = defaultdict(list)
  if not (path and os.path.exists(path) and os.path.getsize(path) > 0):
    return allr
  with open(path) as fh:
    for ln in fh:
      if not ln.strip(): continue
      f = ln.rstrip("\n").split("\t")
      if len(f) < 12: continue
      try:
        q   = f[0]
        tpath = f[5]
        tlen  = int(f[6]); ts = int(f[7]); te = int(f[8])
        alen  = int(f[10]); mapq = int(f[11])
      except ValueError:
        continue
      if mapq < MAPQ_MIN: continue
      allr[q].append(dict(nodes=nodes_from_tpath(tpath), tlen=tlen, ts=ts, te=te, alen=alen, mapq=mapq))
  return allr

def base_id(q):
  q=q.split()[0]
  return re.sub(r"/[12]$", "", q)

def anchor_from_path_end(rec, end_max):
  if not rec["nodes"]:
    return None
  if rec["ts"] <= end_max:
    return rec["nodes"][0]
  if (rec["tlen"] - rec["te"]) <= end_max:
    return rec["nodes"][-1]
  return None

pair_counts = defaultdict(int)
s1_best = parse_gaf_best(s1_gaf)
s2_best = parse_gaf_best(s2_gaf)

if s1_best or s2_best:
  by_base = defaultdict(dict)
  for q, r in s1_best.items(): by_base[base_id(q)]["R1"] = r
  for q, r in s2_best.items(): by_base[base_id(q)]["R2"] = r
  for _, pr in by_base.items():
    if "R1" not in pr or "R2" not in pr: continue
    a, b = pr["R1"], pr["R2"]
    u = anchor_from_path_end(a, END_MAX)
    v = anchor_from_path_end(b, END_MAX)
    if not u or not v or u == v: continue
    x, y = (u, v) if u < v else (v, u)
    if (x, y) in edges:
      pair_counts[(x, y)] += 1

long_counts = defaultdict(int)
l_all = parse_gaf_all(l_gaf)

for q, recs in l_all.items():
  path_nodes = []
  for r in recs:
    ns = r["nodes"]
    if path_nodes and ns:
      if path_nodes[-1] == ns[0]:
        path_nodes.extend(ns[1:])
      else:
        path_nodes.extend(ns)
    else:
      path_nodes.extend(ns)
  if len(path_nodes) < 2:
    continue

  if MODE == "first_last":
    pairs = []
    if len(path_nodes) >= 2: pairs.append((path_nodes[0], path_nodes[1]))
    if len(path_nodes) >= 3: pairs.append((path_nodes[-2], path_nodes[-1]))
  else:
    pairs = list(zip(path_nodes, path_nodes[1:]))

  for a, b in pairs:
    if a == b: continue
    u, v = (a, b) if a < b else (b, a)
    if (u, v) in edges:
      long_counts[(u, v)] += 1

os.makedirs(os.path.dirname(out_csv), exist_ok=True)
with open(out_csv, "w", newline="") as out:
  w = csv.writer(out)
  w.writerow(["contig_u","contig_v","short_pair_count","long_read_count","total_support"])
  for u, v in sorted(edges):
    s = pair_counts.get((u, v), 0)
    l = long_counts.get((u, v), 0)
    w.writerow([u, v, s, l, s + l])
print(f"[ok] wrote {out_csv}")
PY

  awk -F'\t' '$1=="S"{print $2}' "$gfa_path" | LC_ALL=C sort -u > "$TMP/${sample}.nodes.txt"
  awk -F',' 'NR>1{print $1; print $2}' "$out_csv" | LC_ALL=C sort -u > "$TMP/${sample}.edge_nodes.txt"
  if comm -13 "$TMP/${sample}.nodes.txt" "$TMP/${sample}.edge_nodes.txt" | head -n1 | grep -q .; then
    echo "[e] edge_read_support_short: found edge endpoints not present in pruned GFA for sample ${sample}"
    echo "    tip: set OVERWRITE=1 to recompute, or check prune step outputs"
    exit 9
  fi

  echo "[ok] $sample done"
}

for s in "${WORK_SAMPLES[@]}"; do
  process_sample "$s"
done

echo "[OK] Edge-read support (SHORT/pruned) done for $BATCH_ID (samples: ${#WORK_SAMPLES[@]})."
