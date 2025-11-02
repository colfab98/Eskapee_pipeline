#!/usr/bin/env bash
# pack_features_short.sh — sv3000
# Build node/edge feature tables from the SHORT *pruned* GFA + edge read support.
#
# Deterministic locations (v9):
#   - Pruned graph:  batches_being_processed/<BATCH_ID>.SHORT/Unicycler/<sample>.assembly.pruned.gfa.gz
#   - Edge support:  batches_being_processed/<BATCH_ID>.SHORT/Unicycler/<sample>.edge_reads.csv
#   - Labels (opt):  selections/<BATCH_ID>/short_labels/<sample>.short_labels.tsv
#
# Outputs (per-sample):
#   - selections/<BATCH_ID>/features/edges/<sample>.edge_features.tsv
#   - selections/<BATCH_ID>/features/nodes/<sample>.node_features.tsv
#
# Outputs (batch-wide):
#   - selections/<BATCH_ID>/features/edges.features.batch.tsv
#   - selections/<BATCH_ID>/features/nodes.features.batch.tsv
#
# Usage:
#   BATCH_ID=batch_001 bash scripts/pack_features_short.sh
#   BATCH_ID=batch_001 SAMPLE_ID=S123 bash scripts/pack_features_short.sh    # one sample

set -euo pipefail

: "${BATCH_ID:?set BATCH_ID=your_batch_id}"
BASE="${BASE:-$PWD}"

U_SHORT="$BASE/batches_being_processed/${BATCH_ID}.SHORT/Unicycler"
LBL_DIR="$BASE/selections/$BATCH_ID/short_labels"

OUT_ROOT="$BASE/selections/$BATCH_ID/features"
OUT_EDGES_DIR="$OUT_ROOT/edges"
OUT_NODES_DIR="$OUT_ROOT/nodes"
LOG_DIR="$BASE/logs"

mkdir -p "$OUT_EDGES_DIR" "$OUT_NODES_DIR" "$LOG_DIR"

log(){ echo "[$(date -Iseconds)] $*" | tee -a "$LOG_DIR/pack_features_short.${BATCH_ID}.log"; }

[[ -d "$U_SHORT" ]] || { echo "[e] SHORT Unicycler dir not found: $U_SHORT" >&2; exit 2; }

# Discover pruned GFAs (deterministic path)
mapfile -t PRUNED < <(find "$U_SHORT" -maxdepth 1 -type f -name '*.assembly.pruned.gfa.gz' | LC_ALL=C sort)
((${#PRUNED[@]})) || { echo "[e] no *.assembly.pruned.gfa.gz under $U_SHORT — run prune_short_graph.sh first" >&2; exit 2; }

declare -a WORK
if [[ -n "${SAMPLE_ID:-}" ]]; then
  gz="$U_SHORT/${SAMPLE_ID}.assembly.pruned.gfa.gz"
  [[ -s "$gz" ]] || { echo "[e] SAMPLE_ID=$SAMPLE_ID pruned GFA not found at $gz" >&2; exit 3; }
  WORK=("$gz")
else
  WORK=("${PRUNED[@]}")
fi

edge_csv_path_for_sample() {
  # Deterministic: always Unicycler/<sample>.edge_reads.csv
  local sample="$1"
  echo "$U_SHORT/${sample}.edge_reads.csv"
}

log "batch=$BATCH_ID  samples=${#WORK[@]}"

process_one() {
  local pruned_gz="$1"
  local sample; sample="$(basename "$pruned_gz")"; sample="${sample%%.assembly.pruned.gfa.gz}"

  local edge_csv; edge_csv="$(edge_csv_path_for_sample "$sample")"
  local labels_tsv="$LBL_DIR/${sample}.short_labels.tsv"

  local out_edges="$OUT_EDGES_DIR/${sample}.edge_features.tsv"
  local out_nodes="$OUT_NODES_DIR/${sample}.node_features.tsv"

  if [[ ! -s "$edge_csv" ]]; then
    echo "[w] edge support missing for $sample at $edge_csv — proceeding with zeros." \
      | tee -a "$LOG_DIR/pack_features_short.${BATCH_ID}.log"
    edge_csv=""  # cause parser to emit zeros
  fi

  python3 - "$pruned_gz" "${edge_csv:-}" "$labels_tsv" "$out_edges" "$out_nodes" << 'PY'
import sys, os, gzip, csv
from collections import defaultdict

pruned_gz, edge_csv, labels_tsv, out_edges, out_nodes = sys.argv[1:6]

def parse_labels(path):
    """
    Accepts either:
      - headerless: short_contig, length, label, reason, cov_chr, cov_plasmid, cov_unlabeled
      - headered:   ... must include 'contig' and 'label' columns (any order)
    Returns dict: contig -> label
    """
    lab = {}
    if not (path and os.path.exists(path) and os.path.getsize(path) > 0):
        return lab
    with open(path) as f:
        first = f.readline()
        if not first:
            return lab
        cols = first.rstrip("\n").split("\t")
        lowered = [c.strip().lower() for c in cols]
        if "label" in lowered:  # headered
            idx_name = lowered.index("contig") if "contig" in lowered else 0
            idx_label = lowered.index("label")
            for ln in f:
                if not ln.strip(): continue
                parts = ln.rstrip("\n").split("\t")
                if len(parts) <= max(idx_name, idx_label): continue
                lab[parts[idx_name]] = parts[idx_label]
        else:  # headerless
            parts = first.rstrip("\n").split("\t")
            if len(parts) >= 3:
                lab[parts[0]] = parts[2]
            for ln in f:
                if not ln.strip(): continue
                p = ln.rstrip("\n").split("\t")
                if len(p) >= 3:
                    lab[p[0]] = p[2]
    return lab

def parse_edge_support(path):
    """
    CSV with columns: contig_u,contig_v,short_pair_count,long_read_count,total_support
    Returns dict[(u,v_sorted)] = (short, long, total)
    """
    sup = {}
    if not (path and os.path.exists(path) and os.path.getsize(path) > 0):
        return sup
    with open(path) as f:
        r = csv.DictReader(f)  # comma-delimited
        for row in r:
            u = (row.get("contig_u","") or "").strip()
            v = (row.get("contig_v","") or "").strip()
            if not u or not v: continue
            key = (u,v) if u < v else (v,u)
            try: sp = int(row.get("short_pair_count", "0"))
            except: sp = 0
            try: lr = int(row.get("long_read_count", "0"))
            except: lr = 0
            sup[key] = (sp, lr, sp+lr)
    return sup

def seg_len_from_fields(fields):
    name, seq = fields[1], fields[2]
    if seq != "*" and len(seq) > 0:
        return name, len(seq)
    ln = None
    for t in fields[3:]:
        if t.startswith("LN:i:"):
            try: ln = int(t.split(":")[2])
            except: pass
    return name, (0 if ln is None else ln)

# Parse pruned graph
nodes = set()
length = {}
edges = []
deg = defaultdict(int)

with gzip.open(pruned_gz, "rt") as fh:
    for ln in fh:
        if not ln.strip(): continue
        t = ln.rstrip("\n").split("\t")
        if t[0] == "S":
            n, L = seg_len_from_fields(t)
            nodes.add(n)
            length[n] = L
        elif t[0] == "L" and len(t) >= 6:
            u, uo, v, vo = t[1], t[2], t[3], t[4]
            if u == v: continue
            edges.append((u, uo, v, vo))
            deg[u] += 1
            deg[v] += 1

# ensure isolated nodes have degree 0 recorded
for n in nodes: _ = deg[n]

edge_sup = parse_edge_support(edge_csv)
labels  = parse_labels(labels_tsv)

sample = os.path.basename(pruned_gz).split(".assembly.pruned.gfa.gz")[0]

# Edge features
os.makedirs(os.path.dirname(out_edges), exist_ok=True)
with open(out_edges, "w", newline="") as fe:
    w = csv.writer(fe, delimiter="\t")
    w.writerow([
        "sample","u","v",
        "short_pair_count","long_read_count","total_support",
        "u_len","v_len","u_deg","v_deg",
        "min_len","max_len","sum_len","min_deg","max_deg","is_tip_edge"
    ])
    seen = set()
    for (u, uo, v, vo) in edges:
        key = (u,v) if u < v else (v,u)
        if key in seen: continue
        seen.add(key)
        sp, lr, ts = edge_sup.get(key, (0,0,0))
        ul = length.get(u,0); vl = length.get(v,0)
        ud = deg.get(u,0);   vd = deg.get(v,0)
        is_tip = 1 if (ud==1 or vd==1) else 0
        w.writerow([sample, u, v, sp, lr, ts, ul, vl, ud, vd,
                    min(ul,vl), max(ul,vl), ul+vl, min(ud,vd), max(ud,vd), is_tip])

# Node features
os.makedirs(os.path.dirname(out_nodes), exist_ok=True)
with open(out_nodes, "w", newline="") as fn:
    w = csv.writer(fn, delimiter="\t")
    w.writerow(["sample","node","len","degree","label"])
    for n in sorted(nodes):
        w.writerow([sample, n, length.get(n,0), deg.get(n,0), labels.get(n,"")])
PY

  echo "[ok] features: $sample"
}

for g in "${WORK[@]}"; do
  process_one "$g"
done

# Batch aggregates
edges_batch="$OUT_ROOT/edges.features.batch.tsv"
nodes_batch="$OUT_ROOT/nodes.features.batch.tsv"
rm -f "$edges_batch" "$nodes_batch"

first=1
for f in "$OUT_EDGES_DIR"/*.edge_features.tsv; do
  [[ -s "$f" ]] || continue
  if (( first )); then cat "$f" > "$edges_batch"; first=0; else awk 'NR>1' "$f" >> "$edges_batch"; fi
done

first=1
for f in "$OUT_NODES_DIR"/*.node_features.tsv; do
  [[ -s "$f" ]] || continue
  if (( first )); then cat "$f" > "$nodes_batch"; first=0; else awk 'NR>1' "$f" >> "$nodes_batch"; fi
done

log "[OK] packed features for $BATCH_ID → $OUT_ROOT"
