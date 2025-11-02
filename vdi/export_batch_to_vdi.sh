#!/usr/bin/env bash
# v9 export with v8-parity outputs (runs on VDI0044)
# Final on VDI: ~/assemblies/$BATCH_ID/set/*.assembly.gfa.gz + *.gfa.csv (+ edge_reads.csv if present) + eskapee_test_new.csv
set -euo pipefail

: "${BATCH_ID:?set BATCH_ID=your_batch_id}"         # e.g. batch_001
REMOTE="${REMOTE:-fcolanto@sv3000}"                  # ssh target
PROJ="${PROJ:-$(basename "$PWD")}"                   # usually eskapee_assembly_9
MODE="${MODE:-test}"; [[ "$MODE" =~ ^(test|train)$ ]] || { echo "[e] MODE must be train|test"; exit 1; }
DEST_ROOT="${DEST_ROOT:-$HOME/assemblies_${MODE}}"
DEST_DIR="$DEST_ROOT/$BATCH_ID"

mkdir -p "$DEST_DIR"

# ---------------------- Remote build on sv3000 ----------------------
ssh "$REMOTE" BATCH_ID="$BATCH_ID" PROJ="$PROJ" MODE="$MODE" 'bash -s' <<'EOF'
set -euo pipefail
: "${BATCH_ID:?}"; : "${PROJ:?}"

BASE="$HOME/$PROJ"
SEL="$BASE/selections/$BATCH_ID"
LBL_DIR="$SEL/short_labels"

# export_set lives under batches_being_processed/<BATCH_ID>/export_set (v8-compatible location)
E="$BASE/batches_being_processed/$BATCH_ID/export_set"

# Labels dir may be missing if some HYBRID runs failed — warn, don't fail
if [[ ! -d "$LBL_DIR" ]]; then
  echo "[w] labels dir missing: $LBL_DIR — samples without labels will be skipped" >&2
fi

# Find PRUNED SHORT GFAs
mapfile -t GFAS < <(find "$BASE/batches_being_processed" -type f -name '*.assembly.pruned.gfa.gz' \
  | grep -E "/${BATCH_ID}(\.SHORT)?/Unicycler/[^/]+\.assembly\.pruned\.gfa\.gz$" \
  | LC_ALL=C sort)

if [[ ${#GFAS[@]} -eq 0 ]]; then
  echo "[e] no *.assembly.pruned.gfa.gz found for batch '$BATCH_ID' under $BASE/batches_being_processed" >&2
  exit 1
fi

rm -rf "$E"
mkdir -p "$E/set" "$E/meta"

# Helper: synthesize v8-schema gfa.csv from transferred SHORT labels (accept headered or headerless TSV/CSV)
synth_csv() {
  local tsv="$1" out="$2"
  echo "contig,plasmid_score,chrom_score,label,length,chr_coverage,pl_coverage,un_coverage,hybrid_mapsto" > "$out"
  awk -v OFS="," '
    function norm(x){ gsub(/^[[:space:]]+|[[:space:]]+$/,"",x); gsub(/^"|"$/,"",x); x=tolower(x); return x }
    function emit(s,ps,cs,lb,l,cc_bp,pp_bp,uu_bp){ print s,ps,cs,lb,l,cc_bp,pp_bp,uu_bp,lb }
    FNR==1{
      header=0
      for(i=1;i<=NF;i++){
        hx=norm($i)
        if(hx ~ /(contig|short_contig|length|short_len|label|cov_chr|cov_plasmid|cov_unlabeled)/){ header=1 }
      }
      if(header){
        for(i=1;i<=NF;i++){
          h=norm($i); gsub(/[^a-z0-9_]/,"",h)
          if(h=="contig"||h=="sc"||h=="node"||h=="id"||h=="shortcontig") sc=i
          else if(h=="length"||h=="len"||h=="size"||h=="bp"||h=="shortlen") L=i
          else if(h=="label"||h=="hybrid_mapsto") lab=i
          else if(h=="covchr"||h=="chr"||h=="chrcoverage"||h=="c"||h=="chrom_coverage") c=i
          else if(h=="covplasmid"||h=="pl"||h=="plcoverage"||h=="p"||h=="plasmid_coverage") p=i
          else if(h=="covunlabeled"||h=="un"||h=="uncoverage"||h=="u") u=i
        }
        next
      } else {
        # headerless per-sample: 1:contig 2:length 3:label 4:reason 5:cov_chr 6:cov_plasmid 7:cov_unlabeled
        sc=1; L=2; lab=3; c=5; p=6; u=7
        # fall through to process first data row
      }
    }
    {
      s  = (sc ? norm($sc) : ""); if(s=="") next
      l  = (L  ? $L+0 : 0)
      lb = (lab? norm($lab) : "")
      cc0= (c  ? $c+0 : 0)
      pp0= (p  ? $p+0 : 0)

      # fractions -> base pairs (rounded), recompute unlabeled from max to avoid drift
      cc_bp = int(cc0 * l + 0.5)
      pp_bp = int(pp0 * l + 0.5)
      m = (cc_bp > pp_bp ? cc_bp : pp_bp)
      uu_bp = l - m; if (uu_bp < 0) uu_bp = 0

      ps = (lb=="plasmid"?1:(lb=="ambiguous"?1:0))
      cs = (lb=="chromosome"?1:(lb=="ambiguous"?1:0))

      emit(s, ps, cs, lb, l, cc_bp, pp_bp, uu_bp)
    }
  ' FS='[,\t]' "$tsv" >> "$out"
}

exported=0
skipped=0

# Build export_set from PRUNED GFAs + transferred SHORT labels
for gfa in "${GFAS[@]}"; do
  stem="${gfa##*/}"; stem="${stem%%.assembly.pruned.gfa.gz}"  # PRUNED stem (v9)
  out_stem="$stem"                                            # v8 filename stem
  gfa_dir="$(dirname "$gfa")"
  tsv="$LBL_DIR/$out_stem.short_labels.tsv"
  edge_csv="$gfa_dir/$out_stem.edge_reads.csv"

  # Pre-checks: require labels + edge support; skip otherwise
  if [[ ! -s "$tsv" ]]; then
    echo "[w] skipping $out_stem: missing labels TSV ($tsv)" >&2
    ((skipped++)) || true
    continue
  fi
  if [[ ! -s "$edge_csv" ]]; then
    echo "[w] skipping $out_stem: missing edge support ($edge_csv)" >&2
    ((skipped++)) || true
    continue
  fi

  # 1) PRUNED SHORT GFA -> export set/, renamed to v8 name
  cp -f "$gfa" "$E/set/$out_stem.assembly.gfa.gz"

  # 2) v8-schema gfa.csv from transferred labels (fractions -> bp)
  synth_csv "$tsv" "$E/set/$out_stem.gfa.csv"

  # 3) Edge-read support (required)
  cp -f "$edge_csv" "$E/set/$out_stem.edge_reads.csv"

  ((exported++)) || true
done

if [[ "$exported" -eq 0 ]]; then
  echo "[e] no samples were exportable (exported=0, skipped=$skipped). See warnings above." >&2
  exit 1
fi

CSV="$E/eskapee_${MODE}_new.csv"
echo "gfa_gz,gfa_csv,edge_csv,sample_id" > "$CSV"

for g in "$E"/set/*.assembly.gfa.gz; do
  s="$(basename "${g%.assembly.gfa.gz}")"
  gg="set/$s.assembly.gfa.gz"
  cc="set/$s.gfa.csv"
  ee="set/$s.edge_reads.csv"
  echo "$gg,$cc,$ee,$s" >> "$CSV"
done

# Checksums for everything under set/
( cd "$E" && find set -type f -print0 | xargs -0 sha256sum > meta/checksums.sha256 )

# Keep v9 provenance manifest if present
[[ -s "$SEL/plasgraph2_manifest.csv" ]] && cp -f "$SEL/plasgraph2_manifest.csv" "$E/" || true

touch "$E/_OK.export_set"

# Also write markers where cleanup expects them (v9 preferred + v8 legacy)
mkdir -p "$BASE/batches_being_processed/${BATCH_ID}.SHORT"
touch    "$BASE/batches_being_processed/${BATCH_ID}.SHORT/_OK.export_set"
mkdir -p "$BASE/batches_being_processed/$BATCH_ID"
touch    "$BASE/batches_being_processed/$BATCH_ID/_OK.export_set"

echo "[OK] export_set built for batch $BATCH_ID (exported=$exported, skipped=$skipped)" >&2
EOF

# ---------------------- Pull export_set to VDI & verify ----------------------
rsync -av --info=progress2 "$REMOTE:~/$PROJ/batches_being_processed/$BATCH_ID/export_set/" "$DEST_DIR/"

csv_rel="$DEST_DIR/eskapee_${MODE}_new.csv"
[[ -s "$csv_rel" ]] || { echo "[e] missing $csv_rel" >&2; exit 1; }

n_rel=$(( $(wc -l < "$csv_rel") - 1 ))
n_gfa=$(ls -1 "$DEST_DIR"/set/*.assembly.gfa.gz 2>/dev/null | wc -l | tr -d ' ')
n_lbl=$(ls -1 "$DEST_DIR"/set/*.gfa.csv        2>/dev/null | wc -l | tr -d ' ')
n_edge=$(ls -1 "$DEST_DIR"/set/*.edge_reads.csv 2>/dev/null | wc -l | tr -d ' ')
[[ "$n_rel" -gt 0 && "$n_rel" -eq "$n_gfa" && "$n_rel" -eq "$n_lbl" && "$n_rel" -eq "$n_edge" ]] || {
  echo "[e] count mismatch after transfer: rel=$n_rel gfa=$n_gfa csv=$n_lbl edge=$n_edge" >&2; exit 1;
}

# Require every edge_csv in manifest to exist & be non-empty
tail -n +2 "$csv_rel" | while IFS=, read -r _ _ edge_rel _; do
  [[ -n "$edge_rel" && -s "$DEST_DIR/$edge_rel" ]] || {
    echo "[e] listed edge_csv missing or empty: $DEST_DIR/${edge_rel:-<empty>}" >&2
    exit 1
  }
done

# Optional checksum verification
if [[ -s "$DEST_DIR/meta/checksums.sha256" ]]; then
  ( cd "$DEST_DIR" && sha256sum -c meta/checksums.sha256 )
fi

touch "$DEST_DIR/_OK.received"
echo "[OK] Exported $BATCH_ID to $DEST_DIR"
