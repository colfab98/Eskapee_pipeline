#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"
TRUTH="$PROJECT_ROOT/truth"
IDX="$TRUTH/indices"
LOGS="$PROJECT_ROOT/logs"
TMP="$TRUTH/tmp_chr"

CSV="$TRUTH/reference_genomes.csv"
ACC="$TRUTH/ref_accessions.txt"
FA="$TRUTH/ref_chromosomes.fa"
MMI="$IDX/chromosomes.mmi"

PHIX_FA="$TRUTH/ref_phix.fa"
PHIX_MMI="$IDX/phix.mmi"

mkdir -p "$TRUTH" "$IDX" "$LOGS" "$TMP"

log(){ echo "[$(date +'%F %T')] $*" | tee -a "$LOGS/build_chromosome_refs.log"; }

need_rebuild_chr() {
  [[ ! -s "$FA" || ! -s "$MMI" ]] && return 0
  [[ "$CSV" -nt "$FA" || "$CSV" -nt "$MMI" ]] && return 0
  if [[ -s "$ACC" ]]; then
    [[ "$ACC" -nt "$FA" || "$ACC" -nt "$MMI" ]] && return 0
  fi
  return 1
}

need_rebuild_phix() {
  # Rebuild PhiX if either FASTA or index missing
  [[ ! -s "$PHIX_FA" || ! -s "$PHIX_MMI" ]] && return 0 || return 1
}

ensure_minimap2(){
  if command -v minimap2 >/dev/null 2>&1; then return 0; fi
  log "minimap2 not found; attempting project-local env bootstrap"

  ENV_DIR="$PROJECT_ROOT/envs/label_env"
  TGZ="$PROJECT_ROOT/label_env.tar.gz"

  if [[ -x "$ENV_DIR/bin/minimap2" ]]; then
    export PATH="$ENV_DIR/bin:$PATH"
    log "using existing $ENV_DIR"
    return 0
  fi

  if [[ -s "$TGZ" ]]; then
    log "unpacking $TGZ -> $ENV_DIR"
    mkdir -p "$(dirname "$ENV_DIR")"
    rm -rf "$ENV_DIR"
    mkdir -p "$ENV_DIR"
    tar -xzf "$TGZ" -C "$ENV_DIR" --strip-components=0
    if [[ -x "$ENV_DIR/bin/conda-unpack" ]]; then "$ENV_DIR/bin/conda-unpack" || true; fi
    export PATH="$ENV_DIR/bin:$PATH"
    command -v minimap2 >/dev/null 2>&1 || { log "ERROR: minimap2 still missing after unpack"; exit 1; }
    log "using unpacked env $ENV_DIR"
    return 0
  fi

  if command -v mamba >/dev/null 2>&1; then PM="mamba"; else PM="conda"; fi
  log "creating live env with $PM (minimap2+python) under $ENV_DIR"
  mkdir -p "$(dirname "$ENV_DIR")"
  $PM create -y -p "$ENV_DIR" -c bioconda -c conda-forge minimap2 python >>"$LOGS/build_chromosome_refs.log" 2>&1
  export PATH="$ENV_DIR/bin:$PATH"
  command -v minimap2 >/dev/null 2>&1 || { log "ERROR: minimap2 still missing after $PM create"; exit 1; }
  log "using live env $ENV_DIR"
}

have_datasets(){ command -v datasets >/dev/null 2>&1; }
have_efetch(){ command -v efetch   >/dev/null 2>&1; }

ensure_fetch_tool(){
  if have_datasets; then echo "datasets"; return 0; fi
  if have_efetch;   then echo "efetch";   return 0; fi
  echo ""
}

derive_accessions_if_needed(){
  if [[ -s "$ACC" ]]; then
    log "ref_accessions.txt found ($(wc -l < "$ACC") accessions)"
    return 0
  fi
  [[ -s "$CSV" ]] || { log "ERROR: $CSV missing. Place the PlASgraph2 CSV under truth/ and re-run."; exit 2; }

  log "ref_accessions.txt missing; deriving from $CSV"
  if head -n1 "$CSV" | grep -Eq '^(NC_|NZ_)'; then
    # headerless: column1 is accession
    cut -d, -f1 "$CSV" | awk 'NF' | awk '!seen[$0]++' > "$ACC"
  else
    awk -F, '
      NR==1{
        acc=-1
        for(i=1;i<=NF;i++){
          t=tolower($i)
          if(t=="accession" || t ~ /(^|_)accession($|_)/) acc=i
        }
        if(acc<0){ print "ERROR: no accession column in header: "$0 > "/dev/stderr"; exit 1 }
        next
      }
      {
        gsub(/"/,"")
        a=$acc; gsub(/^[ \t]+|[ \t]+$/,"",a)
        if(a!="") print a
      }' "$CSV" | awk '!seen[$0]++' > "$ACC"
  fi
  log "Wrote $ACC (count: $(wc -l < "$ACC"))"
}

fetch_accession_fna(){
  local acc="$1" out="$2" tool="$3"
  if [[ "$tool" == "datasets" ]]; then
    local zip="$TMP/${acc}.zip"
    rm -f "$zip"
    if datasets download genome accession "$acc" --include genome --filename "$zip" >>"$LOGS/build_chromosome_refs.log" 2>&1; then
      unzip -p "$zip" 'ncbi_dataset/data/*/*.fna' > "$out" 2>>"$LOGS/build_chromosome_refs.log" || true
      [[ -s "$out" ]] && return 0
      log "WARN: unzip produced no .fna for $acc; falling back to efetch (if available)"
      rm -f "$out"
    fi
  fi
  if have_efetch; then
    efetch -db nuccore -id "$acc" -format fasta > "$out" 2>>"$LOGS/build_chromosome_refs.log"
    [[ -s "$out" ]] && return 0
  fi
  return 1
}

build_phix(){
  # Build PhiX FASTA + index if missing/outdated (v9-necessary)
  if need_rebuild_phix; then
    log "Building PhiX (NC_001422.1)"
    # Prefer datasets first to keep parity with overall preference; efetch as fallback
    if have_datasets; then
      local zip="$TMP/phix.zip"
      rm -f "$zip"
      datasets download genome accession NC_001422.1 --include genome --filename "$zip" >>"$LOGS/build_chromosome_refs.log" 2>&1
      unzip -p "$zip" 'ncbi_dataset/data/*/*.fna' > "$PHIX_FA" 2>>"$LOGS/build_chromosome_refs.log" || true
      if [[ ! -s "$PHIX_FA" ]] && have_efetch; then
        efetch -db nuccore -id "NC_001422.1" -format fasta > "$PHIX_FA" 2>>"$LOGS/build_chromosome_refs.log"
      fi
    elif have_efetch; then
      efetch -db nuccore -id "NC_001422.1" -format fasta > "$PHIX_FA" 2>>"$LOGS/build_chromosome_refs.log"
    else
      log "ERROR: neither datasets nor efetch available to fetch PhiX"; exit 5
    fi
    [[ -s "$PHIX_FA" ]] || { log "ERROR: PhiX FASTA is empty"; exit 5; }
    log "Building minimap2 index for PhiX: $PHIX_MMI"
    minimap2 -d "$PHIX_MMI" "$PHIX_FA" >>"$LOGS/build_chromosome_refs.log" 2>&1
  else
    log "PhiX up-to-date; skipping"
  fi
}

main(){
  log "Starting chromosome reference build (+ PhiX)"
  [[ -s "$CSV" ]] || { log "ERROR: $CSV missing. Place it under truth/ and re-run."; exit 2; }

  derive_accessions_if_needed

  ensure_minimap2
  local tool; tool="$(ensure_fetch_tool)"
  [[ -n "$tool" ]] || { log "ERROR: need either 'datasets' or 'efetch' in PATH to fetch accessions"; exit 3; }

  if need_rebuild_chr; then
    log "Rebuilding chromosomes from accessions"
    log "Accessions: $(wc -l < "$ACC")"
    local DL_DIR="$TMP/fna"; mkdir -p "$DL_DIR"

    while read -r acc; do
      [[ -z "${acc:-}" ]] && continue
      local out="$DL_DIR/${acc}.fna"
      if [[ -s "$out" ]]; then
        log "skip (exists): $acc"; continue
      fi
      log "fetch: $acc via $tool"
      if ! fetch_accession_fna "$acc" "$out" "$tool"; then
        log "ERROR: failed to fetch $acc"; exit 4
      fi
    done < "$ACC"

    log "Concatenating into $FA"
    : > "$FA.tmp"
    LC_ALL=C ls -1 "$DL_DIR"/*.fna 2>/dev/null | sort -V | while read -r f; do cat "$f" >> "$FA.tmp"; done
    mv -f "$FA.tmp" "$FA"
    sha256sum "$FA" | tee "$LOGS/ref_chromosomes.fa.sha256" >/dev/null

    log "Building minimap2 index: $MMI"
    minimap2 -d "$MMI" "$FA" >>"$LOGS/build_chromosome_refs.log" 2>&1
  else
    log "Chromosome refs up-to-date; skipping rebuild"
  fi

  build_phix

  log "DONE"
  log "FASTA (chromosomes): $FA"
  log "MMI   (chromosomes): $MMI"
  log "FASTA (PhiX):        $PHIX_FA"
  log "MMI   (PhiX):        $PHIX_MMI"
}

main "$@"
