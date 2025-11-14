#!/usr/bin/env bash

set -euo pipefail
shopt -s nullglob

ts(){ date +"[%Y-%m-%d %H:%M:%S]"; }
need_cmd(){ command -v "$1" >/dev/null 2>&1 || { echo "$(ts) ERROR: '$1' not found."; exit 9; }; }
is_gzip(){ if command -v gzip >/dev/null 2>&1; then gzip -t "$1" >/dev/null 2>&1 && return 0 || return 1; fi
           dd if="$1" bs=2 count=1 2>/dev/null | od -An -tx1 | grep -qi "1f 8b" && return 0 || return 1; }

BASE="${BASE:-$PWD}"
PLSDB_DIR="${PLSDB_DIR:-$BASE/truth/PLSDB}"
BUNDLE_DIR="${BUNDLE_DIR:-$BASE/truth/PLSDB_bundle}"
SOURCE_CONF="${SOURCE_CONF:-$BASE/truth/PLSDB_source.conf}"
PLSDB_URL="${PLSDB_URL:-https://ccb-microbe.cs.uni-saarland.de/plsdb2025/download_fasta}"
MINIMAP2="${MINIMAP2:-$BASE/envs/label_env/bin/minimap2}"

OUT_FASTA="${OUT_FASTA:-$BASE/truth/ref_plasmids.fa}"
OUT_MMI="${OUT_MMI:-$BASE/truth/indices/plsdb.mmi}"               
OUT_PART_PREFIX="${OUT_PART_PREFIX:-$BASE/truth/indices/plsdb.part}"
OUT_ARGS="${OUT_ARGS:-$BASE/truth/indices/plsdb.args}"

OUT_MMI_COMPAT="$BASE/truth/indices/plasmids.mmi"
OUT_ARGS_COMPAT="$BASE/truth/indices/plasmids.args"

SHARD_DIR="${SHARD_DIR:-$BASE/truth/PLSDB_shards}"
TARGET_SHARD_BYTES=${TARGET_SHARD_BYTES:-524288000}
FORCE="${FORCE:-0}"

OUT_DIR_FASTA="$(dirname "$OUT_FASTA")"
OUT_DIR_MMI="$(dirname "$OUT_MMI")"
MANIFEST_DIR="$BASE/truth/.manifests"
MANIFEST_CUR="$MANIFEST_DIR/plsdb_inputs.manifest"
MANIFEST_NEW="$MANIFEST_DIR/plsdb_inputs.manifest.new"

mkdir -p "$PLSDB_DIR" "$BUNDLE_DIR" "$OUT_DIR_FASTA" "$OUT_DIR_MMI" "$MANIFEST_DIR" "$SHARD_DIR"

sha256_of(){ command -v sha256sum >/dev/null 2>&1 && sha256sum "$1" | awk '{print $1}' || shasum -a 256 "$1" | awk '{print $1}'; }
download_file(){ local url="$1" dest="$2" tmp; tmp="$(mktemp --tmpdir "$(basename "$dest").XXXXXX")"
  if command -v curl >/dev/null 2>&1; then curl -L --fail --retry 3 --retry-delay 2 -o "$tmp" "$url"
  else need_cmd wget; wget -O "$tmp" "$url"; fi; mv -f -- "$tmp" "$dest"; }
read_conf(){ CONF_URL=""; CONF_SHA256=""; [[ -s "$SOURCE_CONF" ]] || return 0
  while IFS='=' read -r k v; do case "$k" in PLSDB_URL) CONF_URL="${v//$'\r'/}";; PLSDB_SHA256) CONF_SHA256="${v//$'\r'/}";; esac
  done < <(grep -E '^(PLSDB_URL|PLSDB_SHA256)=' "$SOURCE_CONF" || true); }
write_conf(){ echo "PLSDB_URL=$1" > "$SOURCE_CONF"; echo "PLSDB_SHA256=$2" >> "$SOURCE_CONF"; echo "PINNED_AT=$(date -u +%FT%TZ)" >> "$SOURCE_CONF"; }

collect_plsdb_files(){ local root="$1"
  find "$root" -type f \( -iname "*.fa" -o -iname "*.fa.gz" -o -iname "*.fna" -o -iname "*.fna.gz" -o -iname "*.fasta" -o -iname "*.fasta.gz" \) -print
  find "$root" -type f -size +1M ! -name "*.fa" ! -name "*.fa.gz" ! -name "*.fna" ! -name "*.fna.gz" ! -name "*.fasta" ! -name "*.fasta.gz" -print; }

echo "$(ts) PLSDB build: $PLSDB_DIR"
mapfile -t have_now < <(collect_plsdb_files "$PLSDB_DIR")
if (( ${#have_now[@]} == 0 )); then
  echo "$(ts) PLSDB empty; checking bundle"
  mapfile -t bundle < <(collect_plsdb_files "$BUNDLE_DIR")
  if (( ${#bundle[@]} == 0 )); then
    echo "$(ts) No bundle; auto-download"
    read_conf
    url="${CONF_URL:-$PLSDB_URL}"
    raw="$(basename "${url%%\?*}")"; [[ -n "$raw" ]] || raw="plsdb_snapshot"
    dest="$BUNDLE_DIR/$raw"; mkdir -p "$BUNDLE_DIR"
    download_file "$url" "$dest"
    [[ -s "$dest" ]] || { echo "$(ts) ERROR: empty download"; exit 2; }
    norm="$dest"; if is_gzip "$dest"; then mv -f "$dest" "$BUNDLE_DIR/plsdb_snapshot.fa.gz"; norm="$BUNDLE_DIR/plsdb_snapshot.fa.gz"; else mv -f "$dest" "$BUNDLE_DIR/plsdb_snapshot.fa"; norm="$BUNDLE_DIR/plsdb_snapshot.fa"; fi
    got="$(sha256_of "$norm")"; [[ -n "${CONF_SHA256:-}" && -n "$got" && "${got,,}" != "${CONF_SHA256,,}" ]] && { echo "$(ts) ERROR: checksum mismatch"; exit 2; }
    [[ -z "${CONF_SHA256:-}" ]] && write_conf "$url" "${got:-UNAVAILABLE}"
    bundle=("$norm")
  fi
  echo "$(ts) Hydrating bundle -> $PLSDB_DIR"
  mkdir -p "$PLSDB_DIR"; for f in "${bundle[@]}"; do cp -f -- "$f" "$PLSDB_DIR/"; done
else
  echo "$(ts) Using existing PLSDB files (${#have_now[@]})"
fi

mapfile -t PLSDB_FILES < <(collect_plsdb_files "$PLSDB_DIR" | sort)
(( ${#PLSDB_FILES[@]} > 0 )) || { echo "$(ts) ERROR: no PLSDB files"; exit 3; }
find "$PLSDB_DIR" -type f -printf "%p\t%T@\t%s\n" | sort -k1,1 > "$MANIFEST_NEW"
need_concat=1
if [[ -s "$MANIFEST_CUR" ]] && cmp -s "$MANIFEST_CUR" "$MANIFEST_NEW"; then
  [[ -s "$OUT_FASTA" && "$FORCE" != "1" ]] && { echo "$(ts) Inputs unchanged — reusing FASTA"; need_concat=0; }
fi
if (( need_concat )); then
  tmp_fa="$(mktemp --tmpdir "$(basename "$OUT_FASTA").XXXXXX")"; : > "$tmp_fa"
  echo "$(ts) Concatenating -> $OUT_FASTA"
  for f in "${PLSDB_FILES[@]}"; do if is_gzip "$f"; then zcat -- "$f" >> "$tmp_fa"; else cat -- "$f" >> "$tmp_fa"; fi; done
  [[ -s "$tmp_fa" ]] || { echo "$(ts) ERROR: empty concat"; rm -f "$tmp_fa"; exit 5; }
  mv -f -- "$tmp_fa" "$OUT_FASTA"; mv -f -- "$MANIFEST_NEW" "$MANIFEST_CUR"
else rm -f "$MANIFEST_NEW"; fi

need_cmd "$MINIMAP2"

build_single_index(){ local tmp; tmp="$(mktemp --tmpdir "$(basename "$OUT_MMI").XXXXXX")"
  echo "$(ts) Building index -> $OUT_MMI"
  if "$MINIMAP2" -d "$tmp" "$OUT_FASTA"; then mv -f -- "$tmp" "$OUT_MMI"; rm -f "$OUT_ARGS" "$OUT_PART_PREFIX".*.mmi 2>/dev/null || true; echo "$(ts) Wrote $OUT_MMI"; return 0; fi
  rm -f "$tmp"; return 1; }

shard_and_index(){
  echo "$(ts) Single index failed — sharding"
  rm -f "$OUT_ARGS" "$OUT_PART_PREFIX".*.mmi 2>/dev/null || true
  mkdir -p "$SHARD_DIR"
  awk -v T="$TARGET_SHARD_BYTES" -v P="$SHARD_DIR/plsdb.part" '
    BEGIN{sh=1; bytes=0; fn=sprintf("%s%02d.fa",P,sh)}
    /^>/ { if (bytes>0 && bytes>=T){ sh++; bytes=0; fn=sprintf("%s%02d.fa",P,sh)} }
    { print >> fn; bytes += length($0)+1 }
  ' "$OUT_FASTA"
  : > "$OUT_ARGS"
  for fa in "$SHARD_DIR"/plsdb.part*.fa; do
    part="${fa##*/}"; part="${part%.fa}"
    mmi="$OUT_PART_PREFIX.${part#plsdb.part}.mmi"
    echo "$(ts)  - indexing ${fa##*/} -> ${mmi##*/}"
    "$MINIMAP2" -d "$mmi" "$fa"
    echo "$mmi" >> "$OUT_ARGS"
  done
  tr '\n' ' ' < "$OUT_ARGS" | sed 's/ *$//' > "$OUT_ARGS.tmp" && mv -f "$OUT_ARGS.tmp" "$OUT_ARGS"
  echo "$(ts) Shards ready. Args: $OUT_ARGS"
}

build_single_index || shard_and_index

[[ -s "$OUT_MMI" ]] && ln -sf "$(basename "$OUT_MMI")" "$OUT_MMI_COMPAT"
[[ -s "$OUT_ARGS" ]] && cp -f "$OUT_ARGS" "$OUT_ARGS_COMPAT"

echo "$(ts) DONE — PLSDB ready"
if [[ -s "$OUT_MMI" ]]; then echo "  Index: $OUT_MMI (compat: $OUT_MMI_COMPAT)"; else echo "  Shards: $(cat "$OUT_ARGS") (compat: $OUT_ARGS_COMPAT)"; fi
