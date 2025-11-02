#!/usr/bin/env bash
# Run on VDI0044. Prewarm minimap2+minigraph env and transfer refs+env to sv3000 (offline-safe).
# Idempotent: safe to run multiple times. If env exists but is incomplete, recreate cleanly.
set -euo pipefail

# -----------------------------
# Config / defaults
# -----------------------------
REMOTE="${REMOTE:-fcolanto@sv3000}"   # override: REMOTE=user@host ./prep_label_env_and_transfer.sh
BASE="${BASE:-$PWD}"
PROJ="$(basename "$BASE")"

TRUTH="$BASE/truth"
IDX_DIR="$TRUTH/indices"
ENV_DIR="$BASE/envs/label_env"
LOG_DIR="$BASE/logs"; mkdir -p "$LOG_DIR"
SOLVER_LOG="$LOG_DIR/label_env.solver.$(date +%F_%H%M%S).log"

CHR_MMI="$IDX_DIR/chromosomes.mmi"
PHIX_MMI="$IDX_DIR/phix.mmi"                    # << added: PhiX index (v9-necessary)
PL_MMI_SINGLE="$IDX_DIR/plasmids.mmi"           # single index (optional)
PL_ARGS_PLASMIDS="$IDX_DIR/plasmids.args"       # shard list (optional, space- or newline-separated)
PL_ARGS_PLSDB="$IDX_DIR/plsdb.args"             # alternate shard list name (optional)
PL_MMI_GLOB="$IDX_DIR/plasmids.part."           # prefix for shard .mmi files

# -----------------------------
# Helpers
# -----------------------------
ts(){ date +"[%Y-%m-%d %H:%M:%S]"; }
have_cmd(){ command -v "$1" >/dev/null 2>&1; }

any_exists_glob() {
  shopt -s nullglob
  local arr=( $1 )
  shopt -u nullglob
  (( ${#arr[@]} > 0 ))
}

# Echo a space-separated list of plasmid index .mmi files
resolve_plasmid_indices() {
  if [[ -s "$PL_MMI_SINGLE" ]]; then
    echo "$PL_MMI_SINGLE"; return 0
  fi
  # If an args file exists, return a *space-separated* list (we'll normalize later)
  if [[ -s "$PL_ARGS_PLASMIDS" ]]; then
    tr '\n' ' ' < "$PL_ARGS_PLASMIDS" | sed 's/ *$//'; return 0
  fi
  if [[ -s "$PL_ARGS_PLSDB" ]]; then
    tr '\n' ' ' < "$PL_ARGS_PLSDB" | sed 's/ *$//'; return 0
  fi
  if any_exists_glob "$PL_MMI_GLOB"*.mmi; then
    ls -1 "$PL_MMI_GLOB"*.mmi | tr '\n' ' ' | sed 's/ *$//'; return 0
  fi
  return 1
}

# -----------------------------
# EARLY REMOTE GUARD (short-circuit if remote is already ready)
# Accept either a single plasmids.mmi OR a shard set; require PhiX too.
# -----------------------------
remote_ready_cmd="$(cat <<'BASH'
set -euo pipefail
if [[ ! -x ~/"$PROJ"/envs/label_env/bin/minimap2 || ! -x ~/"$PROJ"/envs/label_env/bin/minigraph ]]; then exit 3; fi
if [[ ! -s ~/"$PROJ"/truth/indices/chromosomes.mmi ]]; then exit 4; fi
if [[ ! -s ~/"$PROJ"/truth/indices/phix.mmi        ]]; then exit 4; fi   # << require PhiX
if [[ -s ~/"$PROJ"/truth/indices/plasmids.mmi ]]; then exit 0; fi
shopt -s nullglob
parts=( ~/"$PROJ"/truth/indices/plasmids.part.*.mmi ~/"$PROJ"/truth/indices/plsdb.part.*.mmi )  # << accept both names
shopt -u nullglob
(( ${#parts[@]} > 0 )) && exit 0
exit 5
BASH
)"
if ssh "$REMOTE" "PROJ='$PROJ' bash -lc '$remote_ready_cmd'"; then
  echo "[ok] Remote already has label_env + indices at ~/$PROJ; nothing to do."
  exit 0
fi

# -----------------------------
# LOCAL sanity: required files
# -----------------------------
[[ -s "$CHR_MMI"  ]] || { echo "[e] Missing: $CHR_MMI"  >&2; exit 1; }
[[ -s "$PHIX_MMI" ]] || { echo "[e] Missing: $PHIX_MMI" >&2; exit 1; }  # << ensure PhiX exists

PL_LIST=""
if ! PL_LIST="$(resolve_plasmid_indices)"; then
  echo "[e] Missing plasmid indices: expected $PL_MMI_SINGLE or shards ($PL_MMI_GLOB*.mmi) or args ($PL_ARGS_PLASMIDS|$PL_ARGS_PLSDB)" >&2
  exit 1
fi

# -----------------------------
# Prewarm tiny env with minimap2 + minigraph
# Policy: if env exists but missing either tool, remove and recreate cleanly.
# -----------------------------
PM=""
if have_cmd mamba; then PM="mamba"
elif have_cmd conda; then PM="conda"
else
  echo "[e] Neither mamba nor conda found in PATH." >&2
  exit 1
fi

need_recreate=false
if [[ -d "$ENV_DIR" ]]; then
  [[ -x "$ENV_DIR/bin/minimap2"  ]] || need_recreate=true
  [[ -x "$ENV_DIR/bin/minigraph" ]] || need_recreate=true
  if $need_recreate; then
    echo "[info] $(ts) removing incomplete env: $ENV_DIR"
    rm -rf "$ENV_DIR"
  fi
fi

if [[ ! -d "$ENV_DIR" ]]; then
  echo "[info] $(ts) creating label_env (minimap2 + minigraph) with $PM"
  mkdir -p "$BASE/envs"
  set +e
  "$PM" create -y -p "$ENV_DIR" -c bioconda -c conda-forge minimap2 minigraph >"$SOLVER_LOG" 2>&1
  rc=$?
  set -e
  if (( rc != 0 )); then
    echo "[e] Failed to create env at $ENV_DIR (see $SOLVER_LOG)" >&2
    tail -n 50 "$SOLVER_LOG" >&2 || true
    exit $rc
  fi
fi

"$ENV_DIR/bin/minimap2"  --version
"$ENV_DIR/bin/minigraph" --version

# -----------------------------
# Prepare remote dirs (match local project name)
# -----------------------------
ssh "$REMOTE" "mkdir -p ~/$PROJ/truth/indices ~/$PROJ/envs/label_env"

# -----------------------------
# Transfer indices
# -----------------------------
echo "[info] $(ts) transferring indices to $REMOTE:~/$PROJ/truth/indices/"
rsync -av --info=progress2 "$CHR_MMI"  "$REMOTE:~/$PROJ/truth/indices/"
rsync -av --info=progress2 "$PHIX_MMI" "$REMOTE:~/$PROJ/truth/indices/"   # << send PhiX

if [[ -s "$PL_MMI_SINGLE" ]]; then
  rsync -av --info=progress2 "$PL_MMI_SINGLE" "$REMOTE:~/$PROJ/truth/indices/"
else
  # Build a newline-separated list for rsync --files-from, regardless of how the args file is formatted.
  tmp_list="$(mktemp)"
  if [[ -s "$PL_ARGS_PLASMIDS" ]]; then
    tr ' \t' '\n' < "$PL_ARGS_PLASMIDS" | sed '/^$/d' > "$tmp_list"
  elif [[ -s "$PL_ARGS_PLSDB" ]]; then
    tr ' \t' '\n' < "$PL_ARGS_PLSDB" | sed '/^$/d' > "$tmp_list"
  else
    # Fall back to enumerated list resolved above
    for f in $PL_LIST; do printf "%s\n" "$f" >> "$tmp_list"; done
  fi
  # << make paths BASE-relative so rsync doesn't mirror absolute roots
  sed -i "s#^$BASE/##" "$tmp_list"
  rsync -av --info=progress2 --files-from="$tmp_list" "$BASE/" "$REMOTE:~/$PROJ/"
  rm -f "$tmp_list"
fi

# Optional: transfer provenance
[[ -s "$TRUTH/PLSDB_source.conf" ]] && rsync -av --info=progress2 "$TRUTH/PLSDB_source.conf" "$REMOTE:~/$PROJ/truth/"

# -----------------------------
# Transfer the prewarmed env
# -----------------------------
echo "[info] $(ts) transferring label_env (minimap2 + minigraph)"
rsync -av --info=progress2 "$ENV_DIR/" "$REMOTE:~/$PROJ/envs/label_env/"

# -----------------------------
# Remote verification (unchanged style)
# -----------------------------
ssh "$REMOTE" "bash -lc '
set -euo pipefail
echo \"[verify] indices in ~/$PROJ/truth/indices:\"
ls -lh ~/$PROJ/truth/indices || true
~/$PROJ/envs/label_env/bin/minimap2  --version
~/$PROJ/envs/label_env/bin/minigraph --version
'"

echo "[OK] Transferred indices and prewarmed env to $REMOTE:~/$PROJ"
