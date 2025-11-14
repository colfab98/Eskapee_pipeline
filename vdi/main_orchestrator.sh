#!/usr/bin/env bash

set -euo pipefail

REMOTE="sv3000"                      
PICK_N=5                             

MODE_ARG="${1:-}"  # train, test, or empty
NUM_ARG="${2:-}"   # 001, 002, or empty

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

LOG_DIR="$ROOT/logs"; mkdir -p "$LOG_DIR"
MAIN_LOG="$LOG_DIR/main_orchestrator.$(date +%F_%H%M%S).log"

SCRIPTS="$ROOT/scripts"
REMOTE_REPO_BASENAME="$(basename "$ROOT")"   # e.g. eskapee_assembly_9

log(){ echo "[$(date -Iseconds)] $*" | tee -a "$MAIN_LOG"; }
x(){ [[ -x "$1" ]]; }
mark(){ : > "$LOG_DIR/.done.$1.stamp"; }
is_marked(){ [[ -e "$LOG_DIR/.done.$1.stamp" ]]; }
run(){ log "[run] $*"; eval "$@"; }

# ---------- remote submit (legacy) ----------
remote_job_submit_and_wait(){
  local batch_id="$1"
  local r="$REMOTE"
  local rbase="$REMOTE_REPO_BASENAME"

  log "[remote] sbatch HYBRID and SHORT for $batch_id on $REMOTE and wait (v9 split)"

  if ! ssh "$r" "test -f \"\$HOME/$rbase/scripts/run_bacass_hybrid.sbatch\" && test -f \"\$HOME/$rbase/scripts/run_bacass_short.sbatch\""; then
    log "[error] missing on $REMOTE: \$HOME/$rbase/scripts/run_bacass_hybrid.sbatch or run_bacass_short.sbatch"
    exit 1
  fi

  _submit_and_wait() {
    local sbatch_file="$1"
    local submit_out jid
    submit_out="$(
      ssh "$r" "bash -lc '
        set -u
        cd \"\$HOME/$rbase\"
        if [[ -x \"\$HOME/miniforge3/bin/conda\" ]]; then CONDA_EXE_PATH=\"\$HOME/miniforge3/bin/conda\"
        elif [[ -x \"\$HOME/miniconda3/bin/conda\" ]]; then CONDA_EXE_PATH=\"\$HOME/miniconda3/bin/conda\"
        elif command -v conda >/dev/null 2>&1; then CONDA_EXE_PATH=\"\$(command -v conda)\"
        else echo \"[error] conda executable not found on \$(hostname)\" >&2; exit 1; fi
        export NXF_CONDA_EXE=\"\$CONDA_EXE_PATH\" CONDA_EXE=\"\$CONDA_EXE_PATH\"
        export PATH=\"\$(dirname \"\$CONDA_EXE_PATH\"):\$HOME/miniforge3/condabin:\$PATH\"
        export NXF_JAVA_HOME=\"\$HOME/$rbase/work/global_conda_envs/java17\"
        export BATCH_ID=\"${batch_id}\"
        sbatch --parsable --export=ALL,NXF_CONDA_EXE,CONDA_EXE,NXF_JAVA_HOME,BATCH_ID \"$sbatch_file\"
      '"
    )"
    log "[remote] submitted $sbatch_file -> $submit_out"
    jid="$(printf "%s" "$submit_out" | tr -d '[:space:]')"
    if [[ -z "$jid" || ! "$jid" =~ ^[0-9]+$ ]]; then
      log "[error] could not parse job id from sbatch output above"
      exit 1
    fi
    ssh "$r" "jid=\"$jid\"; while squeue -h -j \"\$jid\" | grep -q .; do sleep 30; done; sacct -n -X -j \"\$jid\" --format=JobID,State,ExitCode 2>/dev/null | tail -n1" | tee -a "$MAIN_LOG"
  }

  _submit_and_wait "scripts/run_bacass_hybrid.sbatch"
  _submit_and_wait "scripts/run_bacass_short.sbatch"
}

remote_run_script(){
  local batch_id="$1"
  local script_rel="$2"    # e.g. scripts/truth_from_hybrid.sh
  local r="$REMOTE"
  local rbase="$REMOTE_REPO_BASENAME"
  log "[remote] run ${script_rel} for ${batch_id} on $REMOTE"
  ssh "$r" "bash -lc 'set -u; cd \"\$HOME/$rbase\"; BATCH_ID=\"${batch_id}\" \"./${script_rel}\"'"
}

assemblies_present_local(){
  local d="$ROOT/assemblies/$BATCH_ID/set"
  [[ -d "$d" ]] && find "$d" -mindepth 1 -maxdepth 1 -type d | grep -q .
}

assemblies_present_remote(){
  local r="$REMOTE"; local rbase="$REMOTE_REPO_BASENAME"; local b="$BATCH_ID"
  ssh "$r" "bash -lc '
    set -u
    cd \"\$HOME/$rbase\" || exit 1
    exp=0
    if [[ -s \"selections/$b/samplesheet.tsv\" ]]; then
      exp=\$(( \$(wc -l < \"selections/$b/samplesheet.tsv\") - 1 ))
    elif [[ -s \"selections/$b/eskapee5_selection.tsv\" ]]; then
      exp=\$(grep -v \"^#\" \"selections/$b/eskapee5_selection.tsv\" | wc -l)
    fi
    got_h=\$(find batches_being_processed/${b}.HYBRID/Unicycler  -maxdepth 1 -type f -name \"*.assembly.gfa.gz\" 2>/dev/null | wc -l)
    got_s=\$(find batches_being_processed/${b}.SHORT/Unicycler   -maxdepth 1 -type f -name \"*.assembly.gfa.gz\" 2>/dev/null | wc -l)
    if [[ \$exp -gt 0 ]]; then
      test \$got_h -ge \$exp && test \$got_s -ge \$exp
    else
      test \$got_h -gt 0 && test \$got_s -gt 0
    fi
  '"
}

remote_x(){ # usage: remote_x scripts/truth_from_hybrid.sh
  local script_rel="$1"
  local r="$REMOTE"; local rbase="$REMOTE_REPO_BASENAME"
  ssh "$r" "bash -lc 'test -x \"\$HOME/$rbase/$script_rel\"'"
}

assemblies_status_remote(){
  local r="$REMOTE"; local rbase="$REMOTE_REPO_BASENAME"; local b="$BATCH_ID"
  ssh "$r" "bash -lc '
    set -u
    cd \"\$HOME/$rbase\" || { echo none; exit 0; }
    exp=0
    if [[ -s \"selections/$b/samplesheet.tsv\" ]]; then
      exp=\$(( \$(wc -l < \"selections/$b/samplesheet.tsv\") - 1 ))
    elif [[ -s \"selections/$b/eskapee5_selection.tsv\" ]]; then
      exp=\$(grep -v \"^#\" \"selections/$b/eskapee5_selection.tsv\" | wc -l)
    fi
    got_h=\$(find batches_being_processed/${b}.HYBRID/Unicycler  -maxdepth 1 -type f -name \"*.assembly.gfa.gz\" 2>/dev/null | wc -l)
    got_s=\$(find batches_being_processed/${b}.SHORT/Unicycler   -maxdepth 1 -type f -name \"*.assembly.gfa.gz\" 2>/dev/null | wc -l)
    if [[ \$exp -gt 0 ]]; then
      cmp_h=\$(( got_h >= exp ))
      cmp_s=\$(( got_s >= exp ))
    else
      cmp_h=\$(( got_h > 0 ))
      cmp_s=\$(( got_s > 0 ))
    fi
    if   [[ \$cmp_h -eq 1 && \$cmp_s -eq 1 ]]; then echo both
    elif [[ \$cmp_h -eq 1 && \$cmp_s -eq 0 ]]; then echo hybrid_only
    elif [[ \$cmp_h -eq 0 && \$cmp_s -eq 1 ]]; then echo short_only
    else echo none
    fi
  '"
}

remote_job_submit_missing_and_wait(){
  local batch_id="$1"
  local r="$REMOTE"
  local rbase="$REMOTE_REPO_BASENAME"

  if ! ssh "$r" "test -f \"\$HOME/$rbase/scripts/run_bacass_hybrid.sbatch\" && test -f \"\$HOME/$rbase/scripts/run_bacass_short.sbatch\""; then
    log "[error] missing on $REMOTE: \$HOME/$rbase/scripts/run_bacass_hybrid.sbatch or run_bacass_short.sbatch"
    exit 1
  fi

  _submit_and_wait() {
    local sbatch_file="$1"
    local submit_out jid
    submit_out="$(
      ssh "$r" "bash -lc '
        set -u
        cd \"\$HOME/$rbase\"
        if [[ -x \"\$HOME/miniforge3/bin/conda\" ]]; then CONDA_EXE_PATH=\"\$HOME/miniforge3/bin/conda\"
        elif [[ -x \"\$HOME/miniconda3/bin/conda\" ]]; then CONDA_EXE_PATH=\"\$HOME/miniconda3/bin/conda\"
        elif command -v conda >/dev/null 2>&1; then CONDA_EXE_PATH=\"\$(command -v conda)\"
        else echo \"[error] conda executable not found on \$(hostname)\" >&2; exit 1; fi
        export NXF_CONDA_EXE=\"\$CONDA_EXE_PATH\" CONDA_EXE=\"\$CONDA_EXE_PATH\"
        export PATH=\"\$(dirname \"\$CONDA_EXE_PATH\"):\$HOME/miniforge3/condabin:\$PATH\"
        export NXF_JAVA_HOME=\"\$HOME/$rbase/work/global_conda_envs/java17\"
        export BATCH_ID=\"${batch_id}\"
        sbatch --parsable --export=ALL,NXF_CONDA_EXE,CONDA_EXE,NXF_JAVA_HOME,BATCH_ID \"$sbatch_file\"
      '"
    )"
    jid="$(printf "%s" "$submit_out" | tr -d '[:space:]')"
    if [[ -z "$jid" || ! "$jid" =~ ^[0-9]+$ ]]; then
      log "[error] could not parse job id from sbatch output above"
      exit 1
    fi
    log "[remote] submitted $sbatch_file -> $jid"
    ssh "$r" "jid=\"$jid\"; while squeue -h -j \"\$jid\" | grep -q .; do sleep 30; done; sacct -n -X -j \"\$jid\" --format=JobID,State,ExitCode 2>/dev/null | tail -n1" | tee -a "$MAIN_LOG"
  }

  local status
  status="$(assemblies_status_remote || echo none)"
  log "[remote] assembly status for ${batch_id}: ${status}"

  case "$status" in
    both)
      log "[ok] HYBRID and SHORT already complete on $REMOTE — skipping sbatch"
      ;;
    hybrid_only)
      _submit_and_wait "scripts/run_bacass_short.sbatch"
      ;;
    short_only)
      _submit_and_wait "scripts/run_bacass_hybrid.sbatch"
      ;;
    none|*)
      _submit_and_wait "scripts/run_bacass_hybrid.sbatch"
      _submit_and_wait "scripts/run_bacass_short.sbatch"
      ;;
  esac
}

run_once(){
  local name="$1"; local script_path="$2"
  if is_marked "$name"; then log "[ok] once-only '$name' already done"; return 0; fi
  if x "$script_path"; then
    run "$script_path"
    mark "$name"; log "[ok] once-only '$name' completed"
  else
    log "[skip] once-only '$name' not present/executable: $script_path"
  fi
}

discover_batches(){
  [[ -d "$ROOT/selections" ]] || return 0
  find "$ROOT/selections" -mindepth 1 -maxdepth 1 -type d -printf "%f\n" | sort
}

log "== once-only preparation =="
# (Removed) run_once "prewarm_fetchngs"        "$SCRIPTS/prewarm_fetchngs.sh"
run_once "env_prewarm"             "$SCRIPTS/env_prewarm.sh"
# (Removed) run_once "stage_reference_genomes" "$SCRIPTS/stage_reference_genomes.sh"
run_once "build_chromosome_refs"   "$SCRIPTS/build_chromosome_refs.sh"
run_once "build_plasmid_refs"      "$SCRIPTS/build_plasmid_refs.sh"
run_once "prep_label_env_transfer" "$SCRIPTS/prep_label_env_and_transfer.sh"

log "== per-batch execution =="
mapfile -t BATCHES < <(discover_batches || true)

if [[ -n "$MODE_ARG" && -n "$NUM_ARG" ]]; then
    # User provided a specific batch (e.g., "train 001")
    if [[ "$MODE_ARG" != "train" && "$MODE_ARG" != "test" ]]; then
        log "[error] Invalid MODE '$MODE_ARG'. Must be 'train' or 'test'."
        exit 1
    fi
    printf -v BATCH_NUM "%03d" "$((10#$NUM_ARG))"

    SINGLE_BATCH_ID="${MODE_ARG}_${BATCH_NUM}"
    BATCHES=("$SINGLE_BATCH_ID")
    log "[info] filtering to single batch: $SINGLE_BATCH_ID"
elif [[ -n "$MODE_ARG" || -n "$NUM_ARG" ]]; then
    log "[error] Usage: $0 [train|test] [batch_num]"
    log "[error] Both MODE and NUMBER are required to filter, or neither."
    exit 1
fi
if [[ ${#BATCHES[@]} -eq 0 ]]; then
  log "[warn] no batches found under selections/. Nothing to do."
  exit 0
fi

for BATCH_ID in "${BATCHES[@]}"; do
  log "--- batch: $BATCH_ID ---"

  MODE="${BATCH_ID%%_*}"

  if [[ "$BATCH_ID" =~ ^batch_ ]]; then
      MODE="test" # Assume old 'batch_' is 'test'
      log "[info] Detected legacy batch '$BATCH_ID', assuming MODE=test"
  elif [[ "$MODE" != "train" && "$MODE" != "test" ]]; then
      log "[warn] Skipping batch '$BATCH_ID': name does not follow 'train_###', 'test_###', or 'batch_###' format."
      continue
  fi

  BATCH_SEL="$ROOT/selections/$BATCH_ID"
  BATCH_STAGING="$ROOT/staging/$BATCH_ID"
  mkdir -p "$BATCH_SEL" "$BATCH_STAGING" "$ROOT/work/$BATCH_ID" "$ROOT/logs/$BATCH_ID"

  SEL_TSV="$BATCH_SEL/eskapee5_selection.tsv"
  SRR_CSV="$BATCH_SEL/sra_ids_for_download.csv"
  FETCH_SHEET="$BATCH_STAGING/results_fetchngs/samplesheet/samplesheet.csv"
  MAP_CSV="$BATCH_SEL/srr_to_sample_map.csv"
  RUN2FILES="$BATCH_SEL/run_to_files.csv"
  AUDIT_CSV="$BATCH_SEL/sample_run_audit.csv"
  SHEET_TSV="$BATCH_SEL/samplesheet.tsv"

  # 1) pick
  if [[ ! -s "$SEL_TSV" || ! -s "$SRR_CSV" ]]; then
    if x "$SCRIPTS/pick_samples.sh"; then
      run "BATCH_ID='$BATCH_ID' '$SCRIPTS/pick_samples.sh' '$ROOT/all_samples.csv' '$PICK_N' '$MODE'"
  else
      log "[skip] pick: script missing"
    fi
  else
    log "[ok] pick: already present"
  fi

  # 2) fetch
  if [[ ! -s "$FETCH_SHEET" ]]; then
    if x "$SCRIPTS/fetch_reads.sh"; then
      run "BATCH_ID='$BATCH_ID' '$SCRIPTS/fetch_reads.sh'"
    else
      log "[skip] fetch: script missing"
    fi
  else
    log "[ok] fetch: already done"
  fi

  # 3) map
  if [[ ! -s "$MAP_CSV" ]]; then
    if x "$SCRIPTS/map_srrs.sh"; then
      run "MODE='$MODE' BATCH_ID='$BATCH_ID' '$SCRIPTS/map_srrs.sh'"
    else
      log "[skip] map: script missing"
    fi
  else
    log "[ok] map: already done"
  fi

  # 4) run_to_files
  if [[ ! -s "$RUN2FILES" ]]; then
    if x "$SCRIPTS/build_run_to_files.sh"; then
      run "BATCH_ID='$BATCH_ID' '$SCRIPTS/build_run_to_files.sh'"
    else
      log "[skip] run_to_files: script missing"
    fi
  else
    log "[ok] run_to_files: already done"
  fi

  # 5) audit
  if [[ ! -s "$AUDIT_CSV" ]]; then
    if x "$SCRIPTS/build_sample_run_audit.sh"; then
      run "BATCH_ID='$BATCH_ID' '$SCRIPTS/build_sample_run_audit.sh'"
    else
      log "[skip] audit: script missing"
    fi
  else
    log "[ok] audit: already done"
  fi

  # 6) samplesheet
  if [[ ! -s "$SHEET_TSV" ]]; then
    if x "$SCRIPTS/build_samplesheet.sh"; then
      run "BATCH_ID='$BATCH_ID' '$SCRIPTS/build_samplesheet.sh'"
    else
      log "[skip] samplesheet: script missing"
    fi
  else
    log "[ok] samplesheet: already done"
  fi

  # 7) stage to sv3000
  if x "$SCRIPTS/stage_for_sv3000.sh"; then
    run "BATCH_ID='$BATCH_ID' '$SCRIPTS/stage_for_sv3000.sh'"
  else
    log "[skip] stage: script missing"
  fi

  # 8) optional verify
  if x "$SCRIPTS/stage_verify_offline.sh"; then
    run "BATCH_ID='$BATCH_ID' '$SCRIPTS/stage_verify_offline.sh'"
  else
    log "[skip] verify: script missing"
  fi

  # 9) submit remote job(s) & wait — submit ONLY what’s missing [v9-necessary]
  remote_job_submit_missing_and_wait "$BATCH_ID"

  # mark complete only when both sides present
  if [[ "$(assemblies_status_remote || echo none)" == "both" ]]; then
    mark "assembly.${BATCH_ID}"
    log "[ok] assemblies complete for ${BATCH_ID}"
  else
    log "[warn] assemblies not fully complete after submission for ${BATCH_ID}"
  fi

  # ===================== POST-ASSEMBLY: LABELING & EXPORT (v9) =====================

  # 1) truth_from_hybrid.sh (remote; HYBRID labeling)
  if remote_x "scripts/truth_from_hybrid.sh"; then
    if ! is_marked "truth_from_hybrid.$BATCH_ID"; then
      remote_run_script "$BATCH_ID" "scripts/truth_from_hybrid.sh"
      mark "truth_from_hybrid.$BATCH_ID"; log "[ok] truth_from_hybrid completed"
    else
      log "[ok] truth_from_hybrid already done"
    fi
  else
    log "[error] required script missing on remote: scripts/truth_from_hybrid.sh"; exit 2
  fi

  # 2) transfer_labels_to_short.sh (remote; HYBRID → SHORT label transfer)
  if remote_x "scripts/transfer_labels_to_short.sh"; then
    if ! is_marked "transfer_labels_to_short.$BATCH_ID"; then
      remote_run_script "$BATCH_ID" "scripts/transfer_labels_to_short.sh"
      mark "transfer_labels_to_short.$BATCH_ID"; log "[ok] transfer_labels_to_short completed"
    else
      log "[ok] transfer_labels_to_short already done"
    fi
  else
    log "[error] required script missing on remote: scripts/transfer_labels_to_short.sh"; exit 2
  fi

  # 3) prune_short_graph.sh (remote; prune SHORT graph)
  if remote_x "scripts/prune_short_graph.sh"; then
    if ! is_marked "prune_short_graph.$BATCH_ID"; then
      remote_run_script "$BATCH_ID" "scripts/prune_short_graph.sh"
      mark "prune_short_graph.$BATCH_ID"; log "[ok] prune_short_graph completed"
    else
      log "[ok] prune_short_graph already done"
    fi
  else
    log "[error] required script missing on remote: scripts/prune_short_graph.sh"; exit 2
  fi

  # 4) pack_features_short.sh (remote; features on PRUNED SHORT)
  if remote_x "scripts/pack_features_short.sh"; then
    if ! is_marked "pack_features_short.$BATCH_ID"; then
      remote_run_script "$BATCH_ID" "scripts/pack_features_short.sh"
      mark "pack_features_short.$BATCH_ID"; log "[ok] pack_features_short completed"
    else
      log "[ok] pack_features_short already done"
    fi
  else
    log "[error] required script missing on remote: scripts/pack_features_short.sh"; exit 2
  fi

  # 5) edge_read_support_short.sh (remote; edge support on PRUNED SHORT)
  if remote_x "scripts/edge_read_support_short.sh"; then
    if ! is_marked "edge_read_support_short.$BATCH_ID"; then
      remote_run_script "$BATCH_ID" "scripts/edge_read_support_short.sh"
      mark "edge_read_support_short.$BATCH_ID"; log "[ok] edge_read_support_short completed"
    else
      log "[ok] edge_read_support_short already done"
    fi
  else
    log "[error] required script missing on remote: scripts/edge_read_support_short.sh"; exit 2
  fi

  # 6) build_plasgraph2_manifest.sh (remote; optional)
  if remote_x "scripts/build_plasgraph2_manifest.sh"; then
    if ! is_marked "build_plasgraph2_manifest.$BATCH_ID"; then
      remote_run_script "$BATCH_ID" "scripts/build_plasgraph2_manifest.sh"
      mark "build_plasgraph2_manifest.$BATCH_ID"; log "[ok] plasgraph2 manifest built"
    else
      log "[ok] plasgraph2 manifest already built"
    fi
  else
    log "[skip] build_plasgraph2_manifest: script missing"
  fi

  # D) export_batch_to_vdi.sh (local; pulls results from sv3000 to VDI)
  if x "$SCRIPTS/export_batch_to_vdi.sh"; then
    if ! is_marked "export_batch_to_vdi.$BATCH_ID"; then
      run "MODE='$MODE' BATCH_ID='$BATCH_ID' '$SCRIPTS/export_batch_to_vdi.sh'"
      mark "export_batch_to_vdi.$BATCH_ID"; log "[ok] export to VDI completed"
    else
      log "[ok] export to VDI already done"
    fi
  else
    log "[skip] export_batch_to_vdi: script missing"
  fi

  # E) cleanup_batch.sh (local; final tidy-up for this batch)
  if x "$SCRIPTS/cleanup_batch.sh"; then
    if ! is_marked "cleanup_batch.$BATCH_ID"; then
      run "BATCH_ID='$BATCH_ID' '$SCRIPTS/cleanup_batch.sh'"
      mark "cleanup_batch.$BATCH_ID"; log "[ok] cleanup_batch completed"
    else
      log "[ok] cleanup_batch already done"
    fi
  else
    log "[skip] cleanup_batch: script missing"
  fi

  log "--- done: $BATCH_ID ---"
done

log "== ALL COMPLETE =="
