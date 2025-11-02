- main_orchestrator.sh
./scripts/main_orchestrator.sh test 000
./scripts/main_orchestrator.sh train 001
./scripts/main_orchestrator.sh

- env_prewarm.sh
./scripts/env_prewarm.sh

- build_chromosome_refs.sh
./scripts/build_chromosome_refs.sh

- build_plasmid_refs.sh
./scripts/build_plasmid_refs.sh

- prep_label_env_and_transfer.sh
./scripts/prep_label_env_and_transfer.sh

- pick_samples.sh
BATCH_ID=test_000 ./scripts/pick_samples.sh 

- fetch_reads.sh
BATCH_ID=test_000 ./scripts/fetch_reads.sh

- map_srrs.sh
MODE=test BATCH_ID=test_000 ./scripts/map_srrs.sh

- build_run_to_files.sh
BATCH_ID=test_000 ./scripts/build_run_to_files.sh

- build_sample_run_audit.sh
BATCH_ID=test_000 ./scripts/build_sample_run_audit.sh

- build_samplesheet.sh
BATCH_ID=test_000 ./scripts/build_samplesheet.sh

- stage_for_sv3000.sh
BATCH_ID=test_000 ./scripts/stage_for_sv3000.sh

- stage_verify_offline.sh
BATCH_ID=test_000 ./scripts/stage_verify_offline.sh

- run_bacass_hybrid.sbatch
BATCH_ID=test_000 sbatch scripts/run_bacass_hybrid.sbatch

- run_bacass_short.sbatch
BATCH_ID=test_000 sbatch scripts/run_bacass_short.sbatch

- truth_from_hybrid.sh
BATCH_ID=test_000 ./scripts/truth_from_hybrid.sh

- transfer_labels_to_short.sh
BATCH_ID=test_000 ./scripts/transfer_labels_to_short.sh

- prune_short_graph.sh
BATCH_ID=test_000 ./scripts/prune_short_graph.sh

- pack_features_short.sh
BATCH_ID=test_000 ./scripts/pack_features_short.sh

- edge_read_support_short.sh
BATCH_ID=test_000 ./scripts/edge_read_support_short.sh

- build_plasgraph2_manifest.sh
BATCH_ID=test_000 ./scripts/build_plasgraph2_manifest.sh

- export_batch_to_vdi.sh
MODE=train BATCH_ID=train_000 ./scripts/export_batch_to_vdi.sh

- cleanup_batch.sh
BATCH_ID=test_000 ./scripts/cleanup_batch.sh

- combine_batches.sh
./scripts/combine_batches.sh test
./scripts/combine_batches.sh train
