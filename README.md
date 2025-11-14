# Eskapee Assembly & Plasmid Truth Pipeline (v9)

This pipeline implements the dataset creation methodology described in the thesis **"Extending plASgraph2 with GRU-Enhanced Edge-Gated GGNNs"**. It creates training datasets for PlasGraph2 using **ESKAPEE** pathogens via a "Hybrid-to-Short" truth-transfer method:

1.  **Hybrid Assembly:** Uses Short + Long reads (Unicycler) to create a high-quality ground truth graph.
2.  **Label Generation:** Classifies hybrid contigs as Chromosome or Plasmid based on circularity, length, and homology (PLSDB/Reference Genomes).
3.  **Label Transfer:** Projects truth labels onto a fragmented **Short-read only** assembly graph.
4.  **Feature Extraction:** Generates node features and **Edge Read Support Counts** for machine learning training.

-----

## Prerequisites & Setup

Before running the pipeline, you must set up the project structure and input data on your **VDI** (orchestrator node).

### 1\. Create Project Directories

Initialize the required folder structure for scripts, batch selections, and logs.

```bash
mkdir -p scripts selections logs
```

### 2\. Upload Input Data (`all_samples.csv`)

The pipeline requires the `all_samples.csv` file (from the original plASgraph2 repository) to select and process samples. Upload this file to the root of your project directory on the VDI.


### 3\. Apply the QUAST Patch (Critical)

To prevent pipeline crashes due to missing optional QUAST outputs (e.g., when no misassemblies are found), you must **manually update** the faulty Nextflow script.

The correct, crash-proof code is located in this repository at:
`patches/nf-core-bacass-2.3.1/quast/main.nf`


### 4\. Reference genomes

The script `scripts/build_chromosome_refs.sh` expects the PlASgraph2
reference metadata file to be available as:

  truth/reference_genomes.csv

Copy `reference_genomes.csv` from the original PlASgraph2 dataset into
`truth/reference_genomes.csv` before running `build_chromosome_refs.sh`.


## Pipeline Orchestration

### Automatic Execution

**`main_orchestrator.sh`**
This script runs the entire sequence automatically (Preparation → Remote Assembly → Post-Processing → Export).

```bash
# Run everything (all discovered batches)
./scripts/main_orchestrator.sh

# Run a specific batch
./scripts/main_orchestrator.sh test 000
```

-----

## Manual Script Execution

If you need to run specific steps individually (e.g., for debugging), use the commands below. **Ensure you are in the project root directory.**

### 1\. Environment & References (One-Time Prep)

  * **Prewarm Environment:**
    ```bash
    ./scripts/env_prewarm.sh
    ```
  * **Build Chromosome Refs:**
    ```bash
    ./scripts/build_chromosome_refs.sh
    ```
  * **Build Plasmid Refs:**
    ```bash
    ./scripts/build_plasmid_refs.sh
    ```
  * **Prepare Label Env & Transfer:**
    ```bash
    ./scripts/prep_label_env_and_transfer.sh
    ```

### 2\. Sample Selection & Metadata (Local)

  * **Pick Samples:**
    ```bash
    BATCH_ID=test_000 ./scripts/pick_samples.sh
    ```
  * **Fetch Reads:**
    ```bash
    BATCH_ID=test_000 ./scripts/fetch_reads.sh
    ```
  * **Map SRRs:**
    ```bash
    MODE=test BATCH_ID=test_000 ./scripts/map_srrs.sh
    ```
  * **Build Run-to-Files:**
    ```bash
    BATCH_ID=test_000 ./scripts/build_run_to_files.sh
    ```
  * **Build Sample Audit:**
    ```bash
    BATCH_ID=test_000 ./scripts/build_sample_run_audit.sh
    ```
  * **Build Samplesheet:**
    ```bash
    BATCH_ID=test_000 ./scripts/build_samplesheet.sh
    ```

### 3\. Staging & Remote Assembly

  * **Stage to Cluster:**
    ```bash
    BATCH_ID=test_000 ./scripts/stage_for_sv3000.sh
    ```
  * **Verify Staging:**
    ```bash
    BATCH_ID=test_000 ./scripts/stage_verify_offline.sh
    ```
  * **Submit Hybrid Assembly:**
    ```bash
    BATCH_ID=test_000 sbatch scripts/run_bacass_hybrid.sbatch
    ```
  * **Submit Short Assembly:**
    ```bash
    BATCH_ID=test_000 sbatch scripts/run_bacass_short.sbatch
    ```

### 4\. Post-Processing & Feature Extraction (Remote)

  * **Truth Generation (Hybrid):**
    ```bash
    BATCH_ID=test_000 ./scripts/truth_from_hybrid.sh
    ```
  * **Label Transfer:**
    ```bash
    BATCH_ID=test_000 ./scripts/transfer_labels_to_short.sh
    ```
  * **Prune Short Graph:**
    ```bash
    BATCH_ID=test_000 ./scripts/prune_short_graph.sh
    ```
  * **Pack Features:**
    ```bash
    BATCH_ID=test_000 ./scripts/pack_features_short.sh
    ```
  * **Edge Read Support:**
    ```bash
    BATCH_ID=test_000 ./scripts/edge_read_support_short.sh
    ```
  * **Build Manifest:**
    ```bash
    BATCH_ID=test_000 ./scripts/build_plasgraph2_manifest.sh
    ```

### 5\. Finalization (Local)

  * **Export to VDI:**
    ```bash
    MODE=test BATCH_ID=test_000 ./scripts/export_batch_to_vdi.sh
    ```
  * **Cleanup Remote:**
    ```bash
    BATCH_ID=test_000 ./scripts/cleanup_batch.sh
    ```
  * **Combine Batches:**
    ```bash
    ./scripts/combine_batches.sh test
    ./scripts/combine_batches.sh train
    ```
