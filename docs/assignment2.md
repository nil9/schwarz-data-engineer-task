# Assignment 2 — Incremental Deduplicating Ingest

## Overview

This program (Ingest) incrementally processes input data streams of gzipped CSV files containing (basket_id, product_id) in batches and writes a deduplicated Parquet dataset to an output directory. Deduplication is performed both within individual files and across batches.

## How to run

All commands are run from the project root (`work/`) with the virtual
environment activated.

**1. Generate test data.** Creates 3 batch files plus one extras file of
cross-file duplicates in `/tmp/assignment2_input/`. Around 11% of the
total rows are intentional duplicates (within-file and cross-file).
```bash
python assignment2/generate_test_files.py /tmp/assignment2_input 3
```

Expected summary (numbers vary slightly per run because the random sample is unseeded):

Files created          : 4
Total rows on disk     : 660,237
Within-file duplicates : 9,862
Cross-file duplicates  : 60,021
Total injected dupes   : 69,883

**2. Clean previous output and checkpoint directories.** Necessary on first
runs or when you want to start from a fresh state.
```bash
rm -rf /tmp/assignment2_output /tmp/assignment2_checkpoint
```

**3. Run the ingestion job.** Reads files incrementally from the input
directory, deduplicates within and across batches, and writes Parquet
to the output directory.
```bash
python assignment2/ingest.py /tmp/assignment2_input /tmp/assignment2_output /tmp/assignment2_checkpoint
```

Expected output (first run):

[batch 0] new records to write: 196,661
[batch 1] new records to write: 196,453
[batch 2] new records to write: 197,240
[batch 3] new records to write: 0
── Ingest complete ───────────────────────────────
Total records in target Parquet: 590,354
─────────────────────────────────────────────────

## Why Parquet

Parquet is a columnar storage format, which makes analytical queries significantly faster. It is also natively supported by Spark, so no additional libraries are required and no extra parsing is needed when reading the data.

For production environments, it is often better to use Delta Lake, as it provides features such as schema evolution and time-based data versioning (snapshots). However, using Delta Lake requires additional non-standard libraries, which are not allowed for this assignment.

## Deduplication strategy

Deduplication is handled at two levels: within a batch and across batches. While `dropDuplicates` removes duplicates within a batch, it does not handle duplicates across batches reliably.

This implementation uses `foreachBatch` with an anti-join. For each batch, it:

1. Removes duplicates within the batch
2. Filters out records already present in the target Parquet dataset
3. Appends only new records

The Parquet output acts as the source of truth, making the pipeline restart-safe even if checkpoints are lost.

The trade-off is that each batch reads the existing dataset. This is acceptable for this scale, but in production, a more efficient approach like Delta Lake `MERGE` would be preferred.

## Why this fits the problem

This solution uses PySpark Structured Streaming features to implement an incremental ingestion pattern:

- **File-based streaming source** — Spark tracks processed files via the checkpoint directory, ensuring that only new files are ingested without any custom bookkeeping.
- **`availableNow=True`** — processes all files currently available in the input directory and then exits cleanly, enabling batch-like execution suitable for scheduled jobs.
- **`maxFilesPerTrigger=1`** — ensures that only one input file is processed per micro-batch. This preserves the incremental behavior of the pipeline by treating each file as a separate batch. As a result, cross-batch deduplication (via anti-join against the existing Parquet dataset) is applied consistently after each file. Without this setting, Spark could process multiple files in a single batch, which would reduce the effectiveness of incremental deduplication, as all data would be handled together rather than step by step.

Together, these settings effectively turn Structured Streaming into a scheduled incremental batch pipeline, which is ideal for ingesting files that arrive over time.

## Productionizing

To make this pipeline production-ready, a few practical improvements would be needed:

- **Use cloud storage instead of local files** — In a real setup, input and output data would live in S3, GCS, or Azure Blob rather than local folders. Spark works seamlessly with these storage systems using paths like `s3a://`, `gs://`, or `abfss://`.
- **Add orchestration** — The job should be scheduled using tools like Airflow or Dagster. Since the pipeline uses `availableNow=True`, each run processes newly arrived files and then stops, which fits naturally with scheduled execution.
- **Improve deduplication for scale** — The current anti-join approach works well for small datasets, but it requires reading the full output each time. At larger scale, using Delta Lake with `MERGE INTO` would be more efficient, as it avoids scanning the entire dataset for every batch.
- **Partition the output data** — Storing the Parquet data partitioned (for example, by ingest date) helps both query performance and reduces the amount of data that needs to be processed in each batch.
- **Add monitoring and alerts** — It's important to track things like how many files were processed, how many duplicates were removed, and how the dataset is growing. Sudden changes (like a spike in duplicates) can indicate upstream issues and should trigger alerts.
- **Include data quality checks** — Before writing data, basic validations (such as schema checks or row counts) should be applied. If something looks wrong, the batch should be rejected instead of silently writing bad data.