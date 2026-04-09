# Assignment 1 — Product Pair Co-occurrences

## Overview


This program counts the frequency of item pairs purchased across various transaction baskets. The input file follows a (basket_id, product_id) format, where each row represents an item purchased within a specific basket.

The output file displays (product_1, product_2, num_baskets), representing the total number of unique baskets in which a specific pair of items appeared together.

The implementation utilizes a two-pass architecture:

Split Pass: Generates all possible combinations of items purchased within the same basket.

Reduce Pass: Aggregates and counts the occurrences of these pairs across the entire dataset.

The algorithm is specifically designed for scalability, ensuring that the process executes successfully even when the input and output datasets are too large to fit in memory.


## How to run

All commands are run from the project root (`work/`) with the virtual environment activated.

**1. Generate test data.** Creates `data_1.csv.gz` (~2 MB, ~197K rows) and moves it into the `input/` folder.
```bash
python generate_data.py --scale 1
mv data_1.csv.gz input/
```

**2. Pass 1 — split into partitions.** Streams the input file and routes pair occurrences into 4 partition files under `/tmp/parts_1/`.
```bash
python assignment1/split.py input/data_1.csv.gz /tmp/parts_1 4
```

Expected summary: Done. baskets_processed=52392, pairs_emitted=263762, baskets_skipped=13139

**3. Pass 2 — count co-occurrences.** Reads each partition file, counts the distinct pairs within it, and writes the final result to `/tmp/cooc_1.csv`.
```bash
python assignment1/reduce.py /tmp/parts_1 /tmp/cooc_1.csv
```

Expected summary: Done. partitions_processed=4, distinct_pairs=32628, total_occurrences=263762

Note that `total_occurrences` from pass 2 equals `pairs_emitted` from pass 1 (263,762), confirming that every pair written by the splitter was counted by the reducer.

**4. Run tests.**
```bash
pytest tests/ -v
```

Expected: `4 passed`.


## Splitting strategy

Three strategies were considered for partitioning the workload:

**Option A — Split by row ranges.** Read the input in chunks of N rows, compute pair counts for each chunk into a local counter, then merge all the chunk counters at the end. The problem is that the final merged counter has to hold every distinct pair ever seen across all chunks — which is exactly the data structure we said doesn't fit in memory. The merge step defeats the purpose.

**Option B — Hash-partition by basket_id.** Route entire baskets to partitions using `hash(basket_id) % N`. This doesn't work for co-occurrence either: two baskets in different partitions can contain the same product pair, so each pair can appear in multiple partition outputs. Pass 2 would still need to merge counts across partitions.

**Option C — Partition the pair space (chosen).** Each pair has a single canonical form `(p1, p2)` with `p1 < p2`, and the partition function `p1 % N` routes every occurrence of the same pair to the same partition file. Pass 2 can count each partition independently, and the per-partition outputs can simply be concatenated. There is no global merge step, so no data structure ever has to hold every distinct pair at once. (See "Why the result is correct" for the partition independence argument.)

## Why the result is correct


In this solution, product_ids in a basket are sorted before creating the item combination. This guarantees exactly one canonical form for each pair. For Example, {2,7} could generate two forms (2,7) and (7,2). Since the items are sorted before pair generation, the canonical form of {2,7} always generates only (2,7). After this, each pair is then partitioned **p1%N** where p1 is the smallest item, and N is the number of partitions. This ensures partition independence: since every pair with the same p1 and with a deterministic partition function always ends up in the same partition. Pass 2 can then independently count the number of pairs in each partition and concatenate across the partition to provide the global count. There is no need to merge counts across partitions. 

## Why it fits in memory

**Pass 1 (`split.py`)** streams the gzipped input line by line. At any moment the only data in memory is the current basket being assembled (at most ~5 product IDs) and the `num_partitions` open output file handles. The footprint is a small constant that does not grow with the input size.

**Pass 2 (`reduce.py`)** processes one partition file at a time. For each partition it builds a `Counter` keyed on `(p1, p2)`, writes the counts to the output CSV in append mode, and then explicitly releases the `Counter` before moving to the next partition. At most one partition's worth of distinct pairs is held in memory at any moment — never the full result set. The output file is a write-only sink, not loaded into memory.

The per-partition memory footprint is bounded by `total_distinct_pairs / num_partitions`, so increasing `num_partitions` directly reduces memory usage without affecting correctness. This is the tunable knob that lets the algorithm fit any memory budget.

## Productionizing

A few changes would be needed to run this pipeline reliably in production:

- **Object storage** — In production, data would be read from S3, GCS, or Azure Blob instead of local files. Spark supports this natively via `s3a://`, `gs://`, or `abfss://`.
- **Orchestration** — The pipeline should be scheduled using tools like Airflow or Dagster, with separate steps for data preparation and aggregation.
- **Idempotency** — Re-running the job on the same input should always produce identical results, enabling safe retries without duplicating data.
- **Parallelization** — Since partitions are independent, processing can be parallelized across workers to improve performance.
- **Skew handling** — Real-world data may be uneven (e.g., popular products). Partitioning strategies may need adjustment to avoid hotspots and large partitions.
- **Monitoring** — Track metrics like row counts, runtime, and partition sizes, and alert on anomalies (e.g., unusually large partitions).

## Tests

Tests are located in `tests/test_assignment1.py` and validate the full pipeline end-to-end (`split.py` → `reduce.py`) using pytest's `tmp_path` fixture for isolation.

Run the tests from the project root:
```bash
pytest tests/ -v
```

**Test coverage:**

- **`test_golden_three_baskets`** — validates the implementation against the example provided in the assignment PDF.
- **`test_empty_input`** — verifies behavior with an empty input file (output contains only the header row).
- **`test_all_singletons`** — ensures no output is produced when all baskets contain only a single product.
- **`test_single_basket_four_products`** — checks correct pair generation and counting for a basket with multiple items.
