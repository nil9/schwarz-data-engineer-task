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
[Why partition the *pair space* rather than splitting by row ranges
or hashing baskets. What property makes this work.]

## Why the result is correct


In this solution, product_ids in a basket are sorted before creating the item combination. This guarantees exactly one canonical form for each pair. For Example, {2,7} could generate two forms (2,7) and (7,2). Since the items are sorted before pair generation, the canonical form of {2,7} always generates only (2,7). After this, each pair is then partitioned **p1%N** where p1 is the smallest item, and N is the number of partitions. This ensures partition independence: since every pair with the same p1 and with a deterministic partition function always ends up in the same partition. Pass 2 can then independently count the number of pairs in each partition and concatenate across the partition to provide the global count. There is no need to merge counts across partitions. 

## Why it fits in memory
[What's held in memory during pass 1 (one basket at a time).
What's held in memory during pass 2 (one partition's Counter at a time).
Why neither grows with the input size.]

## Productionizing
[What changes when this runs in real life: orchestration, monitoring,
failure recovery, parallelization, handling skew. 4-6 bullet points.]

## Tests
[One sentence per test. The golden test is the most important one to mention.]

Command to run the test : **pytest tests/ -v**
