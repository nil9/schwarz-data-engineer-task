# Schwarz IT Data Engineer — Home Task Solution

This repository contains the solution to the Schwarz IT Data Engineer home task,
consisting of two independent assignments based on sales basket data.

- **Assignment 1** — Compute the frequency of product pair co-occurrences across
  baskets, under tight memory constraints where neither the input nor the output
  fits in memory. Implemented as a pure-Python two-pass algorithm.
- **Assignment 2** — Incrementally ingest gzipped CSV files from a directory,
  deduplicate records on `(basket_id, product_id)` both within and across batch
  files, and write the result to a Parquet dataset. Implemented as a PySpark
  Structured Streaming job.

## Directory layout

```
work/
├── assignment1/                  # Assignment 1 — pure Python two-pass algorithm
│   ├── split.py                  #   Pass 1 — stream input, partition the pair space
│   ├── reduce.py                 #   Pass 2 — count pairs per partition, concatenate
│   └── __init__.py               #   Package marker (imported by tests)
├── assignment2/                  # Assignment 2 — PySpark Structured Streaming
│   ├── ingest.py                 #   Main ingest job with intra-batch and cross-batch deduplication
│   └── generate_test_files.py    #   Helper — generate test files with duplicates
├── tests/                        # Pytest tests for Assignment 1
│   └── test_assignment1.py
├── docs/                         # Detailed per-assignment documentation
│   ├── assignment1.md            #   Assignment 1 solution documentation
│   ├── assignment2.md            #   Assignment 2 solution documentation
│   ├── Home Task Data Engineer.pdf   # (Original task description)
│   └── README                    #   (Original task-archive README)
├── input/                        # Input data
│   ├── data_1.csv.gz             #   Scale-1 test dataset
│   └── data_example.csv.gz       #   Small example from the task archive
├── generate_data.py              # Provided data generator (unchanged)
├── conftest.py                   # pytest root marker
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## Setup

Python 3.10+ and Java 17 are required. Java is needed by PySpark for Assignment 2.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Debian/Ubuntu, Java 17 can be installed with:

```bash
sudo apt install openjdk-17-jdk
```

## Quick verification

Once the environment is set up, verify the installation with the Assignment 1
test suite:

```bash
pytest tests/ -v
```

Expected: `4 passed`.

## Documentation

Detailed documentation for each assignment lives in the `docs/` folder:

- **[docs/assignment1.md](docs/assignment1.md)** — algorithm, splitting strategy,
  correctness argument, memory analysis, productionizing notes, and how to run.
- **[docs/assignment2.md](docs/assignment2.md)** — Spark Structured Streaming
  design, deduplication strategy, format choice, productionizing notes, and how
  to run.

Each document includes its own "How to run" section with concrete commands and
expected output, so either assignment can be reproduced independently.