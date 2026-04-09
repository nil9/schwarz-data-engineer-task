"""Generate test data files with intentional duplicates for Assignment 2.

Produces <num_files> batch files plus one extras file of cross-file duplicates
in <output_dir>.  If <output_dir> already exists it is wiped first.

Duplicate types injected
------------------------
1. Within-file duplicates  — a sample of rows from batch_0.csv.gz are
   appended to the end of that same file.  Tests that a deduplication pass
   correctly ignores repeated rows inside a single ingestion batch.

2. Cross-file duplicates   — batch_dupes.csv.gz contains ~10 % of all rows
   drawn randomly from all batch files.  Tests that the pipeline correctly
   identifies rows already seen in a previous batch (the incremental /
   "already-processed" scenario).
"""

import gzip
import os
import random
import shutil
import subprocess
import sys
from pathlib import Path

# Path to generate_data.py, assumed to live in the project root (one level up)
GENERATE_DATA = Path(__file__).resolve().parent.parent / "generate_data.py"
SCALE = 1
WITHIN_FILE_DUPE_RATIO = 0.05   # 5 % of batch_0 rows appended again
CROSS_FILE_DUPE_RATIO  = 0.10   # 10 % of all rows written to batch_dupes


def generate_batch(output_dir: Path, index: int) -> Path:
    """Run generate_data.py --scale SCALE and move the result to batch_<i>.csv.gz."""
    subprocess.run(
        [sys.executable, str(GENERATE_DATA), "--scale", str(SCALE)],
        check=True,
    )
    # generate_data.py writes data_<scale>.csv.gz in the current working dir
    generated = Path(f"data_{SCALE}.csv.gz")
    if not generated.exists():
        raise FileNotFoundError(
            f"Expected {generated} after running generate_data.py — not found."
        )
    dest = output_dir / f"batch_{index}.csv.gz"
    shutil.move(str(generated), dest)
    return dest


def read_rows(path: Path) -> list[str]:
    """Return all non-empty lines from a gzipped CSV as raw strings."""
    with gzip.open(path, "rt") as f:
        return [line.rstrip("\n") for line in f if line.strip()]


def write_rows(path: Path, rows: list[str]) -> None:
    """Write a list of raw CSV line strings to a gzipped file.

    Uses CRLF line endings (\\r\\n) to match the output of csv.writer,
    which defaults to CRLF per RFC 4180.  The file is opened with
    newline="" to suppress Python's automatic newline translation.
    """
    with gzip.open(path, "wt", newline="") as f:
        for row in rows:
            f.write(row + "\r\n")


def append_rows(path: Path, rows: list[str]) -> None:
    """Append rows to an existing gzipped CSV.

    Uses CRLF line endings (\\r\\n) to match the output of csv.writer,
    which defaults to CRLF per RFC 4180.  The file is opened with
    newline="" to suppress Python's automatic newline translation.
    """
    with gzip.open(path, "at", newline="") as f:
        for row in rows:
            f.write(row + "\r\n")


def main(output_dir: str, num_files: int) -> None:
    out = Path(output_dir)

    # Wipe output dir if it already exists so reruns are clean
    if out.exists():
        print(f"[warn] {out} already exists — clearing it.")
        shutil.rmtree(out)
    out.mkdir(parents=True)

    batch_paths: list[Path] = []

    # ------------------------------------------------------------------ #
    # 1. Generate num_files batch files                                    #
    # ------------------------------------------------------------------ #
    print(f"Generating {num_files} batch file(s) with scale={SCALE} …")
    for i in range(num_files):
        path = generate_batch(out, i)
        batch_paths.append(path)
        row_count = len(read_rows(path))
        print(f"  batch_{i}.csv.gz  →  {row_count:,} rows")

    # ------------------------------------------------------------------ #
    # 2. Within-file duplicates — append ~5 % of batch_0 back to itself   #
    # ------------------------------------------------------------------ #
    batch_0_rows = read_rows(batch_paths[0])
    k_within = max(1, int(len(batch_0_rows) * WITHIN_FILE_DUPE_RATIO))
    within_dupes = random.sample(batch_0_rows, k_within)
    append_rows(batch_paths[0], within_dupes)
    print(
        f"\nWithin-file duplicates: appended {k_within:,} duplicate row(s) "
        f"to batch_0.csv.gz"
    )

    # ------------------------------------------------------------------ #
    # 3. Cross-file duplicates — batch_dupes.csv.gz                       #
    # ------------------------------------------------------------------ #
    all_rows: list[str] = []
    for p in batch_paths:
        all_rows.extend(read_rows(p))

    k_cross = max(1, int(len(all_rows) * CROSS_FILE_DUPE_RATIO))
    cross_dupes = random.sample(all_rows, k_cross)
    dupes_path = out / "batch_dupes.csv.gz"
    write_rows(dupes_path, cross_dupes)
    print(
        f"Cross-file duplicates:  wrote {k_cross:,} row(s) "
        f"sampled from all batches → batch_dupes.csv.gz"
    )

    # ------------------------------------------------------------------ #
    # 4. Summary                                                           #
    # ------------------------------------------------------------------ #
    total_rows = sum(len(read_rows(p)) for p in batch_paths) + len(cross_dupes)
    total_injected = k_within + k_cross
    files_created = num_files + 1   # batches + dupes file

    print(
        f"\n── Summary ──────────────────────────────────────\n"
        f"  Files created          : {files_created}\n"
        f"  Total rows on disk     : {total_rows:,}  "
        f"(includes injected dupes)\n"
        f"  Within-file duplicates : {k_within:,}  "
        f"(appended to batch_0.csv.gz)\n"
        f"  Cross-file duplicates  : {k_cross:,}  "
        f"(batch_dupes.csv.gz)\n"
        f"  Total injected dupes   : {total_injected:,}\n"
        f"─────────────────────────────────────────────────"
    )


if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python generate_test_files.py <output_dir> [<num_files>]")
        sys.exit(1)

    _output_dir = sys.argv[1]
    _num_files  = int(sys.argv[2]) if len(sys.argv) == 3 else 3

    if _num_files < 1:
        print("Error: num_files must be >= 1")
        sys.exit(1)

    main(_output_dir, _num_files)