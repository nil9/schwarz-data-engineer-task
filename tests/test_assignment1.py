import csv
import gzip

import pytest

from assignment1 import split
from assignment1.reduce import reduce_partitions


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_input(tmp_path, rows):
    """Write (basket_id, product_id) rows to a gzipped CSV; return the path."""
    path = tmp_path / "input.csv.gz"
    with gzip.open(path, "wt") as f:
        for basket_id, product_id in rows:
            f.write(f"{basket_id},{product_id}\n")
    return str(path)


def read_output(output_path):
    """Return a dict {(p1, p2): count} from the reduce output CSV."""
    counts = {}
    with open(output_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (int(row["product_1"]), int(row["product_2"]))
            counts[key] = int(row["num_baskets"])
    return counts


def run_pipeline(tmp_path, rows, num_partitions=2):
    """Run split → reduce_partitions and return the parsed output dict."""
    input_path = make_input(tmp_path, rows)
    partitions_dir = str(tmp_path / "partitions")
    output_path = str(tmp_path / "output.csv")

    split.split(input_path, partitions_dir, num_partitions)
    reduce_partitions(partitions_dir, output_path)

    return read_output(output_path)


# ---------------------------------------------------------------------------
# Golden test (3-basket example from the task PDF)
# ---------------------------------------------------------------------------

def test_golden_three_baskets(tmp_path):
    """
    basket 123 → {1, 2}       pairs: (1,2)
    basket 456 → {1, 2, 3}    pairs: (1,2), (1,3), (2,3)
    basket 789 → {1}           skipped — fewer than 2 products

    Expected co-occurrence counts:
        (1, 2) → 2
        (1, 3) → 1
        (2, 3) → 1
    """
    rows = [
        ("123", "1"), ("123", "2"),
        ("456", "1"), ("456", "2"), ("456", "3"),
        ("789", "1"),
    ]
    counts = run_pipeline(tmp_path, rows, num_partitions=2)

    assert counts == {
        (1, 2): 2,
        (1, 3): 1,
        (2, 3): 1,
    }


# ---------------------------------------------------------------------------
# Additional tests
# ---------------------------------------------------------------------------

def test_empty_input(tmp_path):
    """An empty gzipped CSV must produce an output file with only the header."""
    counts = run_pipeline(tmp_path, rows=[], num_partitions=2)
    assert counts == {}

    output_path = str(tmp_path / "output.csv")
    with open(output_path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    assert header == ["product_1", "product_2", "num_baskets"]


def test_all_singletons(tmp_path):
    """Baskets with a single product each must produce no pairs at all."""
    rows = [
        ("1", "10"),
        ("2", "20"),
        ("3", "30"),
    ]
    counts = run_pipeline(tmp_path, rows, num_partitions=2)
    assert counts == {}


def test_single_basket_four_products(tmp_path):
    """
    One basket with products {1, 2, 3, 4} must produce exactly C(4,2) = 6
    pairs, each with count 1.
    """
    rows = [("99", str(p)) for p in [1, 2, 3, 4]]
    counts = run_pipeline(tmp_path, rows, num_partitions=2)

    expected = {
        (1, 2): 1,
        (1, 3): 1,
        (1, 4): 1,
        (2, 3): 1,
        (2, 4): 1,
        (3, 4): 1,
    }
    assert len(counts) == 6
    assert counts == expected