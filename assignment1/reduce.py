import collections
import csv
import os
import sys


def reduce_partitions(partitions_dir: str, output_path: str) -> None:
    """Read partition files produced by split.py and count pair co-occurrences.

    Each partition file part_k.csv contains lines of the form p1,p2 where
    p1 < p2 and p1 % N == k.  Pass 1 (split.py) guarantees partition
    independence: every occurrence of a given pair (p1, p2) is confined to
    exactly one partition file.  This means we can count each partition with
    a single Counter, flush the counts to the output file, then discard the
    Counter before loading the next partition — at most one partition's worth
    of data is in memory at any time.

    Output is a single CSV with header product_1,product_2,num_baskets.
    Partitions are processed in sorted filename order for deterministic output.
    """
    partition_files = sorted(
        f for f in os.listdir(partitions_dir) if f.startswith("part_") and f.endswith(".csv")
    )

    partitions_processed = 0
    total_distinct_pairs = 0
    total_occurrences = 0

    with open(output_path, "w", newline="") as out_fh:
        writer = csv.writer(out_fh)
        writer.writerow(["product_1", "product_2", "num_baskets"])

        for filename in partition_files:
            partition_path = os.path.join(partitions_dir, filename)

            counts: collections.Counter = collections.Counter()
            with open(partition_path, "r") as in_fh:
                for line in in_fh:
                    line = line.strip()
                    if not line:
                        continue
                    p1, p2 = line.split(",")
                    counts[(int(p1), int(p2))] += 1

            for (p1, p2), count in counts.items():
                writer.writerow([p1, p2, count])
                total_occurrences += count

            total_distinct_pairs += len(counts)
            partitions_processed += 1
            del counts

    print(
        f"Done. partitions_processed={partitions_processed}, "
        f"distinct_pairs={total_distinct_pairs}, "
        f"total_occurrences={total_occurrences}"
    )


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python reduce.py <partitions_dir> <output.csv>")
        sys.exit(1)

    reduce_partitions(sys.argv[1], sys.argv[2])