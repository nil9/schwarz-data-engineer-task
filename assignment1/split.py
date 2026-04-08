import gzip
import itertools
import os
import sys


def generate_pairs(products):
    """Yield all sorted (p1, p2) pairs where p1 < p2."""
    for p1, p2 in itertools.combinations(sorted(products), 2):
        yield p1, p2


def flush_basket(products, partition_files, num_partitions):
    """Write all pairs from a single basket to the appropriate partition files.

    Returns the number of pairs emitted.
    """
    pairs_emitted = 0
    for p1, p2 in generate_pairs(products):
        k = p1 % num_partitions
        partition_files[k].write(f"{p1},{p2}\n")
        pairs_emitted += 1
    return pairs_emitted


def split(input_path: str, output_dir: str, num_partitions: int) -> None:
    """Stream a gzipped CSV of (basket_id, product_id) rows and write all
    product-pair co-occurrences to N partition files.

    Partitions the pair space by min(p1, p2) % num_partitions so that each
    pair is written to exactly one partition file.  This ensures partition
    independence — pass 2 can process each partition with no cross-partition
    merging.

    Rows for the same basket must be consecutive in the input.  At most one
    basket is held in memory at a time.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Open all partition files upfront and keep them open
    partition_files = [
        open(os.path.join(output_dir, f"part_{k}.csv"), "w")
        for k in range(num_partitions)
    ]

    baskets_processed = 0
    baskets_skipped = 0
    total_pairs = 0

    try:
        current_basket_id = None
        current_products = []

        with gzip.open(input_path, "rt") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                basket_id, product_id = line.split(",")
                product_id = int(product_id)

                if basket_id != current_basket_id:
                    # Basket boundary detected — flush the completed basket
                    if current_basket_id is not None:
                        if len(current_products) >= 2:
                            total_pairs += flush_basket(
                                current_products, partition_files, num_partitions
                            )
                            baskets_processed += 1
                        else:
                            baskets_skipped += 1

                    current_basket_id = basket_id
                    current_products = [product_id]
                else:
                    current_products.append(product_id)

            # Flush the final basket
            if current_basket_id is not None:
                if len(current_products) >= 2:
                    total_pairs += flush_basket(
                        current_products, partition_files, num_partitions
                    )
                    baskets_processed += 1
                else:
                    baskets_skipped += 1

    finally:
        for fh in partition_files:
            fh.close()

    print(
        f"Done. baskets_processed={baskets_processed}, "
        f"pairs_emitted={total_pairs}, "
        f"baskets_skipped={baskets_skipped}"
    )


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python split.py <input.csv.gz> <output_dir> <num_partitions>")
        sys.exit(1)

    split(sys.argv[1], sys.argv[2], int(sys.argv[3]))