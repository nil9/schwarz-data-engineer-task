"""Incremental deduplicating ingest job — PySpark Structured Streaming.

Cross-batch deduplication strategy
------------------------------------
PySpark's built-in dropDuplicates() on a streaming DataFrame only deduplicates
within the current micro-batch (or within a watermark window when used with
dropDuplicatesWithinWatermark).  It has no memory of records written in
*previous* batches, so restarting the stream or processing a file that
contains rows already ingested would silently produce duplicates in the target.

We solve this with foreachBatch + anti-join:

  1. dropDuplicates(["basket_id", "product_id"]) cleans up within the
     current micro-batch.
  2. If the Parquet target already exists we read it back as a static
     DataFrame and anti-join the micro-batch against it, keeping only rows
     whose (basket_id, product_id) key has never been written before.
  3. The survivors are appended to the Parquet target.

Because the seen-set is the Parquet target itself (always up to date after
each write) this approach is correct across restarts: even if the stream
checkpoint is deleted and re-started from scratch, the anti-join will still
filter out every record that reached the target in a prior run.

The trade-off is an extra Parquet read per micro-batch.  For the scale of
this assignment that is negligible; at larger scale a persistent key store
(e.g. Delta Lake MERGE, or an external bloom filter) would be preferable.
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


SCHEMA = StructType([
    StructField("basket_id",  StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
])


def build_spark(app_name: str = "assignment2-ingest") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def make_foreach_batch(output_dir: str, spark: SparkSession):
    """Return a foreachBatch handler closed over output_dir and spark."""

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        # ── 1. Intra-batch deduplication ──────────────────────────────────
        deduped = batch_df.dropDuplicates(["basket_id", "product_id"])

        # ── 2. Anti-join against the existing target (cross-batch dedup) ──
        # Try to read the existing target; treat any exception as "no target
        # yet" (covers: directory missing, empty dir, not a Parquet dataset).
        try:
            already_written = spark.read.parquet(output_dir).select(
                "basket_id", "product_id"
            )
            if already_written.limit(1).count() == 0:
                raise ValueError("empty target")
            new_records = deduped.join(
                already_written,
                on=["basket_id", "product_id"],
                how="left_anti",
            )
        except Exception:
            new_records = deduped

        # ── 3. Cache before double-action (count + write) ─────────────────
        new_records.cache()

        count = new_records.count()
        print(f"[batch {batch_id}] new records to write: {count:,}")

        if count > 0:
            (
                new_records
                .write
                .mode("append")
                .parquet(output_dir)
            )

        new_records.unpersist()

    return process_batch


def ingest(input_dir: str, output_dir: str, checkpoint_dir: str) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ── Streaming source ───────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("csv")
        .schema(SCHEMA)
        .option("header", "false")
        # Spark detects gzip from the .csv.gz extension automatically
        .option("pathGlobFilter", "*.csv.gz")
        .option("recursiveFileLookup", "false")
        # One file per micro-batch — essential for exercising the cross-batch
        # anti-join path; without this all files collapse into one batch
        .option("maxFilesPerTrigger", "1")
        .load(input_dir)
    )

    # ── Streaming query ────────────────────────────────────────────────────
    query = (
        raw_stream.writeStream
        .foreachBatch(make_foreach_batch(output_dir, spark))
        .option("checkpointLocation", checkpoint_dir)
        # Process all files available right now, then stop — batch-like mode
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    # ── Final summary ──────────────────────────────────────────────────────
    try:
        total = spark.read.parquet(output_dir).count()
        print(f"\n── Ingest complete ───────────────────────────────")
        print(f"   Total records in target Parquet: {total:,}")
        print(f"─────────────────────────────────────────────────")
    except Exception:
        print("\nIngest complete — no records were written.")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python ingest.py <input_dir> <output_dir> <checkpoint_dir>")
        sys.exit(1)

    ingest(
        input_dir=sys.argv[1],
        output_dir=sys.argv[2],
        checkpoint_dir=sys.argv[3],
    )