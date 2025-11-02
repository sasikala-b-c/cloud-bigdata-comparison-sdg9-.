import argparse
import json
import os
import time
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def build_spark(app_name: str = "sdg9-benchmark"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def run_job(spark: SparkSession, input_dir: str, out_dir: str, partitions: int, with_ml: bool):
    start = time.time()

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(os.path.join(input_dir, "events.csv"))
    )

    df = df.withColumn("event_ts", F.to_timestamp("event_ts"))

    # Deduplicate
    dedup = df.dropDuplicates(["user_id", "event_ts", "category"]).repartition(partitions)

    # Dimension table (synthetic)
    dim = spark.createDataFrame(
        [("alpha", 1), ("beta", 2), ("gamma", 3), ("delta", 4), ("epsilon", 5)],
        ["category", "cat_id"],
    )

    joined = dedup.join(dim, on="category", how="left")

    # Window aggregation
    w = Window.partitionBy("user_id").orderBy(F.col("event_ts").cast("long")).rowsBetween(-10, 0)
    features = joined.withColumn("rolling_amt_10", F.sum("amount").over(w))

    # Optional MLlib stage (simple bucketization)
    if with_ml:
        features = features.withColumn("amt_bucket", F.floor(F.col("rolling_amt_10") / 10))

    # Materialize and write
    out_data = os.path.join(out_dir, "output")
    features.write.mode("overwrite").parquet(out_data)

    rows = features.count()
    runtime_s = round(time.time() - start, 3)

    metrics = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "rows": rows,
        "runtime_s": runtime_s,
        "partitions": partitions,
        "with_ml": with_ml,
        "input_dir": input_dir,
        "output_dir": out_data,
        "spark_version": spark.version,
    }

    os.makedirs(out_dir, exist_ok=True)
    metrics_path = os.path.join(out_dir, f"metrics_{int(time.time())}.json")
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print(json.dumps(metrics, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input directory containing events.csv")
    parser.add_argument("--out", required=True, help="Output directory for metrics and data")
    parser.add_argument("--partitions", type=int, default=200)
    parser.add_argument("--with-ml", action="store_true")
    args = parser.parse_args()

    spark = build_spark()
    try:
        run_job(spark, args.input, args.out, args.partitions, args.with_ml)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
