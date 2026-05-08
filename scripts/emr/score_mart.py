import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, least, greatest

BATCH_DATE = os.environ.get("BATCH_DATE")
if not BATCH_DATE:
    print("ERROR: BATCH_DATE environment variable is required (format: YYYYMMDD)")
    sys.exit(1)

S3_PROCESSED_BUCKET = os.environ.get("S3_PROCESSED_BUCKET", "lifesync-processed")
S3_CURATED_BUCKET = os.environ.get("S3_CURATED_BUCKET", "lifesync-curated")

date_formatted = f"{BATCH_DATE[:4]}-{BATCH_DATE[4:6]}-{BATCH_DATE[6:8]}"

spark = SparkSession.builder \
    .appName("score_mart") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[score_mart] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")

customer360_path = f"s3://{S3_CURATED_BUCKET}/customer360/dt={date_formatted}/"
print(f"[score_mart] Reading customer360 from {customer360_path}")
df = spark.read.parquet(customer360_path)

print("[score_mart] Calculating LifeSync Score components")

df = df.withColumn(
    "score_balance",
    least(lit(15.0), col("latest_balance") / lit(1_000_000.0))
)

df = df.withColumn(
    "score_invest",
    least(lit(10.0), col("invest_total") / lit(5_000_000.0))
)

df = df.withColumn(
    "score_card",
    least(lit(10.0), col("card_total_spend") / lit(2_000_000.0))
)

df = df.withColumn(
    "score_health",
    (col("health_score") - lit(50.0)) / lit(10.0)
)

df = df.withColumn(
    "score_insurance",
    when(col("insurance_premium") > 0, lit(5.0)).otherwise(lit(0.0))
)

df = df.withColumn(
    "raw_score",
    lit(50.0)
    + col("score_balance")
    + col("score_invest")
    + col("score_card")
    + col("score_health")
    + col("score_insurance")
)

df = df.withColumn(
    "lifesync_score",
    greatest(lit(0.0), least(lit(100.0), col("raw_score")))
)

print("[score_mart] Assigning customer_grade")
df = df.withColumn(
    "customer_grade",
    when(col("lifesync_score") >= 75, lit("GOLD"))
    .when(col("lifesync_score") >= 55, lit("SILVER"))
    .otherwise(lit("BRONZE"))
)

score_mart = df.select(
    col("global_customer_id"),
    col("lifesync_score"),
    col("customer_grade"),
    col("score_balance"),
    col("score_invest"),
    col("score_card"),
    col("score_health"),
    col("score_insurance"),
    col("dt")
)

output_path = f"s3://{S3_CURATED_BUCKET}/score_mart/dt={date_formatted}/"
print(f"[score_mart] Writing output to {output_path}")

score_mart.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[score_mart] Job completed successfully. Output: {output_path}")
spark.stop()
