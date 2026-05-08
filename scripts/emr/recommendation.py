import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, array, array_remove, array_compact, slice

BATCH_DATE = os.environ.get("BATCH_DATE")
if not BATCH_DATE:
    print("ERROR: BATCH_DATE environment variable is required (format: YYYYMMDD)")
    sys.exit(1)

S3_PROCESSED_BUCKET = os.environ.get("S3_PROCESSED_BUCKET", "lifesync-processed")
S3_CURATED_BUCKET = os.environ.get("S3_CURATED_BUCKET", "lifesync-curated")

date_formatted = f"{BATCH_DATE[:4]}-{BATCH_DATE[4:6]}-{BATCH_DATE[6:8]}"

spark = SparkSession.builder \
    .appName("recommendation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[recommendation] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")

customer360_path = f"s3://{S3_CURATED_BUCKET}/customer360/dt={date_formatted}/"
print(f"[recommendation] Reading customer360 from {customer360_path}")
df = spark.read.parquet(customer360_path)

print("[recommendation] Applying rule-based recommendation logic")

df = df.withColumn(
    "rec_securities",
    when(
        (col("invest_total") == 0) & (col("income_grade") == "HIGH"),
        lit("securities")
    ).otherwise(lit(None).cast("string"))
)

df = df.withColumn(
    "rec_insurance",
    when(
        col("insurance_premium") == 0,
        lit("insurance")
    ).otherwise(lit(None).cast("string"))
)

df = df.withColumn(
    "rec_healthcare",
    when(
        col("health_score") < 60,
        lit("healthcare")
    ).otherwise(lit(None).cast("string"))
)

df = df.withColumn(
    "rec_wearable",
    when(
        (col("wearable_flag") == "N") & (col("wearable_flag").isNotNull()),
        lit("wearable")
    ).otherwise(lit(None).cast("string"))
)

print("[recommendation] Building recommendation list (max 3 items)")

df = df.withColumn(
    "rec_array_raw",
    array(
        col("rec_securities"),
        col("rec_insurance"),
        col("rec_healthcare"),
        col("rec_wearable")
    )
)

df = df.withColumn(
    "rec_array_filtered",
    F.expr("filter(rec_array_raw, x -> x is not null)")
)

df = df.withColumn(
    "recommended_products",
    F.slice(col("rec_array_filtered"), 1, 3)
)

df = df.withColumn(
    "recommendation_count",
    F.size(col("recommended_products"))
)

recommendation = df.select(
    col("global_customer_id"),
    col("income_grade"),
    col("invest_total"),
    col("insurance_premium"),
    col("health_score"),
    col("wearable_flag"),
    col("recommended_products"),
    col("recommendation_count"),
    col("dt")
)

output_path = f"s3://{S3_CURATED_BUCKET}/recommendation/dt={date_formatted}/"
print(f"[recommendation] Writing output to {output_path}")

recommendation.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[recommendation] Job completed successfully. Output: {output_path}")
spark.stop()
