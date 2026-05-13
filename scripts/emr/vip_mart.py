import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when

BATCH_DATE = os.environ.get("BATCH_DATE")
if not BATCH_DATE:
    print("ERROR: BATCH_DATE environment variable is required (format: YYYYMMDD)")
    sys.exit(1)

S3_PROCESSED_BUCKET = os.environ.get("S3_PROCESSED_BUCKET", "lifesync-processed")
S3_CURATED_BUCKET = os.environ.get("S3_CURATED_BUCKET", "lifesync-curated")

date_formatted = f"{BATCH_DATE[:4]}-{BATCH_DATE[4:6]}-{BATCH_DATE[6:8]}"

spark = SparkSession.builder \
    .appName("vip_mart") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[vip_mart] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")

customer360_path = f"s3://{S3_CURATED_BUCKET}/customer360/dt={date_formatted}/"
score_mart_path = f"s3://{S3_CURATED_BUCKET}/score_mart/dt={date_formatted}/"

print(f"[vip_mart] Reading customer360 from {customer360_path}")
df_c360 = spark.read.parquet(customer360_path)

print(f"[vip_mart] Reading score_mart from {score_mart_path}")
df_score = spark.read.parquet(score_mart_path)

print("[vip_mart] Joining customer360 and score_mart")
df = df_c360.join(
    df_score.select(
        "global_id",
        "lifesync_score",
        "customer_grade"
    ),
    on="global_id",
    how="inner"
)

print("[vip_mart] Filtering VIP/GOLD customers with wearable_flag=Y")
vip_df = df.filter(
    (col("customer_grade").isin("VIP", "GOLD")) & (col("wearable_flag") == "Y")
)

print("[vip_mart] Computing preferred subsidiary based on highest spend contribution")
vip_df = vip_df.withColumn(
    "total_asset",
    col("latest_balance") + col("invest_total")
)

vip_df = vip_df.withColumn(
    "preferred_subsidiary",
    when(
        (col("invest_total") >= col("card_total_spend")) & (col("invest_total") >= col("insurance_premium")),
        lit("securities")
    ).when(
        col("card_total_spend") >= col("insurance_premium"),
        lit("card")
    ).otherwise(lit("insurance"))
)

vip_df = vip_df.withColumn(
    "tx_pattern",
    when(col("bank_tx_count") > 50, lit("HIGH_FREQUENCY"))
    .when(col("bank_tx_count") > 20, lit("MEDIUM_FREQUENCY"))
    .otherwise(lit("LOW_FREQUENCY"))
)

vip_mart = vip_df.select(
    col("global_id"),
    col("age"),
    col("gender"),
    col("region"),
    col("job_group"),
    col("income_grade"),
    col("asset_grade"),
    col("lifesync_score"),
    col("customer_grade"),
    col("latest_balance"),
    col("invest_total"),
    col("card_total_spend"),
    col("insurance_premium"),
    col("bank_tx_count"),
    col("bank_tx_total"),
    col("card_tx_count"),
    col("total_asset"),
    col("preferred_subsidiary"),
    col("tx_pattern"),
    col("health_score"),
    col("hospital_visit_count"),
    col("dt")
)

output_path = f"s3://{S3_CURATED_BUCKET}/vip_mart/dt={date_formatted}/"
print(f"[vip_mart] Writing output to {output_path}")

vip_mart.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[vip_mart] Job completed successfully. Output: {output_path}")
spark.stop()
