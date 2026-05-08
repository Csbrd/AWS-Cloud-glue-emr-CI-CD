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
    .appName("ai_feature_table") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[ai_feature_table] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")

customer360_path = f"s3://{S3_CURATED_BUCKET}/customer360/dt={date_formatted}/"
score_mart_path = f"s3://{S3_CURATED_BUCKET}/score_mart/dt={date_formatted}/"

print(f"[ai_feature_table] Reading customer360 from {customer360_path}")
df_c360 = spark.read.parquet(customer360_path)

print(f"[ai_feature_table] Reading score_mart from {score_mart_path}")
df_score = spark.read.parquet(score_mart_path)

print("[ai_feature_table] Joining customer360 and score_mart")
df = df_c360.join(
    df_score.select(
        "global_customer_id",
        "lifesync_score",
        "customer_grade"
    ),
    on="global_customer_id",
    how="left"
)

print("[ai_feature_table] Computing derived labels")

df = df.withColumn(
    "vip_label",
    when(
        (col("customer_grade") == "GOLD") & (col("wearable_flag") == "Y"),
        lit("VIP_CONFIRMED")
    ).otherwise(lit("NOT_VIP"))
)

df = df.withColumn(
    "signup_label",
    when(col("wearable_flag") == "Y", lit(1)).otherwise(lit(0))
)

df = df.withColumn(
    "health_grade",
    when(col("health_score") < 60, lit("RISK"))
    .when(col("health_score") < 80, lit("NORMAL"))
    .otherwise(lit("GOOD"))
)

print("[ai_feature_table] Selecting 30 feature columns")

ai_feature = df.select(
    col("global_customer_id").alias("global_id"),
    col("age"),
    col("gender"),
    col("region"),
    col("job_group"),
    col("income_grade"),
    col("asset_grade"),
    col("wearable_flag"),
    col("bank_tx_count"),
    col("bank_tx_total"),
    col("latest_balance"),
    col("bank_avg_tx_amount"),
    col("card_tx_count"),
    col("card_total_spend"),
    col("card_category_count"),
    col("card_avg_spend"),
    col("invest_total"),
    col("invest_product_count"),
    col("insurance_premium"),
    col("insurance_count"),
    col("hospital_visit_count"),
    col("health_score"),
    col("bmi"),
    col("lifesync_score"),
    col("customer_grade"),
    col("vip_label"),
    col("signup_label"),
    col("health_grade"),
    col("dt")
)

output_path = f"s3://{S3_CURATED_BUCKET}/ai_feature_table/dt={date_formatted}/"
print(f"[ai_feature_table] Writing output to {output_path}")

ai_feature.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[ai_feature_table] Job completed successfully. Output: {output_path}")
spark.stop()
