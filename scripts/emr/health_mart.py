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
    .appName("health_mart") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[health_mart] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")

def read_processed(subsidiary):
    path = f"s3://{S3_PROCESSED_BUCKET}/{subsidiary}/dt={date_formatted}/"
    print(f"[health_mart] Reading {subsidiary} from {path}")
    return spark.read.parquet(path)

df_healthcare = read_processed("healthcare")
df_hospital = read_processed("hospital")
df_wearable = read_processed("wearable")

print("[health_mart] Aggregating healthcare data")
healthcare_agg = df_healthcare.groupBy("global_customer_id").agg(
    F.avg("health_score").alias("health_score"),
    F.avg("bmi").alias("bmi"),
    F.first("age").alias("age"),
    F.first("gender").alias("gender"),
    F.first("region").alias("region")
)

print("[health_mart] Aggregating hospital data")
hospital_agg = df_hospital.groupBy("global_customer_id").agg(
    F.count("*").alias("hospital_visit_count"),
    F.sum("treatment_cost").alias("hospital_total_cost"),
    F.countDistinct("department").alias("department_count")
)

print("[health_mart] Aggregating wearable data")
wearable_agg = df_wearable.groupBy("global_customer_id").agg(
    F.avg("heart_rate").alias("avg_heart_rate"),
    F.avg("steps").alias("avg_steps"),
    F.max("record_date").alias("last_sync_date")
)

print("[health_mart] Joining healthcare, hospital, and wearable datasets")
df = healthcare_agg \
    .join(hospital_agg, on="global_customer_id", how="left") \
    .join(wearable_agg, on="global_customer_id", how="left")

df = df.fillna({
    "hospital_visit_count": 0,
    "hospital_total_cost": 0.0,
    "department_count": 0,
    "avg_heart_rate": 0.0,
    "avg_steps": 0.0,
    "health_score": 50.0,
    "bmi": 22.0
})

print("[health_mart] Computing health_grade")
df = df.withColumn(
    "health_grade",
    when(col("health_score") < 60, lit("RISK"))
    .when(col("health_score") < 80, lit("NORMAL"))
    .otherwise(lit("GOOD"))
)

print("[health_mart] Computing bmi_category")
df = df.withColumn(
    "bmi_category",
    when(col("bmi") < 18.5, lit("UNDERWEIGHT"))
    .when(col("bmi") < 25.0, lit("NORMAL"))
    .when(col("bmi") < 30.0, lit("OVERWEIGHT"))
    .otherwise(lit("OBESE"))
)

df = df.withColumn("dt", lit(date_formatted))

health_mart = df.select(
    col("global_customer_id"),
    col("age"),
    col("gender"),
    col("region"),
    col("health_score"),
    col("health_grade"),
    col("bmi"),
    col("bmi_category"),
    col("avg_heart_rate"),
    col("avg_steps"),
    col("last_sync_date"),
    col("hospital_visit_count"),
    col("hospital_total_cost"),
    col("department_count"),
    col("dt")
)

output_path = f"s3://{S3_CURATED_BUCKET}/health_mart/dt={date_formatted}/"
print(f"[health_mart] Writing output to {output_path}")

health_mart.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[health_mart] Job completed successfully. Output: {output_path}")
spark.stop()
