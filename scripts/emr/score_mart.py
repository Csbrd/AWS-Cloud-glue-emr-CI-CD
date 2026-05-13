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

customer360_path = f"s3://{S3_CURATED_BUCKET}/customer_360_profile/dt={date_formatted}/"
print(f"[score_mart] Reading customer_360_profile from {customer360_path}")
df = spark.read.parquet(customer360_path)

print("[score_mart] Calculating LifeSync Score components")

# ── financial_score ───────────────────────────────────────────────────────────
# score = financial + health + relationship + growth - risk
df = df.withColumn("score_balance",  least(lit(15.0), col("latest_balance") / lit(1_000_000.0)))
df = df.withColumn("score_invest",   least(lit(10.0), col("invest_total") / lit(5_000_000.0)))
df = df.withColumn("score_card",     least(lit(10.0), col("card_total_spend") / lit(2_000_000.0)))
df = df.withColumn("score_insurance",
    when(col("insurance_premium") > 0, lit(5.0)).otherwise(lit(0.0))
)
df = df.withColumn(
    "financial_score",
    col("score_balance") + col("score_invest") + col("score_card") + col("score_insurance")
)

# ── health_score ──────────────────────────────────────────────────────────────
# activity + bio + lifestyle + prevent - disease_penalty - visit_penalty
df = df.withColumn("activity_score",
    when(col("wearable_flag") == "Y", lit(5.0)).otherwise(lit(0.0))
)
df = df.withColumn("bio_score",
    when((col("bmi") >= lit(18.5)) & (col("bmi") < lit(25.0)), lit(5.0)).otherwise(lit(2.0))
)
df = df.withColumn("lifestyle_score",
    col("health_score") * lit(0.05)
)
df = df.withColumn("prevent_score",
    when((col("insurance_count") + col("online_insurance_count")) > 0, lit(3.0)).otherwise(lit(0.0))
)
df = df.withColumn("disease_penalty",
    when(col("health_score") < lit(40.0), lit(3.0)).otherwise(lit(0.0))
)
df = df.withColumn("visit_penalty",
    least(lit(5.0), col("hospital_visit_count") * lit(0.5))
)
df = df.withColumn(
    "health_sub_score",
    col("activity_score") + col("bio_score") + col("lifestyle_score")
    + col("prevent_score") - col("disease_penalty") - col("visit_penalty")
)

# ── relationship_score ────────────────────────────────────────────────────────
df = df.withColumn("rel_bank",
    when(col("bank_tx_count") > 0, lit(2.0)).otherwise(lit(0.0))
)
df = df.withColumn("rel_card",
    when(col("card_tx_count") > 0, lit(2.0)).otherwise(lit(0.0))
)
df = df.withColumn("rel_invest",
    when(col("invest_product_count") > 0, lit(3.0)).otherwise(lit(0.0))
)
df = df.withColumn("rel_insurance",
    when((col("insurance_count") + col("online_insurance_count")) > 0, lit(3.0)).otherwise(lit(0.0))
)
df = df.withColumn("rel_hospital",
    when(col("hospital_visit_count") > 0, lit(2.0)).otherwise(lit(0.0))
)
df = df.withColumn(
    "relationship_score",
    col("rel_bank") + col("rel_card") + col("rel_invest") + col("rel_insurance") + col("rel_hospital")
)

# ── growth_score ──────────────────────────────────────────────────────────────
df = df.withColumn("growth_bank",   least(lit(4.0), col("bank_tx_count") / lit(10.0)))
df = df.withColumn("growth_card",   least(lit(3.0), col("card_tx_count") / lit(10.0)))
df = df.withColumn("growth_invest", least(lit(3.0), col("invest_total") / lit(10_000_000.0)))
df = df.withColumn(
    "growth_score",
    col("growth_bank") + col("growth_card") + col("growth_invest")
)

# ── risk_score ────────────────────────────────────────────────────────────────
df = df.withColumn("risk_hospital",
    when(col("hospital_visit_count") > lit(10), lit(5.0))
    .when(col("hospital_visit_count") > lit(5),  lit(2.0))
    .otherwise(lit(0.0))
)
df = df.withColumn("risk_bmi",
    when(col("bmi") >= lit(30.0), lit(3.0)).otherwise(lit(0.0))
)
df = df.withColumn("risk_health",
    when(col("health_score") < lit(40.0), lit(5.0)).otherwise(lit(0.0))
)
df = df.withColumn(
    "risk_score",
    col("risk_hospital") + col("risk_bmi") + col("risk_health")
)

# ── lifesync_score = base + financial + health + relationship + growth - risk ─
df = df.withColumn(
    "raw_score",
    lit(50.0)
    + col("financial_score")
    + col("health_sub_score")
    + col("relationship_score")
    + col("growth_score")
    - col("risk_score")
)

df = df.withColumn(
    "lifesync_score",
    greatest(lit(0.0), least(lit(100.0), col("raw_score")))
)

print("[score_mart] Assigning customer_grade")
df = df.withColumn(
    "customer_grade",
    when(col("lifesync_score") >= 90, lit("VIP"))
    .when(col("lifesync_score") >= 80, lit("GOLD"))
    .when(col("lifesync_score") >= 70, lit("SILVER"))
    .when(col("lifesync_score") >= 60, lit("BASIC"))
    .otherwise(lit("CARE"))
)

score_mart = df.select(
    col("global_id"),
    col("lifesync_score"),
    col("customer_grade"),
    col("financial_score"),
    col("health_sub_score"),
    col("relationship_score"),
    col("growth_score"),
    col("risk_score"),
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
