import os
import sys
import json
import boto3
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
    .appName("recommendation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"[recommendation] Starting job for BATCH_DATE={BATCH_DATE}, date_formatted={date_formatted}")


def _get_aurora_creds() -> dict:
    sm = boto3.client("secretsmanager", region_name="ap-northeast-2")
    return json.loads(sm.get_secret_value(SecretId="lifesync/aurora/credentials")["SecretString"])


# ── Aurora 연결 ───────────────────────────────────────────────────────────────
print("[recommendation] Fetching Aurora credentials from Secrets Manager")
creds = _get_aurora_creds()
jdbc_url = (
    f"jdbc:postgresql://{creds['host']}:{creds.get('port', 5432)}"
    f"/{creds.get('dbname', 'lifesync')}"
)
jdbc_props = {
    "user":   creds["username"],
    "password": creds["password"],
    "driver": "org.postgresql.Driver",
}

# ── recommend_rule 읽기 (활성 규칙만) ─────────────────────────────────────────
print("[recommendation] Reading recommend_rule from Aurora")
df_rules = spark.read.jdbc(
    url=jdbc_url,
    table="recommend_rule",
    properties=jdbc_props,
).filter(col("is_active") == True).select("rule_id", "target_product", "priority")

active_products = {row.target_product for row in df_rules.collect()}
print(f"[recommendation] Active products: {active_products}")

# ── cross_sell_rule 읽기 ──────────────────────────────────────────────────────
print("[recommendation] Reading cross_sell_rule from Aurora")
df_cross_sell = spark.read.jdbc(
    url=jdbc_url,
    table="cross_sell_rule",
    properties=jdbc_props,
).select("product_id", "action_code")

# ── customer360 읽기 ──────────────────────────────────────────────────────────
customer360_path = f"s3://{S3_CURATED_BUCKET}/customer360/dt={date_formatted}/"
print(f"[recommendation] Reading customer360 from {customer360_path}")
df = spark.read.parquet(customer360_path)

# ── 추천 조건 적용 (recommend_rule 활성 목록 기반) ────────────────────────────
print("[recommendation] Applying recommendation rules")

REC_CONDITIONS = {
    "securities": (col("invest_total") == 0) & (col("income_grade") == "HIGH"),
    "insurance":  col("insurance_premium") == 0,
    "healthcare": col("health_score") < 60,
    "wearable":   (col("wearable_flag") == "N") & col("wearable_flag").isNotNull(),
}

for product, condition in REC_CONDITIONS.items():
    df = df.withColumn(
        f"rec_{product}",
        when(lit(product in active_products) & condition, lit(product))
        .otherwise(lit(None).cast("string"))
    )

# ── 추천 목록 생성 (최대 3개) ─────────────────────────────────────────────────
df = df.withColumn(
    "rec_array_raw",
    F.array(
        col("rec_securities"),
        col("rec_insurance"),
        col("rec_healthcare"),
        col("rec_wearable"),
    )
)
df = df.withColumn(
    "rec_array_filtered",
    F.expr("filter(rec_array_raw, x -> x is not null)")
)
df = df.withColumn("recommended_products", F.slice(col("rec_array_filtered"), 1, 3))
df = df.withColumn("recommendation_count", F.size(col("recommended_products")))

# ── cross_sell_rule join → 주 추천 상품 action_code 부여 ──────────────────────
df = df.withColumn("primary_product", F.element_at(col("recommended_products"), 1))
df = df.join(
    df_cross_sell.withColumnRenamed("product_id", "primary_product"),
    on="primary_product",
    how="left",
)

recommendation = df.select(
    col("global_id"),
    col("income_grade"),
    col("invest_total"),
    col("insurance_premium"),
    col("health_score"),
    col("wearable_flag"),
    col("recommended_products"),
    col("recommendation_count"),
    col("primary_product"),
    col("action_code"),
    col("dt"),
)

output_path = f"s3://{S3_CURATED_BUCKET}/recommendation/dt={date_formatted}/"
print(f"[recommendation] Writing output to {output_path}")

recommendation.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[recommendation] Job completed successfully. Output: {output_path}")
spark.stop()
