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


# ── Aurora 연결 (Aurora MySQL) ────────────────────────────────────────────────
print("[recommendation] Fetching Aurora credentials from Secrets Manager")
creds = _get_aurora_creds()
jdbc_url = (
    f"jdbc:mysql://{creds['host']}:{creds.get('port', 3306)}"
    f"/{creds.get('dbname', 'lifesync360')}"
)
jdbc_props = {
    "user":     creds["username"],
    "password": creds["password"],
    "driver":   "com.mysql.cj.jdbc.Driver",
}

# ── recommend_rule 읽기 (활성 규칙만) ─────────────────────────────────────────
# 실제 스키마: rule_id / category_code / action_code / active_flag
print("[recommendation] Reading recommend_rule from Aurora")
df_rules = spark.read.jdbc(
    url=jdbc_url,
    table="recommend_rule",
    properties=jdbc_props,
).filter(col("active_flag") == "Y").select("rule_id", "category_code", "action_code", "priority_rank")

active_products = {row.category_code for row in df_rules.collect()}
print(f"[recommendation] Active products: {active_products}")

# category_code → action_code 매핑 (priority_rank 기준 최우선 규칙)
df_action = df_rules.groupBy("category_code").agg(
    F.first("action_code", ignorenulls=True).alias("action_code")
)

# ── cross_sell_rule 읽기 ──────────────────────────────────────────────────────
# 실제 스키마: cross_id / base_category / target_category / active_flag
print("[recommendation] Reading cross_sell_rule from Aurora")
df_cross_sell = spark.read.jdbc(
    url=jdbc_url,
    table="cross_sell_rule",
    properties=jdbc_props,
).filter(col("active_flag") == "Y").select("base_category", "target_category")

# ── customer360 읽기 ──────────────────────────────────────────────────────────
customer360_path = f"s3://{S3_CURATED_BUCKET}/customer_360_profile/dt={date_formatted}/"
print(f"[recommendation] Reading customer_360_profile from {customer360_path}")
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

# ── 주 추천 상품 기준 action_code 부여 (recommend_rule.category_code 매핑) ──
df = df.withColumn("primary_product", F.element_at(col("recommended_products"), 1))
df = df.join(
    df_action.withColumnRenamed("category_code", "primary_product"),
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

output_path = f"s3://{S3_CURATED_BUCKET}/recommendation_mart/dt={date_formatted}/"
print(f"[recommendation] Writing output to {output_path}")

recommendation.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(output_path)

print(f"[recommendation] Job completed successfully. Output: {output_path}")
spark.stop()
