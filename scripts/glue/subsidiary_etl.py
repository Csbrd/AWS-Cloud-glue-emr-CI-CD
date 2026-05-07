import sys
import json
import boto3
from datetime import datetime, timezone, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, DateType

# ── 계열사별 설정 ──────────────────────────────────────────────────────────────
CONFIGS = {
    "bank": {
        "pk_cols":   ["bank_id", "transaction_date"],
        "keep_cols": ["bank_id", "global_customer_id", "balance",
                      "transaction_amount", "transaction_type", "transaction_date"],
        "schema": [
            ("bank_id",            StringType()),
            ("global_customer_id", StringType()),
            ("balance",            LongType()),
            ("transaction_amount", LongType()),
            ("transaction_type",   StringType()),
            ("transaction_date",   DateType()),
        ],
    },
    "card": {
        "pk_cols":   ["card_id", "transaction_date"],
        "keep_cols": ["card_id", "global_customer_id", "spending_amount",
                      "merchant_category", "transaction_date"],
        "schema": [
            ("card_id",            StringType()),
            ("global_customer_id", StringType()),
            ("spending_amount",    LongType()),
            ("merchant_category",  StringType()),
            ("transaction_date",   DateType()),
        ],
    },
    "securities": {
        "pk_cols":   ["securities_id", "transaction_date"],
        "keep_cols": ["securities_id", "global_customer_id", "asset_value",
                      "stock_code", "transaction_type", "transaction_date"],
        "schema": [
            ("securities_id",      StringType()),
            ("global_customer_id", StringType()),
            ("asset_value",        LongType()),
            ("stock_code",         StringType()),
            ("transaction_type",   StringType()),
            ("transaction_date",   DateType()),
        ],
    },
    "insurance": {
        "pk_cols":   ["insurance_id"],
        "keep_cols": ["insurance_id", "global_customer_id", "premium_amount",
                      "insurance_type", "contract_date", "expiry_date"],
        "schema": [
            ("insurance_id",       StringType()),
            ("global_customer_id", StringType()),
            ("premium_amount",     LongType()),
            ("insurance_type",     StringType()),
            ("contract_date",      DateType()),
            ("expiry_date",        DateType()),
        ],
    },
    "online_insurance": {
        "pk_cols":   ["online_insurance_id"],
        "keep_cols": ["online_insurance_id", "global_customer_id", "premium_amount",
                      "insurance_type", "transaction_date"],
        "schema": [
            ("online_insurance_id", StringType()),
            ("global_customer_id",  StringType()),
            ("premium_amount",      LongType()),
            ("insurance_type",      StringType()),
            ("transaction_date",    DateType()),
        ],
    },
    "healthcare": {
        "pk_cols":   ["healthcare_id", "visit_date"],
        "keep_cols": ["healthcare_id", "global_customer_id", "visit_date",
                      "treatment_code", "treatment_amount"],
        "schema": [
            ("healthcare_id",      StringType()),
            ("global_customer_id", StringType()),
            ("visit_date",         DateType()),
            ("treatment_code",     StringType()),
            ("treatment_amount",   LongType()),
        ],
    },
    "hospital": {
        "pk_cols":   ["hospital_id", "visit_date"],
        "keep_cols": ["hospital_id", "global_customer_id", "visit_date",
                      "diagnosis_code", "treatment_amount"],
        "schema": [
            ("hospital_id",        StringType()),
            ("global_customer_id", StringType()),
            ("visit_date",         DateType()),
            ("diagnosis_code",     StringType()),
            ("treatment_amount",   LongType()),
        ],
    },
}

RAW_BUCKET  = "lifesync-raw"
PROC_BUCKET = "lifesync-processed"
KST         = timezone(timedelta(hours=9))

# ── Glue Job 초기화 ────────────────────────────────────────────────────────────
args   = getResolvedOptions(sys.argv, ["JOB_NAME", "source"])
SOURCE = args["source"]

if SOURCE not in CONFIGS:
    raise ValueError(f"Unknown source '{SOURCE}'. Valid: {list(CONFIGS.keys())}")

cfg = CONFIGS[SOURCE]

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── 1. S3 Raw JSON 읽기 ────────────────────────────────────────────────────────
raw_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [f"s3://{RAW_BUCKET}/{SOURCE}/"]},
    format="json",
    transformation_ctx=f"{SOURCE}_raw_src",
).toDF()

# ── 2. On-Prem MySQL consent 조회 ─────────────────────────────────────────────
def _get_mysql_creds() -> dict:
    sm = boto3.client("secretsmanager", region_name="ap-northeast-2")
    return json.loads(sm.get_secret_value(SecretId="lifesync/mysql/credentials")["SecretString"])

creds = _get_mysql_creds()
consent_df = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "url":      f"jdbc:mysql://{creds['host']}:{creds['port']}/lifesync",
        "user":     creds["username"],
        "password": creds["password"],
        "dbtable":  "consent",
    },
    transformation_ctx="consent_src",
).toDF().filter(F.col("is_consented") == True).select("global_customer_id")

# ── 3. 동의 고객만 필터링 ──────────────────────────────────────────────────────
filtered_df = raw_df.join(consent_df, on="global_customer_id", how="inner")

# ── 4. 스키마 정규화 ───────────────────────────────────────────────────────────
normalized_df = filtered_df
for col_name, col_type in cfg["schema"]:
    normalized_df = normalized_df.withColumn(col_name, F.col(col_name).cast(col_type))

# ── 5. PII 제거 + 필요 컬럼만 선택 ───────────────────────────────────────────
selected_df = normalized_df.select(cfg["keep_cols"])

# ── 6. 중복 제거 ───────────────────────────────────────────────────────────────
deduped_df = selected_df.dropDuplicates(cfg["pk_cols"])

# ── 7. S3 Processed에 Parquet 저장 (Snappy 압축) ──────────────────────────────
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(deduped_df, glueContext, f"{SOURCE}_output"),
    connection_type="s3",
    connection_options={"path": f"s3://{PROC_BUCKET}/{SOURCE}/"},
    format="parquet",
    format_options={"compression": "snappy"},
    transformation_ctx=f"{SOURCE}_output",
)

# ── 8. 마커 파일 생성 → EMR 트리거 감지용 ────────────────────────────────────
date_str = datetime.now(KST).strftime("%Y-%m-%d")
boto3.client("s3").put_object(
    Bucket=PROC_BUCKET,
    Key=f"_markers/{date_str}/{SOURCE}.done",
    Body=b"done",
)

job.commit()
