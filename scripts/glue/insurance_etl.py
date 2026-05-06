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

# ── 설정 ──────────────────────────────────────────────────────────────────────
SOURCE      = "insurance"
RAW_BUCKET  = "lifesync-raw"
PROC_BUCKET = "lifesync-processed"

PK_COLS   = ["insurance_id"]
KEEP_COLS = ["insurance_id", "global_customer_id", "premium_amount",
             "insurance_type", "contract_date", "expiry_date"]

KST = timezone(timedelta(hours=9))

# ── Glue Job 초기화 ────────────────────────────────────────────────────────────
args        = getResolvedOptions(sys.argv, ["JOB_NAME"])
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
normalized_df = (
    filtered_df
    .withColumn("insurance_id",       F.col("insurance_id").cast(StringType()))
    .withColumn("global_customer_id", F.col("global_customer_id").cast(StringType()))
    .withColumn("premium_amount",     F.col("premium_amount").cast(LongType()))
    .withColumn("insurance_type",     F.col("insurance_type").cast(StringType()))
    .withColumn("contract_date",      F.col("contract_date").cast(DateType()))
    .withColumn("expiry_date",        F.col("expiry_date").cast(DateType()))
)

# ── 5. PII 제거 + 필요 컬럼만 선택 ───────────────────────────────────────────
selected_df = normalized_df.select(KEEP_COLS)

# ── 6. 중복 제거 ───────────────────────────────────────────────────────────────
deduped_df = selected_df.dropDuplicates(PK_COLS)

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
