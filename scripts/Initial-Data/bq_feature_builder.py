"""
LifeSync360 BigQuery Feature Builder

입력:
  customer_master.csv          (pre_customer_data.py 출력)
  bank_YYYYMMDD.json 외 7개   (pre_generator_data.py 출력)

출력:
  BigQuery: lifesync_curated.ai_feature_table

실행:
  GCP_PROJECT_ID=<project_id> BATCH_DATE=20250901 python bq_feature_builder.py
"""

import json
import os
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "project-1f8eb19b-1a9a-45cf-ae6")
DATASET    = "lifesync_curated"
TABLE      = "ai_feature_table"
BATCH_DATE = os.environ.get("BATCH_DATE", "20250901")

MASTER_CSV = "customer_master.csv"

JSON_FILES = {
    "bank":             f"bank_{BATCH_DATE}.json",
    "card":             f"card_{BATCH_DATE}.json",
    "insurance":        f"insurance_{BATCH_DATE}.json",
    "online_insurance": f"online_insurance_{BATCH_DATE}.json",
    "hospital":         f"hospital_{BATCH_DATE}.json",
    "healthcare":       f"healthcare_{BATCH_DATE}.json",
    "securities":       f"securities_{BATCH_DATE}.json",
    "wearable":         f"wearable_{BATCH_DATE}.json",
}

# Vertex AI Batch Predict 소스와 컬럼명이 일치해야 함
SCHEMA = [
    bigquery.SchemaField("global_id",              "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("age",                    "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("gender",                 "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("region",                 "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("job_group",              "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("income_grade",           "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("asset_grade",            "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("wearable_flag",          "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("bank_txn_count",         "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("bank_total_amount",      "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("bank_avg_amount",        "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("latest_balance",         "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("card_txn_count",         "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("card_total_spend",       "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("card_avg_spend",         "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("card_main_category",     "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("invest_total",           "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("securities_trade_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("insurance_premium",      "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("has_online_insurance",   "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("hospital_visit_count",   "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("health_score",           "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("bmi",                    "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("avg_heart_rate",         "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("avg_steps",              "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("lifesync_score",         "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("customer_grade",         "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("vip_label",              "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("signup_label",           "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("health_grade",           "STRING",  mode="NULLABLE"),
]


def _load_records(filename: str) -> list:
    if not os.path.exists(filename):
        print(f"  [SKIP] {filename} 없음")
        return []
    with open(filename, encoding="utf-8") as f:
        return json.load(f).get("records", [])


def _lifesync_score(row) -> float:
    score = 50.0
    score += min(row.get("latest_balance", 0) / 1_000_000, 15)
    score += min(row.get("invest_total", 0) / 5_000_000, 10)
    score += min(row.get("card_total_spend", 0) / 2_000_000, 10)
    score += (row.get("health_score", 70) - 50) / 10
    if row.get("insurance_premium", 0) > 0:
        score += 5
    return round(min(max(score, 0), 100), 2)


def _customer_grade(score: float) -> str:
    if score >= 75:
        return "GOLD"
    if score >= 55:
        return "SILVER"
    return "BRONZE"


def _health_grade(h) -> str:
    if not h:
        return "NORMAL"
    if h < 60:
        return "RISK"
    if h < 80:
        return "NORMAL"
    return "GOOD"


def main():
    print("=" * 50)
    print("LifeSync360 BigQuery Feature Builder")
    print("=" * 50)

    # ── 1. 마스터 로드 ────────────────────────────────────
    print("[1/6] customer_master.csv 로드...")
    df = pd.read_csv(MASTER_CSV).rename(columns={"global_customer_id": "global_id"})
    print(f"      고객 수: {len(df):,}")

    # ── 2. 계열사 데이터 집계 → left join ─────────────────
    print("[2/6] 계열사 데이터 집계...")

    recs = _load_records(JSON_FILES["bank"])
    if recs:
        b = pd.DataFrame(recs).groupby("global_customer_id").agg(
            bank_txn_count=("amount", "count"),
            bank_total_amount=("amount", "sum"),
            bank_avg_amount=("amount", "mean"),
            latest_balance=("balance_after", "last"),
        ).reset_index().rename(columns={"global_customer_id": "global_id"})
        b[["bank_total_amount", "latest_balance"]] = b[["bank_total_amount", "latest_balance"]].astype(int)
        df = df.merge(b, on="global_id", how="left")
        print(f"      bank: {len(recs):,} 건")

    recs = _load_records(JSON_FILES["card"])
    if recs:
        c = pd.DataFrame(recs).groupby("global_customer_id").agg(
            card_txn_count=("amount", "count"),
            card_total_spend=("amount", "sum"),
            card_avg_spend=("amount", "mean"),
            card_main_category=("merchant_category", lambda x: x.mode()[0]),
        ).reset_index().rename(columns={"global_customer_id": "global_id"})
        c["card_total_spend"] = c["card_total_spend"].astype(int)
        df = df.merge(c, on="global_id", how="left")
        print(f"      card: {len(recs):,} 건")

    recs = _load_records(JSON_FILES["securities"])
    if recs:
        s = pd.DataFrame(recs)
        s["trade_value"] = s["qty"] * s["price"]
        s = s.groupby("global_customer_id").agg(
            invest_total=("trade_value", "sum"),
            securities_trade_count=("trade_value", "count"),
        ).reset_index().rename(columns={"global_customer_id": "global_id"})
        s["invest_total"] = s["invest_total"].astype(int)
        df = df.merge(s, on="global_id", how="left")
        print(f"      securities: {len(recs):,} 건")

    recs = _load_records(JSON_FILES["insurance"])
    if recs:
        i = pd.DataFrame(recs).groupby("global_customer_id").agg(
            insurance_premium=("premium_amount", "sum"),
        ).reset_index().rename(columns={"global_customer_id": "global_id"})
        i["insurance_premium"] = i["insurance_premium"].astype(int)
        df = df.merge(i, on="global_id", how="left")
        print(f"      insurance: {len(recs):,} 건")

    recs = _load_records(JSON_FILES["online_insurance"])
    if recs:
        oi = pd.DataFrame(recs)[["global_customer_id"]].drop_duplicates()
        oi["has_online_insurance"] = 1
        oi = oi.rename(columns={"global_customer_id": "global_id"})
        df = df.merge(oi, on="global_id", how="left")
        print(f"      online_insurance: {len(recs):,} 건")

    recs = _load_records(JSON_FILES["hospital"])
    if recs:
        h = pd.DataFrame(recs).groupby("global_customer_id").agg(
            hospital_visit_count=("cost", "count"),
        ).reset_index().rename(columns={"global_customer_id": "global_id"})
        df = df.merge(h, on="global_id", how="left")
        print(f"      hospital: {len(recs):,} 건")

    recs = _load_records(JSON_FILES["healthcare"])
    if recs:
        hc = pd.DataFrame(recs).groupby("global_customer_id").agg(
            health_score=("health_score", "mean"),
            bmi=("bmi", "mean"),
        ).reset_index().rename(columns={"global_customer_id": "global_id"})
        hc["health_score"] = hc["health_score"].round(0).astype(int)
        hc["bmi"] = hc["bmi"].round(1)
        df = df.merge(hc, on="global_id", how="left")
        print(f"      healthcare: {len(recs):,} 건")

    recs = _load_records(JSON_FILES["wearable"])
    if recs:
        vitals = [
            {"global_id": r["global_customer_id"],
             "heart_rate": v["heart_rate"],
             "steps": v["steps"]}
            for r in recs for v in r.get("vitals", [])
        ]
        w = pd.DataFrame(vitals).groupby("global_id").agg(
            avg_heart_rate=("heart_rate", "mean"),
            avg_steps=("steps", "mean"),
        ).reset_index()
        df = df.merge(w, on="global_id", how="left")
        print(f"      wearable: {len(recs):,} 건")

    # ── 3. 결측값 처리 ────────────────────────────────────
    print("[3/6] 결측값 처리...")
    int_cols = [
        "bank_txn_count", "bank_total_amount", "latest_balance",
        "card_txn_count", "card_total_spend",
        "invest_total", "securities_trade_count",
        "insurance_premium", "has_online_insurance",
        "hospital_visit_count", "health_score",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)

    for col in ["bank_avg_amount", "card_avg_spend", "bmi", "avg_heart_rate", "avg_steps"]:
        if col in df.columns:
            df[col] = df[col].fillna(0.0).round(2)

    if "card_main_category" not in df.columns:
        df["card_main_category"] = None

    # ── 4. 파생 컬럼 ──────────────────────────────────────
    print("[4/6] 파생 컬럼 생성...")
    df["lifesync_score"] = df.apply(_lifesync_score, axis=1)
    df["customer_grade"] = df["lifesync_score"].apply(_customer_grade)

    # 레이블: 전체 고객에 부여 (train.py에서 IS NOT NULL 필터링)
    df["vip_label"]    = df["vip_flag"].apply(lambda x: "VIP_CONFIRMED" if x == "Y" else "NOT_VIP")
    df["signup_label"] = df["join_status"].apply(lambda x: 1 if x == "ACTIVE" else 0)
    df["health_grade"] = df["health_score"].apply(_health_grade)

    # ── 5. 최종 컬럼 선택 ────────────────────────────────
    print("[5/6] 최종 컬럼 정리...")
    final_cols = [f.name for f in SCHEMA]
    for col in final_cols:
        if col not in df.columns:
            df[col] = None
    df = df[final_cols]

    print(f"      행 수:        {len(df):,}")
    print(f"      VIP 수:       {(df['vip_label']=='VIP_CONFIRMED').sum():,}")
    print(f"      가입자 수:    {(df['signup_label']==1).sum():,}")
    print(f"      건강위험 수:  {(df['health_grade']=='RISK').sum():,}")

    # ── 6. BigQuery 업로드 ────────────────────────────────
    print("[6/6] BigQuery 업로드...")
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    tbl = client.get_table(table_ref)
    print(f"      완료 → {table_ref}")
    print(f"      총 {tbl.num_rows:,} 행 업로드됨")
    print("=" * 50)


if __name__ == "__main__":
    main()
