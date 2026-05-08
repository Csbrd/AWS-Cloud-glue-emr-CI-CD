# ==========================================================
# LifeSync360 FULL Generator.py (최신 완성본)
#
# 입력:
#   customer_master.csv
#   customer_identity_map.csv
#
# 출력:
#   bank_20250901.json
#   card_20250901.json
#   insurance_20250901.json
#   online_insurance_20250901.json
#   hospital_20250901.json
#   healthcare_20250901.json
#   securities_20250901.json
#   wearable_20250901.json
#
# 핵심 반영사항
# 1. merge key 개선 (global_customer_id)
# 2. 가입자 / 동의 / VIP 정책 반영
# 3. wearable_flag 기반 생성
# 4. VIP 소비/자산 우대 패턴
# 5. 현실형 JSON 구조
# ==========================================================

import pandas as pd
import json
import random
import uuid
from tqdm import tqdm
from datetime import datetime, timedelta

# ==========================================================
# 설정
# ==========================================================

MASTER_FILE = "customer_master.csv"
MAP_FILE = "customer_identity_map.csv"

BATCH_DT = "2025-09-01T10:00:00Z"

# ==========================================================
# CSV 로드
# ==========================================================

print("Load CSV...")

df_master = pd.read_csv(MASTER_FILE)
df_map = pd.read_csv(MAP_FILE)

# 핵심 수정: ls_user_id NULL 문제 방지
df = pd.merge(
    df_master,
    df_map,
    on="global_customer_id",
    how="inner",
    suffixes=("_m", "_map")
)

# master 기준 사용
df["ls_user_id"] = df["ls_user_id_m"]

print("Customer Count:", len(df))

# ==========================================================
# 공통 함수
# ==========================================================

def rand_dt():
    base = datetime(2025, 9, 1, 10, 0, 0)
    sec = random.randint(0, 86400)
    return (base + timedelta(seconds=sec)).strftime("%Y-%m-%dT%H:%M:%SZ")

def save_json(path, source, records):
    payload = {
        "source": source,
        "batch_dt": BATCH_DT,
        "record_count": len(records),
        "records": records
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

# ==========================================================
# 리스트
# ==========================================================

bank = []
card = []
insurance = []
online_insurance = []
hospital = []
healthcare = []
securities = []
wearable = []

# ==========================================================
# 메인 생성
# ==========================================================

for _, c in tqdm(df.iterrows(), total=len(df), desc="Generate"):

    vip = c["vip_flag"] == "Y"
    joined = c["join_status"] == "ACTIVE"
    consent = c["consent_flag"] == "Y"

    age = c["age"]
    income = c["income_grade"]

    # ------------------------------------------------------
    # BANK
    # ------------------------------------------------------
    if pd.notna(c["bank_id"]):

        txn_cnt = random.randint(1, 5)
        if vip:
            txn_cnt += 3

        for _ in range(txn_cnt):

            amt = random.randint(10000, 3000000)
            if vip:
                amt *= 3

            bank.append({
                "global_customer_id": c["global_customer_id"],
                "ls_user_id": c["ls_user_id"],
                "bank_id": c["bank_id"],
                "transaction_id": f"TXN-{uuid.uuid4().hex[:12]}",
                "transaction_dt": rand_dt(),
                "transaction_type": random.choice(
                    ["DEPOSIT", "WITHDRAW", "TRANSFER"]
                ),
                "amount": amt,
                "balance_after": random.randint(0, 50000000),
                "channel": random.choice(
                    ["APP", "ATM", "BRANCH"]
                )
            })

    # ------------------------------------------------------
    # CARD
    # ------------------------------------------------------
    if pd.notna(c["card_id"]):

        cnt = random.randint(1, 8)

        for _ in range(cnt):

            amount = random.randint(5000, 800000)

            # 40대 고소득 소비 증가
            if age >= 40 and age < 50 and income == "HIGH":
                amount *= 4

            if vip:
                amount *= 2

            card.append({
                "global_customer_id": c["global_customer_id"],
                "ls_user_id": c["ls_user_id"],
                "card_id": c["card_id"],
                "approval_no": f"APR-{uuid.uuid4().hex[:12]}",
                "approval_dt": rand_dt(),
                "merchant_name": random.choice(
                    ["스타벅스","쿠팡","이마트","병원","CGV"]
                ),
                "merchant_category": random.choice(
                    ["FOOD","SHOPPING","MEDICAL","TRAVEL"]
                ),
                "amount": amount,
                "installment_months": random.choice([0,3,6,12]),
                "payment_status": "APPROVED"
            })

    # ------------------------------------------------------
    # INSURANCE
    # ------------------------------------------------------
    if pd.notna(c["insurance_id"]):

        premium = random.randint(30000, 400000)
        if vip:
            premium *= 3

        insurance.append({
            "global_customer_id": c["global_customer_id"],
            "ls_user_id": c["ls_user_id"],
            "insurance_id": c["insurance_id"],
            "policy_no": f"POL-{random.randint(100000,999999)}",
            "product_name": random.choice(
                ["실손보험","종신보험","자동차보험"]
            ),
            "premium_amount": premium,
            "payment_cycle": random.choice(
                ["MONTHLY","ANNUAL"]
            ),
            "payment_status": "PAID"
        })

    # ------------------------------------------------------
    # ONLINE INSURANCE
    # ------------------------------------------------------
    if pd.notna(c["online_insurance_id"]):

        buy_flag = "Y" if consent else "N"

        online_insurance.append({
            "global_customer_id": c["global_customer_id"],
            "ls_user_id": c["ls_user_id"],
            "online_insurance_id": c["online_insurance_id"],
            "quote_id": f"Q-{uuid.uuid4().hex[:10]}",
            "channel": "MOBILE",
            "product_name": random.choice(
                ["여행자보험","펫보험","미니암보험"]
            ),
            "premium_quote": random.randint(5000,70000),
            "purchase_flag": buy_flag,
            "event_dt": rand_dt()
        })

    # ------------------------------------------------------
    # HOSPITAL
    # ------------------------------------------------------
    if pd.notna(c["hospital_id"]):

        visit_cnt = 1

        # 당뇨 고객 가정
        if age >= 50:
            visit_cnt += random.randint(1, 3)

        for _ in range(visit_cnt):

            hospital.append({
                "global_customer_id": c["global_customer_id"],
                "ls_user_id": c["ls_user_id"],
                "hospital_id": c["hospital_id"],
                "visit_id": f"VIS-{uuid.uuid4().hex[:10]}",
                "visit_dt": rand_dt(),
                "department": random.choice(
                    ["내과","치과","정형외과","피부과"]
                ),
                "diagnosis_code": random.choice(
                    ["J00","E11","I10","M54"]
                ),
                "cost": random.randint(10000,500000)
            })

    # ------------------------------------------------------
    # HEALTHCARE
    # ------------------------------------------------------
    if pd.notna(c["healthcare_id"]):

        healthcare.append({
            "global_customer_id": c["global_customer_id"],
            "ls_user_id": c["ls_user_id"],
            "healthcare_id": c["healthcare_id"],
            "height_cm": random.randint(150,190),
            "weight_kg": random.randint(45,110),
            "bmi": round(random.uniform(18,35),1),
            "blood_pressure": random.choice(
                ["120/80","130/85","140/90"]
            ),
            "health_score": random.randint(50,100),
            "checkup_dt": rand_dt()
        })

    # ------------------------------------------------------
    # SECURITIES
    # ------------------------------------------------------
    if pd.notna(c["securities_id"]):

        trade_cnt = 1
        if vip:
            trade_cnt = 5

        for _ in range(trade_cnt):

            securities.append({
                "global_customer_id": c["global_customer_id"],
                "ls_user_id": c["ls_user_id"],
                "securities_id": c["securities_id"],
                "account_no": f"SEC-{random.randint(100000,999999)}",
                "trade_dt": rand_dt(),
                "symbol": random.choice(
                    ["005930","000660","035420","AAPL","TSLA"]
                ),
                "trade_type": random.choice(["BUY","SELL"]),
                "qty": random.randint(1,50),
                "price": random.randint(10000,300000)
            })

    # ------------------------------------------------------
    # WEARABLE (핵심)
    # ------------------------------------------------------
    if c["wearable_flag"] == "Y":

        vitals = []
        ts_base = datetime.utcnow()

        for i in range(10):

            heart = random.randint(58,110) if vip else random.randint(60,130)
            steps = random.randint(30,180) if vip else random.randint(0,150)

            vitals.append({
                "ts": (
                    ts_base + timedelta(minutes=i)
                ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "heart_rate": heart,
                "spo2": round(random.uniform(94,100),1),
                "body_temp": round(random.uniform(36.0,37.5),1),
                "steps": steps
            })

        wearable.append({
            "global_customer_id": c["global_customer_id"],
            "ls_user_id": c["ls_user_id"],
            "device_id": f"DEV-{uuid.uuid4().hex[:8]}",
            "device_type": c["device_type"],
            "wearable_join_dt": c["wearable_join_dt"],
            "event_dt": rand_dt(),
            "vitals": vitals
        })

# ==========================================================
# 저장
# ==========================================================

save_json("bank_20250901.json", "bank", bank)
save_json("card_20250901.json", "card", card)
save_json("insurance_20250901.json", "insurance", insurance)
save_json("online_insurance_20250901.json", "online_insurance", online_insurance)
save_json("hospital_20250901.json", "hospital", hospital)
save_json("healthcare_20250901.json", "healthcare", healthcare)
save_json("securities_20250901.json", "securities", securities)
save_json("wearable_20250901.json", "wearable", wearable)

# ==========================================================
# 결과
# ==========================================================

print("===================================================")
print("LifeSync360 FULL Generator 완료")
print("===================================================")
print("BANK        :", len(bank))
print("CARD        :", len(card))
print("INSURANCE   :", len(insurance))
print("ONLINE INS  :", len(online_insurance))
print("HOSPITAL    :", len(hospital))
print("HEALTHCARE  :", len(healthcare))
print("SECURITIES  :", len(securities))
print("WEARABLE    :", len(wearable))
print("===================================================")