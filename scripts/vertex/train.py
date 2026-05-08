"""
LifeSync360 XGBoost Training Script

BigQuery lifesync_curated.ai_feature_table 에서 읽어
4개 XGBoost 모델을 학습하고 GCS + Vertex AI Model Registry에 등록합니다.

모델:
  vip      — VIP 승급 가능성 (XGBClassifier, target: vip_label)
  signup   — 신규 가입 가능성 (XGBClassifier, target: signup_label)
  health   — 건강 위험도     (XGBClassifier, target: health_grade)
  rec      — 상품 구매 가능성 (XGBClassifier, target: has_online_insurance)

실행:
  GCP_PROJECT_ID=<project_id> python train.py
"""

import os
import tempfile

import pandas as pd
import xgboost as xgb
from google.cloud import bigquery, storage, aiplatform
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, roc_auc_score

PROJECT_ID  = os.environ.get("GCP_PROJECT_ID", "project-1f8eb19b-1a9a-45cf-ae6")
REGION      = os.environ.get("REGION", "asia-northeast3")
GCS_BUCKET  = os.environ.get("GCS_BUCKET", "lifesync-data-lake")
MODEL_DIR   = "models"

SOURCE_TABLE = f"{PROJECT_ID}.lifesync_curated.ai_feature_table"

# Vertex AI 사전빌드 XGBoost 서빙 컨테이너
XGBOOST_CONTAINER = "us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-7:latest"

# XGBoost 학습에 사용할 수치형 피처 (BQ 컬럼명과 일치해야 함)
NUMERIC_FEATURES = [
    "age",
    "bank_txn_count", "bank_total_amount", "bank_avg_amount", "latest_balance",
    "card_txn_count", "card_total_spend", "card_avg_spend",
    "invest_total", "securities_trade_count",
    "insurance_premium", "has_online_insurance",
    "hospital_visit_count", "health_score", "bmi",
    "avg_heart_rate", "avg_steps",
    "lifesync_score",
]

XGB_PARAMS = {
    "n_estimators":  300,
    "max_depth":     6,
    "learning_rate": 0.05,
    "subsample":     0.8,
    "eval_metric":   "logloss",
    "random_state":  42,
}


# ── 유틸 ──────────────────────────────────────────────────────────────────────

def _load_table() -> pd.DataFrame:
    print(f"  BigQuery 로드: {SOURCE_TABLE}")
    client = bigquery.Client(project=PROJECT_ID)
    query  = f"SELECT * FROM `{SOURCE_TABLE}`"
    df     = client.query(query).to_dataframe()
    print(f"  행 수: {len(df):,}  컬럼 수: {len(df.columns)}")
    return df


def _upload_model(local_path: str, model_name: str) -> str:
    """GCS에 model.bst 업로드 → GCS URI 반환"""
    gcs_path = f"{MODEL_DIR}/{model_name}/model.bst"
    bucket   = storage.Client(project=PROJECT_ID).bucket(GCS_BUCKET)
    bucket.blob(gcs_path).upload_from_filename(local_path)
    uri = f"gs://{GCS_BUCKET}/{MODEL_DIR}/{model_name}/"
    print(f"  GCS 업로드: {uri}")
    return uri


def _register_model(display_name: str, artifact_uri: str) -> str:
    """Vertex AI Model Registry 등록 → resource name 반환"""
    model = aiplatform.Model.upload(
        display_name=display_name,
        artifact_uri=artifact_uri,
        serving_container_image_uri=XGBOOST_CONTAINER,
    )
    print(f"  Vertex AI 등록: {model.resource_name}")
    return model.resource_name


def _train_classifier(X_train, X_test, y_train, y_test, label: str) -> xgb.XGBClassifier:
    model = xgb.XGBClassifier(**XGB_PARAMS)
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    preds = model.predict(X_test)
    acc   = accuracy_score(y_test, preds)
    try:
        proba = model.predict_proba(X_test)[:, 1]
        auc   = roc_auc_score(y_test, proba)
        print(f"  [{label}] Accuracy={acc:.4f}  AUC={auc:.4f}")
    except Exception:
        print(f"  [{label}] Accuracy={acc:.4f}")
    return model


# ── 모델별 학습 함수 ───────────────────────────────────────────────────────────

def train_vip(df: pd.DataFrame) -> str:
    print("\n[VIP 예측 모델]")
    sub = df[NUMERIC_FEATURES + ["vip_label"]].copy()
    sub["target"] = (sub["vip_label"] == "VIP_CONFIRMED").astype(int)
    X = sub[NUMERIC_FEATURES].fillna(0)
    y = sub["target"]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    model = _train_classifier(X_tr, X_te, y_tr, y_te, "VIP")

    with tempfile.NamedTemporaryFile(suffix=".bst", delete=False) as f:
        model.get_booster().feature_names = None
        model.get_booster().feature_types = None
        model.save_model(f.name)
        uri = _upload_model(f.name, "vip")
    return _register_model("lifesync-vip-model", uri)


def train_signup(df: pd.DataFrame) -> str:
    print("\n[가입 예측 모델]")
    X = df[NUMERIC_FEATURES].fillna(0)
    y = df["signup_label"].astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    model = _train_classifier(X_tr, X_te, y_tr, y_te, "Signup")

    with tempfile.NamedTemporaryFile(suffix=".bst", delete=False) as f:
        model.get_booster().feature_names = None
        model.get_booster().feature_types = None
        model.save_model(f.name)
        uri = _upload_model(f.name, "signup")
    return _register_model("lifesync-signup-model", uri)


def train_health(df: pd.DataFrame) -> str:
    print("\n[건강 위험도 모델]")
    sub = df[NUMERIC_FEATURES + ["health_grade"]].copy()
    le  = LabelEncoder()
    sub["target"] = le.fit_transform(sub["health_grade"].fillna("NORMAL"))
    X = sub[NUMERIC_FEATURES].fillna(0)
    y = sub["target"]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)

    params = {**XGB_PARAMS, "objective": "multi:softmax",
              "num_class": len(le.classes_), "eval_metric": "mlogloss"}
    model = xgb.XGBClassifier(**params)
    model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)
    acc = accuracy_score(y_te, model.predict(X_te))
    print(f"  [Health] Accuracy={acc:.4f}  Classes={list(le.classes_)}")

    with tempfile.NamedTemporaryFile(suffix=".bst", delete=False) as f:
        model.get_booster().feature_names = None
        model.get_booster().feature_types = None
        model.save_model(f.name)
        uri = _upload_model(f.name, "health")
    return _register_model("lifesync-health-model", uri)


def train_rec(df: pd.DataFrame) -> str:
    print("\n[추천 예측 모델]")
    X = df[NUMERIC_FEATURES].fillna(0)
    y = df["has_online_insurance"].fillna(0).astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    model = _train_classifier(X_tr, X_te, y_tr, y_te, "Rec")

    with tempfile.NamedTemporaryFile(suffix=".bst", delete=False) as f:
        model.get_booster().feature_names = None
        model.get_booster().feature_types = None
        model.save_model(f.name)
        uri = _upload_model(f.name, "rec")
    return _register_model("lifesync-rec-model", uri)


# ── 메인 ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("LifeSync360 XGBoost Training")
    print("=" * 55)

    aiplatform.init(project=PROJECT_ID, location=REGION)

    df = _load_table()

    results = {
        "vip":    train_vip(df),
        "signup": train_signup(df),
        "health": train_health(df),
        "rec":    train_rec(df),
    }

    print("\n" + "=" * 55)
    print("학습 완료 — Vertex AI Model Resource Names")
    print("=" * 55)
    for name, resource in results.items():
        print(f"  {name:8s}: {resource}")

    print("\n▶ terraform.tfvars에 아래 값을 입력하세요:")
    print(f'  vertex_model_resource_name = "{results["vip"]}"')
    print("  (VIP 모델이 predict_runner.py의 기본 배치 예측 대상)")
    print("=" * 55)


if __name__ == "__main__":
    main()
