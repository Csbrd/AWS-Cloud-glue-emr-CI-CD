"""
GCS에 저장된 model.bst에서 feature_names를 제거합니다.
Vertex AI XGBoost 컨테이너가 배열 형태 입력을 받을 때 feature_names mismatch 오류 방지.
"""
import os
import tempfile

import xgboost as xgb
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "project-1f8eb19b-1a9a-45cf-ae6")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "lifesync-data-lake")

MODELS = ["vip", "signup", "health", "rec"]


def fix_model(model_name: str):
    gcs_path = f"models/{model_name}/model.bst"
    client = storage.Client(project=PROJECT_ID)
    blob = client.bucket(GCS_BUCKET).blob(gcs_path)

    with tempfile.NamedTemporaryFile(suffix=".bst", delete=False) as f:
        tmp = f.name

    blob.download_to_filename(tmp)

    booster = xgb.Booster()
    booster.load_model(tmp)
    print(f"  [{model_name}] feature_names: {booster.feature_names}")

    booster.feature_names = None
    booster.feature_types = None
    booster.save_model(tmp)

    blob.upload_from_filename(tmp)
    os.unlink(tmp)
    print(f"  [{model_name}] 수정 완료 → gs://{GCS_BUCKET}/{gcs_path}")


if __name__ == "__main__":
    print("=== GCS 모델 feature_names 제거 ===")
    for m in MODELS:
        try:
            fix_model(m)
        except Exception as e:
            print(f"  [{m}] 건너뜀: {e}")
    print("완료")
