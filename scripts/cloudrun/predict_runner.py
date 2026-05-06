import os
import logging
from datetime import datetime, timezone, timedelta

from flask import Flask, request, jsonify
from google.cloud import bigquery, storage

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "")
REGION = os.environ.get("REGION", "asia-northeast3")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "lifesync-data-lake")
MODEL_RESOURCE_NAME = os.environ.get("MODEL_RESOURCE_NAME", "")

KST = timezone(timedelta(hours=9))


def today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/run", methods=["POST"])
def run():
    """Cloud Scheduler 호출: Vertex AI Batch Prediction 실행"""
    date_str = today_kst()
    log.info("[/run] date=%s", date_str)
    try:
        if MODEL_RESOURCE_NAME:
            _run_batch_prediction(date_str)
        else:
            log.warning("[/run] MODEL_RESOURCE_NAME 미설정 — 더미 예측 결과 생성")
            _write_mock_predictions()
        _write_gcs_marker(f"prediction_result/{date_str}.done")
        return jsonify({"status": "ok", "date": date_str}), 200
    except Exception as exc:
        log.exception("[/run] 실패")
        return jsonify({"error": str(exc)}), 500


@app.route("/dynamic-score", methods=["POST"])
def dynamic_score():
    """Eventarc 호출: prediction_result/ 마커 감지 → 서빙 레이어 갱신"""
    event = request.get_json(silent=True) or {}
    obj_name = event.get("name", "")

    # prediction_result/ 경로가 아니면 무시 (Eventarc 전체 버킷 트리거 대응)
    if not obj_name.startswith("prediction_result/"):
        log.info("[/dynamic-score] 무시: %s", obj_name)
        return jsonify({"status": "skipped"}), 200

    date_str = today_kst()
    log.info("[/dynamic-score] 서빙 레이어 갱신 date=%s", date_str)
    try:
        _refresh_serving_table()
        _write_gcs_marker(f"serving_complete/{date_str}.done")
        return jsonify({"status": "ok", "date": date_str}), 200
    except Exception as exc:
        log.exception("[/dynamic-score] 실패")
        return jsonify({"error": str(exc)}), 500


def _run_batch_prediction(date_str: str):
    from google.cloud import aiplatform
    aiplatform.init(project=PROJECT_ID, location=REGION)
    job = aiplatform.BatchPredictionJob.create(
        job_display_name=f"lifesync-prediction-{date_str}",
        model_name=MODEL_RESOURCE_NAME,
        instances_format="bigquery",
        predictions_format="bigquery",
        bigquery_source=f"bq://{PROJECT_ID}.lifesync_curated.ai_feature_table",
        bigquery_destination_prefix=f"bq://{PROJECT_ID}.lifesync_ml.prediction_results",
        sync=True,
    )
    log.info("[_run_batch_prediction] 완료: %s", job.display_name)


def _write_mock_predictions():
    """Vertex AI 모델 미배포 시 더미 예측 결과 생성"""
    bq = bigquery.Client(project=PROJECT_ID)
    query = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_ml.prediction_results` AS
        SELECT
            CONCAT('G', LPAD(CAST(seq AS STRING), 8, '0')) AS global_customer_id,
            CURRENT_TIMESTAMP()                             AS prediction_time,
            ['PRODUCT_A', 'PRODUCT_B', 'PRODUCT_C']        AS recommendations,
            ROUND(RAND(), 4)                                AS confidence_score
        FROM UNNEST(GENERATE_ARRAY(1, 100)) AS seq
    """
    bq.query(query).result()
    log.info("[_write_mock_predictions] 더미 데이터 100건 생성 완료")


def _refresh_serving_table():
    bq = bigquery.Client(project=PROJECT_ID)
    query = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.lifesync_serving.customer_recommendations` AS
        SELECT *
        FROM `{PROJECT_ID}.lifesync_ml.prediction_results`
        WHERE DATE(prediction_time, "Asia/Seoul") = CURRENT_DATE("Asia/Seoul")
    """
    bq.query(query).result()
    log.info("[_refresh_serving_table] 완료")


def _write_gcs_marker(path: str):
    gcs = storage.Client(project=PROJECT_ID)
    gcs.bucket(GCS_BUCKET).blob(path).upload_from_string("done", content_type="text/plain")
    log.info("[_write_gcs_marker] gs://%s/%s", GCS_BUCKET, path)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
