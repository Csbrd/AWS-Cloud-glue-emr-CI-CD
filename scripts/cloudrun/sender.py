import os
import logging
from datetime import datetime, date, timezone, timedelta
from decimal import Decimal

import requests
from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "")
AWS_API_GW_URL = os.environ.get("AWS_API_GW_URL", "")

KST = timezone(timedelta(hours=9))


def today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/", methods=["POST"])
def handle_event():
    """Eventarc 호출: serving_complete/ 마커 감지 → AWS API GW 전송"""
    event = request.get_json(silent=True) or {}
    obj_name = event.get("name", "")

    # serving_complete/ 경로가 아니면 무시 (Eventarc 전체 버킷 트리거 대응)
    if not obj_name.startswith("serving_complete/"):
        log.info("[sender] 무시: %s", obj_name)
        return jsonify({"status": "skipped"}), 200

    date_str = today_kst()
    log.info("[sender] AWS API GW 전송 시작 date=%s", date_str)
    try:
        rows = _read_recommendations()
        _send_to_aws(rows, date_str)
        log.info("[sender] 전송 완료 count=%d", len(rows))
        return jsonify({"status": "ok", "count": len(rows)}), 200
    except Exception as exc:
        log.exception("[sender] 실패")
        return jsonify({"error": str(exc)}), 500


def _serialize_row(row: dict) -> dict:
    result = {}
    for k, v in row.items():
        if isinstance(v, (datetime, date)):
            result[k] = v.isoformat()
        elif isinstance(v, Decimal):
            result[k] = float(v)
        elif isinstance(v, list):
            result[k] = [item.isoformat() if isinstance(item, (datetime, date)) else item for item in v]
        else:
            result[k] = v
    return result


def _read_recommendations() -> list[dict]:
    bq = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.lifesync_serving.customer_recommendations`
        WHERE DATE(prediction_time, "Asia/Seoul") = CURRENT_DATE("Asia/Seoul")
    """
    return [_serialize_row(dict(row)) for row in bq.query(query).result()]


def _send_to_aws(rows: list[dict], date_str: str):
    if not AWS_API_GW_URL:
        log.warning("[sender] AWS_API_GW_URL 미설정 — 전송 생략")
        return

    payload = {
        "source": "lifesync-gcp",
        "date": date_str,
        "count": len(rows),
        "recommendations": rows,
    }
    resp = requests.post(AWS_API_GW_URL, json=payload, timeout=60)
    resp.raise_for_status()
    log.info("[sender] HTTP %d", resp.status_code)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
