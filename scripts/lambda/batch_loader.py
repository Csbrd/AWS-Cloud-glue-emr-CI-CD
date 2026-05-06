import json
import os
import logging
import boto3
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET", "lifesync-raw")
KST = timezone(timedelta(hours=9))

# 계열사별 필수 필드 정의
REQUIRED_FIELDS = {
    "bank":             ["bank_id", "global_customer_id", "balance", "transaction_amount", "transaction_date"],
    "card":             ["card_id", "global_customer_id", "spending_amount", "merchant_category", "transaction_date"],
    "securities":       ["securities_id", "global_customer_id", "asset_value", "transaction_date"],
    "insurance":        ["insurance_id", "global_customer_id", "premium_amount", "insurance_type", "transaction_date"],
    "online_insurance": ["online_insurance_id", "global_customer_id", "premium_amount", "transaction_date"],
    "healthcare":       ["healthcare_id", "global_customer_id", "visit_date", "treatment_code"],
    "hospital":         ["hospital_id", "global_customer_id", "visit_date", "diagnosis_code"],
}

# Glue ETL에서 제거하기 전 Raw 단계에서도 PII 원문 적재 금지
PII_FIELDS = {"name", "email", "rrn", "phone", "address", "account_number"}

s3 = boto3.client("s3")


def lambda_handler(event, context):
    try:
        body = _parse_body(event)
        source = body.get("source", "").lower()
        date_str = body.get("date", _today_kst())
        records = body.get("data", [])
        record_count = body.get("record_count", 0)

        _validate(source, records, record_count)
        _check_duplicate(source, date_str)

        cleaned = [_strip_pii(r) for r in records]
        _write_s3(source, date_str, cleaned)

        logger.info("[batch_loader] source=%s date=%s stored=%d", source, date_str, len(cleaned))
        return _response(200, {"source": source, "date": date_str, "stored": len(cleaned)})

    except ValueError as e:
        logger.warning("[batch_loader] 검증 실패: %s", e)
        return _response(400, {"error": str(e)})
    except DuplicateError as e:
        logger.warning("[batch_loader] 중복 적재 시도: %s", e)
        return _response(409, {"error": str(e)})
    except Exception:
        logger.exception("[batch_loader] 예기치 못한 오류")
        return _response(500, {"error": "internal server error"})


def _parse_body(event):
    # API Gateway 경유 시 body가 문자열로 전달됨
    if "body" in event:
        body = event["body"]
        return json.loads(body) if isinstance(body, str) else body
    return event


def _today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def _validate(source: str, records: list, record_count: int):
    if source not in REQUIRED_FIELDS:
        raise ValueError(f"알 수 없는 source: {source}")
    if not records:
        raise ValueError("data 필드가 비어 있습니다")
    if len(records) != record_count:
        raise ValueError(f"record_count 불일치: 선언={record_count} 실제={len(records)}")

    required = REQUIRED_FIELDS[source]
    for i, record in enumerate(records[:5]):  # 앞 5건 샘플 검증
        missing = [f for f in required if f not in record]
        if missing:
            raise ValueError(f"레코드[{i}] 필수 필드 누락: {missing}")


def _check_duplicate(source: str, date_str: str):
    key = f"{source}/{date_str}/{source}.json"
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        raise DuplicateError(f"이미 적재됨: s3://{S3_BUCKET}/{key}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise


def _strip_pii(record: dict) -> dict:
    return {k: v for k, v in record.items() if k not in PII_FIELDS}


def _write_s3(source: str, date_str: str, records: list):
    key = f"{source}/{date_str}/{source}.json"
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )


def _response(status: int, body: dict) -> dict:
    return {"statusCode": status, "body": json.dumps(body)}


class DuplicateError(Exception):
    pass
