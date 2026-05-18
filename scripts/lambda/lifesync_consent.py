import logging
import os
import urllib.request
import json
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

PRIVATE_API_URL = os.environ['PRIVATE_API_URL']
OUTPUT_BUCKET   = os.environ['OUTPUT_BUCKET']
OUTPUT_PREFIX   = os.environ.get('OUTPUT_PREFIX', 'consent/')
AWS_REGION      = os.environ.get('AWS_REGION', 'ap-northeast-2')
HTTP_TIMEOUT    = int(os.environ.get('HTTP_TIMEOUT', '600'))

KST = timezone(timedelta(hours=9))

_s3 = None


def _get_s3():
    global _s3
    if _s3 is None:
        config = Config(retries={'max_attempts': 5, 'mode': 'standard'})
        _s3 = boto3.client('s3', region_name=AWS_REGION, config=config)
    return _s3


def _fetch_to_tmp(tmp_path):
    """바이트 청크 단위로 스트리밍하여 100만 건 이상도 처리."""
    url = PRIVATE_API_URL.rstrip('/') + '/internal/consent/active'
    headers = {
        'Accept': 'application/x-ndjson',
        'Connection': 'close',
    }
    req = urllib.request.Request(url, headers=headers)

    CHUNK_SIZE = 128 * 1024  # 128KB

    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp, \
            open(tmp_path, 'wb') as f:
        while True:
            chunk = resp.read(CHUNK_SIZE)
            if not chunk:
                break
            f.write(chunk)

    with open(tmp_path, 'rb') as f:
        row_count = sum(1 for _ in f)

    return row_count


def handler(event, context):
    bucket = event.get('output_bucket', OUTPUT_BUCKET)
    prefix = event.get('output_prefix', OUTPUT_PREFIX)

    # Glue ETL과 동일하게 KST 기준 YYYY-MM-DD 형식 사용
    # Glue reads: s3://lifesync-raw/consent/dt={date_str}/
    date_str = datetime.now(KST).strftime('%Y-%m-%d')
    s3_key   = f"{prefix.strip('/')}dt={date_str}/consent_active.jsonl"
    s3_uri   = f"s3://{bucket}/{s3_key}"
    tmp_path = '/tmp/consent_active.jsonl'

    try:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        logger.info("대용량 데이터 수집 시작 → %s", s3_uri)
        row_count = _fetch_to_tmp(tmp_path)
        logger.info("수신 완료: 총 %d행 기록됨", row_count)

        if row_count == 0:
            logger.warning("데이터가 없습니다. 업로드를 중단합니다.")
            return {'statusCode': 200, 'row_count': 0}

        logger.info("S3 업로드 중...")
        _get_s3().upload_file(tmp_path, bucket, s3_key)
        logger.info("최종 업로드 완료: %s", s3_uri)

        return {
            'statusCode': 200,
            'body': json.dumps({'row_count': row_count, 's3_uri': s3_uri}),
        }

    except Exception as e:
        logger.error("처리 중 에러 발생: %s", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
        }
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
