# LifeSync360 — ARCHITECTURE.md
> 전체 아키텍처 구성도 설명 문서 | 아키텍처구성도_2조_V3_4_Lite.pptx 기준

---

## 전체 구성 개요

```
┌─────────────────────────────────────────────────────────────────┐
│  Local Network (On-Prem)                                        │
│  MySQL VM / Tokenization VM / Private API VM                    │
└──────────────────┬──────────────────────────────────────────────┘
                   │ IPSec Tunnel (Site-to-Site VPN)
┌──────────────────▼──────────────────────────────────────────────┐
│  AWS Cloud — ap-northeast-2 (Seoul)                             │
│                                                                 │
│  LifeSync360 Service VPC                                        │
│  Data VPC                                                       │
│  Group VM VPC                                                   │
│  Wearable VPC                                                   │
│  Management VPC                                                 │
│                                                                 │
│  Transit Gateway (허브) ↔ Site-to-Site VPN                      │
└──────────────────┬──────────────────────────────────────────────┘
                   │ Cloud VPN (IPSec Tunnel)
┌──────────────────▼──────────────────────────────────────────────┐
│  GCP — asia-northeast3 (Seoul)                                  │
│  GCS → BigQuery → Vertex AI → Cloud Run → Eventarc              │
└─────────────────────────────────────────────────────────────────┘
```

---

## AWS 아키텍처 상세

### Transit Gateway (네트워크 허브)
```
역할: 모든 VPC + On-Prem VPN 중앙 연결
Attachments:
  - LifeSync360 Service VPC
  - Data VPC
  - Management VPC
  - Group VM VPC
  - Wearable VPC
  - Site-to-Site VPN (On-Prem 연결)
  - VPN Connection (GCP Cloud VPN 연결)
```

---

### LifeSync360 Service VPC
```
Public Subnet:
  - Internet Gateway
  - AWS WAF (ALB 앞단 DDoS / SQL Injection 방어)
  - Application Load Balancer

Private Subnet:
  - Amazon ECS (Fargate / EC2)
    └ lifesync360_ECS_Main.py (메인 대시보드)
    └ user-register-api.py / user-login-api.py / user-consent-api.py
  - Amazon EC2 × 2 (ECS 노드)
  - Amazon ECR (컨테이너 이미지 저장소)
  - AWS Auto Scaling

  - Amazon Aurora PostgreSQL (Service DB)
    ├ users                      -- 플랫폼 회원 계정
    ├ master_customer            -- 그룹 대표 고객
    ├ customer_identity_map      -- 계열사 고객번호 연결
    ├ customer_pii_secure        -- PII 암호화 저장
    ├ matching_audit_log         -- 가입/매칭 이력
    ├ customer_360_profile       -- 분석/추천 프로필
    ├ consent                    -- 데이터 활용 동의
    ├ company_master             -- 그룹사 정보
    ├ category_master            -- 상품 종류 분류
    ├ product_master             -- 상품/서비스 목록
    ├ product_option             -- 상품 세부 옵션
    ├ recommend_rule             -- 추천 규칙
    ├ cross_sell_rule            -- 교차판매 룰
    ├ campaign_master            -- 캠페인 관리
    ├ customer_recommend_history -- 추천 이력
    └ customer_dashboard_log     -- 고객 행동 로그

  - Amazon DynamoDB
    └ lifesync_customer_result   -- 고객 최신 점수/등급/AI 결과 (TTL 적용)

  - Amazon ElastiCache Redis
    └ 메인 대시보드 추천 캐시 (10분 TTL)

  - AWS Lambda (Recommendation Engine)
    └ GCP AI 결과 수신 → DynamoDB 저장 → Aurora 상품 조회 → Redis 캐시

  - Amazon API Gateway (Private)
    └ GCP Cloud Run → Lambda 호출 엔드포인트
```

---

### Data VPC
```
Private Subnet (Streaming):
  - Amazon Kinesis Data Streams
    └ Shard 2개 (TPS 50건/초 기준)
    └ Kinesis Public Endpoint 경유 (웨어러블 → 인터넷 → Kinesis)
  - AWS Lambda (lifesync-wearable-stream-lambda)
    └ Event Source Mapping (Batch Size 100 / Window 1초)
    └ base64 decode → 이상치 판단 → S3 Raw 저장

Private Subnet (Processing):
  - AWS Glue
    └ Crawler × 8 (계열사별 S3 Raw 스키마 탐지)
    └ ETL Job × 7 (계열사별 1차 정제 PySpark)
       - Raw JSON 읽기 → Consent Snapshot 필터링
       - Schema 정규화 → PII 제거 → 중복 제거 → Parquet 저장
    └ Data Quality Ruleset
    └ Bookmark 활성화 (신규 데이터만 처리)

  - Amazon EMR Serverless
    └ Job 1: customer360.py      -- Customer360 Base Dataset
    └ Job 2: score_mart.py       -- Life Score 계산
    └ Job 3: health_mart.py      -- 건강 Mart
    └ Job 4: ai_feature_table.py -- ML Feature Dataset
    └ Job 5: vip_mart.py         -- VIP 후보 Mart
    └ Job 6: recommendation.py   -- 추천 Mart
    └ S3 마커 파일 방식으로 7개 Glue Job 완료 감지 후 트리거

  - AWS Lambda (lifesync-batch-loader-lambda)
    └ 계열사 Daily Batch JSON 수신 → S3 Raw 적재
    └ schema 검증 / record_count 확인 / 중복 방지

  - Amazon S3
    └ lifesync-raw      (원천 JSON 보관)
    └ lifesync-processed (정제 Parquet)
    └ lifesync-curated   (통합 Customer360 Parquet)
    └ lifesync-scripts   (Glue/EMR 스크립트)
    └ lifesync-cicd      (CI/CD 산출물)
```

---

### Group VM VPC
```
Public Subnet:
  - Internet Gateway

Private Subnet:
  - NAT Gateway
  - Amazon EC2 × 7 (계열사 VM)
    └ 은행 / 카드 / 증권 / 보험 / 온라인보험 / 헬스케어 / 병원
    └ 각 VM: MySQL에 거래 데이터 누적 → 00:20 JSON Export → Lambda 전송
    └ 배포 파일: bank_sender.py / card_sender.py 등
```

---

### Wearable VPC
```
Public Subnet:
  - Internet Gateway
  - Amazon EC2 (웨어러블 VM)
    └ 초당 50건 생성 (TPS=50)
    └ boto3 → Kinesis Public Endpoint → Kinesis Data Streams
    └ 배포 파일: wearable_sender.py
```

---

### Management VPC
```
Private Subnet:
  - Amazon EC2 (Ansible Control Node)
    └ On-Prem VM 배포 자동화
    └ VPN 경유 SSH → MySQL / Tokenization / Private API VM
    └ AWS Systems Manager Session Manager 연동
```

---

### Local Network (On-Prem)
```
VM (MySQL):
  └ 플랫폼 회원 데이터 원본 저장
  └ Global Customer ID 관리
  └ 계열사 고객번호 매핑
  └ PII 원본 보관 (customer_pii_secure)
  └ 테이블: users / master_customer / customer_identity_map
           customer_pii_secure / matching_audit_log
           customer_360_profile / consent
           bank_customer / bank_transaction 등 계열사 테이블

VM (Tokenization / Masking Server):
  └ PII 토큰화 (계좌번호 → TOK-XXXXXXXX)
  └ 마스킹 (카드번호 → ****-****-****-XXXX)
  └ KMS AES-256 암호화 (이름 / 이메일 / 주민번호)

VM (Private API):
  └ FastAPI / Flask
  └ JWT 인증
  └ Global ID 신규 생성 / 조회 API
  └ Nginx / systemd 관리
```

---

### 공통 인프라 (Shared Platform Zone)
```
AWS CloudFormation    -- IaC (AWS 인프라 전체)
Amazon CloudWatch     -- 전체 서비스 모니터링 / 알람
AWS IAM               -- 서비스별 최소 권한 Role
Amazon Route 53       -- 도메인 DNS 관리 (가비아 도메인 연동)
AWS Secrets Manager   -- DB 접속정보 / API 키 / KMS ARN
AWS Systems Manager   -- EC2 접속 / Parameter Store
Amazon EventBridge    -- 배치 스케줄러 (Glue / EMR 트리거)
Amazon SNS            -- 장애 / DQ 실패 알람
Amazon Kinesis ESM    -- Kinesis → Lambda Event Source Mapping
```

---

## CI/CD 파이프라인 구성

### 파이프라인 목록 (5개)

| 파이프라인 | 레포 | 배포 대상 | 방식 |
|-----------|------|---------|------|
| LifeSync360 Service | lifesync360-service-repo | ECS Rolling Deploy | CodeDeploy |
| Wearable EC2 | wearable-agent | EC2 재기동 | CodeDeploy |
| Group VM | groupvm-simulator | EC2 재기동 | CodeDeploy |
| Legacy System | legacy-system | Ansible Playbook | CodeCommit → Ansible |
| Data (Lambda/Glue/EMR) | lifesync-data-prod | Lambda ZIP / Glue update-job | CodeBuild CLI |

### 공통 흐름
```
Developer → GitHub Push
    ↓
GitHub Actions (Unit Test / Security Scan / Docker Build)
    ↓ 통과
CodeCommit Mirror
    ↓
CodePipeline → CodeBuild → CodeDeploy (또는 CLI 직접)
    ↓
배포 완료
```

---

## GCP 아키텍처 상세

### 네트워킹
```
VPC (Global)
  └ Subnet (asia-northeast3)
  └ Cloud VPN Gateway ↔ AWS Transit Gateway
  └ Cloud Router (BGP)
  └ Private Service Connect Endpoint
     └ bigquery.googleapis.com → PSC Endpoint IP
  └ Private Google Access
```

### 데이터 흐름
```
AWS S3 Curated
    ↓ (매일 03:00)
Storage Transfer Service
  └ Source: s3://lifesync-curated/
  └ Destination: gs://lifesync-data-lake/curated/
  └ Object Conditions: Transfer only new objects
    ↓
GCS Bucket (gs://lifesync-data-lake/)
  └ customer_360_profile / score_mart / ai_feature_table
  └ vip_candidate_mart / recommendation_mart / health_mart
    ↓ (매일 03:25 Scheduled Query)
BigQuery Load Job
  └ Dataset: lifesync_curated (6개 테이블)
  └ Dataset: lifesync_ml     (학습 데이터)
  └ Dataset: lifesync_serving (View / 서빙 레이어)
    ↓ (매일 04:00)
Cloud Scheduler → Cloud Run (lifesync-predict-runner)
    ↓
Vertex AI Batch Prediction
  └ VIP 예측 (XGBoost Classifier)
  └ 가입 예측 (XGBoost Classifier)
  └ 추천 예측 (XGBoost / Ranking)
  └ 건강점수 예측 (XGBoost Regressor)
    ↓ (매일 04:30)
BigQuery Result 저장
  └ lifesync_ml.vip_prediction_result
  └ lifesync_ml.signup_prediction_result
  └ lifesync_ml.rec_prediction_result
  └ lifesync_ml.health_prediction_result
    ↓ (매일 04:40)
Dynamic Score 재계산 (BigQuery SQL)
  └ lifesync_serving.v_user_dashboard 갱신
    ↓ (매일 05:00)
Cloud Run (lifesync-sender)
  └ Serving View 조회
  └ AWS Private API Gateway 호출
    ↓
Eventarc → Cloud Run → AWS API GW → Lambda
  └ DynamoDB 저장 (고객 최신 점수)
  └ Aurora 상품 조회
  └ ElastiCache Redis 캐시 생성
```

### BigQuery 구조
```
lifesync_curated:
  customer_360_profile   -- 고객 통합 데이터셋
  score_mart             -- Life Score
  ai_feature_table       -- ML Feature
  vip_candidate_mart     -- VIP 후보
  recommendation_mart    -- 추천 Mart
  health_mart            -- 건강 Mart

lifesync_ml:
  vip_training_data
  rec_training_data
  health_training_data
  vip_prediction_result
  signup_prediction_result
  rec_prediction_result
  health_prediction_result

lifesync_serving:
  v_customer_summary     -- 고객 통합 View
  v_vip_customer         -- VIP 고객 View
  v_recommend_top3       -- 추천 Top3 View
  v_user_dashboard       -- 최종 서빙 View
```

---

## Customer360 Life Score 구성

```
최종 Score = 금융점수 + 건강점수 + 관계점수 + 성장점수 - 리스크점수
범위: 0 ~ 100점

영역            최대 점수
금융 점수         40
  예금/잔액        15
  카드 소비        10
  투자 자산        15
건강 점수         25
  평균 걸음수      10
  Wellness        10
  수면/스트레스     5
관계 점수         15
  계열사 이용      10
  동의율           5
성장 점수         10
리스크 차감       -20

등급 체계:
  90+   → VIP
  80~89 → GOLD
  70~79 → SILVER
  60~69 → BASIC
  <60   → CARE
```

---

## 보안 아키텍처

```
PII 처리 흐름:
  On-Prem MySQL (원본 보관)
    ↓
  Tokenization Server (AES-256 KMS 암호화)
    ↓
  S3 Raw (토큰값만 저장 / PII 절대 포함 금지)

암호화:
  AWS KMS CMK (customer_pii_secure 전용)
  AES-256 암호화 대상: 이름 / 이메일 / 주민번호 / 휴대폰 / 주소

네트워크 보안:
  AWS WAF → ALB → ECS
  Site-to-Site VPN (IPSec IKEv2)
  Transit Gateway (VPC 간 트래픽 제어)
  Security Group (서비스별 최소 포트)
  Private Subnet (DB / 처리 레이어 외부 노출 없음)
```

---

## 데이터 타임라인 (전체)

| 시각 | 작업 | 담당 리소스 |
|------|------|-----------|
| 00:00 | 계열사 업무 마감 | Group VM |
| 00:10 | 계열사 Raw 생성 (MySQL → JSON) | Group VM EC2 |
| 00:20 | Batch 전송 → Lambda → S3 Raw | Lambda / S3 |
| 01:00 | Glue ETL 1차 정제 (동의 필터링) | AWS Glue |
| 01:30 | Consent Snapshot export | Lambda / MySQL |
| 02:00 | EMR Serverless Customer360 생성 | EMR Serverless |
| 03:00 | S3 → GCS 전송 | Storage Transfer |
| 03:25 | GCS → BigQuery Load Job | BigQuery |
| 03:30 | ML 학습 데이터 생성 (Scheduled Query) | BigQuery |
| 04:00 | Vertex AI Batch Prediction | Vertex AI |
| 04:30 | 예측 결과 BigQuery 저장 | BigQuery |
| 04:40 | Dynamic Score 재계산 | BigQuery SQL |
| 05:00 | Cloud Run → AWS API GW → Lambda | Cloud Run / Lambda |
| 05:00 | DynamoDB 저장 + Aurora 조회 + Redis 캐시 | Lambda |
| 05:30 | Dashboard 조회 가능 | ECS / Redis |

---

*Last Updated: 2026-04-30*
*Version: v1.0*
*Based on: 아키텍처구성도_2조_V3_4_Lite.pptx*
