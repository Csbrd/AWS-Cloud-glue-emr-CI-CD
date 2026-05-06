# LifeSync360 — AGENTS.md
> AI 에이전트(Claude Code 등)가 이 프로젝트를 이해하고 작업하기 위한 최상위 가이드

---

## 프로젝트 한 줄 요약

**건강·금융·보험 데이터를 통합하여 100만 고객에게 개인 맞춤형 AI 추천을 제공하는 하이브리드 + 멀티클라우드 데이터 인프라 플랫폼**

---

## 문서 구조

```
AGENTS.md                          ← AI 에이전트 최상위 가이드 (이 파일)
ARCHITECTURE.md                    ← 전체 아키텍처 구성도 설명
CLAUDE.md                          ← Claude Code IaC 작업 가이드

docs/
├── design-docs/
│   ├── index.md                   ← 설계 문서 목차
│   ├── network-design.md          ← VPC / CIDR / VPN 네트워크 설계
│   ├── data-pipeline.md           ← Raw → Processed → Curated 파이프라인
│   ├── security-design.md         ← PII 토큰화 / KMS / IAM 설계
│   └── scoring-rules.md           ← Customer360 / Life Score 규칙 정의
│
├── exec-plans/
│   ├── active/
│   │   ├── phase1-network.md      ← 1단계: 네트워크 구축 계획
│   │   ├── phase2-data.md         ← 2단계: 데이터 파이프라인 구축 계획
│   │   └── phase3-ai.md           ← 3단계: AI 모델 학습·배포 계획
│   ├── completed/
│   │   └── .gitkeep
│   └── tech-debt-tracker.md       ← 기술 부채 추적
│
├── generated/
│   ├── db-schema-onprem.md        ← On-Prem MySQL 스키마 (자동 생성)
│   ├── db-schema-aurora.md        ← Aurora PostgreSQL 스키마 (자동 생성)
│   └── db-schema-dynamodb.md      ← DynamoDB 스키마 (자동 생성)
│
├── product-specs/
│   ├── index.md                   ← 제품 스펙 목차
│   ├── user-registration.md       ← 회원가입 프로세스 스펙
│   ├── user-consent.md            ← 데이터 동의 프로세스 스펙
│   ├── dashboard.md               ← 메인 대시보드 스펙
│   └── recommendation-engine.md   ← 추천 엔진 스펙
│
├── references/
│   ├── aws-services-list.md       ← AWS 서비스·리소스 전체 목록
│   ├── gcp-services-list.md       ← GCP 서비스·리소스 전체 목록
│   ├── pricing-model.md           ← 비용 최적화 모델
│   ├── cidr-table.md              ← IP 대역 설계표
│   └── vpn-config-reference.md    ← Site-to-Site VPN 설정 참조값
│
├── PIPELINE.md                    ← 데이터 파이프라인 전체 흐름
├── CICD.md                        ← CI/CD 파이프라인 구성
├── INFRA.md                       ← IaC 구축 목록 (Terraform vs 직접)
├── SECURITY.md                    ← 보안 정책 (PII / KMS / VPN)
├── DATA.md                        ← 데이터 정의·스키마·규칙
├── AI.md                          ← Vertex AI 모델 학습·배포 가이드
└── RELIABILITY.md                 ← 장애 대응 / 모니터링 / 알람
```

---

## 클라우드 구성 한눈에

```
┌──────────────────────────────────────────────────────────┐
│  Local (On-Prem)                                         │
│  MySQL VM / Tokenization VM / Private API VM             │
│  IPSec Tunnel ↕                                          │
├──────────────────────────────────────────────────────────┤
│  AWS Cloud (ap-northeast-2, Seoul) — 메인 인프라          │
│                                                          │
│  LifeSync360 VPC   Data VPC   Management VPC            │
│  Group VM VPC      Wearable VPC                          │
│                                                          │
│  Transit Gateway (허브) ↔ Site-to-Site VPN               │
├──────────────────────────────────────────────────────────┤
│  GCP (asia-northeast3, Seoul) — 분석 · AI               │
│                                                          │
│  GCS → BigQuery → Vertex AI → Cloud Run                 │
│  Cloud VPN ↔ AWS Transit Gateway                         │
└──────────────────────────────────────────────────────────┘
```

---

## 핵심 데이터 흐름 (타임라인)

| 시각 | 작업 |
|------|------|
| 00:00 | 계열사 업무 마감 |
| 00:10 | 계열사 Raw 생성 (MySQL → JSON) |
| 00:20 | Group VM → Lambda → S3 Raw 적재 |
| 01:00 | Glue ETL (동의 필터링 + 표준화) |
| 01:30 | Consent Snapshot export |
| 02:00 | EMR Serverless (Customer360 Curated 생성) |
| 03:00 | Storage Transfer Service (S3 → GCS) |
| 03:25 | BigQuery Load Job (GCS → BigQuery) |
| 03:30 | BigQuery Scheduled Query (ML 학습 데이터 생성) |
| 04:00 | Vertex AI Batch Prediction |
| 04:30 | 예측 결과 BigQuery 저장 |
| 04:40 | Dynamic Score 재계산 |
| 05:00 | Cloud Run → AWS API GW → Lambda → DynamoDB/Aurora |
| 05:30 | Dashboard 조회 가능 |

---

## 에이전트 작업 규칙

### 필수 확인 사항
```
1. CLAUDE.md 를 먼저 읽고 시작할 것
2. 네트워크 CIDR 변경 금지 (cidr-table.md 참조)
3. PII 관련 코드는 반드시 SECURITY.md 확인 후 작성
4. Aurora 스키마 변경 시 generated/db-schema-aurora.md 업데이트
5. 비용에 영향을 주는 리소스 변경 시 pricing-model.md 반영
```

### 코드 작성 원칙
```
IaC (Terraform):
  - 모든 리소스에 tag 필수
    tags = { Project = "lifesync360", Env = "dev" }
  - 변수는 variables.tf에 선언
  - 하드코딩 금지 (Secrets Manager / 환경변수 사용)

Python (Glue / EMR / Lambda):
  - ls_user_id NULL 체크 필수
  - PII 컬럼 (name / email / rrn 등) 로그 출력 금지
  - Glue Job은 반드시 Bookmark 활성화

SQL (BigQuery):
  - 전체 스캔 금지 (WHERE 절 파티션 필터 필수)
  - CREATE OR REPLACE 사용 시 주석으로 이유 명시
```

### 절대 하지 말아야 할 것
```
❌ AWS Access Key / Secret Key 코드에 하드코딩
❌ PII 데이터 (이름 / 주민번호 / 계좌번호 원문) S3 Raw 적재
❌ Aurora Single-AZ → Multi-AZ 임의 변경 (비용 2배)
❌ ElastiCache Serverless 사용 (노드 기반 사용)
❌ Glue Job Bookmark 비활성화
❌ BigQuery 테이블 파티션 필터 없이 전체 스캔
❌ Global ID (global_customer_id) 임의 생성 (MySQL Private API만 생성 가능)
```

---

## 주요 ID 체계

| ID | 형식 | 발급 주체 | 설명 |
|----|------|---------|------|
| ls_user_id | LS-YYYYMMDD-XXXXXX | LifeSync360 ECS | 플랫폼 회원번호 |
| global_customer_id | G + 8자리숫자 | On-Prem Private API | 그룹 대표 고객 ID |
| pii_token | UUID v4 | Tokenization Server | PII 참조 토큰 |
| bank_id / card_id 등 | 계열사별 포맷 | 각 계열사 VM | 계열사 고객번호 |

---

## 환경변수 / 시크릿 참조

```
AWS Secrets Manager (Region 1):
  lifesync/aurora/credentials       → Aurora 접속 정보
  lifesync/mysql/credentials        → MySQL 접속 정보
  lifesync/gcp/service-account-key  → GCP 연동 키
  lifesync/glue/env                 → Glue 환경변수
  lifesync/kms/arn                  → KMS Key ARN

GCP Secret Manager:
  lifesync-aws-access-key-id        → STS용 AWS Access Key
  lifesync-aws-secret-access-key    → STS용 AWS Secret Key
  lifesync-aws-api-gw-url           → AWS API GW 엔드포인트
```

---

## 리포지토리 구조

```
lifesync360/
├── AGENTS.md
├── ARCHITECTURE.md
├── CLAUDE.md
├── docs/                          ← 위 문서 구조
│
├── cloudformation/
│   ├── aws/
│   │   ├── main.tf
│   │   ├── networking/
│   │   ├── ec2/
│   │   ├── ecs/
│   │   ├── s3/
│   │   ├── aurora/
│   │   ├── elasticache/
│   │   ├── dynamodb/
│   │   ├── glue/
│   │   ├── lambda/
│   │   ├── kinesis/
│   │   ├── apigateway/
│   │   ├── stepfunctions/
│   │   ├── eventbridge/
│   │   ├── cicd/
│   │   └── security/
├── terraform/   
│   └── gcp/
│       ├── main.tf
│       ├── networking/
│       ├── storage/
│       ├── bigquery/
│       ├── cloudrun/
│       ├── vertexai/
│       └── security/
│
├── scripts/
│   ├── glue/
│   │   ├── bank_etl.py
│   │   ├── card_etl.py
│   │   ├── securities_etl.py
│   │   ├── insurance_etl.py
│   │   ├── online_insurance_etl.py
│   │   ├── healthcare_etl.py
│   │   └── hospital_etl.py
│   ├── emr/
│   │   ├── customer360.py
│   │   ├── score_mart.py
│   │   ├── health_mart.py
│   │   ├── ai_feature_table.py
│   │   ├── vip_mart.py
│   │   └── recommendation.py
│   ├── lambda/
│   │   ├── batch_loader.py
│   │   ├── wearable_stream.py
│   │   └── recommendation_engine.py
│   ├── cloudrun/
│   │   ├── predict_runner.py
│   │   ├── sender.py
│   │   └── Dockerfile
│   └── init/
│       ├── setup_dummy_data.py
│       └── schema.sql
│
├── ansible/
│   ├── inventory/
│   │   └── hosts.yml
│   ├── playbooks/
│   │   ├── site.yml
│   │   ├── mysql.yml
│   │   ├── tokenization.yml
│   │   └── private_api.yml
│   └── roles/
│       ├── mysql/
│       ├── tokenization/
│       └── private_api/
│
└── .github/
    └── workflows/
        ├── service_cicd.yml
        ├── wearable_cicd.yml
        ├── groupvm_cicd.yml
        ├── legacy_cicd.yml
        └── data_cicd.yml
```

---

## 참고 문서 바로가기

| 목적 | 문서 |
|------|------|
| 전체 아키텍처 파악 | `ARCHITECTURE.md` |
| IaC 작업 시작 | `CLAUDE.md` |
| 데이터 파이프라인 이해 | `docs/PIPELINE.md` |
| CI/CD 파이프라인 이해 | `docs/CICD.md` |
| 보안 정책 확인 | `docs/SECURITY.md` |
| AI 모델 작업 | `docs/AI.md` |
| 스키마 확인 | `docs/generated/` |
| 비용 확인 | `docs/references/pricing-model.md` |
| VPN 설정 참조 | `docs/references/vpn-config-reference.md` |

---

*Last Updated: 2025-04-30*
*Version: v1.0*
*Based on: 아키텍처구성도_2조_V3_4_Lite.pptx*
