# LifeSync360 — CLAUDE.md
> Claude Code IaC 작업 가이드 | 아키텍처구성도_2조_V3_4_Lite.pptx 최종 기준

---

## 프로젝트 개요

| 항목 | 내용 |
|------|------|
| 프로젝트명 | LifeSync360 통합 데이터 인프라 구축 |
| 팀 구성 | 3인 팀 |
| 기간 | 7주 (2026-04-02 ~ 2026-06-02) |
| AWS IaC | AWS CloudFormation |
| GCP IaC | Terraform |
| 자동화 | Ansible (On-Prem VM 배포) |
| 리전 | AWS: ap-northeast-2 (서울) 단일 / GCP: asia-northeast3 |

---

## 클라우드 구성 요약

```
Local (On-Prem) VM
  └ MySQL / Tokenization / Private API
  └ IPSec Tunnel (Site-to-Site VPN)
        ↕
AWS Cloud (ap-northeast-2) — 메인
  └ LifeSync360 Service VPC
  └ Data VPC
  └ Group VM VPC
  └ Wearable VPC
  └ Management VPC
  └ Transit Gateway (허브)
        ↕
GCP (asia-northeast3) — 분석 · AI
  └ GCS → BigQuery → Vertex AI → Cloud Run
```

---

## IaC 디렉토리 구조

```
lifesync360/
├── AGENTS.md
├── ARCHITECTURE.md
├── CLAUDE.md                        ← 이 파일
│
├── cloudformation/                  ← AWS IaC (CloudFormation)
│   ├── networking/
│   │   ├── vpc.yaml                 ← VPC / Subnet / IGW / NAT GW
│   │   ├── transit-gateway.yaml     ← Transit Gateway / Attachment
│   │   ├── vpn.yaml                 ← Site-to-Site VPN / CGW
│   │   └── security-groups.yaml    ← 서비스별 SG
│   ├── compute/
│   │   ├── ec2-onprem.yaml         ← MySQL / Tokenization / API VM
│   │   ├── ec2-groupvm.yaml        ← 계열사 VM × 7
│   │   ├── ec2-wearable.yaml       ← 웨어러블 VM
│   │   ├── ec2-ansible.yaml        ← Ansible Control Node
│   │   ├── ecs-cluster.yaml        ← ECS Cluster / ECR
│   │   └── autoscaling.yaml        ← Auto Scaling Group
│   ├── loadbalancer/
│   │   ├── alb.yaml                ← ALB / Target Group / Listener
│   │   └── waf.yaml                ← WAF WebACL
│   ├── storage/
│   │   ├── s3-buckets.yaml         ← S3 버킷 5개 + Lifecycle
│   │   └── s3-policies.yaml        ← 버킷 정책
│   ├── database/
│   │   ├── aurora.yaml             ← Aurora PostgreSQL
│   │   ├── elasticache.yaml        ← Redis cache.t3.micro
│   │   └── dynamodb.yaml           ← lifesync_customer_result
│   ├── data/
│   │   ├── kinesis.yaml            ← Kinesis Data Streams (2 Shard)
│   │   ├── glue.yaml               ← Crawler / Job / DQ Ruleset
│   │   ├── emr-serverless.yaml     ← EMR Serverless Application
│   │   └── lambda.yaml             ← Lambda 함수 생성
│   ├── integration/
│   │   ├── apigateway.yaml         ← API Gateway (Private)
│   │   ├── stepfunctions.yaml      ← Step Functions
│   │   └── eventbridge.yaml        ← EventBridge Scheduler
│   ├── cicd/
│   │   ├── codecommit.yaml         ← CodeCommit Repository × 5
│   │   ├── codepipeline.yaml       ← CodePipeline × 5
│   │   ├── codebuild.yaml          ← CodeBuild Project
│   │   └── codedeploy.yaml         ← CodeDeploy App / Group
│   ├── security/
│   │   ├── iam.yaml                ← IAM Role / Policy
│   │   ├── kms.yaml                ← KMS CMK
│   │   ├── secrets-manager.yaml    ← Secrets Manager
│   │   └── cloudtrail.yaml         ← CloudTrail
│   └── monitoring/
│       ├── cloudwatch.yaml         ← Log Group / Alarm
│       ├── sns.yaml                ← SNS Topic
│       └── route53.yaml            ← Hosted Zone / Record
│
├── terraform/                       ← GCP IaC (Terraform)
│   └── gcp/
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       ├── networking/
│       │   ├── vpc.tf              ← VPC / Subnet
│       │   ├── vpn.tf              ← Cloud VPN / Cloud Router
│       │   └── psc.tf              ← Private Service Connect
│       ├── storage/
│       │   └── gcs.tf              ← GCS 버킷
│       ├── transfer/
│       │   └── sts.tf              ← Storage Transfer Job
│       ├── bigquery/
│       │   ├── datasets.tf         ← Dataset 생성
│       │   └── scheduled_query.tf  ← Scheduled Query
│       ├── cloudrun/
│       │   └── cloudrun.tf         ← Cloud Run 서비스
│       ├── vertexai/
│       │   └── vertexai.tf         ← Vertex AI Endpoint / Dataset
│       ├── eventarc/
│       │   └── eventarc.tf         ← Eventarc Trigger
│       ├── scheduler/
│       │   └── scheduler.tf        ← Cloud Scheduler
│       └── security/
│           ├── iam.tf              ← Service Account / IAM
│           └── secret_manager.tf  ← Secret Manager
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
│   │   ├── batch_loader.py         ← 계열사 Batch → S3 Raw
│   │   ├── wearable_stream.py      ← Kinesis → S3 Raw
│   │   └── recommendation_engine.py← AI 결과 → DynamoDB / Aurora / Redis
│   ├── cloudrun/
│   │   ├── predict_runner.py       ← Vertex AI Batch Prediction 실행
│   │   ├── sender.py               ← Serving View → AWS API GW
│   │   └── Dockerfile
│   └── init/
│       ├── setup_dummy_data.py     ← 100만명 초기 더미 생성
│       └── schema.sql              ← Aurora DDL 전체
│
├── ansible/
│   ├── inventory/hosts.yml
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
        ├── service_cicd.yml        ← ECS Rolling Deploy
        ├── wearable_cicd.yml       ← EC2 CodeDeploy
        ├── groupvm_cicd.yml        ← EC2 CodeDeploy
        ├── legacy_cicd.yml         ← Ansible 배포
        └── data_cicd.yml           ← Lambda / Glue / EMR 배포
```

---

## CloudFormation 핵심 설정값

### 네트워크 CIDR (변경 금지)
```yaml
VPCs:
  LifeSync360 Service VPC:  10.0.0.0/16
  Data VPC:                 10.1.0.0/16
  Group VM VPC:             10.2.0.0/16
  Wearable VPC:             10.3.0.0/16
  Management VPC:           10.4.0.0/16
  On-Prem (Local):          192.168.10.0/24
  GCP VPC:                  172.16.0.0/16
```

### EC2 인스턴스 타입
```yaml
MySQL EC2:          t3.medium (2vCPU / 4GB)
Tokenization EC2:   t3.medium (2vCPU / 4GB)
Private API EC2:    t3.small  (2vCPU / 2GB)
Ansible EC2:        t3.small  (2vCPU / 2GB)
계열사 VM × 7:     t3.small  (2vCPU / 2GB)
웨어러블 VM:        t3.small  (2vCPU / 2GB)

운영 패턴: 9시간 × 5일 × 4주 = 180시간/월
```

### Aurora 설정
```yaml
Engine:       aurora-postgresql
InstanceClass: db.t3.medium
MultiAZ:      false          ← 더미 프로젝트 Single-AZ 유지
BackupRetentionPeriod: 7
StorageEncrypted: true
```

### ElastiCache 설정
```yaml
Engine:       redis
CacheNodeType: cache.t3.micro
NumCacheNodes: 1             ← Single Node 유지
Serverless:   false          ← Serverless 절대 사용 금지
```

### DynamoDB 설정
```yaml
TableName: lifesync_customer_result
PartitionKey: global_id (String)
SortKey:      update_time (String)
BillingMode:  PAY_PER_REQUEST
TTL:          ttl (Number)
```

### S3 버킷 설정
```yaml
Buckets:
  lifesync-raw:
    Lifecycle: 30일 후 Glacier Instant Retrieval
  lifesync-processed:
    Lifecycle: 30일 후 Glacier
  lifesync-curated:
    VersioningEnabled: true
  lifesync-scripts:
    퍼블릭 액세스 차단
  lifesync-cicd:
    CI/CD 산출물 전용
```

### Kinesis 설정
```yaml
ShardCount: 2
RetentionPeriodHours: 24
EventSourceMapping:
  FunctionName: lifesync-wearable-stream-lambda
  StartingPosition: LATEST
  BatchSize: 100
  BisectBatchOnFunctionError: true
  ParallelizationFactor: 2
```

### Glue Job 설정
```yaml
WorkerType: G.1X
NumberOfWorkers: 2            ← 1차 ETL 최소값
GlueVersion: "4.0"
DefaultArguments:
  --job-bookmark-option: job-bookmark-enable   ← 반드시 활성화
  --enable-metrics: "true"
```

### EMR Serverless 설정
```yaml
Type: SPARK
ReleaseLabel: emr-6.15.0
AutoStopConfiguration:
  Enabled: true
  IdleTimeoutMinutes: 15
MaximumCapacity:
  Cpu: "40 vCPU"
  Memory: "160 GB"
```

---

## Terraform (GCP) 핵심 설정값

```hcl
# Provider
provider "google" {
  project = var.gcp_project_id
  region  = "asia-northeast3"
}

# Storage Transfer Job
schedule:
  repeat_interval: "86400s"    # 매일 1회
  start_time: 03:00 Asia/Seoul

# BigQuery Scheduled Query
schedule: "25 3 * * *"         # 매일 03:25

# Cloud Scheduler (Vertex AI)
schedule: "0 4 * * *"          # 매일 04:00

# Cloud Scheduler (Dynamic Score)
schedule: "40 4 * * *"         # 매일 04:40

# Cloud Scheduler (AWS 전달)
schedule: "0 5 * * *"          # 매일 05:00
```

---

## ID 체계 (변경 금지)

| ID | 형식 | 발급 주체 |
|----|------|---------|
| ls_user_id | LS-YYYYMMDD-XXXXXX | ECS 회원가입 API |
| global_customer_id | G + 8자리숫자 | On-Prem Private API |
| pii_token | UUID v4 | Tokenization Server |
| bank_id | BNK-XXXXXXXX | 은행 VM |
| card_id | CRD-XXXXXXXX | 카드 VM |

---

## Secrets Manager 키 목록

```
AWS Secrets Manager (ap-northeast-2):
  lifesync/aurora/credentials       → Aurora 접속 정보
  lifesync/mysql/credentials        → On-Prem MySQL 접속
  lifesync/gcp/service-account-key  → GCP 연동 키
  lifesync/glue/env                 → Glue 환경변수
  lifesync/kms/arn                  → KMS Key ARN

GCP Secret Manager (asia-northeast3):
  lifesync-aws-access-key-id        → STS S3 읽기 전용
  lifesync-aws-secret-access-key    → STS S3 읽기 전용
  lifesync-aws-api-gw-url           → AWS Private API GW 엔드포인트
```

---

## CI/CD 파이프라인 (5개)

| 파이프라인 | GitHub 레포 | CodeCommit 레포 | 배포 방식 |
|-----------|-----------|---------------|---------|
| LifeSync360 Service | lifesync360-service | lifesync360-service-repo | ECS Rolling |
| Wearable | wearable-agent | wearable-agent | EC2 CodeDeploy |
| Group VM | groupvm-simulator | groupvm-simulator | EC2 CodeDeploy |
| Legacy (On-Prem) | - | legacy-system | Ansible |
| Data (Lambda/Glue/EMR) | lifesync-data-platform | lifesync-data-prod | CodeBuild CLI |

### GitHub Actions 공통 체크 (3가지)
```
1. Unit Test    → pytest (Mock 사용, 실제 AWS 리소스 접근 금지)
2. Security Scan → Gitleaks + Bandit + pip-audit
3. Docker Build  → docker build + Trivy 이미지 스캔
```

### Data 파이프라인 배포 (buildspec.yml 핵심)
```bash
# Glue Job 업데이트
aws s3 sync scripts/glue/ s3://lifesync-scripts/glue/
aws glue update-job --job-name lifesync-bank-etl \
  --job-update file://glue-update.json

# EMR Serverless 스크립트 업데이트
aws s3 sync scripts/emr/ s3://lifesync-scripts/emr/

# Lambda 배포
zip function.zip lambda_batch_loader.py
aws lambda update-function-code \
  --function-name lifesync-batch-loader \
  --zip-file fileb://function.zip
```

---

## 데이터 파이프라인 핵심 규칙

### Glue ETL 처리 순서 (1차)
```python
1. S3 Raw JSON 읽기 (Bookmark 활성화)
2. On-Prem MySQL consent 테이블 조회 (JDBC)
3. is_consented = TRUE 고객만 필터링
4. Schema 정규화 (타입 통일 / 컬럼명 표준화)
5. PII 컬럼 제거 (이름 / 이메일 / 주민번호 원문)
6. 중복 제거
7. S3 Processed Parquet 저장 (Snappy 압축)
8. S3 마커 파일 생성 (s3://lifesync-processed/_markers/YYYY-MM-DD/계열사.done)
```

### EMR Serverless 트리거 조건
```python
# Glue Job 7개 완료 후 자동 트리거
# S3 마커 파일 방식 (DynamoDB 불필요)

def check_all_complete():
    subsidiaries = [
        'bank', 'card', 'securities', 'insurance',
        'online_insurance', 'healthcare', 'hospital'
    ]
    # 7개 .done 파일 확인 후 EMR start_job_run() 호출
```

### EMR Job 실행 순서 (의존성 있음)
```
Step 1: customer360.py      (Base — 모든 Job의 기반)
Step 2: score_mart.py       (customer360 완료 후)
Step 3: ai_feature_table.py (customer360 + score 완료 후)
Step 4: vip_mart.py         (customer360 + score 기반)
Step 5: recommendation.py   (customer360 + 행동로그 + Rule)
Step 6: health_mart.py      (헬스케어 + 병원 + 웨어러블)
```

### GCP BigQuery Load Job
```python
# Dataflow 사용 안 함 → BigQuery Load Job으로 대체
# 변환/정제 없이 GCS Parquet → BigQuery 직접 적재

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    autodetect=True,
)
```

---

## 절대 하지 말아야 할 것

```
❌ AWS Access Key / Secret Key 코드 하드코딩
❌ PII 원문 (이름/주민번호/계좌번호) S3 Raw 적재
❌ Aurora Single-AZ → Multi-AZ 임의 변경 (비용 2배)
❌ ElastiCache Serverless 사용 (노드 기반만 사용)
❌ Glue Job Bookmark 비활성화
❌ CIDR 대역 임의 변경 (VPN 라우팅 충돌 발생)
❌ global_customer_id 임의 생성 (Private API만 가능)
❌ BigQuery 테이블 파티션 필터 없이 전체 스캔
❌ GCP Dataflow 사용 (BigQuery Load Job으로 대체 확정)
❌ CloudFormation 대신 Terraform으로 AWS 리소스 생성
❌ Terraform 대신 CloudFormation으로 GCP 리소스 생성
```

---

## 구축 순서 (의존성 기준)

```
Phase 1. 네트워크 기반
  1-1. CIDR 설계 확정
  1-2. On-Prem 공인 IP 확보
  1-3. AWS VPC × 5개 생성
  1-4. Transit Gateway 생성
  1-5. Site-to-Site VPN 생성
  1-6. GCP VPC / Cloud VPN 생성
  1-7. VPN Tunnel UP 확인
  1-8. 전체 통신 테스트

Phase 2. 보안 기반
  2-1. IAM Role / Policy
  2-2. KMS CMK 생성
  2-3. Secrets Manager 시크릿 등록
  2-4. GCP Secret Manager 등록

Phase 3. 스토리지 / DB
  3-1. S3 버킷 5개 생성
  3-2. Aurora 생성 + 스키마 DDL 실행
  3-3. ElastiCache Redis 생성
  3-4. DynamoDB 테이블 생성
  3-5. GCS 버킷 생성

Phase 4. 컴퓨팅
  4-1. On-Prem VM 3대 구성 (Ansible)
  4-2. 계열사 VM EC2 × 7
  4-3. 웨어러블 VM EC2
  4-4. Ansible Control Node EC2
  4-5. ECS Cluster / ECR

Phase 5. 데이터 수집
  5-1. Kinesis Data Streams
  5-2. API Gateway (Private)
  5-3. Lambda 함수 배포
  5-4. Event Source Mapping 설정

Phase 6. ETL
  6-1. Glue Crawler / Job / DQ 생성
  6-2. EMR Serverless Application 생성
  6-3. EventBridge Scheduler 설정

Phase 7. GCP 데이터 처리
  7-1. Storage Transfer Job 생성
  7-2. BigQuery Dataset / Scheduled Query
  7-3. Cloud Run 배포
  7-4. Vertex AI Endpoint / 모델 학습·배포
  7-5. Eventarc Trigger 설정
  7-6. Cloud Scheduler 설정

Phase 8. CI/CD
  8-1. CodeCommit 레포 생성
  8-2. CodePipeline / CodeBuild / CodeDeploy 설정
  8-3. GitHub Actions Workflow 작성
  8-4. GitHub Secrets 등록

Phase 9. 더미 데이터 + 검증
  9-1. 100만명 초기 더미 데이터 생성
  9-2. 전체 파이프라인 E2E 테스트
  9-3. CloudWatch 알람 / 모니터링 확인
```

---

*Last Updated: 2026-04-30*
*Version: v2.0 (Final)*
*Based on: 아키텍처구성도_2조_V3_4_Lite.pptx*
