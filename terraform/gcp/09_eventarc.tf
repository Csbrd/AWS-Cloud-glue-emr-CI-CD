# ── GCS Service Account — Eventarc용 Pub/Sub 내부 전달 권한 ──────────────────
# GCS Eventarc 트리거는 내부적으로 Pub/Sub을 transport로 사용하므로 필수
resource "google_project_iam_member" "gcs_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.project.number}@gs-project-accounts.iam.gserviceaccount.com"
}

# ── Eventarc Trigger 1: 예측 결과 → dynamic_score ────────────────────────────
# predict_runner.py가 Vertex AI 배치 잡 완료 후 GCS prediction_result/ 에 결과 저장
# → Eventarc가 감지하여 /dynamic-score 엔드포인트 호출
resource "google_eventarc_trigger" "prediction_result_complete" {
  count    = var.predict_runner_image != "" ? 1 : 0
  name     = "lifesync-prediction-result-trigger"
  location = var.region

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }
  matching_criteria {
    attribute = "bucket"
    value     = google_storage_bucket.data_lake.name
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.predict_runner[0].name
      region  = var.region
      path    = "/dynamic-score"
    }
  }

  service_account = google_service_account.eventarc.email
  depends_on      = [google_project_iam_member.gcs_pubsub_publisher]
}

# ── Eventarc Trigger 2: dynamic_score 완료 → sender ──────────────────────────
# dynamic_score.py 완료 후 GCS serving_complete/ 에 마커 파일 작성
# → Eventarc가 감지하여 sender 호출 → AWS API GW 전달
resource "google_eventarc_trigger" "serving_complete" {
  count    = var.sender_image != "" ? 1 : 0
  name     = "lifesync-serving-complete-trigger"
  location = var.region

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }
  matching_criteria {
    attribute = "bucket"
    value     = google_storage_bucket.data_lake.name
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.sender[0].name
      region  = var.region
    }
  }

  service_account = google_service_account.eventarc.email
  depends_on      = [google_project_iam_member.gcs_pubsub_publisher]
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "eventarc_prediction_trigger_name" {
  value = one(google_eventarc_trigger.prediction_result_complete[*].name)
}

output "eventarc_serving_trigger_name" {
  value = one(google_eventarc_trigger.serving_complete[*].name)
}
