# ── Cloud Scheduler Jobs ──────────────────────────────────────────────────────
# predict_runner만 시간 기반 트리거
# dynamic_score / sender는 09_eventarc.tf에서 Vertex AI Job 완료 이벤트 기반으로 처리

# 04:00 KST — Vertex AI 배치 예측 잡 실행
resource "google_cloud_scheduler_job" "predict_runner" {
  count            = var.predict_runner_image != "" ? 1 : 0
  name             = "lifesync-predict-runner-trigger"
  region           = var.region
  schedule         = "0 4 * * *"
  time_zone        = "Asia/Seoul"
  attempt_deadline = "300s"

  retry_config {
    retry_count          = 3
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
    max_retry_duration   = "900s"
  }

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.predict_runner[0].uri}/run"

    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloud_run_v2_service.predict_runner[0].uri
    }
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "scheduler_predict_runner_name" {
  value = one(google_cloud_scheduler_job.predict_runner[*].name)
}
