# ── Datasets ──────────────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "curated" {
  dataset_id                 = "lifesync_curated"
  location                   = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "ml" {
  dataset_id                 = "lifesync_ml"
  location                   = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "serving" {
  dataset_id                 = "lifesync_serving"
  location                   = var.bq_location
  delete_contents_on_destroy = true
}

# ── Dataset-level IAM ─────────────────────────────────────────────────────────

# Vertex AI SA: curated 피처 읽기
resource "google_bigquery_dataset_iam_member" "vertexai_curated_viewer" {
  dataset_id = google_bigquery_dataset.curated.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.vertexai.email}"
}

# Vertex AI SA: ML 예측 결과 쓰기
resource "google_bigquery_dataset_iam_member" "vertexai_ml_editor" {
  dataset_id = google_bigquery_dataset.ml.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.vertexai.email}"
}

# Vertex AI SA: 서빙 레이어 갱신 (Scheduled Query 실행 SA)
resource "google_bigquery_dataset_iam_member" "vertexai_serving_editor" {
  dataset_id = google_bigquery_dataset.serving.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.vertexai.email}"
}

# predict-runner SA: ML 예측 결과 읽기 (/dynamic-score)
resource "google_bigquery_dataset_iam_member" "predict_runner_ml_viewer" {
  dataset_id = google_bigquery_dataset.ml.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.predict_runner.email}"
}

# predict-runner SA: serving 레이어 갱신 (/dynamic-score)
resource "google_bigquery_dataset_iam_member" "predict_runner_serving_editor" {
  dataset_id = google_bigquery_dataset.serving.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.predict_runner.email}"
}

# sender SA: 서빙 뷰 조회 (AWS API GW 전달용)
resource "google_bigquery_dataset_iam_member" "sender_serving_viewer" {
  dataset_id = google_bigquery_dataset.serving.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.sender.email}"
}

# ── Scheduled Query ───────────────────────────────────────────────────────────
# BQ Data Transfer Service Agent 강제 생성
resource "google_project_service_identity" "bq_dts_agent" {
  provider = google-beta
  project  = var.project_id
  service  = "bigquerydatatransfer.googleapis.com"
}

# BQ Data Transfer Service Agent가 vertexai SA를 impersonate할 수 있도록 허용
resource "google_service_account_iam_member" "bq_dts_token_creator" {
  service_account_id = google_service_account.vertexai.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
  depends_on         = [google_project_service_identity.bq_dts_agent]
}

resource "google_bigquery_data_transfer_config" "serving_refresh" {
  display_name           = "lifesync-serving-refresh"
  location               = var.bq_location
  data_source_id         = "scheduled_query"
  schedule               = "every day 18:25"  # 03:25 KST (UTC+9 → 18:25 UTC)
  destination_dataset_id = google_bigquery_dataset.serving.dataset_id
  service_account_name   = google_service_account.vertexai.email

  params = {
    query = <<-EOT
      CREATE OR REPLACE TABLE `${var.project_id}.lifesync_serving.customer_recommendations` AS
      SELECT *
      FROM `${var.project_id}.lifesync_ml.prediction_results`
      WHERE DATE(prediction_time, "Asia/Seoul") = CURRENT_DATE("Asia/Seoul")
    EOT
  }

  depends_on = [google_service_account_iam_member.bq_dts_token_creator]
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "bq_dataset_curated" {
  value = google_bigquery_dataset.curated.dataset_id
}

output "bq_dataset_ml" {
  value = google_bigquery_dataset.ml.dataset_id
}

output "bq_dataset_serving" {
  value = google_bigquery_dataset.serving.dataset_id
}
