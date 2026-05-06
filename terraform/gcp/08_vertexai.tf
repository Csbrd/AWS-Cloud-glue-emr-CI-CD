# ── Vertex AI Dataset ─────────────────────────────────────────────────────────
resource "google_vertex_ai_dataset" "customer_features" {
  display_name        = "lifesync-customer-features"
  metadata_schema_uri = "gs://google-cloud-aiplatform/schema/dataset/metadata/tabular_1.0.0.yaml"
  region              = var.region
}

# ── Vertex AI Endpoint ────────────────────────────────────────────────────────
resource "google_vertex_ai_endpoint" "predict" {
  name         = "lifesync-predict-endpoint"
  display_name = "LifeSync360 Predict Endpoint"
  location     = var.region
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "vertex_dataset_id" {
  value = google_vertex_ai_dataset.customer_features.id
}

output "vertex_endpoint_id" {
  value = google_vertex_ai_endpoint.predict.id
}
