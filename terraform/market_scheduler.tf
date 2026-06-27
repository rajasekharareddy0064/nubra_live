# ============================================================
# Terraform: Market Start/Stop Schedulers
# Add this to your existing Terraform configuration.
# ============================================================

locals {
  project    = "stock-anaysis"
  region     = "asia-south1"
  timezone   = "Asia/Kolkata"
  image      = "gcr.io/stock-anaysis/nubra-live:latest"
  compute_sa = "791197716058-compute@developer.gserviceaccount.com"
}

# --- Cloud Run Job: Market Start ---

resource "google_cloud_run_v2_job" "market_start" {
  name     = "nubra-live-start"
  location = local.region
  project  = local.project

  template {
    template {
      containers {
        image   = local.image
        command = ["python"]
        args    = ["jobs/market_start.py"]

        resources {
          limits = {
            memory = "512Mi"
            cpu    = "1"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = local.project
        }
        env {
          name  = "GCP_REGION"
          value = local.region
        }
        env {
          name  = "CLOUD_RUN_SERVICE"
          value = "nubra-live"
        }
        env {
          name  = "LOG_LEVEL"
          value = "INFO"
        }
        env {
          name  = "HEALTH_TIMEOUT"
          value = "120"
        }
      }

      timeout         = "300s"
      max_retries     = 1
      service_account = local.compute_sa
    }
  }
}

# --- Cloud Run Job: Market Stop ---

resource "google_cloud_run_v2_job" "market_stop" {
  name     = "nubra-live-stop"
  location = local.region
  project  = local.project

  template {
    template {
      containers {
        image   = local.image
        command = ["python"]
        args    = ["jobs/market_stop.py"]

        resources {
          limits = {
            memory = "512Mi"
            cpu    = "1"
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = local.project
        }
        env {
          name  = "GCP_REGION"
          value = local.region
        }
        env {
          name  = "CLOUD_RUN_SERVICE"
          value = "nubra-live"
        }
        env {
          name  = "LOG_LEVEL"
          value = "INFO"
        }
        env {
          name  = "DRAIN_TIMEOUT"
          value = "30"
        }
      }

      timeout         = "300s"
      max_retries     = 1
      service_account = local.compute_sa
    }
  }
}

# --- Cloud Scheduler: Start (09:10 AM Mon-Fri IST) ---

resource "google_cloud_scheduler_job" "market_start" {
  name      = "nubra-live-start"
  project   = local.project
  region    = local.region
  schedule  = "10 9 * * 1-5"
  time_zone = local.timezone

  http_target {
    http_method = "POST"
    uri         = "https://${local.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${local.project}/jobs/nubra-live-start:run"

    oauth_token {
      service_account_email = local.compute_sa
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  retry_config {
    retry_count = 2
  }
}

# --- Cloud Scheduler: Stop (03:40 PM Mon-Fri IST) ---

resource "google_cloud_scheduler_job" "market_stop" {
  name      = "nubra-live-stop"
  project   = local.project
  region    = local.region
  schedule  = "40 15 * * 1-5"
  time_zone = local.timezone

  http_target {
    http_method = "POST"
    uri         = "https://${local.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${local.project}/jobs/nubra-live-stop:run"

    oauth_token {
      service_account_email = local.compute_sa
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  retry_config {
    retry_count = 2
  }
}

# --- IAM: Service account needs run.admin for scaling ---

resource "google_project_iam_member" "run_admin" {
  project = local.project
  role    = "roles/run.admin"
  member  = "serviceAccount:${local.compute_sa}"
}

resource "google_project_iam_member" "run_invoker" {
  project = local.project
  role    = "roles/run.invoker"
  member  = "serviceAccount:${local.compute_sa}"
}
