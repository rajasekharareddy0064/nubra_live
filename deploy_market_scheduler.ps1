# ============================================================
# Deploy Market Start/Stop Schedulers for nubra-live
# Project: stock-anaysis | Region: asia-south1
#
# Architecture:
#   05:30 AM  Cloud Scheduler -> nubra-auth-refresh (job)
#   09:10 AM  Cloud Scheduler -> nubra-live-start   (job)
#   03:40 PM  Cloud Scheduler -> nubra-live-stop    (job)
# ============================================================

$PROJECT = "stock-anaysis"
$REGION  = "asia-south1"
$IMAGE   = "gcr.io/$PROJECT/nubra-live:latest"
$SA      = "791197716058-compute@developer.gserviceaccount.com"

Write-Host "=== Deploying Market Start/Stop Schedulers ===" -ForegroundColor Cyan
Write-Host "  Project : $PROJECT"
Write-Host "  Region  : $REGION"
Write-Host ""

# --- IAM: Grant Cloud Run Admin for scaling ---
Write-Host "[1/6] Granting IAM: Cloud Run Admin..." -ForegroundColor DarkCyan
gcloud projects add-iam-policy-binding $PROJECT `
    --member="serviceAccount:$SA" `
    --role="roles/run.admin" `
    --condition=None 2>$null | Out-Null
Write-Host "  Done." -ForegroundColor Green

# --- Create/Update Start Job ---
Write-Host "`n[2/6] Creating Cloud Run Job: nubra-live-start..." -ForegroundColor DarkCyan

$startExists = gcloud run jobs describe nubra-live-start --region=$REGION --project=$PROJECT 2>$null
if ($LASTEXITCODE -eq 0) {
    gcloud run jobs update nubra-live-start `
        --region=$REGION --project=$PROJECT `
        --image=$IMAGE `
        --command="python" --args="jobs/market_start.py" `
        --memory="512Mi" --cpu=1 --task-timeout=300 --max-retries=1 `
        --set-env-vars="GCP_PROJECT_ID=$PROJECT,GCP_REGION=$REGION,CLOUD_RUN_SERVICE=nubra-live,LOG_LEVEL=INFO,HEALTH_TIMEOUT=120" 2>&1 | Out-Null
} else {
    gcloud run jobs create nubra-live-start `
        --region=$REGION --project=$PROJECT `
        --image=$IMAGE `
        --command="python" --args="jobs/market_start.py" `
        --memory="512Mi" --cpu=1 --task-timeout=300 --max-retries=1 `
        --set-env-vars="GCP_PROJECT_ID=$PROJECT,GCP_REGION=$REGION,CLOUD_RUN_SERVICE=nubra-live,LOG_LEVEL=INFO,HEALTH_TIMEOUT=120" 2>&1 | Out-Null
}
if ($LASTEXITCODE -eq 0) { Write-Host "  nubra-live-start deployed." -ForegroundColor Green }
else { Write-Host "  FAILED" -ForegroundColor Red }

# --- Create/Update Stop Job ---
Write-Host "`n[3/6] Creating Cloud Run Job: nubra-live-stop..." -ForegroundColor DarkCyan

$stopExists = gcloud run jobs describe nubra-live-stop --region=$REGION --project=$PROJECT 2>$null
if ($LASTEXITCODE -eq 0) {
    gcloud run jobs update nubra-live-stop `
        --region=$REGION --project=$PROJECT `
        --image=$IMAGE `
        --command="python" --args="jobs/market_stop.py" `
        --memory="512Mi" --cpu=1 --task-timeout=300 --max-retries=1 `
        --set-env-vars="GCP_PROJECT_ID=$PROJECT,GCP_REGION=$REGION,CLOUD_RUN_SERVICE=nubra-live,LOG_LEVEL=INFO,DRAIN_TIMEOUT=30" 2>&1 | Out-Null
} else {
    gcloud run jobs create nubra-live-stop `
        --region=$REGION --project=$PROJECT `
        --image=$IMAGE `
        --command="python" --args="jobs/market_stop.py" `
        --memory="512Mi" --cpu=1 --task-timeout=300 --max-retries=1 `
        --set-env-vars="GCP_PROJECT_ID=$PROJECT,GCP_REGION=$REGION,CLOUD_RUN_SERVICE=nubra-live,LOG_LEVEL=INFO,DRAIN_TIMEOUT=30" 2>&1 | Out-Null
}
if ($LASTEXITCODE -eq 0) { Write-Host "  nubra-live-stop deployed." -ForegroundColor Green }
else { Write-Host "  FAILED" -ForegroundColor Red }

# --- Cloud Scheduler: Start ---
Write-Host "`n[4/6] Creating Cloud Scheduler: nubra-live-start (09:10 Mon-Fri)..." -ForegroundColor DarkCyan

$uri = "https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT/jobs/nubra-live-start:run"
$schStartExists = gcloud scheduler jobs describe nubra-live-start --location=$REGION --project=$PROJECT 2>$null
if ($LASTEXITCODE -eq 0) {
    gcloud scheduler jobs update http nubra-live-start `
        --location=$REGION --project=$PROJECT `
        --schedule="10 9 * * 1-5" --time-zone="Asia/Kolkata" `
        --uri=$uri --http-method=POST `
        --oauth-service-account-email=$SA 2>&1 | Out-Null
} else {
    gcloud scheduler jobs create http nubra-live-start `
        --location=$REGION --project=$PROJECT `
        --schedule="10 9 * * 1-5" --time-zone="Asia/Kolkata" `
        --uri=$uri --http-method=POST `
        --oauth-service-account-email=$SA 2>&1 | Out-Null
}
if ($LASTEXITCODE -eq 0) { Write-Host "  Scheduler nubra-live-start created." -ForegroundColor Green }
else { Write-Host "  FAILED" -ForegroundColor Red }

# --- Cloud Scheduler: Stop ---
Write-Host "`n[5/6] Creating Cloud Scheduler: nubra-live-stop (15:40 Mon-Fri)..." -ForegroundColor DarkCyan

$uri = "https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT/jobs/nubra-live-stop:run"
$schStopExists = gcloud scheduler jobs describe nubra-live-stop --location=$REGION --project=$PROJECT 2>$null
if ($LASTEXITCODE -eq 0) {
    gcloud scheduler jobs update http nubra-live-stop `
        --location=$REGION --project=$PROJECT `
        --schedule="40 15 * * 1-5" --time-zone="Asia/Kolkata" `
        --uri=$uri --http-method=POST `
        --oauth-service-account-email=$SA 2>&1 | Out-Null
} else {
    gcloud scheduler jobs create http nubra-live-stop `
        --location=$REGION --project=$PROJECT `
        --schedule="40 15 * * 1-5" --time-zone="Asia/Kolkata" `
        --uri=$uri --http-method=POST `
        --oauth-service-account-email=$SA 2>&1 | Out-Null
}
if ($LASTEXITCODE -eq 0) { Write-Host "  Scheduler nubra-live-stop created." -ForegroundColor Green }
else { Write-Host "  FAILED" -ForegroundColor Red }

# --- Summary ---
Write-Host "`n[6/6] Summary" -ForegroundColor Cyan
Write-Host "  ================================================"
Write-Host "  05:30 AM IST  nubra-auth-refresh-daily   (existing)"
Write-Host "  09:10 AM IST  nubra-live-start           (NEW)"
Write-Host "  03:40 PM IST  nubra-live-stop            (NEW)"
Write-Host "  ================================================"
Write-Host ""
Write-Host "  Manual commands:" -ForegroundColor Gray
Write-Host "    # Execute start job now"
Write-Host "    gcloud run jobs execute nubra-live-start --region=$REGION --project=$PROJECT"
Write-Host ""
Write-Host "    # Execute stop job now"
Write-Host "    gcloud run jobs execute nubra-live-stop --region=$REGION --project=$PROJECT"
Write-Host ""
Write-Host "    # Trigger scheduler manually"
Write-Host "    gcloud scheduler jobs run nubra-live-start --location=$REGION --project=$PROJECT"
Write-Host "    gcloud scheduler jobs run nubra-live-stop --location=$REGION --project=$PROJECT"
Write-Host ""
Write-Host "  Rollback (remove schedulers but keep service running):" -ForegroundColor Gray
Write-Host "    gcloud scheduler jobs delete nubra-live-start --location=$REGION --project=$PROJECT"
Write-Host "    gcloud scheduler jobs delete nubra-live-stop --location=$REGION --project=$PROJECT"
Write-Host "    gcloud run jobs delete nubra-live-start --region=$REGION --project=$PROJECT"
Write-Host "    gcloud run jobs delete nubra-live-stop --region=$REGION --project=$PROJECT"
Write-Host ""
