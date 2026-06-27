# ============================================================
# Deploy the nubra-auth-refresh Cloud Run Job + Cloud Scheduler
# Project: stock-anaysis
# Region: asia-south1
# ============================================================

$PROJECT = "stock-anaysis"
$REGION  = "asia-south1"
$JOB     = "nubra-auth-refresh"
$IMAGE   = "gcr.io/$PROJECT/nubra-live:latest"
$SA_NUM  = "791197716058"

Write-Host "=== Deploying Auth Refresh Job ===" -ForegroundColor Cyan

# --- Step 1: Enable required APIs ---
Write-Host "`n[1/6] Enabling APIs..." -ForegroundColor DarkCyan
gcloud services enable run.googleapis.com --project=$PROJECT 2>$null
gcloud services enable cloudscheduler.googleapis.com --project=$PROJECT 2>$null
gcloud services enable secretmanager.googleapis.com --project=$PROJECT 2>$null
Write-Host "  Done." -ForegroundColor Green

# --- Step 2: Grant IAM permissions ---
Write-Host "`n[2/6] Granting IAM permissions..." -ForegroundColor DarkCyan

$COMPUTE_SA = "$SA_NUM-compute@developer.gserviceaccount.com"

# Compute SA needs Secret Manager access (to read+write secrets)
gcloud projects add-iam-policy-binding $PROJECT `
    --member="serviceAccount:$COMPUTE_SA" `
    --role="roles/secretmanager.secretAccessor" `
    --condition=None 2>$null | Out-Null

gcloud projects add-iam-policy-binding $PROJECT `
    --member="serviceAccount:$COMPUTE_SA" `
    --role="roles/secretmanager.secretVersionAdder" `
    --condition=None 2>$null | Out-Null

# Cloud Scheduler SA needs permission to invoke the job
gcloud projects add-iam-policy-binding $PROJECT `
    --member="serviceAccount:$COMPUTE_SA" `
    --role="roles/run.invoker" `
    --condition=None 2>$null | Out-Null

Write-Host "  IAM bindings set." -ForegroundColor Green

# --- Step 3: Create/update the Cloud Run Job ---
Write-Host "`n[3/6] Creating Cloud Run Job '$JOB'..." -ForegroundColor DarkCyan

$jobExists = gcloud run jobs describe $JOB --region=$REGION --project=$PROJECT 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  Job exists, updating..." -ForegroundColor Yellow
    gcloud run jobs update $JOB `
        --region=$REGION `
        --project=$PROJECT `
        --image=$IMAGE `
        --command="python" `
        --args="jobs/auth_refresh.py" `
        --memory="512Mi" `
        --cpu=1 `
        --task-timeout=300 `
        --max-retries=2 `
        --set-env-vars="NUBRA_ENV=PROD,GCP_PROJECT_ID=$PROJECT,LOG_LEVEL=INFO" `
        --set-secrets="NUBRA_AUTH_TOKEN=nubra-auth-token:latest,NUBRA_X_DEVICE_ID=nubra-x-device-id:latest,NUBRA_SESSION_TOKEN=nubra-session-token:latest,PHONE_NO=PHONE_NO:latest,MPIN=MPIN:latest,NUBRA_TOTP_SECRET=NUBRA_TOTP_SECRET:latest"
} else {
    Write-Host "  Creating new job..." -ForegroundColor Yellow
    gcloud run jobs create $JOB `
        --region=$REGION `
        --project=$PROJECT `
        --image=$IMAGE `
        --command="python" `
        --args="jobs/auth_refresh.py" `
        --memory="512Mi" `
        --cpu=1 `
        --task-timeout=300 `
        --max-retries=2 `
        --set-env-vars="NUBRA_ENV=PROD,GCP_PROJECT_ID=$PROJECT,LOG_LEVEL=INFO" `
        --set-secrets="NUBRA_AUTH_TOKEN=nubra-auth-token:latest,NUBRA_X_DEVICE_ID=nubra-x-device-id:latest,NUBRA_SESSION_TOKEN=nubra-session-token:latest,PHONE_NO=PHONE_NO:latest,MPIN=MPIN:latest,NUBRA_TOTP_SECRET=NUBRA_TOTP_SECRET:latest"
}

if ($LASTEXITCODE -eq 0) { Write-Host "  Job deployed." -ForegroundColor Green }
else { Write-Host "  Job deployment failed!" -ForegroundColor Red; exit 1 }

# --- Step 4: Create Cloud Scheduler ---
Write-Host "`n[4/6] Creating Cloud Scheduler (weekdays 05:30 IST)..." -ForegroundColor DarkCyan

$SCHEDULER = "nubra-auth-refresh-daily"
$SCHEDULE  = "30 5 * * 1-5"  # 05:30 Mon-Fri
$TIMEZONE  = "Asia/Kolkata"

$schExists = gcloud scheduler jobs describe $SCHEDULER --location=$REGION --project=$PROJECT 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  Scheduler exists, updating..." -ForegroundColor Yellow
    gcloud scheduler jobs update http $SCHEDULER `
        --location=$REGION `
        --project=$PROJECT `
        --schedule="$SCHEDULE" `
        --time-zone=$TIMEZONE `
        --uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT/jobs/$JOB`:run" `
        --http-method=POST `
        --oauth-service-account-email="$COMPUTE_SA"
} else {
    Write-Host "  Creating new scheduler..." -ForegroundColor Yellow
    gcloud scheduler jobs create http $SCHEDULER `
        --location=$REGION `
        --project=$PROJECT `
        --schedule="$SCHEDULE" `
        --time-zone=$TIMEZONE `
        --uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT/jobs/$JOB`:run" `
        --http-method=POST `
        --oauth-service-account-email="$COMPUTE_SA"
}

if ($LASTEXITCODE -eq 0) { Write-Host "  Scheduler configured." -ForegroundColor Green }
else { Write-Host "  Scheduler setup failed!" -ForegroundColor Red }

# --- Step 5: Test run ---
Write-Host "`n[5/6] Executing a test run..." -ForegroundColor DarkCyan
gcloud run jobs execute $JOB --region=$REGION --project=$PROJECT --wait 2>&1

# --- Step 6: Summary ---
Write-Host "`n[6/6] Deployment Summary" -ForegroundColor Cyan
Write-Host "  ================================================"
Write-Host "  Cloud Run Job   : $JOB"
Write-Host "  Image           : $IMAGE"
Write-Host "  Entry point     : python jobs/auth_refresh.py"
Write-Host "  Scheduler       : $SCHEDULER"
Write-Host "  Schedule        : $SCHEDULE ($TIMEZONE)"
Write-Host "  ================================================"
Write-Host ""
Write-Host "  Useful commands:" -ForegroundColor Gray
Write-Host "    # Execute manually"
Write-Host "    gcloud run jobs execute $JOB --region=$REGION --project=$PROJECT"
Write-Host ""
Write-Host "    # View execution logs"
Write-Host "    gcloud run jobs executions list --job=$JOB --region=$REGION --project=$PROJECT"
Write-Host ""
Write-Host "    # Trigger scheduler manually"
Write-Host "    gcloud scheduler jobs run $SCHEDULER --location=$REGION --project=$PROJECT"
Write-Host ""
