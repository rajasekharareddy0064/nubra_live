# Nubra Live - Cloud Run Deployment Script (PowerShell)
# This script handles the complete deployment to Google Cloud Run

$ErrorActionPreference = "Stop"

# Configuration
$PROJECT_ID = $env:GCP_PROJECT_ID
$REGION = if ($env:GCP_REGION) { $env:GCP_REGION } else { "asia-south1" }
$SERVICE_NAME = "nubra-live"
$IMAGE_NAME = "gcr.io/$PROJECT_ID/$SERVICE_NAME"

Write-Host "╔════════════════════════════════════════╗" -ForegroundColor Blue
Write-Host "║   Nubra Live - Cloud Run Deployment   ║" -ForegroundColor Blue
Write-Host "╚════════════════════════════════════════╝" -ForegroundColor Blue
Write-Host ""

# Check if gcloud is installed
try {
    $null = Get-Command gcloud -ErrorAction Stop
} catch {
    Write-Host "✗ gcloud CLI is not installed. Please install it first." -ForegroundColor Red
    Write-Host "  Visit: https://cloud.google.com/sdk/docs/install"
    exit 1
}

# Get project ID if not set
if (-not $PROJECT_ID) {
    $PROJECT_ID = (gcloud config get-value project 2>$null)
    if (-not $PROJECT_ID) {
        Write-Host "✗ No GCP project configured." -ForegroundColor Red
        Write-Host "  Run: gcloud config set project YOUR_PROJECT_ID"
        exit 1
    }
}

Write-Host "✓ Using project: $PROJECT_ID" -ForegroundColor Green
Write-Host "✓ Using region: $REGION" -ForegroundColor Green
Write-Host ""

# Check if .env file exists locally (for reference)
if (-not (Test-Path ".env")) {
    Write-Host "⚠ Warning: .env file not found locally." -ForegroundColor Yellow
    Write-Host "  This is expected for CI/CD, but make sure secrets are configured in GCP Secret Manager."
}

# Step 1: Check required secrets exist in Secret Manager
Write-Host "[1/5] Checking Secret Manager..." -ForegroundColor Blue

$REQUIRED_SECRETS = @(
    "nubra-auth-token",
    "nubra-x-device-id",
    "nubra-session-token",
    "nubra-phone",
    "nubra-mpin",
    "nubra-totp-secret"
)

$MISSING_SECRETS = @()

foreach ($secret in $REQUIRED_SECRETS) {
    try {
        $null = gcloud secrets describe $secret --project=$PROJECT_ID 2>$null
    } catch {
        $MISSING_SECRETS += $secret
    }
}

if ($MISSING_SECRETS.Count -gt 0) {
    Write-Host "⚠ Missing secrets in Secret Manager:" -ForegroundColor Yellow
    foreach ($secret in $MISSING_SECRETS) {
        Write-Host "  - $secret"
    }
    Write-Host ""
    Write-Host "To create secrets, run:" -ForegroundColor Yellow
    Write-Host "  .\setup_secrets.ps1"
    Write-Host ""
    $response = Read-Host "Continue anyway? (y/N)"
    if ($response -ne "y" -and $response -ne "Y") {
        exit 1
    }
} else {
    Write-Host "✓ All required secrets found in Secret Manager" -ForegroundColor Green
}

# Step 2: Build the Docker image
Write-Host ""
Write-Host "[2/5] Building Docker image..." -ForegroundColor Blue
docker build -t "${IMAGE_NAME}:latest" .
if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Docker image built successfully" -ForegroundColor Green
} else {
    Write-Host "✗ Docker build failed" -ForegroundColor Red
    exit 1
}

# Step 3: Push to Container Registry
Write-Host ""
Write-Host "[3/5] Pushing image to GCR..." -ForegroundColor Blue
docker push "${IMAGE_NAME}:latest"
if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Image pushed to GCR" -ForegroundColor Green
} else {
    Write-Host "✗ Failed to push image" -ForegroundColor Red
    exit 1
}

# Step 4: Deploy to Cloud Run
Write-Host ""
Write-Host "[4/5] Deploying to Cloud Run..." -ForegroundColor Blue

gcloud run deploy $SERVICE_NAME `
  --image="${IMAGE_NAME}:latest" `
  --region=$REGION `
  --platform=managed `
  --allow-unauthenticated `
  --port=8080 `
  --memory=2Gi `
  --cpu=2 `
  --timeout=3600 `
  --max-instances=1 `
  --min-instances=1 `
  --no-cpu-throttling `
  --concurrency=1 `
  --set-env-vars="NUBRA_ENV=UAT,NUBRA_EXCHANGE=NSE,ENABLE_NUBRA_SOCKET=true,USE_DATABASE=false,USE_REDIS=false,LOG_LEVEL=INFO,ENVIRONMENT=production,STRIKE_RADIUS=15,CANDLE_INTERVAL_MINUTES=3,MARKET_TIMEZONE=Asia/Kolkata,SUBSCRIBE_SDK_OPTION_CHAIN=true,INITIAL_NIFTY_PRICE=22000.0" `
  --update-secrets="NUBRA_AUTH_TOKEN=nubra-auth-token:latest,NUBRA_X_DEVICE_ID=nubra-x-device-id:latest,NUBRA_SESSION_TOKEN=nubra-session-token:latest,PHONE_NO=nubra-phone:latest,MPIN=nubra-mpin:latest,NUBRA_TOTP_SECRET=nubra-totp-secret:latest" `
  --project=$PROJECT_ID

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Deployment successful" -ForegroundColor Green
} else {
    Write-Host "✗ Deployment failed" -ForegroundColor Red
    exit 1
}

# Step 5: Get service URL and test
Write-Host ""
Write-Host "[5/5] Testing deployment..." -ForegroundColor Blue

$SERVICE_URL = (gcloud run services describe $SERVICE_NAME `
  --region=$REGION `
  --project=$PROJECT_ID `
  --format='value(status.url)')

Write-Host "✓ Service URL: $SERVICE_URL" -ForegroundColor Green
Write-Host ""

# Test health endpoint
Write-Host "Testing health endpoint..."
try {
    $response = Invoke-WebRequest -Uri "$SERVICE_URL/health" -UseBasicParsing
    if ($response.StatusCode -eq 200) {
        Write-Host "✓ Health check passed" -ForegroundColor Green
        Write-Host $response.Content
    }
} catch {
    Write-Host "✗ Health check failed" -ForegroundColor Red
    Write-Host $_.Exception.Message
}

Write-Host ""
Write-Host "╔════════════════════════════════════════╗" -ForegroundColor Blue
Write-Host "║         Deployment Complete!           ║" -ForegroundColor Blue
Write-Host "╚════════════════════════════════════════╝" -ForegroundColor Blue
Write-Host ""
Write-Host "Service URL: $SERVICE_URL" -ForegroundColor Green
Write-Host ""
Write-Host "Available endpoints:"
Write-Host "  • Health:       $SERVICE_URL/health"
Write-Host "  • Ready:        $SERVICE_URL/health/ready"
Write-Host "  • API Docs:     $SERVICE_URL/docs"
Write-Host "  • WebSocket:    $SERVICE_URL/ws/live"
Write-Host "  • Snapshot:     $SERVICE_URL/realtime/snapshot"
Write-Host ""
Write-Host "View logs:"
Write-Host "  gcloud logs tail --project=$PROJECT_ID --service=$SERVICE_NAME"
Write-Host ""
