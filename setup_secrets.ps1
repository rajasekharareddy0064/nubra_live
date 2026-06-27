# Nubra Live - GCP Secret Manager Setup Script (PowerShell)
# This script creates and populates secrets in Google Cloud Secret Manager

$ErrorActionPreference = "Stop"

$PROJECT_ID = $env:GCP_PROJECT_ID

Write-Host "╔════════════════════════════════════════╗" -ForegroundColor Blue
Write-Host "║   GCP Secret Manager Setup for Nubra  ║" -ForegroundColor Blue
Write-Host "╚════════════════════════════════════════╝" -ForegroundColor Blue
Write-Host ""

# Check if gcloud is installed
try {
    $null = Get-Command gcloud -ErrorAction Stop
} catch {
    Write-Host "✗ gcloud CLI is not installed." -ForegroundColor Red
    exit 1
}

# Get project ID
if (-not $PROJECT_ID) {
    $PROJECT_ID = (gcloud config get-value project 2>$null)
    if (-not $PROJECT_ID) {
        Write-Host "✗ No GCP project configured." -ForegroundColor Red
        Write-Host "  Run: gcloud config set project YOUR_PROJECT_ID"
        exit 1
    }
}

Write-Host "✓ Using project: $PROJECT_ID" -ForegroundColor Green
Write-Host ""

# Enable required APIs
Write-Host "Enabling required APIs..." -ForegroundColor Blue
gcloud services enable secretmanager.googleapis.com --project=$PROJECT_ID 2>$null
gcloud services enable run.googleapis.com --project=$PROJECT_ID 2>$null
Write-Host "✓ APIs enabled" -ForegroundColor Green
Write-Host ""

# Function to create or update a secret
function CreateOrUpdateSecret {
    param(
        [string]$SecretName,
        [string]$SecretValue,
        [string]$Description
    )

    if (-not $SecretValue) {
        Write-Host "⚠ Skipping $SecretName (no value provided)" -ForegroundColor Yellow
        return
    }

    # Check if secret exists
    try {
        $null = gcloud secrets describe $SecretName --project=$PROJECT_ID 2>$null
        Write-Host "Updating existing secret: $SecretName" -ForegroundColor Yellow
        $SecretValue | gcloud secrets versions add $SecretName --data-file=- --project=$PROJECT_ID
    } catch {
        Write-Host "Creating new secret: $SecretName" -ForegroundColor Green
        $SecretValue | gcloud secrets create $SecretName --replication-policy=automatic --data-file=- --project=$PROJECT_ID
    }

    # Grant Cloud Run service account access
    gcloud secrets add-iam-policy-binding $SecretName `
        --member="serviceAccount:${PROJECT_ID}@appspot.gserviceaccount.com" `
        --role="roles/secretmanager.secretAccessor" `
        --project=$PROJECT_ID 2>$null

    Write-Host "✓ Secret $SecretName configured" -ForegroundColor Green
}

# Check if .env file exists
if (Test-Path ".env") {
    Write-Host "Found .env file. Loading values..." -ForegroundColor Blue
    Get-Content .env | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            Set-Item -Path "env:$key" -Value $value
        }
    }
} else {
    Write-Host "⚠ No .env file found. You'll need to enter values manually." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "════════════════════════════════════════" -ForegroundColor Blue
Write-Host "   Setting up Nubra authentication      " -ForegroundColor Blue
Write-Host "════════════════════════════════════════" -ForegroundColor Blue
Write-Host ""

# Prompt for values if not in environment
$NUBRA_AUTH_TOKEN = if ($env:NUBRA_AUTH_TOKEN) { $env:NUBRA_AUTH_TOKEN } else { Read-Host "NUBRA_AUTH_TOKEN" }
$NUBRA_X_DEVICE_ID = if ($env:NUBRA_X_DEVICE_ID) { $env:NUBRA_X_DEVICE_ID } else { Read-Host "NUBRA_X_DEVICE_ID" }
$NUBRA_SESSION_TOKEN = if ($env:NUBRA_SESSION_TOKEN) { $env:NUBRA_SESSION_TOKEN } else { Read-Host "NUBRA_SESSION_TOKEN" }
$PHONE_NO = if ($env:PHONE_NO) { $env:PHONE_NO } else { Read-Host "PHONE_NO" }
$MPIN = if ($env:MPIN) { $env:MPIN } else { Read-Host "MPIN" -AsSecureString | ConvertFrom-SecureString -AsPlainText }
$NUBRA_TOTP_SECRET = if ($env:NUBRA_TOTP_SECRET) { $env:NUBRA_TOTP_SECRET } else { Read-Host "NUBRA_TOTP_SECRET" -AsSecureString | ConvertFrom-SecureString -AsPlainText }

Write-Host ""
Write-Host "Creating/updating secrets..." -ForegroundColor Blue
Write-Host ""

# Create secrets
CreateOrUpdateSecret "nubra-auth-token" $NUBRA_AUTH_TOKEN "Nubra API auth token"
CreateOrUpdateSecret "nubra-x-device-id" $NUBRA_X_DEVICE_ID "Nubra device ID"
CreateOrUpdateSecret "nubra-session-token" $NUBRA_SESSION_TOKEN "Nubra session JWT token"
CreateOrUpdateSecret "nubra-phone" $PHONE_NO "Nubra account phone number"
CreateOrUpdateSecret "nubra-mpin" $MPIN "Nubra account MPIN"
CreateOrUpdateSecret "nubra-totp-secret" $NUBRA_TOTP_SECRET "Nubra TOTP secret for 2FA"

Write-Host ""
Write-Host "╔════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║      Secrets Setup Complete!           ║" -ForegroundColor Green
Write-Host "╚════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "All secrets have been created in Secret Manager."
Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Verify secrets: gcloud secrets list --project=$PROJECT_ID"
Write-Host "  2. Deploy to Cloud Run: .\deploy.ps1"
Write-Host ""
