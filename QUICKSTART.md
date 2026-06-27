# Quick Start - Deploy to Cloud Run in 5 Minutes

## 1️⃣ Prerequisites (2 min)

```powershell
# Install Google Cloud SDK
# Download from: https://cloud.google.com/sdk/docs/install

# Authenticate
gcloud auth login

# Set your project
gcloud config set project YOUR_PROJECT_ID

# Verify Docker is running
docker --version
```

## 2️⃣ Get Auth Credentials (2 min)

Run locally to generate fresh Nubra session tokens:

```powershell
# Install dependencies first (if not done)
pip install -r requirements.txt

# Generate session tokens
python setup_totp.py
```

This creates `auth_data.db.*` files with your tokens. Keep your `.env` file handy.

## 3️⃣ Deploy (1 min)

### Option A: Automated (Easiest)

```powershell
# Upload secrets to GCP
.\setup_secrets.ps1

# Deploy to Cloud Run
.\deploy.ps1
```

### Option B: Manual (More Control)

```powershell
# 1. Create secrets manually
$PROJECT_ID = "your-project-id"

# Copy values from your .env file
echo -n "your-auth-token" | gcloud secrets create nubra-auth-token --replication-policy=automatic --data-file=-
echo -n "your-device-id" | gcloud secrets create nubra-x-device-id --replication-policy=automatic --data-file=-
echo -n "your-session-token" | gcloud secrets create nubra-session-token --replication-policy=automatic --data-file=-
echo -n "your-phone" | gcloud secrets create nubra-phone --replication-policy=automatic --data-file=-
echo -n "your-mpin" | gcloud secrets create nubra-mpin --replication-policy=automatic --data-file=-
echo -n "your-totp-secret" | gcloud secrets create nubra-totp-secret --replication-policy=automatic --data-file=-

# 2. Build & push
docker build -t gcr.io/$PROJECT_ID/nubra-live:latest .
docker push gcr.io/$PROJECT_ID/nubra-live:latest

# 3. Deploy
gcloud run deploy nubra-live `
  --image=gcr.io/$PROJECT_ID/nubra-live:latest `
  --region=us-central1 `
  --platform=managed `
  --allow-unauthenticated `
  --memory=2Gi `
  --cpu=2 `
  --set-env-vars="NUBRA_ENV=UAT,ENABLE_NUBRA_SOCKET=true" `
  --update-secrets="NUBRA_AUTH_TOKEN=nubra-auth-token:latest,NUBRA_X_DEVICE_ID=nubra-x-device-id:latest,NUBRA_SESSION_TOKEN=nubra-session-token:latest,PHONE_NO=nubra-phone:latest,MPIN=nubra-mpin:latest,NUBRA_TOTP_SECRET=nubra-totp-secret:latest"
```

## 4️⃣ Test

```powershell
# Get your service URL
$URL = (gcloud run services describe nubra-live --region=us-central1 --format='value(status.url)')

# Test health
Invoke-WebRequest "$URL/health"

# Test readiness
Invoke-WebRequest "$URL/health/ready" | Select-Object -Expand Content | ConvertFrom-Json

# Open API docs in browser
Start-Process "$URL/docs"
```

## 5️⃣ Monitor

```powershell
# View logs
gcloud logs tail --service=nubra-live

# View in console
Start-Process "https://console.cloud.google.com/run/detail/us-central1/nubra-live/logs"
```

## Common Issues

### ❌ "Missing secrets"
**Fix:** Run `.\setup_secrets.ps1` first

### ❌ "Session token expired"
**Fix:** 
1. Run `python setup_totp.py` locally
2. Update secret: `echo -n "NEW_TOKEN" | gcloud secrets versions add nubra-session-token --data-file=-`
3. Redeploy: `gcloud run services update nubra-live --region=us-central1`

### ❌ "Container fails to start"
**Fix:** Check logs: `gcloud logs read --service=nubra-live --limit=50`

### ❌ "Docker build fails"
**Fix:** Ensure Docker Desktop is running

## What's Next?

- ✅ [Full deployment guide](DEPLOYMENT.md)
- ✅ Set up monitoring alerts
- ✅ Add custom domain
- ✅ Configure CI/CD

## Useful Commands

```powershell
# View all secrets
gcloud secrets list

# Update a secret
echo -n "new-value" | gcloud secrets versions add SECRET_NAME --data-file=-

# Redeploy with latest secrets
gcloud run services update nubra-live --region=us-central1

# Delete service
gcloud run services delete nubra-live --region=us-central1

# View service details
gcloud run services describe nubra-live --region=us-central1
```

## Success! 🎉

Your Nubra Live backend is now running on Cloud Run!

**Service URL:** Check output of deployment script  
**API Docs:** https://your-service-url/docs  
**WebSocket:** wss://your-service-url/ws/live  
