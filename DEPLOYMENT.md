# Nubra Live - Cloud Run Deployment Guide

This guide explains how to deploy the Nubra Live trading backend to Google Cloud Run.

## Prerequisites

1. **Google Cloud Project**
   - Active GCP project with billing enabled
   - Project ID noted

2. **Google Cloud SDK**
   - Install: https://cloud.google.com/sdk/docs/install
   - Authenticate: `gcloud auth login`
   - Set project: `gcloud config set project YOUR_PROJECT_ID`

3. **Docker**
   - Required for local image building
   - Alternative: Use Cloud Build (see below)

4. **Nubra Authentication Credentials**
   - Valid session tokens OR TOTP credentials
   - Obtain by running `python setup_totp.py` or `python enroll_totp.py` locally

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Google Cloud Run                        │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Nubra Live Container                                  │ │
│  │  - FastAPI app (REST + WebSocket)                      │ │
│  │  - Realtime market data pipeline                       │ │
│  │  - In-memory candle aggregation                        │ │
│  └────────────────────────────────────────────────────────┘ │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Environment Variables                                  │ │
│  │  - NUBRA_ENV, ENABLE_NUBRA_SOCKET, etc.               │ │
│  └────────────────────────────────────────────────────────┘ │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  GCP Secret Manager (Auth Credentials)                 │ │
│  │  - NUBRA_AUTH_TOKEN                                    │ │
│  │  - NUBRA_X_DEVICE_ID                                   │ │
│  │  - NUBRA_SESSION_TOKEN                                 │ │
│  │  - PHONE_NO, MPIN, NUBRA_TOTP_SECRET                  │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                           ↓
              Nubra WebSocket API (UAT/PROD)
```

## Deployment Methods

### Method 1: Automated Script (Recommended)

#### Windows (PowerShell)

```powershell
# 1. Set up secrets in GCP Secret Manager
.\setup_secrets.ps1

# 2. Deploy to Cloud Run
.\deploy.ps1
```

#### Linux/Mac (Bash)

```bash
# 1. Set up secrets in GCP Secret Manager
chmod +x setup_secrets.sh
./setup_secrets.sh

# 2. Deploy to Cloud Run
chmod +x deploy.sh
./deploy.sh
```

### Method 2: Manual Deployment

#### Step 1: Create Secrets in Secret Manager

Get your auth credentials locally first:

```bash
# Generate fresh session tokens
python setup_totp.py

# This will create/update auth_data.db.* and show you the tokens
# Copy these values for the next step
```

Create secrets in GCP:

```bash
# Set your project
export PROJECT_ID="your-gcp-project-id"
gcloud config set project $PROJECT_ID

# Enable APIs
gcloud services enable secretmanager.googleapis.com
gcloud services enable run.googleapis.com

# Create secrets (replace with your actual values)
echo -n "YOUR_AUTH_TOKEN" | gcloud secrets create nubra-auth-token \
    --replication-policy=automatic --data-file=-

echo -n "YOUR_DEVICE_ID" | gcloud secrets create nubra-x-device-id \
    --replication-policy=automatic --data-file=-

echo -n "YOUR_SESSION_TOKEN" | gcloud secrets create nubra-session-token \
    --replication-policy=automatic --data-file=-

echo -n "YOUR_PHONE_NUMBER" | gcloud secrets create nubra-phone \
    --replication-policy=automatic --data-file=-

echo -n "YOUR_MPIN" | gcloud secrets create nubra-mpin \
    --replication-policy=automatic --data-file=-

echo -n "YOUR_TOTP_SECRET" | gcloud secrets create nubra-totp-secret \
    --replication-policy=automatic --data-file=-
```

#### Step 2: Build and Push Docker Image

```bash
# Build image
docker build -t gcr.io/$PROJECT_ID/nubra-live:latest .

# Push to Google Container Registry
docker push gcr.io/$PROJECT_ID/nubra-live:latest
```

#### Step 3: Deploy to Cloud Run

```bash
gcloud run deploy nubra-live \
  --image=gcr.io/$PROJECT_ID/nubra-live:latest \
  --region=us-central1 \
  --platform=managed \
  --allow-unauthenticated \
  --port=8080 \
  --memory=2Gi \
  --cpu=2 \
  --timeout=3600 \
  --max-instances=10 \
  --min-instances=0 \
  --concurrency=80 \
  --set-env-vars="NUBRA_ENV=UAT,NUBRA_EXCHANGE=NSE,ENABLE_NUBRA_SOCKET=true,USE_DATABASE=false,USE_REDIS=false,LOG_LEVEL=INFO,ENVIRONMENT=production,STRIKE_RADIUS=15,CANDLE_INTERVAL_MINUTES=3,MARKET_TIMEZONE=Asia/Kolkata,SUBSCRIBE_SDK_OPTION_CHAIN=true,INITIAL_NIFTY_PRICE=22000.0" \
  --update-secrets="NUBRA_AUTH_TOKEN=nubra-auth-token:latest,NUBRA_X_DEVICE_ID=nubra-x-device-id:latest,NUBRA_SESSION_TOKEN=nubra-session-token:latest,PHONE_NO=nubra-phone:latest,MPIN=nubra-mpin:latest,NUBRA_TOTP_SECRET=nubra-totp-secret:latest"
```

### Method 3: Cloud Build (No Docker Required)

Use `cloudbuild.yaml` for automated builds:

```bash
# Submit build to Cloud Build
gcloud builds submit --config=cloudbuild.yaml
```

This will:
1. Build the Docker image in the cloud
2. Push to Container Registry
3. Deploy to Cloud Run
4. All in one command!

## Configuration

### Environment Variables

Set these in Cloud Run console or via `--set-env-vars`:

| Variable | Default | Description |
|----------|---------|-------------|
| `NUBRA_ENV` | `UAT` | Nubra environment (`UAT` or `PROD`) |
| `NUBRA_EXCHANGE` | `NSE` | Exchange name |
| `ENABLE_NUBRA_SOCKET` | `true` | Enable live WebSocket ingestion |
| `USE_DATABASE` | `false` | Enable PostgreSQL persistence |
| `USE_REDIS` | `false` | Enable Redis state store |
| `LOG_LEVEL` | `INFO` | Logging level |
| `STRIKE_RADIUS` | `15` | Number of strikes around ATM |
| `CANDLE_INTERVAL_MINUTES` | `3` | Candle aggregation interval |
| `INITIAL_NIFTY_PRICE` | `22000.0` | Bootstrap price for options |

### Secrets (GCP Secret Manager)

These are injected as environment variables at runtime:

| Secret Name | Environment Variable | Description |
|-------------|---------------------|-------------|
| `nubra-auth-token` | `NUBRA_AUTH_TOKEN` | Auth token from Nubra |
| `nubra-x-device-id` | `NUBRA_X_DEVICE_ID` | Device ID |
| `nubra-session-token` | `NUBRA_SESSION_TOKEN` | Session JWT (expires) |
| `nubra-phone` | `PHONE_NO` | Account phone number |
| `nubra-mpin` | `MPIN` | Account MPIN |
| `nubra-totp-secret` | `NUBRA_TOTP_SECRET` | TOTP secret for 2FA |

## Testing the Deployment

After deployment, get your service URL:

```bash
SERVICE_URL=$(gcloud run services describe nubra-live \
  --region=us-central1 \
  --format='value(status.url)')

echo "Service URL: $SERVICE_URL"
```

Test endpoints:

```bash
# Health check
curl $SERVICE_URL/health

# Readiness check (detailed status)
curl $SERVICE_URL/health/ready

# API documentation
open $SERVICE_URL/docs

# WebSocket test (use a WS client)
wscat -c $SERVICE_URL/ws/live
```

## Monitoring

### View Logs

```bash
# Tail live logs
gcloud logs tail --service=nubra-live

# View logs in console
gcloud logs read --service=nubra-live --limit=50
```

### Check Metrics

```bash
# Get service details
gcloud run services describe nubra-live --region=us-central1

# View in GCP Console
# Navigate to: Cloud Run > nubra-live > Metrics
```

## Troubleshooting

### Container fails to start

**Check logs:**
```bash
gcloud logs read --service=nubra-live --limit=100 --format=json | jq -r '.[] | .textPayload'
```

**Common issues:**
1. Missing secrets → Run `setup_secrets.ps1` or `setup_secrets.sh`
2. Expired session token → Refresh locally with `python setup_totp.py`
3. Wrong `NUBRA_ENV` → Check if tokens are for UAT or PROD

### Health check fails

```bash
# Check readiness endpoint
curl https://your-service-url/health/ready
```

Look for:
- `ingestion.state` → should be `ready` or `disabled`
- `startup_mode` → `realtime` or `database`

### WebSocket connection fails

1. Check `ENABLE_NUBRA_SOCKET=true` is set
2. Verify auth secrets are present
3. Check logs for auth errors:
   ```bash
   gcloud logs read --service=nubra-live --filter="textPayload:NubraAuthError"
   ```

### Session token expires

Session tokens (JWTs) expire. To refresh:

```bash
# 1. Refresh locally
python setup_totp.py

# 2. Update the secret
echo -n "NEW_SESSION_TOKEN" | gcloud secrets versions add nubra-session-token --data-file=-

# 3. Redeploy (Cloud Run will pick up new secret on next instance)
gcloud run services update nubra-live --region=us-central1
```

## Scaling Configuration

Cloud Run auto-scales based on traffic. Adjust limits:

```bash
gcloud run services update nubra-live \
  --region=us-central1 \
  --min-instances=1 \
  --max-instances=20 \
  --concurrency=100 \
  --memory=4Gi \
  --cpu=4
```

## Cost Optimization

1. **Set min-instances=0** for development (no idle cost)
2. **Use appropriate CPU/memory** (default 2Gi/2CPU is usually enough)
3. **Enable request timeout** (default 3600s for WebSocket, reduce for REST)
4. **Monitor cold starts** in metrics

## Security Best Practices

1. ✅ **Never commit secrets** (`.env` is gitignored)
2. ✅ **Use Secret Manager** for all sensitive values
3. ✅ **Rotate session tokens** regularly (or let TOTP refresh handle it)
4. ✅ **Use IAM roles** for service account permissions
5. ✅ **Enable Cloud Armor** for DDoS protection (optional)
6. ✅ **Restrict Cloud Run IAM** if auth is needed

## Production Checklist

- [ ] Secrets configured in Secret Manager
- [ ] Fresh session tokens (not expired)
- [ ] `NUBRA_ENV` set correctly (UAT vs PROD)
- [ ] Cloud Run region selected (latency to Nubra API)
- [ ] Monitoring/alerting configured
- [ ] Logs retention policy set
- [ ] Auto-scaling limits appropriate
- [ ] Health checks passing
- [ ] WebSocket connections working
- [ ] Backup session refresh strategy in place

## Next Steps

1. Set up Cloud Monitoring alerts
2. Configure Cloud Scheduler for health pings
3. Add PostgreSQL/Redis if persistence needed
4. Set up CI/CD pipeline (Cloud Build triggers)
5. Configure custom domain (optional)

## Support

For issues:
1. Check logs: `gcloud logs tail --service=nubra-live`
2. Verify secrets: `gcloud secrets list`
3. Check service status: `gcloud run services describe nubra-live`
4. Test health endpoint: `curl https://your-url/health/ready`
