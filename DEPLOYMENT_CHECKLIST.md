# Cloud Run Deployment Checklist

Use this checklist to ensure a successful deployment to Google Cloud Run.

## Pre-Deployment

### Local Setup
- [ ] Python 3.12+ installed
- [ ] Dependencies installed: `pip install -r requirements.txt`
- [ ] Docker Desktop running (for local image build)
- [ ] Google Cloud SDK installed and authenticated
- [ ] Project ID configured: `gcloud config set project YOUR_PROJECT_ID`

### Authentication
- [ ] Run `python setup_totp.py` to generate auth tokens
- [ ] Verify `auth_data.db.*` files created
- [ ] `.env` file exists with all required variables
- [ ] Copy these values (you'll need them for Secret Manager):
  - [ ] `NUBRA_AUTH_TOKEN`
  - [ ] `NUBRA_X_DEVICE_ID`
  - [ ] `NUBRA_SESSION_TOKEN`
  - [ ] `PHONE_NO`
  - [ ] `MPIN`
  - [ ] `NUBRA_TOTP_SECRET`

### GCP Project Setup
- [ ] Billing enabled on GCP project
- [ ] APIs enabled:
  - [ ] Cloud Run API
  - [ ] Secret Manager API
  - [ ] Container Registry API
  - [ ] Cloud Build API (optional)
- [ ] Service account has required permissions
- [ ] Region selected (e.g., `us-central1`)

## Deployment

### Method 1: Automated Script (Fastest)

**Windows:**
- [ ] Run: `.\setup_secrets.ps1`
- [ ] Verify all secrets created successfully
- [ ] Run: `.\deploy.ps1`
- [ ] Wait for deployment to complete (~5 minutes)
- [ ] Note the service URL from output

**Linux/Mac:**
- [ ] Make scripts executable: `chmod +x setup_secrets.sh deploy.sh`
- [ ] Run: `./setup_secrets.sh`
- [ ] Verify all secrets created successfully
- [ ] Run: `./deploy.sh`
- [ ] Wait for deployment to complete (~5 minutes)
- [ ] Note the service URL from output

### Method 2: Manual Deployment

#### Step 1: Create Secrets
- [ ] Run: `gcloud services enable secretmanager.googleapis.com`
- [ ] Create secret: `nubra-auth-token`
- [ ] Create secret: `nubra-x-device-id`
- [ ] Create secret: `nubra-session-token`
- [ ] Create secret: `nubra-phone`
- [ ] Create secret: `nubra-mpin`
- [ ] Create secret: `nubra-totp-secret`
- [ ] Verify: `gcloud secrets list`

#### Step 2: Build Image
- [ ] Build: `docker build -t gcr.io/PROJECT_ID/nubra-live:latest .`
- [ ] Verify build succeeded (no errors)
- [ ] Push: `docker push gcr.io/PROJECT_ID/nubra-live:latest`
- [ ] Verify image in GCR: `gcloud container images list`

#### Step 3: Deploy to Cloud Run
- [ ] Run deploy command (see DEPLOYMENT.md)
- [ ] Wait for deployment (~3-5 minutes)
- [ ] Note the service URL

### Method 3: Cloud Build
- [ ] Review `cloudbuild.yaml`
- [ ] Ensure secrets exist in Secret Manager
- [ ] Run: `gcloud builds submit --config=cloudbuild.yaml`
- [ ] Monitor build in console
- [ ] Verify deployment succeeded

## Post-Deployment Verification

### Basic Health Checks
- [ ] Health endpoint returns 200: `curl https://SERVICE_URL/health`
- [ ] Response is: `{"status":"ok"}`

### Detailed Readiness Check
- [ ] Readiness endpoint accessible: `curl https://SERVICE_URL/health/ready`
- [ ] Response shows:
  - [ ] `"status": "ok"`
  - [ ] `"startup_mode": "realtime"`
  - [ ] `"ingestion": {"state": "ready"}` or `"disabled"`
  - [ ] No errors in tasks array

### API Endpoints
- [ ] Root endpoint works: `curl https://SERVICE_URL/`
- [ ] OpenAPI docs accessible: `https://SERVICE_URL/docs`
- [ ] Snapshot endpoint works: `curl https://SERVICE_URL/realtime/snapshot`

### WebSocket (if ENABLE_NUBRA_SOCKET=true)
- [ ] Connect to: `wss://SERVICE_URL/ws/live`
- [ ] Receives welcome message
- [ ] Receives live tick data
- [ ] No disconnections within 1 minute

### Logs
- [ ] View logs: `gcloud logs tail --service=nubra-live`
- [ ] No `ERROR` level messages
- [ ] No `NubraAuthError` messages
- [ ] Startup logs show:
  - [ ] `"startup begin"`
  - [ ] `"startup complete"`
  - [ ] `"Nubra ingestion enabled"` (if socket enabled)

## Configuration Verification

### Environment Variables (via Cloud Run Console)
- [ ] Navigate to Cloud Run → nubra-live → Variables & Secrets
- [ ] Verify these are set:
  - [ ] `NUBRA_ENV=UAT` (or PROD)
  - [ ] `NUBRA_EXCHANGE=NSE`
  - [ ] `ENABLE_NUBRA_SOCKET=true`
  - [ ] `USE_DATABASE=false`
  - [ ] `LOG_LEVEL=INFO`
  - [ ] `ENVIRONMENT=production`

### Secrets (via Secret Manager Console)
- [ ] Navigate to Secret Manager
- [ ] Verify all 6 secrets exist
- [ ] Check "Last accessed" shows recent timestamp
- [ ] Verify IAM permissions:
  - [ ] Service account has `secretAccessor` role

### Resource Configuration
- [ ] Memory: 2Gi (adjust if needed)
- [ ] CPU: 2 (adjust if needed)
- [ ] Timeout: 3600s (for WebSocket)
- [ ] Max instances: 10 (adjust based on load)
- [ ] Min instances: 0 (for cost) or 1+ (for latency)
- [ ] Concurrency: 80

## Monitoring Setup

### Cloud Monitoring (Recommended)
- [ ] Create alert for error rate > 5%
- [ ] Create alert for latency > 1s (p95)
- [ ] Create alert for instance count = 0 (if min > 0)
- [ ] Set up notification channel (email/Slack/PagerDuty)

### Logs-based Metrics
- [ ] Create metric for `NubraAuthError` count
- [ ] Create alert if count > 0 in 5 minutes
- [ ] Create metric for startup failures

### Uptime Checks (Optional)
- [ ] Create uptime check for `/health`
- [ ] Frequency: 1 minute
- [ ] Regions: at least 3
- [ ] Alert on failures

## Documentation

- [ ] Team knows where to find deployment docs
- [ ] Session refresh procedure documented
- [ ] On-call has access to GCP console
- [ ] Rollback procedure tested and documented
- [ ] Secrets rotation schedule defined

## Security Review

- [ ] Secrets stored in Secret Manager (not files/env)
- [ ] No secrets in Docker image
- [ ] No secrets in code repository
- [ ] IAM permissions follow least privilege
- [ ] Cloud Run allow unauthenticated = appropriate
- [ ] Consider enabling IAP if internal only
- [ ] Review audit logs setup

## Performance Testing (Optional but Recommended)

- [ ] Load test `/health` endpoint (100 req/s for 1 min)
- [ ] Load test WebSocket connections (10 concurrent)
- [ ] Monitor response times under load
- [ ] Check auto-scaling behavior
- [ ] Verify no memory leaks over 1 hour

## Cost Estimation

- [ ] Estimate requests per month: _______
- [ ] Estimate instance hours: _______
- [ ] Use GCP Pricing Calculator
- [ ] Set up billing alerts
- [ ] Configure budget alerts

## Rollback Plan

If something goes wrong:

- [ ] Previous revision identified:
  ```bash
  gcloud run revisions list --service=nubra-live --region=us-central1
  ```
- [ ] Rollback command ready:
  ```bash
  gcloud run services update-traffic nubra-live \
    --to-revisions=PREVIOUS_REVISION=100 \
    --region=us-central1
  ```
- [ ] Team knows who can execute rollback
- [ ] Communication plan for downtime

## Sign-Off

**Deployed by:** ___________________  
**Date:** ___________________  
**Service URL:** ___________________  
**GCP Project:** ___________________  
**Region:** ___________________  

**Deployment Status:**
- [ ] ✅ All checks passed - Production ready
- [ ] ⚠️  Some issues - Document and monitor
- [ ] ❌ Failed - Rollback required

**Notes:**
_____________________________________________________
_____________________________________________________
_____________________________________________________

## Next Actions

After successful deployment:

1. **Week 1:**
   - [ ] Monitor logs daily
   - [ ] Check metrics dashboard
   - [ ] Verify session refresh works
   - [ ] Document any issues

2. **Week 2:**
   - [ ] Review cost vs estimate
   - [ ] Optimize resource allocation
   - [ ] Fine-tune auto-scaling
   - [ ] Plan session token rotation

3. **Month 1:**
   - [ ] Review monitoring alerts
   - [ ] Analyze performance trends
   - [ ] Plan capacity for peak load
   - [ ] Schedule maintenance window

4. **Ongoing:**
   - [ ] Refresh session tokens before expiry
   - [ ] Update secrets as needed
   - [ ] Review security best practices
   - [ ] Keep dependencies updated

---

**Need Help?**
- Read: [DEPLOYMENT.md](DEPLOYMENT.md)
- Quick guide: [QUICKSTART.md](QUICKSTART.md)
- Check logs: `gcloud logs tail --service=nubra-live`
- Health check: `curl https://your-url/health/ready`
