# 🚀 Start Here - Deploy Nubra Live to Cloud Run

Welcome! This guide will get your Nubra Live trading backend running on Google Cloud Run in **5 minutes**.

## ⚡ Quick Deploy (Recommended)

### Windows
```powershell
# 1. Setup secrets
.\setup_secrets.ps1

# 2. Deploy
.\deploy.ps1
```

### Linux/Mac
```bash
# 1. Setup secrets
chmod +x setup_secrets.sh deploy.sh
./setup_secrets.sh

# 2. Deploy
./deploy.sh
```

That's it! 🎉

## 📚 Documentation

| Guide | When to Use |
|-------|------------|
| **[QUICKSTART.md](QUICKSTART.md)** | First-time deployment (5 min read) |
| **[DEPLOYMENT.md](DEPLOYMENT.md)** | Detailed instructions & troubleshooting |
| **[DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md)** | Step-by-step verification |
| **[FIXES_SUMMARY.md](FIXES_SUMMARY.md)** | What was fixed & why |
| **[CHANGES.md](CHANGES.md)** | Technical change log |

## ✅ Prerequisites (5 minutes)

1. **Google Cloud SDK**
   ```powershell
   # Download from: https://cloud.google.com/sdk/docs/install
   
   # After install, authenticate:
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Docker Desktop**
   ```powershell
   # Download from: https://www.docker.com/products/docker-desktop
   # Start Docker Desktop
   ```

3. **Python 3.12+**
   ```powershell
   # Install dependencies:
   pip install -r requirements.txt
   ```

4. **Nubra Auth Credentials**
   ```powershell
   # Generate fresh tokens:
   python setup_totp.py
   
   # This creates auth_data.db.* files
   # Keep the values from your .env file
   ```

## 🎯 Deployment Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. LOCAL: Generate Auth Tokens                              │
│    → python setup_totp.py                                    │
└────────────────┬────────────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. GCP: Upload Secrets to Secret Manager                    │
│    → .\setup_secrets.ps1 (Windows)                          │
│    → ./setup_secrets.sh (Linux/Mac)                         │
└────────────────┬────────────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. DOCKER: Build & Push Image                               │
│    → docker build + docker push                             │
│    (Handled automatically by deploy script)                 │
└────────────────┬────────────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. CLOUD RUN: Deploy with Secrets                           │
│    → gcloud run deploy with env vars + secrets              │
│    (Handled automatically by deploy script)                 │
└────────────────┬────────────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. TEST: Verify Deployment                                  │
│    → curl https://YOUR-URL/health                           │
│    → curl https://YOUR-URL/health/ready                     │
└─────────────────────────────────────────────────────────────┘
```

## 🧪 Test Your Deployment

```powershell
# Get service URL
$URL = (gcloud run services describe nubra-live --region=us-central1 --format='value(status.url)')

# Test health
Invoke-WebRequest "$URL/health"
# ✅ Should return: {"status":"ok"}

# Test detailed status
Invoke-WebRequest "$URL/health/ready" | Select-Object -Expand Content | ConvertFrom-Json
# ✅ Should show: ingestion.state = "ready"

# View live logs
gcloud logs tail --service=nubra-live

# Open API docs in browser
Start-Process "$URL/docs"
```

## 🔍 What Gets Deployed?

### Application
- **FastAPI backend** (REST + WebSocket)
- **Real-time market data** ingestion from Nubra
- **OHLCV candle aggregation** (in-memory)
- **Options chain tracking** with dynamic ATM
- **Order book aggregation**
- **WebSocket broadcasting** to clients

### Infrastructure
- **Cloud Run** (serverless, auto-scaling)
- **Container Registry** (Docker images)
- **Secret Manager** (auth credentials)
- **Cloud Logging** (logs & monitoring)

### Endpoints
| URL | Description |
|-----|-------------|
| `/` | Service info |
| `/health` | Health check |
| `/health/ready` | Detailed status |
| `/docs` | API documentation |
| `/ws/live` | WebSocket stream |
| `/realtime/snapshot` | Market snapshot |

## 🔐 Secrets Management

Your sensitive credentials are stored in **GCP Secret Manager**, not in your code or container:

| Secret | Environment Variable | Purpose |
|--------|---------------------|---------|
| `nubra-auth-token` | `NUBRA_AUTH_TOKEN` | Auth token |
| `nubra-x-device-id` | `NUBRA_X_DEVICE_ID` | Device ID |
| `nubra-session-token` | `NUBRA_SESSION_TOKEN` | Session JWT |
| `nubra-phone` | `PHONE_NO` | Phone number |
| `nubra-mpin` | `MPIN` | Account MPIN |
| `nubra-totp-secret` | `NUBRA_TOTP_SECRET` | TOTP 2FA secret |

These are automatically injected into your Cloud Run container at runtime.

## ⚙️ Configuration

Set these via Cloud Run environment variables:

```yaml
NUBRA_ENV: UAT               # or PROD
NUBRA_EXCHANGE: NSE
ENABLE_NUBRA_SOCKET: true    # Enable live data
USE_DATABASE: false          # Postgres (optional)
USE_REDIS: false             # Redis (optional)
LOG_LEVEL: INFO
STRIKE_RADIUS: 15
CANDLE_INTERVAL_MINUTES: 3
INITIAL_NIFTY_PRICE: 22000.0
```

## 🐛 Troubleshooting

### ❌ Deployment fails with "Missing secrets"
**Solution:**
```powershell
.\setup_secrets.ps1  # Run secrets setup first
```

### ❌ Container starts but health check fails
**Solution:**
```powershell
# View logs
gcloud logs tail --service=nubra-live

# Look for NubraAuthError or startup errors
```

### ❌ "Session token expired" error
**Solution:**
```powershell
# 1. Refresh locally
python setup_totp.py

# 2. Update the secret
$TOKEN = Get-Content .env | Select-String "NUBRA_SESSION_TOKEN" | ForEach-Object { $_.ToString().Split('=')[1] }
echo -n $TOKEN | gcloud secrets versions add nubra-session-token --data-file=-

# 3. Restart Cloud Run
gcloud run services update nubra-live --region=us-central1
```

### ❌ WebSocket won't connect
**Check:**
1. `ENABLE_NUBRA_SOCKET=true` is set
2. Auth secrets are valid (not expired)
3. `NUBRA_ENV` matches your enrollment (UAT vs PROD)

## 📊 Monitoring

### View Logs
```powershell
# Live tail
gcloud logs tail --service=nubra-live

# Filter errors
gcloud logs read --service=nubra-live --filter="severity>=ERROR"

# Search for auth issues
gcloud logs read --service=nubra-live --filter="textPayload:NubraAuthError"
```

### Check Metrics (GCP Console)
```
https://console.cloud.google.com/run/detail/us-central1/nubra-live/metrics
```

## 🔄 Ongoing Maintenance

### When Session Expires (~30 days)
```powershell
# 1. Refresh tokens locally
python setup_totp.py

# 2. Re-run secrets setup
.\setup_secrets.ps1

# 3. Restart (automatically picks up new secrets)
gcloud run services update nubra-live --region=us-central1
```

### Update Application Code
```powershell
# Just re-deploy
.\deploy.ps1

# Old instances are replaced automatically
# Zero downtime!
```

## 💰 Cost

Typical costs (will vary based on usage):

| Usage Pattern | Estimated Cost/Month |
|--------------|---------------------|
| **Development** (min-instances=0, low traffic) | $5-10 |
| **Production** (min-instances=1, moderate) | $50-100 |
| **High Traffic** (auto-scaling) | $200-500 |

**Cost factors:**
- Instance hours (CPU + memory)
- Request count
- Bandwidth
- Secret Manager access (first 6 versions/secret free)

**Save money:**
- Set `--min-instances=0` for dev environments
- Adjust CPU/memory based on actual usage
- Use appropriate timeout settings

## 🎓 Learn More

- **[QUICKSTART.md](QUICKSTART.md)** - Fast 5-minute guide
- **[DEPLOYMENT.md](DEPLOYMENT.md)** - Complete documentation
- **[FIXES_SUMMARY.md](FIXES_SUMMARY.md)** - What changed & why
- **[Cloud Run Docs](https://cloud.google.com/run/docs)** - Official GCP docs

## 🆘 Need Help?

### Quick Checks
```powershell
# 1. Is Cloud Run running?
gcloud run services describe nubra-live --region=us-central1

# 2. Are secrets accessible?
gcloud secrets list

# 3. What do logs say?
gcloud logs tail --service=nubra-live --limit=50

# 4. Is health OK?
curl https://YOUR-URL/health/ready
```

### Common Commands
```powershell
# Redeploy
.\deploy.ps1

# View all revisions
gcloud run revisions list --service=nubra-live --region=us-central1

# Rollback to previous
gcloud run services update-traffic nubra-live --to-revisions=REVISION-NAME=100 --region=us-central1

# Delete service
gcloud run services delete nubra-live --region=us-central1

# Update environment variable
gcloud run services update nubra-live --set-env-vars="LOG_LEVEL=DEBUG" --region=us-central1
```

## ✅ Success Checklist

Your deployment is successful when:

- [x] `/health` returns `{"status":"ok"}`
- [x] `/health/ready` shows detailed status
- [x] No errors in logs
- [x] WebSocket connects (if enabled)
- [x] API docs load at `/docs`
- [x] Live data flows (check logs)

## 🎉 You're Ready!

**Start with:** [QUICKSTART.md](QUICKSTART.md)

**Or just run:**
```powershell
.\setup_secrets.ps1
.\deploy.ps1
```

Good luck! 🚀

---

**Questions?** Check the troubleshooting section above or [DEPLOYMENT.md](DEPLOYMENT.md) for detailed help.
