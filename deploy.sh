#!/bin/bash

# Nubra Live - Cloud Run Deployment Script
# This script handles the complete deployment to Google Cloud Run

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-}"
REGION="${GCP_REGION:-asia-south1}"
SERVICE_NAME="nubra-live"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Nubra Live - Cloud Run Deployment   ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}✗ gcloud CLI is not installed. Please install it first.${NC}"
    echo "  Visit: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Get project ID if not set
if [ -z "$PROJECT_ID" ]; then
    PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
    if [ -z "$PROJECT_ID" ]; then
        echo -e "${RED}✗ No GCP project configured.${NC}"
        echo "  Run: gcloud config set project YOUR_PROJECT_ID"
        exit 1
    fi
fi

echo -e "${GREEN}✓ Using project: ${PROJECT_ID}${NC}"
echo -e "${GREEN}✓ Using region: ${REGION}${NC}"
echo ""

# Check if .env file exists locally (for reference)
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠ Warning: .env file not found locally.${NC}"
    echo "  This is expected for CI/CD, but make sure secrets are configured in GCP Secret Manager."
fi

# Step 1: Check required secrets exist in Secret Manager
echo -e "${BLUE}[1/5] Checking Secret Manager...${NC}"

REQUIRED_SECRETS=(
    "nubra-auth-token"
    "nubra-x-device-id"
    "nubra-session-token"
    "nubra-phone"
    "nubra-mpin"
    "nubra-totp-secret"
)

MISSING_SECRETS=()

for secret in "${REQUIRED_SECRETS[@]}"; do
    if ! gcloud secrets describe "$secret" --project="$PROJECT_ID" &>/dev/null; then
        MISSING_SECRETS+=("$secret")
    fi
done

if [ ${#MISSING_SECRETS[@]} -ne 0 ]; then
    echo -e "${YELLOW}⚠ Missing secrets in Secret Manager:${NC}"
    for secret in "${MISSING_SECRETS[@]}"; do
        echo "  - $secret"
    done
    echo ""
    echo -e "${YELLOW}To create secrets, run:${NC}"
    echo "  ./setup_secrets.sh"
    echo ""
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo -e "${GREEN}✓ All required secrets found in Secret Manager${NC}"
fi

# Step 2: Build the Docker image
echo ""
echo -e "${BLUE}[2/5] Building Docker image...${NC}"
docker build -t "$IMAGE_NAME:latest" .
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Docker image built successfully${NC}"
else
    echo -e "${RED}✗ Docker build failed${NC}"
    exit 1
fi

# Step 3: Push to Container Registry
echo ""
echo -e "${BLUE}[3/5] Pushing image to GCR...${NC}"
docker push "$IMAGE_NAME:latest"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Image pushed to GCR${NC}"
else
    echo -e "${RED}✗ Failed to push image${NC}"
    exit 1
fi

# Step 4: Deploy to Cloud Run
echo ""
echo -e "${BLUE}[4/5] Deploying to Cloud Run...${NC}"

gcloud run deploy "$SERVICE_NAME" \
  --image="$IMAGE_NAME:latest" \
  --region="$REGION" \
  --platform=managed \
  --allow-unauthenticated \
  --port=8080 \
  --memory=2Gi \
  --cpu=2 \
  --timeout=3600 \
  --max-instances=1 \
  --min-instances=1 \
  --no-cpu-throttling \
  --concurrency=1 \
  --set-env-vars="NUBRA_ENV=UAT,NUBRA_EXCHANGE=NSE,ENABLE_NUBRA_SOCKET=true,USE_DATABASE=false,USE_REDIS=false,LOG_LEVEL=INFO,ENVIRONMENT=production,STRIKE_RADIUS=15,CANDLE_INTERVAL_MINUTES=3,MARKET_TIMEZONE=Asia/Kolkata,SUBSCRIBE_SDK_OPTION_CHAIN=true,INITIAL_NIFTY_PRICE=22000.0" \
  --update-secrets="NUBRA_AUTH_TOKEN=nubra-auth-token:latest,NUBRA_X_DEVICE_ID=nubra-x-device-id:latest,NUBRA_SESSION_TOKEN=nubra-session-token:latest,PHONE_NO=nubra-phone:latest,MPIN=nubra-mpin:latest,NUBRA_TOTP_SECRET=nubra-totp-secret:latest" \
  --project="$PROJECT_ID"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Deployment successful${NC}"
else
    echo -e "${RED}✗ Deployment failed${NC}"
    exit 1
fi

# Step 5: Get service URL and test
echo ""
echo -e "${BLUE}[5/5] Testing deployment...${NC}"

SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --format='value(status.url)')

echo -e "${GREEN}✓ Service URL: ${SERVICE_URL}${NC}"
echo ""

# Test health endpoint
echo "Testing health endpoint..."
HEALTH_RESPONSE=$(curl -s -w "\n%{http_code}" "${SERVICE_URL}/health" || echo "000")
HTTP_CODE=$(echo "$HEALTH_RESPONSE" | tail -n1)

if [ "$HTTP_CODE" = "200" ]; then
    echo -e "${GREEN}✓ Health check passed${NC}"
    echo "$HEALTH_RESPONSE" | head -n-1
else
    echo -e "${RED}✗ Health check failed (HTTP $HTTP_CODE)${NC}"
    echo "$HEALTH_RESPONSE" | head -n-1
fi

echo ""
echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         Deployment Complete!           ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}Service URL: ${SERVICE_URL}${NC}"
echo ""
echo "Available endpoints:"
echo "  • Health:       ${SERVICE_URL}/health"
echo "  • Ready:        ${SERVICE_URL}/health/ready"
echo "  • API Docs:     ${SERVICE_URL}/docs"
echo "  • WebSocket:    ${SERVICE_URL}/ws/live"
echo "  • Snapshot:     ${SERVICE_URL}/realtime/snapshot"
echo ""
echo "View logs:"
echo "  gcloud logs tail --project=$PROJECT_ID --service=$SERVICE_NAME"
echo ""
