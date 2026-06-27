#!/bin/bash

# Nubra Live - GCP Secret Manager Setup Script
# This script creates and populates secrets in Google Cloud Secret Manager

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

PROJECT_ID="${GCP_PROJECT_ID:-}"

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   GCP Secret Manager Setup for Nubra  ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}✗ gcloud CLI is not installed.${NC}"
    exit 1
fi

# Get project ID
if [ -z "$PROJECT_ID" ]; then
    PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
    if [ -z "$PROJECT_ID" ]; then
        echo -e "${RED}✗ No GCP project configured.${NC}"
        echo "  Run: gcloud config set project YOUR_PROJECT_ID"
        exit 1
    fi
fi

echo -e "${GREEN}✓ Using project: ${PROJECT_ID}${NC}"
echo ""

# Enable required APIs
echo -e "${BLUE}Enabling required APIs...${NC}"
gcloud services enable secretmanager.googleapis.com --project="$PROJECT_ID" 2>/dev/null || true
gcloud services enable run.googleapis.com --project="$PROJECT_ID" 2>/dev/null || true
echo -e "${GREEN}✓ APIs enabled${NC}"
echo ""

# Function to create or update a secret
create_or_update_secret() {
    local secret_name=$1
    local secret_value=$2
    local description=$3

    if [ -z "$secret_value" ]; then
        echo -e "${YELLOW}⚠ Skipping $secret_name (no value provided)${NC}"
        return
    fi

    # Check if secret exists
    if gcloud secrets describe "$secret_name" --project="$PROJECT_ID" &>/dev/null; then
        echo -e "${YELLOW}Updating existing secret: $secret_name${NC}"
        echo -n "$secret_value" | gcloud secrets versions add "$secret_name" \
            --data-file=- \
            --project="$PROJECT_ID"
    else
        echo -e "${GREEN}Creating new secret: $secret_name${NC}"
        echo -n "$secret_value" | gcloud secrets create "$secret_name" \
            --replication-policy="automatic" \
            --data-file=- \
            --project="$PROJECT_ID"
        
        # Set description if provided
        if [ -n "$description" ]; then
            gcloud secrets update "$secret_name" \
                --update-labels=description="$description" \
                --project="$PROJECT_ID" 2>/dev/null || true
        fi
    fi

    # Grant Cloud Run service account access
    gcloud secrets add-iam-policy-binding "$secret_name" \
        --member="serviceAccount:${PROJECT_ID}@appspot.gserviceaccount.com" \
        --role="roles/secretmanager.secretAccessor" \
        --project="$PROJECT_ID" 2>/dev/null || true

    echo -e "${GREEN}✓ Secret $secret_name configured${NC}"
}

# Check if .env file exists
if [ -f ".env" ]; then
    echo -e "${BLUE}Found .env file. Loading values...${NC}"
    source .env
else
    echo -e "${YELLOW}⚠ No .env file found. You'll need to enter values manually.${NC}"
fi

echo ""
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo -e "${BLUE}   Setting up Nubra authentication      ${NC}"
echo -e "${BLUE}════════════════════════════════════════${NC}"
echo ""

# Prompt for values if not in environment
read -p "NUBRA_AUTH_TOKEN [${NUBRA_AUTH_TOKEN:0:10}...]: " INPUT_AUTH_TOKEN
NUBRA_AUTH_TOKEN="${INPUT_AUTH_TOKEN:-$NUBRA_AUTH_TOKEN}"

read -p "NUBRA_X_DEVICE_ID [${NUBRA_X_DEVICE_ID:0:10}...]: " INPUT_DEVICE_ID
NUBRA_X_DEVICE_ID="${INPUT_DEVICE_ID:-$NUBRA_X_DEVICE_ID}"

read -p "NUBRA_SESSION_TOKEN [${NUBRA_SESSION_TOKEN:0:10}...]: " INPUT_SESSION_TOKEN
NUBRA_SESSION_TOKEN="${INPUT_SESSION_TOKEN:-$NUBRA_SESSION_TOKEN}"

read -p "PHONE_NO [${PHONE_NO}]: " INPUT_PHONE
PHONE_NO="${INPUT_PHONE:-$PHONE_NO}"

read -s -p "MPIN: " INPUT_MPIN
echo ""
MPIN="${INPUT_MPIN:-$MPIN}"

read -s -p "NUBRA_TOTP_SECRET: " INPUT_TOTP_SECRET
echo ""
NUBRA_TOTP_SECRET="${INPUT_TOTP_SECRET:-$NUBRA_TOTP_SECRET}"

echo ""
echo -e "${BLUE}Creating/updating secrets...${NC}"
echo ""

# Create secrets
create_or_update_secret "nubra-auth-token" "$NUBRA_AUTH_TOKEN" "Nubra API auth token"
create_or_update_secret "nubra-x-device-id" "$NUBRA_X_DEVICE_ID" "Nubra device ID"
create_or_update_secret "nubra-session-token" "$NUBRA_SESSION_TOKEN" "Nubra session JWT token"
create_or_update_secret "nubra-phone" "$PHONE_NO" "Nubra account phone number"
create_or_update_secret "nubra-mpin" "$MPIN" "Nubra account MPIN"
create_or_update_secret "nubra-totp-secret" "$NUBRA_TOTP_SECRET" "Nubra TOTP secret for 2FA"

echo ""
echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║      Secrets Setup Complete!           ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
echo ""
echo "All secrets have been created in Secret Manager."
echo ""
echo "Next steps:"
echo "  1. Verify secrets: gcloud secrets list --project=$PROJECT_ID"
echo "  2. Deploy to Cloud Run: ./deploy.sh"
echo ""
