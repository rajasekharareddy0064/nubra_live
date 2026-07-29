#!/usr/bin/env bash
# Apply Cloud Logging exclusions for nubra-live (see setup_log_exclusions.py).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ID="${PROJECT_ID:-stock-anaysis}"
SERVICE_NAME="${SERVICE_NAME:-nubra-live}"
python "$SCRIPT_DIR/setup_log_exclusions.py" --project "$PROJECT_ID" --service "$SERVICE_NAME"
