# Apply Cloud Logging exclusions for nubra-live (see setup_log_exclusions.py).
param(
    [string]$ProjectId = "stock-anaysis",
    [string]$ServiceName = "nubra-live"
)

$ErrorActionPreference = "Stop"
python (Join-Path $PSScriptRoot "setup_log_exclusions.py") --project $ProjectId --service $ServiceName
