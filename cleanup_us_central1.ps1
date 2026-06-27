# ============================================================
# cleanup_us_central1.ps1
# Safe cleanup of old us-central1 resources after migration
# Project: stock-anaysis
# Old region: us-central1 (to be cleaned)
# New region: asia-south1 (must be healthy before cleanup)
# ============================================================

$PROJECT  = "stock-anaysis"
$OLD      = "us-central1"
$NEW      = "asia-south1"
$deleted  = @()
$skipped  = @()
$errors   = @()

function OK($m)   { Write-Host "  [PASS] $m" -ForegroundColor Green }
function ERR($m)  { Write-Host "  [FAIL] $m" -ForegroundColor Red }
function SKIP($m) { Write-Host "  [SKIP] $m" -ForegroundColor DarkGray }
function INFO($m) { Write-Host "  [INFO] $m" -ForegroundColor Cyan }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  SAFE CLEANUP: us-central1 -> asia-south1 migration" -ForegroundColor Cyan
Write-Host "  Project: $PROJECT" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ==============================================================
# PHASE 1: PRE-FLIGHT - Verify asia-south1 is healthy
# ==============================================================
Write-Host ""
Write-Host "--- PHASE 1: Pre-flight verification (asia-south1) ---" -ForegroundColor Yellow
Write-Host ""

$abort = $false

# Check Cloud Run Service
$svc = gcloud run services describe nubra-live --region=$NEW --project=$PROJECT --format="value(status.url)" 2>$null
if ($LASTEXITCODE -eq 0 -and $svc) {
    OK "Cloud Run Service 'nubra-live' exists in $NEW ($svc)"
} else {
    ERR "Cloud Run Service 'nubra-live' NOT FOUND in $NEW"
    $abort = $true
}

# Check Cloud Run Jobs
foreach ($job in @("nubra-auth-refresh", "nubra-live-start", "nubra-live-stop")) {
    $null = gcloud run jobs describe $job --region=$NEW --project=$PROJECT 2>$null
    if ($LASTEXITCODE -eq 0) { OK "Cloud Run Job '$job' exists in $NEW" }
    else { ERR "Cloud Run Job '$job' NOT FOUND in $NEW"; $abort = $true }
}

# Check Cloud Schedulers
foreach ($sch in @("nubra-auth-refresh-daily", "nubra-live-start", "nubra-live-stop")) {
    $null = gcloud scheduler jobs describe $sch --location=$NEW --project=$PROJECT 2>$null
    if ($LASTEXITCODE -eq 0) { OK "Cloud Scheduler '$sch' exists in $NEW" }
    else { ERR "Cloud Scheduler '$sch' NOT FOUND in $NEW"; $abort = $true }
}

# Health check
if (-not $abort -and $svc) {
    try {
        $health = Invoke-WebRequest "$svc/health" -UseBasicParsing -TimeoutSec 10
        if ($health.StatusCode -eq 200) { OK "Health check passed ($svc/health -> 200)" }
        else { ERR "Health check returned $($health.StatusCode)"; $abort = $true }
    } catch {
        ERR "Health check failed: $($_.Exception.Message)"
        $abort = $true
    }
}

if ($abort) {
    Write-Host ""
    ERR "PRE-FLIGHT FAILED: asia-south1 resources are not ready."
    ERR "ABORTING - nothing will be deleted from us-central1."
    Write-Host ""
    exit 1
}

Write-Host ""
OK "All asia-south1 resources verified. Safe to proceed with cleanup."

# ==============================================================
# PHASE 2: Delete old us-central1 resources
# ==============================================================
Write-Host ""
Write-Host "--- PHASE 2: Deleting old resources from us-central1 ---" -ForegroundColor Yellow
Write-Host ""

# 2a. Delete Cloud Scheduler Jobs
Write-Host "  Schedulers:" -ForegroundColor DarkCyan
foreach ($sch in @("nubra-auth-refresh-daily", "nubra-live-start", "nubra-live-stop")) {
    $null = gcloud scheduler jobs describe $sch --location=$OLD --project=$PROJECT 2>$null
    if ($LASTEXITCODE -eq 0) {
        gcloud scheduler jobs delete $sch --location=$OLD --project=$PROJECT --quiet 2>$null
        if ($LASTEXITCODE -eq 0) {
            OK "Deleted scheduler '$sch' from $OLD"
            $deleted += "scheduler/$sch"
        } else {
            ERR "Failed to delete scheduler '$sch' from $OLD"
            $errors += "scheduler/$sch"
        }
    } else {
        SKIP "Scheduler '$sch' does not exist in $OLD (already cleaned)"
        $skipped += "scheduler/$sch"
    }
}

# 2b. Delete Cloud Run Jobs
Write-Host ""
Write-Host "  Jobs:" -ForegroundColor DarkCyan
foreach ($job in @("nubra-auth-refresh", "nubra-live-start", "nubra-live-stop")) {
    $null = gcloud run jobs describe $job --region=$OLD --project=$PROJECT 2>$null
    if ($LASTEXITCODE -eq 0) {
        gcloud run jobs delete $job --region=$OLD --project=$PROJECT --quiet 2>$null
        if ($LASTEXITCODE -eq 0) {
            OK "Deleted job '$job' from $OLD"
            $deleted += "job/$job"
        } else {
            ERR "Failed to delete job '$job' from $OLD"
            $errors += "job/$job"
        }
    } else {
        SKIP "Job '$job' does not exist in $OLD (already cleaned)"
        $skipped += "job/$job"
    }
}

# 2c. Delete Cloud Run Service
Write-Host ""
Write-Host "  Service:" -ForegroundColor DarkCyan
$null = gcloud run services describe nubra-live --region=$OLD --project=$PROJECT 2>$null
if ($LASTEXITCODE -eq 0) {
    gcloud run services delete nubra-live --region=$OLD --project=$PROJECT --quiet 2>$null
    if ($LASTEXITCODE -eq 0) {
        OK "Deleted service 'nubra-live' from $OLD"
        $deleted += "service/nubra-live"
    } else {
        ERR "Failed to delete service 'nubra-live' from $OLD"
        $errors += "service/nubra-live"
    }
} else {
    SKIP "Service 'nubra-live' does not exist in $OLD (already cleaned)"
    $skipped += "service/nubra-live"
}

# ==============================================================
# PHASE 3: Post-cleanup verification
# ==============================================================
Write-Host ""
Write-Host "--- PHASE 3: Post-cleanup verification ---" -ForegroundColor Yellow
Write-Host ""

Write-Host "  us-central1 (should be empty):" -ForegroundColor DarkCyan
$oldSvc = gcloud run services list --region=$OLD --project=$PROJECT --format="value(name)" 2>$null
$oldJobs = gcloud run jobs list --region=$OLD --project=$PROJECT --format="value(name)" 2>$null
$oldSch = gcloud scheduler jobs list --location=$OLD --project=$PROJECT --format="value(name)" 2>$null

if (-not $oldSvc)  { OK "No Cloud Run services in $OLD" }
else { ERR "Services still in ${OLD}: $oldSvc" }

if (-not $oldJobs) { OK "No Cloud Run jobs in $OLD" }
else { ERR "Jobs still in ${OLD}: $oldJobs" }

if (-not $oldSch)  { OK "No Cloud Scheduler jobs in $OLD" }
else { ERR "Schedulers still in ${OLD}: $oldSch" }

Write-Host ""
Write-Host "  asia-south1 (should have all resources):" -ForegroundColor DarkCyan
$newSvc = gcloud run services list --region=$NEW --project=$PROJECT --format="value(name)" 2>$null | Select-String "nubra-live"
$newJobs = gcloud run jobs list --region=$NEW --project=$PROJECT --format="value(name)" 2>$null
$newSch = gcloud scheduler jobs list --location=$NEW --project=$PROJECT --format="value(name)" 2>$null

if ($newSvc) { OK "Service 'nubra-live' running in $NEW" }
else { ERR "Service 'nubra-live' NOT in $NEW" }

foreach ($j in @("nubra-auth-refresh", "nubra-live-start", "nubra-live-stop")) {
    if ($newJobs -match $j) { OK "Job '$j' in $NEW" }
    else { ERR "Job '$j' NOT in $NEW" }
}

foreach ($s in @("nubra-auth-refresh-daily", "nubra-live-start", "nubra-live-stop")) {
    if ($newSch -match $s) { OK "Scheduler '$s' in $NEW" }
    else { ERR "Scheduler '$s' NOT in $NEW" }
}

# ==============================================================
# PHASE 4: Summary
# ==============================================================
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  CLEANUP SUMMARY" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Deleted from us-central1 : $($deleted.Count)" -ForegroundColor Green
foreach ($d in $deleted) { Write-Host "    - $d" -ForegroundColor Green }
Write-Host ""
if ($skipped.Count -gt 0) {
    Write-Host "  Already cleaned (skipped): $($skipped.Count)" -ForegroundColor DarkGray
    foreach ($s in $skipped) { Write-Host "    - $s" -ForegroundColor DarkGray }
    Write-Host ""
}
if ($errors.Count -gt 0) {
    Write-Host "  Errors: $($errors.Count)" -ForegroundColor Red
    foreach ($e in $errors) { Write-Host "    - $e" -ForegroundColor Red }
    Write-Host ""
}

if ($errors.Count -eq 0) {
    Write-Host "  [PASS] Migration cleanup complete. Only asia-south1 resources remain." -ForegroundColor Green
} else {
    Write-Host "  [WARN] Cleanup had errors. Review above." -ForegroundColor Yellow
}
Write-Host ""