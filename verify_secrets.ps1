$PROJECT = "stock-anaysis"
$SERVICE = "nubra-live"
$REGION  = "asia-south1"

function Write-Header($t) {
    Write-Host ""
    Write-Host ("="*70) -ForegroundColor Cyan
    Write-Host "  $t" -ForegroundColor Cyan
    Write-Host ("="*70) -ForegroundColor Cyan
}
function Write-Sub($t) { Write-Host "" ; Write-Host "  -- $t --" -ForegroundColor DarkCyan }
function OK($m)   { Write-Host "  [PASS] $m" -ForegroundColor Green }
function ERR($m)  { Write-Host "  [FAIL] $m" -ForegroundColor Red }
function WARN($m) { Write-Host "  [WARN] $m" -ForegroundColor Yellow }

function Mask-Value($name,$val) {
    if (-not $val) { return "<empty>" }
    $never = @("MPIN","DB_PASSWORD","DB_PASSWORD_SECRET")
    $full  = @("PHONE_NO","NUBRA_ENV")
    if ($never -contains $name) { return "***REDACTED***" }
    if ($full  -contains $name) { return $val }
    if ($val.Length -le 10)     { return ("*" * $val.Length) }
    return "$($val.Substring(0,6))...$($val.Substring($val.Length-4))"
}

$report  = [System.Collections.Generic.List[hashtable]]::new()
$updates = [System.Collections.Generic.List[string]]::new()

function Add-Report($secret,$check,$status,$detail) {
    $report.Add(@{ Secret=$secret; Check=$check; Status=$status; Detail=$detail })
}

Write-Header "STEP 1 -- All secrets in project $PROJECT"
$allSecrets = gcloud secrets list --project=$PROJECT --format="value(name)" 2>$null
Write-Host ""
$allSecrets | ForEach-Object { Write-Host "  - $_" }

Write-Header "STEP 2 -- Detailed check of each secret"
$secretMeta = @{}

foreach ($sec in $allSecrets) {
    $vline = gcloud secrets versions list $sec --project=$PROJECT --sort-by="~createTime" --limit=1 --format="value(name,state,createTime)" 2>$null
    if (-not $vline) { $secretMeta[$sec] = @{ exists=$false }; continue }
    $parts = ($vline -split "\s+")
    $verNum = ($parts[0] -split "/")[-1]
    $state  = $parts[1]
    $ctime  = if ($parts.Count -ge 3) { $parts[2] } else { "unknown" }
    $value  = gcloud secrets versions access latest --secret=$sec --project=$PROJECT 2>$null
    $secretMeta[$sec] = @{ exists=$true; versionNum=$verNum; state=$state; createTime=$ctime; value=$value }

    Write-Sub "Secret: $sec"
    Write-Host "    Latest version : $verNum"
    Write-Host "    State          : $state"
    Write-Host "    Created        : $ctime"
    Write-Host "    Value preview  : $(Mask-Value $sec $value)"
    if ($state -eq "ENABLED") { OK  "Version $verNum ENABLED" }
    else                      { ERR "Version $verNum state=$state" }
}

Write-Header "STEP 3 -- Cloud Run service description"
$svcJson = gcloud run services describe $SERVICE --region=$REGION --project=$PROJECT --format=json 2>$null | ConvertFrom-Json
if (-not $svcJson) { ERR "Cannot retrieve Cloud Run service '$SERVICE'"; exit 1 }

Write-Sub "3a. Plain environment variables"
$plainEnv = $svcJson.spec.template.spec.containers[0].env | Where-Object { $_.value -ne $null }
if ($plainEnv) { $plainEnv | ForEach-Object { Write-Host "    $($_.name) = $($_.value)" } }
else { WARN "No plain env vars found" }

Write-Sub "3b. Secret-backed environment variables"
$secretEnvs = $svcJson.spec.template.spec.containers[0].env | Where-Object { $_.valueFrom.secretKeyRef -ne $null }
$crRefs = @{}
if ($secretEnvs) {
    foreach ($e in $secretEnvs) {
        $ref = $e.valueFrom.secretKeyRef
        Write-Host "    $($e.name)  <--  secret=$($ref.name)  version=$($ref.key)"
        $crRefs[$e.name] = @{ secretName=$ref.name; version=$ref.key }
    }
} else { WARN "No secret-backed env vars found in Cloud Run" }

Write-Sub "3c. Mounted secret volumes"
$vols = $svcJson.spec.template.spec.volumes
if ($vols) { $vols | Where-Object { $_.secret } | ForEach-Object { Write-Host "    Volume '$($_.name)' -> secret=$($_.secret.secretName)" } }
else { Write-Host "    (none)" }

Write-Header "STEP 4 -- Cross-check Secret Manager vs Cloud Run"
$expected = @{
    "NUBRA_AUTH_TOKEN"    = "nubra-auth-token"
    "NUBRA_X_DEVICE_ID"   = "nubra-x-device-id"
    "NUBRA_SESSION_TOKEN" = "nubra-session-token"
    "PHONE_NO"            = "PHONE_NO"
    "MPIN"                = "MPIN"
    "NUBRA_TOTP_SECRET"   = "NUBRA_TOTP_SECRET"
}

foreach ($envName in $expected.Keys) {
    $smName = $expected[$envName]
    Write-Sub "Checking: $envName -> $smName"

    if ($secretMeta.ContainsKey($smName) -and $secretMeta[$smName].exists) {
        OK "Secret '$smName' exists in Secret Manager"
        Add-Report $smName "exists" "PASS" "found"
    } else {
        ERR "Secret '$smName' NOT FOUND in Secret Manager"
        Add-Report $smName "exists" "FAIL" "missing"
        $updates.Add("echo -n 'VALUE' | gcloud secrets create $smName --replication-policy=automatic --data-file=- --project=$PROJECT")
        continue
    }

    $m = $secretMeta[$smName]
    if ($m.state -eq "ENABLED") { OK "v$($m.versionNum) ENABLED"; Add-Report $smName "version_enabled" "PASS" "v$($m.versionNum) ENABLED" }
    else { ERR "v$($m.versionNum) state=$($m.state)"; Add-Report $smName "version_enabled" "FAIL" "v$($m.versionNum) $($m.state)" }

    $cr = $crRefs[$envName]
    if ($cr) {
        if ($cr.secretName -eq $smName) { OK "Cloud Run '$envName' -> correct secret '$smName'"; Add-Report $smName "cr_reference" "PASS" "$envName -> $smName" }
        else { ERR "Cloud Run '$envName' -> WRONG secret '$($cr.secretName)' (expected '$smName')"; Add-Report $smName "cr_reference" "FAIL" "wrong secret" }

        if ($cr.version -eq "latest") { OK "Version pinned to 'latest'"; Add-Report $smName "cr_version" "PASS" "latest" }
        else {
            WARN "Pinned to version $($cr.version) -- not 'latest'"
            Add-Report $smName "cr_version" "WARN" "pinned v$($cr.version)"
            $updates.Add("gcloud run services update $SERVICE --region=$REGION --project=$PROJECT --update-secrets=""$envName=${smName}:latest""")
        }
    } else {
        ERR "Cloud Run does NOT have '$envName' from secrets"
        Add-Report $smName "cr_reference" "FAIL" "not attached"
        $updates.Add("gcloud run services update $SERVICE --region=$REGION --project=$PROJECT --update-secrets=""$envName=${smName}:latest""")
    }

    if ($m.value) { OK "Value non-empty ($(Mask-Value $smName $m.value))"; Add-Report $smName "value_loaded" "PASS" "non-empty" }
    else { ERR "Value is EMPTY"; Add-Report $smName "value_loaded" "FAIL" "empty" }
}

Write-Header "STEP 5 -- Auth sanity checks"

$nubraEnv = ($svcJson.spec.template.spec.containers[0].env | Where-Object { $_.name -eq "NUBRA_ENV" }).value
Write-Host "  NUBRA_ENV = $nubraEnv"
if ($nubraEnv -eq "PROD")     { OK "NUBRA_ENV=PROD (production)" }
elseif ($nubraEnv -eq "UAT")  { WARN "NUBRA_ENV=UAT (not production)" }
else                          { ERR "NUBRA_ENV='$nubraEnv' unexpected" }

$totpM = $secretMeta["NUBRA_TOTP_SECRET"]
if ($totpM -and $totpM.value) {
    $clean = ($totpM.value.Trim().ToUpper() -replace "\s","")
    if ($clean -match "^[A-Z2-7]+=*$" -and $clean.Length -ge 16) {
        OK "NUBRA_TOTP_SECRET is valid base32 (len=$($clean.Length))"
        Add-Report "NUBRA_TOTP_SECRET" "base32_valid" "PASS" "len=$($clean.Length)"
    } else {
        ERR "NUBRA_TOTP_SECRET is NOT valid base32 -- TOTP will fail"
        Add-Report "NUBRA_TOTP_SECRET" "base32_valid" "FAIL" "not base32"
    }
}

$devM = $secretMeta["nubra-x-device-id"]
if ($devM -and $devM.value) {
    $dv = $devM.value.Trim()
    if ($dv -match "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$") {
        OK "NUBRA_X_DEVICE_ID is a valid UUID"
        Add-Report "nubra-x-device-id" "uuid_valid" "PASS" "valid UUID"
    } else {
        WARN "NUBRA_X_DEVICE_ID does not match UUID pattern"
        Add-Report "nubra-x-device-id" "uuid_valid" "WARN" "not UUID"
    }
}

$sessM = $secretMeta["nubra-session-token"]
if ($sessM -and $sessM.value) {
    $jwt   = $sessM.value.Trim()
    $parts = $jwt -split "\."
    if ($parts.Count -eq 3) {
        try {
            $pad     = 4 - ($parts[1].Length % 4); if ($pad -eq 4) { $pad = 0 }
            $payload = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($parts[1] + ("=" * $pad))) | ConvertFrom-Json
            $exp     = $payload.exp
            if ($exp) {
                $expDate = [System.DateTimeOffset]::FromUnixTimeSeconds([long]$exp).LocalDateTime
                if ($expDate -gt (Get-Date)) {
                    OK "NUBRA_SESSION_TOKEN valid until $expDate"
                    Add-Report "nubra-session-token" "jwt_expiry" "PASS" "valid until $expDate"
                } else {
                    ERR "NUBRA_SESSION_TOKEN EXPIRED at $expDate"
                    Add-Report "nubra-session-token" "jwt_expiry" "FAIL" "expired $expDate"
                    $updates.Add("# Refresh by running:  python setup_totp.py --env $nubraEnv")
                    $updates.Add("# Then update secret:  echo -n 'NEW_TOKEN' | gcloud secrets versions add nubra-session-token --data-file=- --project=$PROJECT")
                }
            } else { WARN "JWT has no 'exp' claim"; Add-Report "nubra-session-token" "jwt_expiry" "WARN" "no exp" }
        } catch {
            WARN "Could not decode JWT: $_"
            Add-Report "nubra-session-token" "jwt_expiry" "WARN" "decode error"
        }
    } else {
        WARN "Not a 3-part JWT"
        Add-Report "nubra-session-token" "jwt_expiry" "WARN" "not JWT"
    }
}

Write-Header "STEP 6 -- Fix commands"
if ($updates.Count -eq 0) {
    OK "No fixes required"
} else {
    Write-Host "`n  Run these commands to fix issues:`n" -ForegroundColor Yellow
    $i = 1
    foreach ($cmd in $updates) {
        Write-Host "  [$i] $cmd" -ForegroundColor Yellow
        $i++
    }
}
Write-Host ""
Write-Host "  Generic update command:" -ForegroundColor Gray
Write-Host '  echo -n "NEW_VALUE" | gcloud secrets versions add SECRET_NAME --data-file=- --project=stock-anaysis' -ForegroundColor DarkGray
Write-Host ""
Write-Host "  Restart Cloud Run (reloads latest secrets):" -ForegroundColor Gray
Write-Host "  gcloud run services update $SERVICE --region=$REGION --project=$PROJECT" -ForegroundColor DarkGray

Write-Header "FINAL VALIDATION REPORT"
Write-Host ""

$report | Sort-Object Secret,Check | ForEach-Object {
    $icon  = if ($_.Status -eq "PASS") { "[+]" } elseif ($_.Status -eq "FAIL") { "[X]" } else { "[!]" }
    $color = if ($_.Status -eq "PASS") { "Green" } elseif ($_.Status -eq "FAIL") { "Red" } else { "Yellow" }
    Write-Host ("  {0,-5} {1,-26} {2,-22} {3}" -f $icon, $_.Secret, $_.Check, $_.Detail) -ForegroundColor $color
}

$pass = ($report | Where-Object { $_.Status -eq "PASS" }).Count
$fail = ($report | Where-Object { $_.Status -eq "FAIL" }).Count
$warn = ($report | Where-Object { $_.Status -eq "WARN" }).Count

Write-Host ""
Write-Host ("  Total: {0} passed  {1} warnings  {2} failed" -f $pass,$warn,$fail) `
    -ForegroundColor $(if ($fail -gt 0) { "Red" } elseif ($warn -gt 0) { "Yellow" } else { "Green" })

Write-Host ""
if ($fail -eq 0 -and $warn -eq 0) {
    Write-Host "  [+] Authentication configuration looks correct" -ForegroundColor Green
} elseif ($fail -eq 0) {
    Write-Host "  [!] Configuration has warnings -- review above" -ForegroundColor Yellow
} else {
    Write-Host "  [X] $fail critical issue(s) found -- fix before redeploying" -ForegroundColor Red
    Write-Host "      See STEP 6 above for fix commands" -ForegroundColor Red
}
Write-Host ""