# test.ps1 - E2E testy pro gud9_taskgroups_dynamic
# Spousti pytest e2e testy (stack se startuje/zastavuje automaticky)

$ErrorActionPreference = "Stop"
$guidesDir = Split-Path -Parent $PSScriptRoot

Write-Host ""
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "  TEST: gud9_taskgroups_dynamic" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""

python -m pytest "$guidesDir\pytests\e2e\test_gud9.py" -v --tb=short 2>&1
exit $LASTEXITCODE
