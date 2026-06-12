# test.ps1 - E2E testy pro gud10_scheduling_datasets
# Spousti pytest e2e testy (stack se startuje/zastavuje automaticky)

$ErrorActionPreference = "Stop"
$guidesDir = Split-Path -Parent $PSScriptRoot

Write-Host ""
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "  TEST: gud10_scheduling_datasets" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""

python -m pytest "$guidesDir\pytests\e2e\test_gud10.py" -v --tb=short 2>&1
exit $LASTEXITCODE
