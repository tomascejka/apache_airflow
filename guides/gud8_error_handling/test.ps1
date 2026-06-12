# test.ps1 - E2E testy pro gud8_error_handling
# Spousti pytest e2e testy (stack se startuje/zastavuje automaticky)

$ErrorActionPreference = "Stop"
$guidesDir = Split-Path -Parent $PSScriptRoot

Write-Host ""
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "  TEST: gud8_error_handling" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""

python -m pytest "$guidesDir\pytests\e2e\test_gud8.py" -v --tb=short 2>&1
exit $LASTEXITCODE
