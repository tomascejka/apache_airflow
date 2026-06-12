# test.ps1 - E2E testy pro gud4_python_operator
# Spousti pytest e2e testy (stack se startuje/zastavuje automaticky)

$ErrorActionPreference = "Stop"
$guidesDir = Split-Path -Parent $PSScriptRoot

Write-Host ""
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "  TEST: gud4_python_operator" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""

python -m pytest "$guidesDir\pytests\e2e\test_gud4.py" -v --tb=short 2>&1
exit $LASTEXITCODE
