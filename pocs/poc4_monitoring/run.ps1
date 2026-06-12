# run.ps1 - Spusti Airflow + Prometheus + Grafana stack
# Pouziti: .\run.ps1
# Stop:    docker compose down

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host "  PoC 4: Monitoring (Prometheus)" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

# Build
Write-Host "Building images..." -ForegroundColor Yellow
docker compose build 2>&1 | Out-String -Stream | Where-Object { $_ -match "Built|Error" }

# Init
Write-Host "Inicializace DB..." -ForegroundColor Yellow
$ErrorActionPreference = "Continue"
docker compose up airflow-init 2>&1 | Out-String | Select-String "exited with code"
$ErrorActionPreference = "Stop"

# Start
Write-Host "Startuji stack..." -ForegroundColor Yellow
docker compose up -d 2>&1 | Out-String -Stream | Where-Object { $_ -and $_ -notmatch "level=warning" }

Write-Host ""
Write-Host "======================================" -ForegroundColor Green
Write-Host "  Stack bezi!" -ForegroundColor Green
Write-Host "  Airflow UI:  http://localhost:8080" -ForegroundColor Green
Write-Host "  Grafana:     http://localhost:3000  (admin/admin)" -ForegroundColor Green
Write-Host "  Prometheus:  http://localhost:9090" -ForegroundColor Green
Write-Host "  Login:       airflow / airflow" -ForegroundColor Green
Write-Host "======================================" -ForegroundColor Green
Write-Host ""
Write-Host "Trigger demo:" -ForegroundColor Yellow
Write-Host "  .\trigger-and-watch.ps1"
Write-Host ""
Write-Host "Sprava:" -ForegroundColor Yellow
Write-Host "  docker compose ps       # stav"
Write-Host "  docker compose logs -f  # logy"
Write-Host "  docker compose down     # zastavit"
Write-Host "  docker compose down -v  # zastavit + smazat data"
