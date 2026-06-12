# run.ps1 - Spusti Airflow + Zabbix stack
# Pouziti: .\run.ps1
# Stop:    docker compose down

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host "  PoC 5: Monitoring (Zabbix)" -ForegroundColor Cyan
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

# Wait for Zabbix + Airflow to be ready
Write-Host ""
Write-Host "Cekam na start Zabbix + Airflow (~60s)..." -ForegroundColor Yellow
$ready = $false
for ($i = 0; $i -lt 12; $i++) {
    Start-Sleep -Seconds 10
    try {
        $health = Invoke-RestMethod -Uri "http://localhost:8080/api/v2/monitor/health" -ErrorAction SilentlyContinue
        $zabbix = Invoke-WebRequest -Uri "http://localhost:8081" -ErrorAction SilentlyContinue
        if ($health -and $zabbix.StatusCode -eq 200) {
            $ready = $true
            break
        }
    } catch {}
    Write-Host "  ...cekam ($((($i+1)*10))s)" -ForegroundColor DarkGray
}

if ($ready) {
    # Auto-configure Zabbix
    Write-Host "Konfiguruji Zabbix monitoring..." -ForegroundColor Yellow
    bash scripts/setup-zabbix.sh 2>&1 | ForEach-Object {
        if ($_ -match "OK|Created|complete") { Write-Host "  $_" -ForegroundColor Green }
        elseif ($_ -match "ERROR") { Write-Host "  $_" -ForegroundColor Red }
        elseif ($_ -match "exists") { Write-Host "  $_" -ForegroundColor DarkGray }
    }
} else {
    Write-Host "Stack jeste neni ready — spustte rucne: bash scripts/setup-zabbix.sh" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "======================================" -ForegroundColor Green
Write-Host "  Stack bezi!" -ForegroundColor Green
Write-Host "  Airflow UI: http://localhost:8080  (airflow/airflow)" -ForegroundColor Green
Write-Host "  Zabbix UI:  http://localhost:8081  (Admin/zabbix)" -ForegroundColor Green
Write-Host "======================================" -ForegroundColor Green
Write-Host ""
Write-Host "Zabbix data:" -ForegroundColor Yellow
Write-Host "  Monitoring -> Latest data -> Host: Airflow Server"
Write-Host ""
Write-Host "Sprava:" -ForegroundColor Yellow
Write-Host "  docker compose ps       # stav"
Write-Host "  docker compose logs -f  # logy"
Write-Host "  docker compose down     # zastavit"
Write-Host "  docker compose down -v  # zastavit + smazat data"
