# run.ps1 - Spusti Airflow stack s dynamickym portem
# Pouziti: .\run.ps1
# Stop:    docker compose down

$ErrorActionPreference = "Stop"

# Zjisti kolik Airflow instanci uz bezi (pocet apiserver kontejneru)
$runningCount = (docker ps --filter "ancestor=apache/airflow:3.2.2" --filter "publish=8080" -q 2>$null | Measure-Object -Line).Lines

# Vypocti port: zakladni 8080, kazda dalsi instance +1
$port = 8080 + $runningCount
Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host "  Airflow stack - port $port" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

# Zmen port v docker-compose pres environment variable
$env:AIRFLOW_APISERVER_PORT = $port

# Over ze port je volny
$portInUse = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue
if ($portInUse) {
    Write-Host "CHYBA: Port $port je obsazeny!" -ForegroundColor Red
    Write-Host "Bezici Airflow instance:" -ForegroundColor Yellow
    docker ps --filter "ancestor=apache/airflow:3.2.2" --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}"
    exit 1
}

# Init (pokud jeste nebyla DB inicializovana)
Write-Host "Inicializace DB..." -ForegroundColor Yellow
$ErrorActionPreference = "Continue"
docker compose up airflow-init 2>&1 | Out-String | Select-String "exited with code"

# Start
Write-Host "Startuji Airflow..." -ForegroundColor Yellow
docker compose up -d 2>&1 | Out-String -Stream | Where-Object { $_ -and $_ -notmatch "level=warning" }
$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "======================================" -ForegroundColor Green
Write-Host "  Airflow bezi!" -ForegroundColor Green
Write-Host "  UI:    http://localhost:$port" -ForegroundColor Green
Write-Host "  Login: airflow / airflow" -ForegroundColor Green
Write-Host "======================================" -ForegroundColor Green
Write-Host ""
Write-Host "Sprava:" -ForegroundColor Yellow
Write-Host "  docker compose ps       # stav"
Write-Host "  docker compose logs -f  # logy"
Write-Host "  docker compose down     # zastavit"
Write-Host "  docker compose down -v  # zastavit + smazat data"
