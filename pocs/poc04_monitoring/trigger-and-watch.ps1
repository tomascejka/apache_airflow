<#
.SYNOPSIS
    Triggers monitoring_demo DAG and opens Grafana dashboard for live observation.

.DESCRIPTION
    1. Unpauses and triggers the monitoring_demo DAG via CLI (inside container)
    2. Opens Grafana dashboard in default browser
    3. Polls task instance status every 5s until the run completes

.EXAMPLE
    .\trigger-and-watch.ps1
    .\trigger-and-watch.ps1 -DagId edge_automotive_etl
    .\trigger-and-watch.ps1 -SkipBrowser
#>
param(
    [string]$DagId = "monitoring_demo",
    [switch]$SkipBrowser
)

$ErrorActionPreference = "Stop"
$composeDir = $PSScriptRoot

Write-Host "=== Airflow Trigger & Watch ===" -ForegroundColor Cyan
Write-Host "DAG: $DagId"
Write-Host ""

# 1. Unpause
Write-Host "[1/4] Unpausing DAG..." -ForegroundColor Yellow
docker compose -f "$composeDir/docker-compose.yaml" exec airflow-scheduler airflow dags unpause $DagId 2>&1 | Out-Null
Write-Host "      OK" -ForegroundColor Green

# 2. Trigger
Write-Host "[2/4] Triggering DAG..." -ForegroundColor Yellow
$triggerOutput = docker compose -f "$composeDir/docker-compose.yaml" exec airflow-scheduler airflow dags trigger $DagId 2>&1
$runLine = $triggerOutput | Select-String "run_id"
if ($runLine) {
    Write-Host "      $runLine" -ForegroundColor Green
} else {
    Write-Host "      Triggered (output: $($triggerOutput | Select-Object -First 2))" -ForegroundColor Green
}

# 3. Open Grafana
if (-not $SkipBrowser) {
    Write-Host "[3/4] Opening Grafana dashboard..." -ForegroundColor Yellow
    Start-Process "http://localhost:3000/d/airflow-monitoring/airflow-monitoring?orgId=1&refresh=5s&from=now-5m&to=now"
    Write-Host "      Opened in browser (admin/admin)" -ForegroundColor Green
} else {
    Write-Host "[3/4] Skipping browser (use http://localhost:3000/d/airflow-monitoring)" -ForegroundColor DarkGray
}

# 4. Poll status
Write-Host "[4/4] Watching task instances..." -ForegroundColor Yellow
Write-Host ""

$maxWait = 300  # 5 minutes max
$elapsed = 0
$interval = 5

while ($elapsed -lt $maxWait) {
    $status = docker compose -f "$composeDir/docker-compose.yaml" exec airflow-scheduler `
        airflow tasks states-for-dag-run $DagId "last" 2>&1

    # Parse task states
    $running = ($status | Select-String "running").Count
    $success = ($status | Select-String "success").Count
    $failed  = ($status | Select-String "failed").Count
    $queued  = ($status | Select-String "queued").Count
    $scheduled = ($status | Select-String "scheduled").Count
    $none    = ($status | Select-String "none").Count

    $timestamp = Get-Date -Format "HH:mm:ss"

    # Color-coded status line
    $line = "  [$timestamp] "
    $parts = @()
    if ($running -gt 0)   { $parts += "running=$running" }
    if ($queued -gt 0)    { $parts += "queued=$queued" }
    if ($scheduled -gt 0) { $parts += "scheduled=$scheduled" }
    if ($success -gt 0)   { $parts += "success=$success" }
    if ($failed -gt 0)    { $parts += "FAILED=$failed" }

    $statusText = $parts -join " | "

    if ($failed -gt 0) {
        Write-Host "$line$statusText" -ForegroundColor Red
    } elseif ($running -gt 0) {
        Write-Host "$line$statusText" -ForegroundColor Cyan
    } elseif ($queued -gt 0 -or $scheduled -gt 0) {
        Write-Host "$line$statusText" -ForegroundColor Yellow
    } else {
        Write-Host "$line$statusText" -ForegroundColor Green
    }

    # Check if done
    if ($running -eq 0 -and $queued -eq 0 -and $scheduled -eq 0 -and $none -eq 0 -and ($success -gt 0 -or $failed -gt 0)) {
        Write-Host ""
        if ($failed -gt 0) {
            Write-Host "DAG run FINISHED with $failed failed task(s)." -ForegroundColor Red
        } else {
            Write-Host "DAG run FINISHED successfully ($success tasks)." -ForegroundColor Green
        }
        Write-Host "Check Grafana: http://localhost:3000/d/airflow-monitoring" -ForegroundColor Cyan
        break
    }

    Start-Sleep -Seconds $interval
    $elapsed += $interval
}

if ($elapsed -ge $maxWait) {
    Write-Host "Timeout after ${maxWait}s - DAG still running. Check Grafana manually." -ForegroundColor Yellow
}
