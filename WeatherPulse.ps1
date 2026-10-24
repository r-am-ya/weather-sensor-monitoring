$ErrorActionPreference = 'Stop'
$Project = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $Project '.venv\Scripts\python.exe'
$Url = 'http://127.0.0.1:8503'
$Log = Join-Path $Project 'weatherpulse-startup.log'
Set-Location -LiteralPath $Project
Start-Transcript -Path $Log -Append | Out-Null

function Fail([string]$Message) {
    Write-Host "`nERROR: $Message`nSee $Log for launcher diagnostics." -ForegroundColor Red
    Stop-Transcript | Out-Null
    Read-Host 'Press Enter to close'
    exit 1
}
function Test-ProcessCommand([string]$Needle) {
    return [bool](Get-CimInstance Win32_Process -Filter "name = 'python.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.ExecutablePath -eq $Python -and $_.CommandLine -like "*$Needle*" } | Select-Object -First 1)
}

Write-Host '=== WeatherPulse startup ===' -ForegroundColor Cyan
if (-not (Test-Path $Python)) { Fail "Virtual environment Python not found: $Python. Run .venv setup first." }
if (-not (Test-Path (Join-Path $Project 'docker-compose.yml'))) { Fail 'docker-compose.yml is missing.' }
try { docker info *> $null; if ($LASTEXITCODE -ne 0) { Fail 'Docker Desktop is not ready. Start Docker Desktop and retry.' } }
catch { Fail 'Docker is unavailable. Start Docker Desktop and retry.' }

Write-Host '[1/5] Starting Kafka and MySQL...' -ForegroundColor Yellow
docker compose up -d
if ($LASTEXITCODE -ne 0) { Fail 'docker compose up -d failed; inspect output above and docker compose logs.' }

Write-Host '[2/5] Waiting for Kafka topic and MySQL health...' -ForegroundColor Yellow
$Ready = $false
for ($Attempt=0; $Attempt -lt 60; $Attempt++) {
    $Topics = docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list 2>$null
    $KafkaReady = ($LASTEXITCODE -eq 0)
    $TopicReady = ($Topics -contains 'weather-events')
    $MysqlId = docker compose ps -q mysql
    $MysqlHealth = if ($MysqlId) { docker inspect --format '{{.State.Health.Status}}' $MysqlId 2>$null } else { '' }
    $MySqlReady = ($MysqlHealth -eq 'healthy')
    if ($KafkaReady -and $TopicReady -and $MySqlReady) { $Ready = $true; break }
    Start-Sleep -Seconds 2
}
if (-not $Ready) { docker compose ps; docker compose logs --tail 40 kafka mysql; Fail 'Kafka/MySQL did not become ready within 120 seconds.' }
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --describe --topic weather-events
if ($LASTEXITCODE -ne 0) { Fail 'Kafka topic weather-events is missing; check the kafka-init service logs.' }

Write-Host '[3/5] Starting producer and processor...' -ForegroundColor Yellow
if (-not (Test-ProcessCommand '-m src.producer')) {
    Start-Process powershell.exe -ArgumentList @('-NoExit','-NoProfile','-ExecutionPolicy','Bypass','-Command',"Set-Location -LiteralPath '$Project'; & '$Python' -m src.producer")
} else { Write-Host 'Producer already running; reusing it.' }
if (-not (Test-ProcessCommand '-m src.stream_processor')) {
    Start-Process powershell.exe -ArgumentList @('-NoExit','-NoProfile','-ExecutionPolicy','Bypass','-Command',"Set-Location -LiteralPath '$Project'; & '$Python' -m src.stream_processor")
} else { Write-Host 'Stream processor already running; reusing it.' }

Write-Host '[4/5] Starting dashboard on port 8503...' -ForegroundColor Yellow
$Listener = Get-NetTCPConnection -LocalPort 8503 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if ($Listener) {
    $DashboardRunning = Test-ProcessCommand 'streamlit run'
    if (-not $DashboardRunning) { Fail 'Port 8503 is occupied by another application. Stop it or configure another port.' }
    Write-Host 'WeatherPulse dashboard already running; reusing it.'
} else {
    Start-Process powershell.exe -ArgumentList @('-NoExit','-NoProfile','-ExecutionPolicy','Bypass','-Command',"Set-Location -LiteralPath '$Project'; & '$Python' -m streamlit run 'src/dashboard.py' --server.port 8503")
}

Write-Host '[5/5] Waiting for the dashboard HTTP endpoint...' -ForegroundColor Yellow
$DashboardReady = $false
for ($Attempt=0; $Attempt -lt 40; $Attempt++) {
    try { $Response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2; if ($Response.StatusCode -ge 200 -and $Response.StatusCode -lt 500) { $DashboardReady=$true; break } } catch { }
    Start-Sleep -Seconds 2
}
if (-not $DashboardReady) { Fail 'Streamlit did not respond within 80 seconds. Inspect its terminal and weatherpulse-startup.log.' }
Write-Host "WeatherPulse is ready: $Url" -ForegroundColor Green
Start-Process $Url
Write-Host 'To stop: close producer/processor/dashboard terminals, then run: docker compose stop'
Write-Host 'Persistent Docker volumes are kept.'
Stop-Transcript | Out-Null
Read-Host 'Press Enter to close'
