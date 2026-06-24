# ============================================================
# JVM Tuning Experiment Runner - Phase 6 Session 6.5
# ============================================================
# Runs all 6 JVM configurations, captures before/after GC stats
# and HdrHistogram latency percentiles via the REST API, and
# saves results to docs/performance/jvm-experiments/
#
# Usage:
#   .\run-jvm-experiments.ps1
#   .\run-jvm-experiments.ps1 -ConfigName ZGC_2GB   # run a single config
#   .\run-jvm-experiments.ps1 -DurationSeconds 30    # shorter runs for testing
#
# Prerequisites: Docker infrastructure running (kafka, redis, timescaledb)
# ============================================================

param(
    [string]$ConfigName = "ALL",
    [int]$DurationSeconds = 60,
    [int]$ServerStartTimeoutSeconds = 90
)

$projectDir = $PSScriptRoot
$resultsDir = Join-Path $projectDir "docs\performance\jvm-experiments"
$gradlew = Join-Path $projectDir "gradlew.bat"

# ---- Config definitions ----
$configs = [ordered]@{
    "G1GC_512MB"     = "-XX:+UseG1GC -Xmx512m -Xms256m"
    "G1GC_2GB"       = "-XX:+UseG1GC -Xmx2g -Xms1g"
    "ZGC_512MB"      = "-XX:+UseZGC -Xmx512m -Xms256m"
    "ZGC_2GB"        = "-XX:+UseZGC -Xmx2g -Xms1g"
    "G1GC_PRETOUCH"  = "-XX:+UseG1GC -Xmx2g -Xms2g -XX:+AlwaysPreTouch"
    "ZGC_PRETOUCH"   = "-XX:+UseZGC -Xmx2g -Xms2g -XX:+AlwaysPreTouch"
}

if ($ConfigName -ne "ALL" -and -not $configs.Contains($ConfigName)) {
    Write-Error "Unknown config: $ConfigName. Valid: $($configs.Keys -join ', ')"
    exit 1
}

# ---- Helpers ----

function Load-EnvFile {
    $envFile = Join-Path $projectDir ".env"
    if (-not (Test-Path $envFile)) {
        Write-Warning ".env file not found at $envFile - database credentials may be missing"
        return
    }
    Get-Content $envFile | Where-Object { $_ -match '^\s*[^#\s].*=.*' } | ForEach-Object {
        $parts = $_ -split '=', 2
        if ($parts.Count -eq 2) {
            $key = $parts[0].Trim()
            $val = $parts[1].Trim()
            [System.Environment]::SetEnvironmentVariable($key, $val, 'Process')
        }
    }
    Write-Host "[env] Loaded .env" -ForegroundColor Cyan
}

function Wait-ForServer {
    # Checks TCP port binding, not HTTP health status.
    # Rationale: Spring Boot's aggregate /actuator/health returns 503 (DOWN) during Kafka
    # consumer group rebalancing (which takes 30-90s after restart). By the time Netty
    # binds to port 8080, ALL Spring beans are initialized and the API is usable.
    # Kafka readiness is checked separately by Wait-ForKafkaWarmup.
    param([int]$TimeoutSeconds)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    Write-Host "[server] Waiting for port 8080 (timeout=${TimeoutSeconds}s)..." -ForegroundColor Cyan
    while ((Get-Date) -lt $deadline) {
        if (Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue) {
            Write-Host "[server] Port 8080 UP" -ForegroundColor Green
            return $true
        }
        Start-Sleep -Seconds 3
    }
    Write-Error "[server] Port 8080 did not open within ${TimeoutSeconds}s"
    return $false
}

function Wait-ForKafkaWarmup {
    # After a server restart, Kafka consumers catch up from their last committed offset.
    # Old messages have receivedTimestamp from the previous JVM - when checked against
    # the new JVM's System.nanoTime(), they appear stale (age = hours >> 500ms threshold).
    # The detection engine skips them all as stale, so no opportunities are detected and
    # HdrHistogram latency stays at 0.
    #
    # Solution: wait until the stale-skip RATE drops below a threshold, indicating
    # that the consumer has caught up to live messages with fresh receivedTimestamps.
    param([int]$WarmupTimeoutSeconds = 300, [double]$MaxStaleRatePerSec = 200)
    Write-Host "[warmup] Waiting for Kafka backlog to drain (stale skip rate < $MaxStaleRatePerSec/sec)..." -ForegroundColor Cyan
    $deadline = (Get-Date).AddSeconds($WarmupTimeoutSeconds)
    $prev = 0.0
    $prevTime = Get-Date

    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 15
        try {
            $metrics = Invoke-RestMethod -Uri "http://localhost:8080/actuator/prometheus" -TimeoutSec 5 -ErrorAction Stop
            $line = $metrics -split "`n" | Where-Object { $_ -match 'detection_stale_skips_total\{exchange="binance"\}' } | Select-Object -First 1
            $current = if ($line -match '[\d.]+$') { [double]$Matches[0] } else { 0.0 }
            $elapsed = ((Get-Date) - $prevTime).TotalSeconds
            $rate = if ($prev -gt 0 -and $elapsed -gt 0) { ($current - $prev) / $elapsed } else { 999.0 }
            Write-Host "[warmup] stale_skips=$([int]$current)  rate=$([int]$rate)/sec  target<${MaxStaleRatePerSec}/sec"
            if ($rate -lt $MaxStaleRatePerSec -and $current -gt 0) {
                Write-Host "[warmup] Backlog drained - live ticks flowing" -ForegroundColor Green
                return $true
            }
            $prev = $current
            $prevTime = Get-Date
        } catch {
            Write-Warning "[warmup] Could not read Prometheus metrics: $_"
        }
    }
    Write-Warning "[warmup] Backlog did not drain within ${WarmupTimeoutSeconds}s - running experiment anyway"
    return $false
}

function Stop-Server {
    # Note: $pid is a PowerShell read-only automatic variable (current process ID).
    # Use $proc as the loop variable to avoid the conflict.
    $procs = Get-NetTCPConnection -LocalPort 8080 -ErrorAction SilentlyContinue |
             Where-Object { $_.State -eq 'Listen' } |
             Select-Object -ExpandProperty OwningProcess
    foreach ($proc in $procs) {
        if ($proc -gt 0) {
            Write-Host "[server] Stopping PID $proc on port 8080" -ForegroundColor Yellow
            Stop-Process -Id $proc -Force -ErrorAction SilentlyContinue
        }
    }
    Start-Sleep -Seconds 3
}

function Invoke-Experiment {
    param([string]$Name, [int]$Duration)
    Write-Host "[experiment] POST /api/jvm-tuning/run-experiment?configName=$Name&mode=REALTIME&durationSeconds=$Duration" -ForegroundColor Cyan
    $totalTimeoutSec = $Duration + 30
    try {
        $resp = Invoke-RestMethod -Method Post `
            -Uri "http://localhost:8080/api/jvm-tuning/run-experiment?configName=$Name&mode=REALTIME&durationSeconds=$Duration" `
            -TimeoutSec $totalTimeoutSec -ErrorAction Stop
        return $resp
    } catch {
        Write-Error "Experiment failed: $_"
        return $null
    }
}

function Get-GcStats {
    try {
        return Invoke-RestMethod -Uri "http://localhost:8080/api/jvm-tuning/gc-stats" -TimeoutSec 5 -ErrorAction Stop
    } catch {
        return $null
    }
}

function Save-Result {
    param([string]$Name, $Result)
    $outDir = $resultsDir
    if (-not (Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir -Force | Out-Null }
    $outFile = Join-Path $outDir "$Name.json"
    $Result | ConvertTo-Json -Depth 10 | Out-File -FilePath $outFile -Encoding utf8
    Write-Host "[results] Saved to $outFile" -ForegroundColor Green
}

function Format-Summary {
    param($Result, [string]$Name)
    if ($null -eq $Result) { return "  ${Name}: FAILED" }
    $detP99 = if ($Result.detectionP99AfterMs) { "{0:F2}ms" -f $Result.detectionP99AfterMs } else { "n/a" }
    $e2eP99 = if ($Result.latencyResult.endToEndP99AfterMs) { "{0:F2}ms" -f $Result.latencyResult.endToEndP99AfterMs } else { "n/a" }
    $gcEvt  = $Result.gcEventsDuringTest
    $gcTime = if ($Result.gcTimeDuringTestMs -eq -1) { "n/a (ZGC)" } else { "$($Result.gcTimeDuringTestMs)ms" }
    return "  $Name : DETECTION_P99=$detP99  E2E_P99=$e2eP99  GC_events=$gcEvt  GC_time=$gcTime"
}

# ---- Main ----

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " JVM Tuning Experiments - Phase 6 Session 6.5"             -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

Load-EnvFile

# Build the fat JAR once so all 6 experiments use identical bytecode.
# Using java -jar avoids Gradle daemon env-var inheritance issues: a reused
# daemon started before JVM_GC_ARGS was set would return the old value from
# System.getenv(), causing wrong JVM flags on the forked Spring Boot process.
Write-Host "[build] Building dashboard-api fat JAR..." -ForegroundColor Cyan
& $gradlew ":dashboard-api:bootJar"
$jar = Get-ChildItem -Path (Join-Path $projectDir "dashboard-api\build\libs") -Filter "*.jar" |
       Where-Object { $_.Name -notlike "*plain*" } |
       Select-Object -First 1
if (-not $jar) { Write-Error "[build] Fat JAR not found. Aborting."; exit 1 }
Write-Host "[build] JAR ready: $($jar.Name)" -ForegroundColor Green

if (-not (Test-Path $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null }

$toRun = if ($ConfigName -eq "ALL") { $configs.Keys } else { @($ConfigName) }
$summaries = @()

foreach ($name in $toRun) {
    $gcArgs = $configs[$name]
    Write-Host ""
    Write-Host "---- Config: $name ----" -ForegroundColor Yellow
    Write-Host "  JVM_GC_ARGS: $gcArgs"
    Write-Host ""

    # Ensure port 8080 is free
    Stop-Server

    # Set GC flags for this run
    [System.Environment]::SetEnvironmentVariable("JVM_GC_ARGS", $gcArgs, 'Process')

    # Start the server directly via java -jar (no Gradle overhead, ~15s startup vs 120s+).
    # The project directory path contains spaces ("Personal Projects"), so the JAR path
    # must be explicitly quoted in the argument string. Passing as an array does NOT quote
    # paths with spaces in PowerShell 5.1's Start-Process, causing "Unable to access jarfile".
    $serverLog = Join-Path $resultsDir "${name}-server.log"
    $argString = "$gcArgs -jar `"$($jar.FullName)`""
    $serverProc = Start-Process -FilePath "java" -ArgumentList $argString `
        -WorkingDirectory $projectDir `
        -RedirectStandardOutput $serverLog -RedirectStandardError "$serverLog.err" `
        -PassThru
    Write-Host "[server] Starting PID $($serverProc.Id) with $gcArgs"

    # Wait for health
    $started = Wait-ForServer -TimeoutSeconds $ServerStartTimeoutSeconds
    if (-not $started) {
        Write-Warning "Skipping $name - server did not start"
        Stop-Server
        continue
    }

    # Wait for Kafka backlog to drain (old messages with stale receivedTimestamps)
    Wait-ForKafkaWarmup -WarmupTimeoutSeconds 300 -MaxStaleRatePerSec 200 | Out-Null

    # Confirm GC type
    $gcStats = Get-GcStats
    if ($gcStats) {
        Write-Host "[gc] Active GC: $($gcStats.gcType)  heap: $($gcStats.heapUsedMb)MB/$($gcStats.heapMaxMb)MB" -ForegroundColor Cyan
    }

    # Run the experiment (blocks for DurationSeconds + settle)
    Write-Host "[experiment] Running REALTIME load test for ${DurationSeconds}s + 12s settle..."
    $result = Invoke-Experiment -Name $name -Duration $DurationSeconds

    # Save raw JSON result
    if ($result) {
        Save-Result -Name $name -Result $result
    }

    $summaries += Format-Summary -Result $result -Name $name

    # Stop server
    Stop-Server
    Write-Host "[server] Stopped"
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " SUMMARY"                                                     -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
$summaries | ForEach-Object { Write-Host $_ }
Write-Host ""
Write-Host "Raw JSON results in: $resultsDir"
Write-Host ""
