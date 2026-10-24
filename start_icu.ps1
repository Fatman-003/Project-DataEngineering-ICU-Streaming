param([switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
$IcuPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
$ComposeFile = 'docker/flink/compose.yaml'

function Wait-Ready([scriptblock]$Check, [int]$Seconds, [string]$Failure) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    do {
        try { if (& $Check) { return } } catch { }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw $Failure
}
function Assert-Exit([string]$Failure) {
    if ($LASTEXITCODE -ne 0) { throw $Failure }
}

docker info | Out-Null
Assert-Exit 'Start Docker Desktop and rerun start_icu.ps1.'
if (-not (Test-Path -LiteralPath $IcuPython)) { throw 'Create .venv and install requirements.txt first.' }
& $IcuPython -B docker/flink/prepare_kafka.py
Assert-Exit 'Existing Kafka broker preparation failed; no broker was created.'
if ($SkipBuild) {
    docker compose -f $ComposeFile up -d --no-build
} else {
    docker compose -f $ComposeFile up -d --build
}
Assert-Exit 'ICU services could not start.'
Wait-Ready { (Invoke-RestMethod 'http://localhost:8081/overview').'slots-total' -ge 1 } 120 'Flink is not ready at 8081.'

$jobs = @((Invoke-RestMethod 'http://localhost:8081/jobs/overview').jobs)
$v2 = @($jobs | Where-Object { $_.state -eq 'RUNNING' -and $_.name -eq 'ICU business telemetry, 5m windows and 30s hypotension' })
foreach ($job in @($jobs | Where-Object { $_.state -in @('RUNNING', 'RESTARTING') -and $_.name -like 'ICU*' -and $_.name -match 'TICKET-00[5678]' })) {
    docker compose -f $ComposeFile exec -T jobmanager /opt/flink/bin/flink cancel $job.jid
    Assert-Exit 'Could not stop the previous ICU job.'
}
if ($v2.Count -eq 0) {
    Wait-Ready { (Invoke-RestMethod 'http://localhost:8081/overview').'slots-available' -ge 1 } 90 'Flink slot is occupied.'
    docker compose -f $ComposeFile exec -T jobmanager /opt/flink/bin/flink run -d -py /opt/flink/jobs/runtime_pipeline.py --offset earliest
    Assert-Exit 'V2 Flink submission failed.'
}
Wait-Ready {
    $running = @((Invoke-RestMethod 'http://localhost:8081/jobs/overview').jobs | Where-Object { $_.state -eq 'RUNNING' -and $_.name -eq 'ICU business telemetry, 5m windows and 30s hypotension' })
    if ($running.Count -ne 1) { return $false }
    $details = Invoke-RestMethod "http://localhost:8081/jobs/$($running[0].jid)"
    $checks = Invoke-RestMethod "http://localhost:8081/jobs/$($running[0].jid)/checkpoints"
    return @($details.vertices | Where-Object { $_.status -ne 'RUNNING' }).Count -eq 0 -and $checks.counts.completed -gt 0
} 180 'V2 job did not reach RUNNING with a completed checkpoint.'
Wait-Ready { (Invoke-WebRequest 'http://localhost:8082' -UseBasicParsing).StatusCode -eq 200 } 90 'Kafka UI is not ready at 8082.'
Wait-Ready { (Invoke-RestMethod 'http://localhost:8083/api/health').kafka_connected } 90 'Gateway is not connected at 8083.'
Write-Host 'Ready: Kafka 9092 | Flink 8081 | Kafka UI 8082 | Gateway 8083'
Write-Host 'Run: python scripts/demo/run_icu_demo.py'
