param(
    [double]$DurationMinutes = 6,
    [double]$IntervalSeconds = 1,
    [ValidateSet(100, 200)]
    [int]$HighFrequency,
    [switch]$Loop,
    [switch]$Raw,
    [switch]$FastVerify,
    [switch]$KeepDatabaseData,
    [switch]$SkipBuild,
    [switch]$SkipDockerStart,
    [string]$StateReport
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
$IcuPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $IcuPython)) {
    Write-Error '[ERROR] Prerequisites: .venv\Scripts\python.exe is missing. Create the project venv and install requirements.txt.'
    exit 1
}
$DemoArguments = @('-u', '-B', (Join-Path $PSScriptRoot 'scripts\demo\orchestrate.py'),
    '--duration-minutes', $DurationMinutes.ToString([Globalization.CultureInfo]::InvariantCulture),
    '--interval-seconds', $IntervalSeconds.ToString([Globalization.CultureInfo]::InvariantCulture))
if ($Loop) { $DemoArguments += '--loop' }
if ($Raw) { $DemoArguments += '--raw' }
if ($FastVerify) { $DemoArguments += '--fast-verify' }
if ($PSBoundParameters.ContainsKey('HighFrequency')) { $DemoArguments += @('--high-frequency', $HighFrequency.ToString()) }
if ($KeepDatabaseData) { $DemoArguments += '--keep-database-data' }
if ($SkipBuild) { $DemoArguments += '--skip-build' }
if ($SkipDockerStart) { $DemoArguments += '--skip-docker-start' }
if ($StateReport) { $DemoArguments += @('--state-report', $StateReport) }
try {
    & $IcuPython @DemoArguments
    $DemoExitCode = $LASTEXITCODE
} finally {
    Write-Host '[INFO] Infrastructure remains running for inspection.'
}
exit $DemoExitCode
