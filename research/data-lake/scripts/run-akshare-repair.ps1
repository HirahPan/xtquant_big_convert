[CmdletBinding()]
param(
    [string]$Root = "D:\QMT-data",
    # Each invocation reads the primary bar store once.  A larger bounded batch
    # amortizes that read while keeping remote AKShare requests sequential.
    [int]$BatchSize = 100,
    [int]$PauseSeconds = 1,
    [int]$MaxBatches = 0,
    [switch]$RetryUnverified
)

$ErrorActionPreference = "Stop"
$scriptRoot = Split-Path -Parent $PSScriptRoot
$python = "D:\Programs\python\python.exe"
$env:PYTHONPATH = "$scriptRoot\src;D:\QMT\vendor\xtquant_big_convert-main\src"
$logDirectory = Join-Path $Root "logs"
New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null
$logPath = Join-Path $logDirectory "akshare_repair.log"
$completedBatches = 0

while ($true) {
    $repairArgs = @("-u", "-m", "qmt_data_lake.cli", "--root", $Root, "repair-akshare", "--limit", $BatchSize, "--timeout", "15")
    if ($RetryUnverified) { $repairArgs += "--retry-unverified" }
    # AkShare/Tencent emits progress text on stderr even for a successful
    # request.  Capture it as diagnostic output rather than allowing
    # ErrorActionPreference=Stop to terminate the runner before logging.
    $priorErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = & $python @repairArgs 2>&1
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $priorErrorAction
    $output | ForEach-Object { "$(Get-Date -Format o) $_" } | Add-Content -LiteralPath $logPath -Encoding utf8
    if ($exitCode -ne 0) { throw "repair command exited with $exitCode; see $logPath" }
    $jsonLine = $output | Where-Object { $_ -match '^\{' } | Select-Object -Last 1
    if (-not $jsonLine) { throw "repair command returned no JSON result; see $logPath" }
    $result = $jsonLine | ConvertFrom-Json
    if (-not $result.ok) { throw "repair command failed: $jsonLine" }
    if ([int]$result.attempted -eq 0) { break }
    $completedBatches++
    if ($MaxBatches -gt 0 -and $completedBatches -ge $MaxBatches) { break }
    Start-Sleep -Seconds $PauseSeconds
}
