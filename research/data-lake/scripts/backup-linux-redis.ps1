[CmdletBinding()]
param(
    [string]$Distro = "Ubuntu",
    [int]$Port = 6380,
    [string]$BackupRoot = "D:\QMT-backups\redis\linux-primary"
)

# Snapshot the Linux primary without exposing configuration secrets in logs.
$ErrorActionPreference = "Stop"
$shareRoot = "\\wsl.localhost\$Distro"
$privateConfig = "D:\Programs\iQuant\python\bigqmt_signal_trader_local_config.py"

if (-not (Test-Path -LiteralPath $shareRoot)) { throw "WSL distribution is unavailable: $Distro" }
if (-not (Test-Path -LiteralPath $privateConfig)) { throw "QMT private config is unavailable" }

$lastSave = (& wsl.exe -d $Distro -- bash -lc "redis-cli --raw -p $Port LASTSAVE").Trim()
if (-not $lastSave) { throw "Cannot read LASTSAVE from Linux Redis" }
& wsl.exe -d $Distro -- bash -lc "redis-cli -p $Port BGSAVE" | Out-Null
for ($attempt = 0; $attempt -lt 60; $attempt++) {
    Start-Sleep -Milliseconds 500
    $nextSave = (& wsl.exe -d $Distro -- bash -lc "redis-cli --raw -p $Port LASTSAVE").Trim()
    if ($nextSave -and $nextSave -ne $lastSave) { break }
}
if (-not $nextSave -or $nextSave -eq $lastSave) { throw "Linux Redis BGSAVE did not finish" }

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$destination = Join-Path $BackupRoot $stamp
New-Item -ItemType Directory -Path $destination -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $shareRoot "var\lib\redis\dump.rdb") -Destination (Join-Path $destination "dump.rdb") -Force
if (Test-Path -LiteralPath (Join-Path $shareRoot "var\lib\redis\appendonlydir")) {
    Copy-Item -LiteralPath (Join-Path $shareRoot "var\lib\redis\appendonlydir") -Destination (Join-Path $destination "appendonlydir") -Recurse -Force
}
Copy-Item -LiteralPath (Join-Path $shareRoot "etc\redis\redis.conf") -Destination (Join-Path $destination "redis.conf") -Force
Copy-Item -LiteralPath $privateConfig -Destination (Join-Path $destination "bigqmt_signal_trader_local_config.py") -Force

$files = Get-ChildItem -LiteralPath $destination -Recurse -File
[pscustomobject]@{
    Backup = $destination
    Files = $files.Count
    Bytes = ($files | Measure-Object -Property Length -Sum).Sum
    RdbSha256 = (Get-FileHash -LiteralPath (Join-Path $destination "dump.rdb") -Algorithm SHA256).Hash
} | ConvertTo-Json -Compress
