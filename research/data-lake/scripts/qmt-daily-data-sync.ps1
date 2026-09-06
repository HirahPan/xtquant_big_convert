[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$taskRoot = 'D:\QMT-data'
$taskPython = 'D:\Programs\python\python.exe'
$taskQmtDir = 'D:\Programs\iQuant'
$taskQmtCli = 'D:\QMT\vendor\xtquant_big_convert-main\qmt-trader\scripts\qmt.py'
$taskDataLakeDir = 'D:\QMT\local-data-lake\src'
$taskVendorSrc = 'D:\QMT\vendor\xtquant_big_convert-main\src'
$taskLogDir = 'D:\QMT-runtime\logs'
$taskRedisKeepAlive = 'D:\QMT\local-data-lake\scripts\start-wsl-redis.ps1'

$env:BIGQMT_QMT_PYTHON_DIR = 'D:\Programs\iQuant\python'
$env:PYTHONPATH = "$taskDataLakeDir;$taskVendorSrc"
New-Item -ItemType Directory -Path $taskLogDir -Force | Out-Null
$taskLog = Join-Path $taskLogDir ('qmt-daily-data-' + (Get-Date -Format 'yyyyMMdd') + '.log')

try {
    # Keep the WSL-hosted Redis alive before restarting only the iQuant terminal.
    & $taskRedisKeepAlive *>&1 | Tee-Object -FilePath $taskLog -Append

    & $taskPython -m bigqmt_signal_trader.qmt_launcher --dir $taskQmtDir --mode exe --no-wait restart *>&1 |
        Tee-Object -FilePath $taskLog -Append
    if ($LASTEXITCODE -ne 0) { throw "iQuant restart failed with exit code $LASTEXITCODE" }

    # The QMT strategy is configured in the terminal to auto-run at startup.
    Start-Sleep -Seconds 30
    & $taskPython $taskQmtCli ping *>&1 | Tee-Object -FilePath $taskLog -Append
    if ($LASTEXITCODE -ne 0) { throw "QMT RPC did not become healthy after the 30-second startup window" }

    & $taskPython -m qmt_data_lake.cli --root $taskRoot sync *>&1 | Tee-Object -FilePath $taskLog -Append
    if ($LASTEXITCODE -ne 0) { throw "QMT daily sync failed with exit code $LASTEXITCODE" }
    & $taskPython -m qmt_data_lake.cli --root $taskRoot sync-calendar *>&1 | Tee-Object -FilePath $taskLog -Append
    if ($LASTEXITCODE -ne 0) { throw "QMT calendar sync failed with exit code $LASTEXITCODE" }
    & $taskPython -m qmt_data_lake.cli --root $taskRoot validate-akshare *>&1 | Tee-Object -FilePath $taskLog -Append
    if ($LASTEXITCODE -ne 0) { throw "AKShare validation failed with exit code $LASTEXITCODE" }
}
catch {
    $_ | Out-String | Tee-Object -FilePath $taskLog -Append
    exit 1
}
