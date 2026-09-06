[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$QmtArgs
)

# Keep the private iQuant account configuration in the iQuant installation.
# This wrapper only points the client at it for this child process; it never
# copies account, password, or Redis values into the workspace or environment.
$ErrorActionPreference = "Stop"
$qmtPython = "D:\Programs\iQuant\python"
$clientPython = "D:\Programs\python\python.exe"
$clientCli = "D:\QMT\vendor\xtquant_big_convert-main\qmt-trader\scripts\qmt.py"

if (-not (Test-Path -LiteralPath (Join-Path $qmtPython "bigqmt_signal_trader_local_config.py"))) {
    throw "iQuant private QMT configuration was not found: $qmtPython"
}

$env:BIGQMT_QMT_PYTHON_DIR = $qmtPython
$env:PYTHONPATH = "D:\QMT\vendor\xtquant_big_convert-main\src"
& $clientPython $clientCli @QmtArgs
exit $LASTEXITCODE
