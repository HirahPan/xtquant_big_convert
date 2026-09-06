[CmdletBinding()]
param(
    [string]$Distro = "Ubuntu"
)

$ErrorActionPreference = "Stop"
$marker = "systemctl start redis-server; exec sleep infinity"
$existing = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
    $_.Name -eq "wsl.exe" -and $_.CommandLine -like "*$marker*"
}
if (-not $existing) {
    Start-Process -FilePath "wsl.exe" -WindowStyle Hidden -ArgumentList @(
        "-d", $Distro, "--", "bash", "-lc", $marker
    ) | Out-Null
}
