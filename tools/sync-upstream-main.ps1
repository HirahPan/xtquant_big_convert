[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$worktreeRoot = 'D:\QMT\vendor\_automation-worktrees'
$worktreePath = Join-Path $worktreeRoot 'xtquant-upstream-main'

if (-not (Test-Path -LiteralPath (Join-Path $repoRoot '.git'))) { throw "Not a Git repository: $repoRoot" }
if (Test-Path -LiteralPath $worktreePath) { throw "Temporary upstream-sync worktree already exists: $worktreePath" }

git -C $repoRoot fetch --quiet origin
if ($LASTEXITCODE -ne 0) { throw 'Fetching fork origin failed.' }
git -C $repoRoot fetch --quiet upstream
if ($LASTEXITCODE -ne 0) { throw 'Fetching upstream failed.' }

$upstreamAhead = [int](git -C $repoRoot rev-list --count origin/main..upstream/main)
if ($upstreamAhead -eq 0) {
    Write-Output 'Fork main already contains all upstream main commits.'
    exit 0
}

git -C $repoRoot merge-base origin/main upstream/main | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'Fork main and upstream main have unrelated histories; refusing an unsafe automatic merge.'
}

New-Item -ItemType Directory -Path $worktreeRoot -Force | Out-Null
git -C $repoRoot worktree add --detach $worktreePath origin/main
if ($LASTEXITCODE -ne 0) { throw 'Creating isolated upstream-sync worktree failed.' }

try {
    git -C $worktreePath merge --no-ff --no-edit upstream/main
    if ($LASTEXITCODE -ne 0) {
        git -C $worktreePath rev-parse -q --verify MERGE_HEAD | Out-Null
        if ($LASTEXITCODE -eq 0) { git -C $worktreePath merge --abort }
        throw 'Upstream merge has conflicts; main was left unchanged.'
    }

    $env:PYTHONUTF8 = '1'
    python (Join-Path $worktreePath 'qmt-trader\scripts\qmt.py') --help | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'QMT CLI validation failed after the upstream merge.' }

    git -C $worktreePath push origin HEAD:main
    if ($LASTEXITCODE -ne 0) { throw 'Pushing the validated upstream merge to fork main failed.' }
    Write-Output "Merged and pushed $upstreamAhead upstream commit(s) into fork main."
}
finally {
    git -C $repoRoot worktree remove --force $worktreePath
}
