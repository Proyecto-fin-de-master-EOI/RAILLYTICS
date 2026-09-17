# Instala los git hooks versionados en .githooks/ (pre-commit, pre-push) para este repo.
# Uso: .\scripts\install-githooks.ps1
$ErrorActionPreference = "Stop"

$repoRoot = (git rev-parse --show-toplevel).Trim()
Set-Location $repoRoot

git config core.hooksPath .githooks

Write-Host "OK core.hooksPath -> .githooks"
Write-Host "OK hooks instalados: $(Get-ChildItem .githooks -Name)"
