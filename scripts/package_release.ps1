# Build OzlinkConsole with PyInstaller (onedir) and zip ONLY the distributable folder.
# Matches the usual Codex-style workflow: client extracts zip and runs OzlinkConsole.exe
# with the _internal folder alongside (do not move the .exe away from _internal).

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$spec = Join-Path $RepoRoot "OzlinkConsole.spec"
if (-not (Test-Path $spec)) {
    throw "OzlinkConsole.spec not found at $spec"
}

Write-Host "Running PyInstaller..."
python -m PyInstaller --noconfirm $spec

$distDir = Join-Path $RepoRoot "dist\OzlinkConsole"
if (-not (Test-Path (Join-Path $distDir "OzlinkConsole.exe"))) {
    throw "Build failed: dist\OzlinkConsole\OzlinkConsole.exe not found."
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$zipName = "OzlinkConsole_release_$stamp.zip"
$zipPath = Join-Path $RepoRoot $zipName

if (Test-Path $zipPath) { Remove-Item -Force $zipPath }

# Zip the folder so extract yields OzlinkConsole\OzlinkConsole.exe + OzlinkConsole\_internal\...
Compress-Archive -Path $distDir -DestinationPath $zipPath -CompressionLevel Optimal

Write-Host ""
Write-Host "Created: $zipPath"
Write-Host "Ship this zip to the client. Do not include dist\build\ (intermediate); this archive is dist-only."
