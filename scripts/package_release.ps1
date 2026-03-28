# Build OzlinkConsole with PyInstaller (onedir) and zip ONLY the distributable folder.
# Matches the usual Codex-style workflow: client extracts zip and runs OzlinkConsole.exe
# with the _internal folder alongside (do not move the .exe away from _internal).
#
# Build/work dirs default under %TEMP% so OneDrive locks on repo dist\ do not break COLLECT.

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$spec = Join-Path $RepoRoot "OzlinkConsole.spec"
if (-not (Test-Path $spec)) {
    throw "OzlinkConsole.spec not found at $spec"
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$piRoot = Join-Path $env:TEMP "OzlinkConsole_pyinstaller_$stamp"
$distRoot = Join-Path $piRoot "dist"
$workRoot = Join-Path $piRoot "build"
New-Item -ItemType Directory -Force -Path $distRoot, $workRoot | Out-Null

Write-Host "Running PyInstaller (dist=$distRoot, work=$workRoot)..."
python -m PyInstaller --noconfirm --distpath $distRoot --workpath $workRoot $spec
if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller failed with exit code $LASTEXITCODE"
}

$distDir = Join-Path $distRoot "OzlinkConsole"
if (-not (Test-Path (Join-Path $distDir "OzlinkConsole.exe"))) {
    throw "Build failed: OzlinkConsole.exe not found under $distDir"
}

$zipName = "OzlinkConsole_release_$stamp.zip"
$zipPath = Join-Path $RepoRoot $zipName

if (Test-Path $zipPath) { Remove-Item -Force $zipPath }

# Zip the folder so extract yields OzlinkConsole\OzlinkConsole.exe + OzlinkConsole\_internal\...
Compress-Archive -Path $distDir -DestinationPath $zipPath -CompressionLevel Optimal

Write-Host ""
Write-Host "Created: $zipPath"
Write-Host "Ship this zip to the client. Do not include dist\build\ (intermediate); this archive is dist-only."
Write-Host "PyInstaller scratch (safe to delete): $piRoot"
