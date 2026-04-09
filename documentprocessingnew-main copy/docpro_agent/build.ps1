# Build Script for DocPro Agent
# Requirements: pip install pyinstaller requests watchdog pystray Pillow winshell pywin32

$AppName = "docpro_agent"
$MainScript = "docpro_agent/app.py"

Write-Host "--- Packaging $AppName ---" -ForegroundColor Cyan

# Use --add-data to include assets if needed, but we generate placeholder icon in app.py if missing.
# However, for a production build, inclusion is better.
# Usage: --add-data "SOURCE;DEST" (Windows uses ;)

# The command requested by user:
# pyinstaller --onefile --noconsole --clean --name docpro_agent --add-data "docpro_agent/config.example.json;docpro_agent" docpro_agent/app.py

pyinstaller --onefile --noconsole --clean --name $AppName `
    --add-data "docpro_agent/config.example.json;docpro_agent" `
    --hidden-import win32event `
    --hidden-import win32api `
    --hidden-import winerror `
    --collect-all pystray `
    $MainScript

if ($LASTEXITCODE -eq 0) {
    $FinalExe = "dist\$AppName.exe"
    Unblock-File -Path $FinalExe -ErrorAction SilentlyContinue
    Write-Host "Build complete: $FinalExe" -ForegroundColor Green
} else {
    Write-Host "Build failed." -ForegroundColor Red
}
