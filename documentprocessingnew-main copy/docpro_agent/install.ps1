# install.ps1
# DocPro Protocol Installer (Enterprise Hardened Version)

param(
    [string]$Mode = "install"  # install | uninstall
)

$ErrorActionPreference = "Stop"

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Ok($msg)   { Write-Host "[OK]   $msg" -ForegroundColor Green }
function Write-Fail($msg) { Write-Host "[ERR]  $msg" -ForegroundColor Red }

try {
    # ===============================
    # 1. Resolve Working Directory
    # ===============================
    $CurrentDir = (Get-Location).Path
    Write-Info "Working Directory: $CurrentDir"

    # ===============================
    # 2. Resolve Handler (EXE > Python)
    # ===============================
    $ExePath = Join-Path $CurrentDir "dist\DocProAgent.exe"
    $ScriptPath = Join-Path $CurrentDir "docpro_agent.py"

    $Handler = $null

    if (Test-Path $ExePath) {
        $ExeFull = (Resolve-Path $ExePath).Path
        $Handler = "`"$ExeFull`" `"%1`""
        Write-Info "Mode: EXE"
        Write-Info "Handler: $ExeFull"
    }
    elseif (Test-Path $ScriptPath) {

        # Try multiple Python detection strategies
        $Python = $null

        try {
            $Python = (Get-Command python -ErrorAction Stop).Source
        } catch {}

        if (!$Python) {
            try {
                $Python = (Get-Command py -ErrorAction Stop).Source
                $Handler = "`"$Python`" -3 `"$ScriptPath`" `"%1`""
            } catch {}
        } else {
            $ScriptFull = (Resolve-Path $ScriptPath).Path
            $Handler = "`"$Python`" `"$ScriptFull`" `"%1`""
        }

        if (!$Handler) {
            throw "Python not found. Install Python or build EXE."
        }

        Write-Warn "Mode: DEV (Python)"
        Write-Info "Python: $Python"
    }
    else {
        throw "Missing both:
 - dist\DocProAgent.exe
 - docpro_agent.py"
    }

    # ===============================
    # 3. Registry Key Setup
    # ===============================
    $protoKey = "HKCU:\Software\Classes\docpro"
    $commandKey = "$protoKey\shell\open\command"

    if ($Mode -eq "uninstall") {
        if (Test-Path $protoKey) {
            Remove-Item -Path $protoKey -Recurse -Force
            Write-Ok "Protocol removed successfully."
        } else {
            Write-Warn "Protocol not found."
        }
        exit 0
    }

    Write-Info "Registering protocol: docpro://"

    # Create base key
    if (!(Test-Path $protoKey)) {
        New-Item -Path $protoKey -Force | Out-Null
    }

    # Set description
    Set-ItemProperty -Path $protoKey -Name "(default)" -Value "URL:DocPro Protocol"

    # Required flag
    if (!(Get-ItemProperty -Path $protoKey -Name "URL Protocol" -ErrorAction SilentlyContinue)) {
        New-ItemProperty -Path $protoKey -Name "URL Protocol" -Value "" -PropertyType String | Out-Null
    }

    # Create command key
    if (!(Test-Path $commandKey)) {
        New-Item -Path $commandKey -Force | Out-Null
    }

    Set-ItemProperty -Path $commandKey -Name "(default)" -Value $Handler

    # ===============================
    # 4. Validation
    # ===============================
    $Registered = (Get-ItemProperty -Path $commandKey)."(default)"

    if ($Registered -ne $Handler) {
        throw "Registry validation failed"
    }

    Write-Ok "Protocol registration SUCCESSFUL"
    Write-Host ""
    Write-Host "docpro:// is now active"
    Write-Host ""

    # ===============================
    # 5. Post Instructions
    # ===============================
    Write-Host "NEXT STEPS:" -ForegroundColor Cyan
    Write-Host "1. Restart Chrome / Edge"
    Write-Host "2. Ensure config.json exists"
    Write-Host "3. Test in browser:"
    Write-Host "   docpro://test"
    Write-Host ""

}
catch {
    Write-Fail $_
    exit 1
}