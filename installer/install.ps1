# HeavyDrops Transcoder v5.8.1 Installer
# Run as Administrator: Right-click -> Run with PowerShell

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  HeavyDrops Transcoder v5.8.1 Installer" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if running as admin
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "ERROR: Please run this script as Administrator!" -ForegroundColor Red
    Write-Host "Right-click on PowerShell and select 'Run as Administrator'" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

# Configuration
$InstallDir = "C:\Program Files\HeavyDrops_Transcoder"
$FFmpegDir = "C:\Program Files\FFmpeg"
$FFmpegUrl = "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip"
$TempDir = "$env:TEMP\heavydrops_install"

# Create temp directory
Write-Host "[1/7] Preparing installation..." -ForegroundColor Green
New-Item -ItemType Directory -Force -Path $TempDir | Out-Null

# ---- FFmpeg ----
$ffmpegInstalled = $false

if (Test-Path "$FFmpegDir\ffmpeg.exe") {
    Write-Host "[2/7] FFmpeg already installed at: $FFmpegDir" -ForegroundColor Green
    $ffmpegInstalled = $true
} elseif (Get-Command ffmpeg -ErrorAction SilentlyContinue) {
    Write-Host "[2/7] FFmpeg already available in PATH" -ForegroundColor Green
    $ffmpegInstalled = $true
}

if (-not $ffmpegInstalled) {
    Write-Host "[2/7] Downloading FFmpeg (this may take a few minutes)..." -ForegroundColor Green
    $ffmpegZip = "$TempDir\ffmpeg.zip"
    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        $ProgressPreference = 'SilentlyContinue'
        Invoke-WebRequest -Uri $FFmpegUrl -OutFile $ffmpegZip -UseBasicParsing
        Write-Host "   Download complete!" -ForegroundColor Gray
    } catch {
        Write-Host "   Failed to download FFmpeg. Trying winget..." -ForegroundColor Yellow
        try {
            winget install --id Gyan.FFmpeg -e --accept-source-agreements --accept-package-agreements
            $ffmpegInstalled = $true
        } catch {
            Write-Host "ERROR: Could not install FFmpeg. Install manually: winget install ffmpeg" -ForegroundColor Red
            Read-Host "Press Enter to continue anyway"
        }
    }
}

if (-not $ffmpegInstalled -and (Test-Path "$TempDir\ffmpeg.zip")) {
    Write-Host "[3/7] Installing FFmpeg..." -ForegroundColor Green
    New-Item -ItemType Directory -Force -Path $FFmpegDir | Out-Null
    Expand-Archive -Path "$TempDir\ffmpeg.zip" -DestinationPath $TempDir -Force
    $binFolder = Get-ChildItem -Path $TempDir -Recurse -Directory -Filter "bin" | Select-Object -First 1
    if ($binFolder) {
        Copy-Item -Path "$($binFolder.FullName)\*" -Destination $FFmpegDir -Force
        Write-Host "   FFmpeg installed to: $FFmpegDir" -ForegroundColor Gray
    }
    $currentPath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    if ($currentPath -notlike "*$FFmpegDir*") {
        [Environment]::SetEnvironmentVariable("Path", "$currentPath;$FFmpegDir", "Machine")
        Write-Host "   Added FFmpeg to system PATH" -ForegroundColor Gray
    }
} else {
    Write-Host "[3/7] FFmpeg OK, skipping..." -ForegroundColor Green
}

# ---- Python ----
$pythonInstalled = $false
if (Get-Command python -ErrorAction SilentlyContinue) {
    $pyVer = python --version 2>&1
    Write-Host "[4/7] Python already installed: $pyVer" -ForegroundColor Green
    $pythonInstalled = $true
} else {
    Write-Host "[4/7] Installing Python via winget..." -ForegroundColor Green
    try {
        winget install --id Python.Python.3.12 -e --accept-source-agreements --accept-package-agreements --silent
        $pythonInstalled = $true
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
    } catch {
        Write-Host "   Failed to install Python. Install manually: winget install Python.Python.3.12" -ForegroundColor Yellow
    }
}

# ---- Python dependencies ----
Write-Host "[5/7] Installing Python dependencies (dropbox, pyyaml)..." -ForegroundColor Green
# Refresh PATH so we find the real python, not the Windows Store alias
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")

# Find the real python.exe (not the Windows Store alias)
$PythonExe = $null
$PythonPaths = @(
    (Get-Command python -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -ErrorAction SilentlyContinue),
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
    "$env:ProgramFiles\Python312\python.exe",
    "$env:ProgramFiles\Python311\python.exe"
)
foreach ($p in $PythonPaths) {
    if ($p -and (Test-Path $p) -and ($p -notlike "*WindowsApps*")) {
        $PythonExe = $p
        break
    }
}
if (-not $PythonExe) { $PythonExe = "python" }
Write-Host "   Using Python: $PythonExe" -ForegroundColor Gray

try {
    & $PythonExe -m pip install --upgrade pip 2>&1 | Out-Null
    $pipResult = & $PythonExe -m pip install dropbox pyyaml 2>&1
    Write-Host "   Dependencies installed" -ForegroundColor Gray
    # Verify
    $check = & $PythonExe -c "import dropbox; print('dropbox OK')" 2>&1
    Write-Host "   Verified: $check" -ForegroundColor Gray
} catch {
    Write-Host "   Warning: pip install may have failed" -ForegroundColor Yellow
    Write-Host "   Run manually: $PythonExe -m pip install dropbox pyyaml" -ForegroundColor Yellow
}

# ---- Copy application files ----
Write-Host "[6/7] Installing HeavyDrops Transcoder..." -ForegroundColor Green
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Copy transcoder_gui.py (main GUI app)
$SourceGUI = Join-Path $ScriptDir "..\transcoder_gui.py"
if (Test-Path $SourceGUI) {
    Copy-Item -Path $SourceGUI -Destination "$InstallDir\transcoder_gui.py" -Force
    Write-Host "   transcoder_gui.py copied" -ForegroundColor Gray
} else {
    Write-Host "   ERROR: transcoder_gui.py not found at $SourceGUI" -ForegroundColor Red
}

# Copy transcode.py (CLI module)
$SourceScript = Join-Path $ScriptDir "..\transcode.py"
if (Test-Path $SourceScript) {
    Copy-Item -Path $SourceScript -Destination "$InstallDir\transcode.py" -Force
    Write-Host "   transcode.py copied" -ForegroundColor Gray
} else {
    Write-Host "   WARNING: transcode.py not found at $SourceScript" -ForegroundColor Yellow
}

# Copy config example and create config.yaml if needed
$SourceConfig = Join-Path $ScriptDir "..\config.example.yaml"
if (Test-Path $SourceConfig) {
    Copy-Item -Path $SourceConfig -Destination "$InstallDir\config.example.yaml" -Force
    $ConfigDest = "$InstallDir\config.yaml"
    if (-not (Test-Path $ConfigDest)) {
        Copy-Item -Path $SourceConfig -Destination $ConfigDest -Force
        Write-Host "   config.yaml created (edit with your Dropbox token!)" -ForegroundColor Yellow
    } else {
        Write-Host "   config.yaml already exists, keeping your settings" -ForegroundColor Gray
    }
}

# Create batch launcher (auto-installs deps if missing)
$LauncherContent = @"
@echo off
title HeavyDrops Transcoder v5.8.1
cd /d "%~dp0"
echo ========================================
echo   HeavyDrops Transcoder v5.8.1
echo ========================================
echo.
REM Auto-install dependencies if missing
python -c "import dropbox" >nul 2>&1
if errorlevel 1 (
    echo Installing dependencies...
    python -m pip install dropbox pyyaml
    echo.
)
python transcoder_gui.py
if errorlevel 1 (
    echo.
    echo Something went wrong. Check the output above.
    echo.
    echo Common fixes:
    echo   1. Set dropbox_token in config.yaml
    echo   2. Run: pip install dropbox pyyaml
    echo   3. Make sure FFmpeg is installed
)
echo.
pause
"@
Set-Content -Path "$InstallDir\HeavyDrops Transcoder.bat" -Value $LauncherContent

# PowerShell launcher (auto-installs deps if missing, keeps window open)
$PSLauncherContent = @"
`$Host.UI.RawUI.WindowTitle = "HeavyDrops Transcoder v5.8.1"
Set-Location "`$PSScriptRoot"
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  HeavyDrops Transcoder v5.8.1" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check and auto-install dependencies if missing
try {
    python -c "import dropbox" 2>`$null
} catch {
    Write-Host "Installing dependencies..." -ForegroundColor Yellow
    python -m pip install dropbox pyyaml
    Write-Host ""
}
`$depCheck = python -c "import dropbox" 2>&1
if (`$LASTEXITCODE -ne 0) {
    Write-Host "Installing dependencies..." -ForegroundColor Yellow
    python -m pip install dropbox pyyaml
    Write-Host ""
}

python transcoder_gui.py
Write-Host ""
Read-Host "Press Enter to exit"
"@
Set-Content -Path "$InstallDir\Launch.ps1" -Value $PSLauncherContent

# ---- Shortcuts (console VISIBLE, not hidden) ----
Write-Host "[7/7] Creating shortcuts..." -ForegroundColor Green
$WshShell = New-Object -ComObject WScript.Shell

# Desktop shortcut
$Shortcut = $WshShell.CreateShortcut("$env:PUBLIC\Desktop\HeavyDrops Transcoder.lnk")
$Shortcut.TargetPath = "powershell.exe"
$Shortcut.Arguments = "-ExecutionPolicy Bypass -NoExit -File `"$InstallDir\Launch.ps1`""
$Shortcut.WorkingDirectory = $InstallDir
$Shortcut.Description = "HeavyDrops Transcoder v5.8.1"
$Shortcut.Save()
Write-Host "   Desktop shortcut created" -ForegroundColor Gray

# Start Menu shortcut
$StartMenuFolder = "$env:ProgramData\Microsoft\Windows\Start Menu\Programs\HeavyDrops"
New-Item -ItemType Directory -Force -Path $StartMenuFolder | Out-Null
$StartShortcut = $WshShell.CreateShortcut("$StartMenuFolder\HeavyDrops Transcoder.lnk")
$StartShortcut.TargetPath = "powershell.exe"
$StartShortcut.Arguments = "-ExecutionPolicy Bypass -NoExit -File `"$InstallDir\Launch.ps1`""
$StartShortcut.WorkingDirectory = $InstallDir
$StartShortcut.Save()
Write-Host "   Start Menu shortcut created" -ForegroundColor Gray

# ---- Cleanup ----
Remove-Item -Path $TempDir -Recurse -Force -ErrorAction SilentlyContinue

# ---- Done ----
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  Installation Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Installed to: $InstallDir" -ForegroundColor Cyan
Write-Host ""

# Verify dependencies
Write-Host "Checking dependencies..." -ForegroundColor Yellow
try {
    $pv = python --version 2>&1
    Write-Host "  [OK] Python: $pv" -ForegroundColor Green
} catch {
    Write-Host "  [!] Python not found! Install: winget install Python.Python.3.12" -ForegroundColor Red
}
try {
    $fv = ffmpeg -version 2>&1 | Select-Object -First 1
    Write-Host "  [OK] FFmpeg: $fv" -ForegroundColor Green
} catch {
    Write-Host "  [!] FFmpeg not in PATH (restart may be required)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "NEXT STEP: Edit your config with your Dropbox token:" -ForegroundColor Yellow
Write-Host "  notepad `"$InstallDir\config.yaml`"" -ForegroundColor White
Write-Host ""
Write-Host "Then launch from Desktop shortcut: 'HeavyDrops Transcoder'" -ForegroundColor Cyan
Write-Host ""
Read-Host "Press Enter to exit"
