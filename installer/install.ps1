# HeavyDrops Transcoder v4.3 Installer
# Run as Administrator: Right-click -> Run with PowerShell

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  HeavyDrops Transcoder v4.3 Installer" -ForegroundColor Cyan
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
try {
    python -m pip install --quiet --upgrade pip 2>&1 | Out-Null
    python -m pip install --quiet dropbox pyyaml 2>&1 | Out-Null
    Write-Host "   Dependencies installed" -ForegroundColor Gray
} catch {
    Write-Host "   Warning: pip install failed. Run manually: pip install dropbox pyyaml" -ForegroundColor Yellow
}

# ---- Copy application files ----
Write-Host "[6/7] Installing HeavyDrops Transcoder..." -ForegroundColor Green
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Copy transcode.py
$SourceScript = Join-Path $ScriptDir "..\transcode.py"
if (Test-Path $SourceScript) {
    Copy-Item -Path $SourceScript -Destination "$InstallDir\transcode.py" -Force
    Write-Host "   transcode.py copied" -ForegroundColor Gray
} else {
    Write-Host "   ERROR: transcode.py not found at $SourceScript" -ForegroundColor Red
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

# Create batch launcher (keeps console window open — this is a CLI app)
$LauncherContent = @"
@echo off
title HeavyDrops Transcoder v4.3
cd /d "%~dp0"
echo ========================================
echo   HeavyDrops Transcoder v4.3
echo ========================================
echo.
python transcode.py
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

# PowerShell launcher (keeps window open — NOT hidden)
$PSLauncherContent = @"
`$Host.UI.RawUI.WindowTitle = "HeavyDrops Transcoder v4.3"
Set-Location "`$PSScriptRoot"
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  HeavyDrops Transcoder v4.3" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
python transcode.py
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
$Shortcut.Description = "HeavyDrops Transcoder v4.3"
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
