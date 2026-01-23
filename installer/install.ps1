# HeavyDrops Transcoder Installer
# Run as Administrator: Right-click -> Run with PowerShell

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  HeavyDrops Transcoder Installer" -ForegroundColor Cyan
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
$InstallDir = "C:\Program Files\HeavyDrops Transcoder"
$FFmpegDir = "C:\Program Files\FFmpeg"
$FFmpegUrl = "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip"
$TempDir = "$env:TEMP\heavydrops_install"

# Create temp directory
Write-Host "[1/5] Preparing installation..." -ForegroundColor Green
New-Item -ItemType Directory -Force -Path $TempDir | Out-Null

# Download FFmpeg
Write-Host "[2/5] Downloading FFmpeg (this may take a few minutes)..." -ForegroundColor Green
$ffmpegZip = "$TempDir\ffmpeg.zip"
try {
    # Use TLS 1.2
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

    $ProgressPreference = 'SilentlyContinue'  # Faster download
    Invoke-WebRequest -Uri $FFmpegUrl -OutFile $ffmpegZip -UseBasicParsing
    Write-Host "   Download complete!" -ForegroundColor Gray
} catch {
    Write-Host "   Failed to download FFmpeg. Trying alternative method..." -ForegroundColor Yellow

    # Try using winget as fallback
    try {
        winget install --id Gyan.FFmpeg -e --accept-source-agreements --accept-package-agreements
        Write-Host "   FFmpeg installed via winget!" -ForegroundColor Gray
        $ffmpegInstalled = $true
    } catch {
        Write-Host "ERROR: Could not download FFmpeg. Please install manually:" -ForegroundColor Red
        Write-Host "   winget install ffmpeg" -ForegroundColor Yellow
        Read-Host "Press Enter to continue anyway"
    }
}

# Extract and install FFmpeg
if (-not $ffmpegInstalled -and (Test-Path $ffmpegZip)) {
    Write-Host "[3/5] Installing FFmpeg..." -ForegroundColor Green

    # Create FFmpeg directory
    New-Item -ItemType Directory -Force -Path $FFmpegDir | Out-Null

    # Extract
    Expand-Archive -Path $ffmpegZip -DestinationPath $TempDir -Force

    # Find the bin folder and copy executables
    $binFolder = Get-ChildItem -Path $TempDir -Recurse -Directory -Filter "bin" | Select-Object -First 1
    if ($binFolder) {
        Copy-Item -Path "$($binFolder.FullName)\*" -Destination $FFmpegDir -Force
        Write-Host "   FFmpeg installed to: $FFmpegDir" -ForegroundColor Gray
    }

    # Add to PATH if not already there
    $currentPath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    if ($currentPath -notlike "*$FFmpegDir*") {
        [Environment]::SetEnvironmentVariable("Path", "$currentPath;$FFmpegDir", "Machine")
        Write-Host "   Added FFmpeg to system PATH" -ForegroundColor Gray
    }
}

# Install the transcoder application
Write-Host "[4/5] Installing HeavyDrops Transcoder..." -ForegroundColor Green

# Create install directory
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null

# Copy the Python script (assuming it's in the same folder as this installer)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$SourceScript = Join-Path $ScriptDir "..\transcoder_gui.py"

if (Test-Path $SourceScript) {
    Copy-Item -Path $SourceScript -Destination "$InstallDir\transcoder_gui.py" -Force
    Write-Host "   Application files copied" -ForegroundColor Gray
} else {
    Write-Host "   Warning: transcoder_gui.py not found in parent folder" -ForegroundColor Yellow
}

# Create a batch launcher
$LauncherContent = @"
@echo off
cd /d "%~dp0"
python transcoder_gui.py
if errorlevel 1 (
    echo.
    echo Python not found! Please install Python from python.org
    echo Or run: winget install Python.Python.3.12
    pause
)
"@
Set-Content -Path "$InstallDir\HeavyDrops Transcoder.bat" -Value $LauncherContent

# Create a PowerShell launcher (more reliable)
$PSLauncherContent = @"
Set-Location "`$PSScriptRoot"
python transcoder_gui.py
"@
Set-Content -Path "$InstallDir\Launch.ps1" -Value $PSLauncherContent

# Create Desktop shortcut
Write-Host "[5/5] Creating shortcuts..." -ForegroundColor Green
$WshShell = New-Object -ComObject WScript.Shell
$Shortcut = $WshShell.CreateShortcut("$env:PUBLIC\Desktop\HeavyDrops Transcoder.lnk")
$Shortcut.TargetPath = "powershell.exe"
$Shortcut.Arguments = "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$InstallDir\Launch.ps1`""
$Shortcut.WorkingDirectory = $InstallDir
$Shortcut.Description = "HeavyDrops Dropbox Video Transcoder H.264 to H.265"
$Shortcut.Save()
Write-Host "   Desktop shortcut created" -ForegroundColor Gray

# Create Start Menu shortcut
$StartMenuFolder = "$env:ProgramData\Microsoft\Windows\Start Menu\Programs\HeavyDrops"
New-Item -ItemType Directory -Force -Path $StartMenuFolder | Out-Null
$StartShortcut = $WshShell.CreateShortcut("$StartMenuFolder\HeavyDrops Transcoder.lnk")
$StartShortcut.TargetPath = "powershell.exe"
$StartShortcut.Arguments = "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$InstallDir\Launch.ps1`""
$StartShortcut.WorkingDirectory = $InstallDir
$StartShortcut.Save()
Write-Host "   Start Menu shortcut created" -ForegroundColor Gray

# Cleanup
Remove-Item -Path $TempDir -Recurse -Force -ErrorAction SilentlyContinue

# Verify installation
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  Installation Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Installed to: $InstallDir" -ForegroundColor Cyan
Write-Host ""

# Check Python
Write-Host "Checking dependencies..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "  [OK] Python: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "  [!] Python not found!" -ForegroundColor Red
    Write-Host "      Install with: winget install Python.Python.3.12" -ForegroundColor Yellow
}

# Check FFmpeg
try {
    $ffmpegVersion = ffmpeg -version 2>&1 | Select-Object -First 1
    Write-Host "  [OK] FFmpeg: $ffmpegVersion" -ForegroundColor Green
} catch {
    Write-Host "  [!] FFmpeg not in PATH yet (restart may be required)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "You can now launch the application from:" -ForegroundColor Cyan
Write-Host "  - Desktop shortcut: 'HeavyDrops Transcoder'" -ForegroundColor White
Write-Host "  - Start Menu: 'HeavyDrops Transcoder'" -ForegroundColor White
Write-Host ""
Write-Host "NOTE: You may need to restart your computer for PATH changes to take effect." -ForegroundColor Yellow
Write-Host ""
Read-Host "Press Enter to exit"
