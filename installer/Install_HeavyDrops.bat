@echo off
:: HeavyDrops Transcoder Installer Launcher
:: Double-click this file to install

echo ========================================
echo   HeavyDrops Transcoder Installer
echo ========================================
echo.
echo This will install:
echo   - FFmpeg (video encoder)
echo   - HeavyDrops Transcoder application
echo.
echo Press any key to continue or close this window to cancel...
pause > nul

:: Run PowerShell installer as Administrator
powershell -Command "Start-Process powershell -ArgumentList '-ExecutionPolicy Bypass -File \"%~dp0install.ps1\"' -Verb RunAs"
