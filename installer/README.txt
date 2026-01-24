╔════════════════════════════════════════════════════════════════════════════╗
║                    HeavyDrops Transcoder v1.1.10                              ║
║                    Installation Instructions                                ║
╚════════════════════════════════════════════════════════════════════════════╝

OPTION 1: Professional Installer (Recommended)
══════════════════════════════════════════════

If you have the Setup.exe file:
1. Run "HeavyDrops_Transcoder_v1.1.10_Setup.exe"
2. Follow the installation wizard
3. FFmpeg will be downloaded automatically during installation
4. Done! Launch from Start Menu or Desktop shortcut


OPTION 2: Build the Installer Yourself
══════════════════════════════════════════════

Requirements:
- Python 3.8+
- PyInstaller (pip install pyinstaller)
- Inno Setup (https://jrsoftware.org/isdl.php)

Steps:
1. Open Command Prompt in the project folder

2. Install PyInstaller:
   pip install pyinstaller

3. Run the build script:
   python build_exe.py

4. Install Inno Setup from: https://jrsoftware.org/isdl.php

5. Open Inno Setup Compiler

6. Open: installer/HeavyDrops_Setup.iss

7. Press Ctrl+F9 to compile

8. Find your installer at:
   installer/output/HeavyDrops_Transcoder_v1.1.10_Setup.exe


OPTION 3: Quick Portable Setup (No Installer)
══════════════════════════════════════════════

1. Run "Install_HeavyDrops.bat" as Administrator
   - This will download FFmpeg automatically
   - Or install FFmpeg manually: winget install ffmpeg

2. Run "transcoder_gui.py" with Python:
   python transcoder_gui.py


SYSTEM REQUIREMENTS
══════════════════════════════════════════════

- Windows 10 or later
- FFmpeg (installed automatically by installer)
- For hardware encoding:
  - NVIDIA: GTX 1000 series or newer (NVENC)
  - Intel: 6th gen Core or newer (QSV)
  - Or use CPU encoding (works on any system)


FEATURES
══════════════════════════════════════════════

- Convert H.264 videos to H.265/HEVC (50%+ smaller files)
- Hardware acceleration (NVIDIA NVENC, Intel QSV)
- Automatic fallback to CPU if hardware encoding fails
- Dropbox Smart Sync integration
  - Detects and triggers download of online-only files
  - Marks processed backups as online-only to free space
- Intelligent queue management
  - Processes smaller files first
  - Monitors disk space
  - Shows progress with ETA
- Technical report generation
- Multi-machine support (logs include machine name)


SUPPORT
══════════════════════════════════════════════

GitHub: https://github.com/luisimperator/rep01
