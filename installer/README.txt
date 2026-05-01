╔════════════════════════════════════════════════════════════════════════════╗
║                    HeavyDrops Transcoder v6.0.19                           ║
║                    Installation Instructions                                ║
╚════════════════════════════════════════════════════════════════════════════╝

QUICK SETUP (Recommended)
══════════════════════════════════════════════

1. Run "Install_HeavyDrops.bat" as Administrator
   - This will install FFmpeg, Python, and pip dependencies automatically

2. Edit your config file:
   notepad "C:\Program Files\HeavyDrops_Transcoder\config.yaml"
   - Set your Dropbox token (dropbox_token field)

3. Launch from Desktop shortcut: "HeavyDrops Transcoder"


MANUAL SETUP
══════════════════════════════════════════════

1. Install Python 3.11+: winget install Python.Python.3.12
2. Install FFmpeg: winget install ffmpeg
3. Install dependencies: pip install dropbox pyyaml
4. Copy transcoder_gui.py and transcode.py wherever you want
5. Copy config.example.yaml to config.yaml, set your token
6. Run: python transcoder_gui.py


WHAT IT DOES
══════════════════════════════════════════════

- Scans your Dropbox folder for H.264 videos
- Downloads, transcodes to H.265/HEVC, uploads result
- Deletes the original H.264 to free space
- Preserves all metadata
- Auto-detects hardware encoder (Intel QSV > NVIDIA > CPU)
- Runs unattended for months


SYSTEM REQUIREMENTS
══════════════════════════════════════════════

- Windows 10 or later
- Python 3.11+
- FFmpeg with HEVC encoder support
- Dropbox API access token
- For hardware encoding:
  - NVIDIA: GTX 1000 series or newer (NVENC)
  - Intel: 6th gen Core or newer (QSV)
  - Or use CPU encoding (works on any system)


SUPPORT
══════════════════════════════════════════════

GitHub: https://github.com/luisimperator/rep01
