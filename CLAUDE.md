# Project Instructions

## Installer Rules

The installer files in `installer/` must always follow this exact structure:

- **install.ps1**: PowerShell installer that:
  1. Checks for admin privileges
  2. Installs FFmpeg (download or winget fallback)
  3. Installs Python via winget
  4. Installs pip dependencies (dropbox, pyyaml)
  5. Copies **both** `transcoder_gui.py` (main GUI app) and `transcode.py` (CLI module) to the install directory
  6. Copies `config.example.yaml` and creates `config.yaml` if it doesn't exist
  7. Creates a `.bat` launcher and a `Launch.ps1` launcher — both must run `python transcoder_gui.py` (NOT `transcode.py`)
  8. Creates Desktop and Start Menu shortcuts pointing to `Launch.ps1`

- **HeavyDrops_Setup.iss**: Inno Setup script for building a Windows installer wizard

- **README.txt**: Installation instructions with Quick Setup and Manual Setup sections

- **Install_HeavyDrops.bat**: Batch file that kicks off install.ps1

All installer files must have the version number kept in sync with the current app version. The launcher scripts and shortcuts must always launch `transcoder_gui.py` (the GUI), never `transcode.py` directly.

When building the source zip, always include: `transcoder_gui.py`, `transcode.py`, `config.example.yaml`, `pyproject.toml`, `requirements.txt`, and the entire `installer/` directory.
