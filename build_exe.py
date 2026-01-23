#!/usr/bin/env python3
"""
Build script to create Windows executable using PyInstaller.

Usage:
    1. Install PyInstaller: pip install pyinstaller
    2. Run this script: python build_exe.py
    3. Find the .exe in the 'dist' folder
"""

import subprocess
import sys

def main():
    # PyInstaller command
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--name=HeavyDrops_Transcoder',
        '--onefile',           # Single .exe file
        '--windowed',          # No console window (GUI app)
        '--noconfirm',         # Overwrite without asking
        '--clean',             # Clean cache before build
        '--icon=NONE',         # No custom icon (use default)
        'transcoder_gui.py'
    ]

    print("Building executable...")
    print(f"Command: {' '.join(cmd)}")
    print()

    result = subprocess.run(cmd)

    if result.returncode == 0:
        print()
        print("=" * 50)
        print("BUILD SUCCESSFUL!")
        print("=" * 50)
        print()
        print("Executable location:")
        print("  dist/HeavyDrops_Transcoder.exe")
        print()
        print("You can copy this .exe to any Windows machine.")
        print("Make sure FFmpeg is installed on the target machine!")
    else:
        print()
        print("Build failed! Check the error messages above.")
        return 1

    return 0

if __name__ == '__main__':
    sys.exit(main())
