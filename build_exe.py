#!/usr/bin/env python3
"""
Build script to create Windows executable and installer.

This script:
1. Creates the .exe using PyInstaller
2. Prepares files for Inno Setup installer
3. Provides instructions for creating the final installer

Requirements:
    pip install pyinstaller

Usage:
    python build_exe.py
"""

import subprocess
import sys
import shutil
from pathlib import Path


def check_pyinstaller():
    """Check if PyInstaller is installed."""
    try:
        import PyInstaller
        return True
    except ImportError:
        return False


def build_executable():
    """Build the executable using PyInstaller."""
    print("=" * 60)
    print("STEP 1: Building executable with PyInstaller")
    print("=" * 60)
    print()

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

    print(f"Command: {' '.join(cmd)}")
    print()

    result = subprocess.run(cmd)

    if result.returncode != 0:
        print("\nBuild failed! Check the error messages above.")
        return False

    print("\n✓ Executable built successfully: dist/HeavyDrops_Transcoder.exe")
    return True


def prepare_installer_files():
    """Prepare files for Inno Setup."""
    print()
    print("=" * 60)
    print("STEP 2: Preparing files for Inno Setup installer")
    print("=" * 60)
    print()

    installer_dir = Path("installer")
    installer_dist = installer_dir / "dist"

    # Create installer/dist directory
    installer_dist.mkdir(parents=True, exist_ok=True)

    # Copy executable to installer/dist
    exe_src = Path("dist/HeavyDrops_Transcoder.exe")
    exe_dst = installer_dist / "HeavyDrops_Transcoder.exe"

    if exe_src.exists():
        shutil.copy2(exe_src, exe_dst)
        print(f"✓ Copied executable to: {exe_dst}")
    else:
        print(f"✗ Executable not found: {exe_src}")
        return False

    return True


def print_inno_setup_instructions():
    """Print instructions for creating the installer with Inno Setup."""
    print()
    print("=" * 60)
    print("STEP 3: Create Installer with Inno Setup")
    print("=" * 60)
    print()
    print("To create the professional installer:")
    print()
    print("1. Download and install Inno Setup from:")
    print("   https://jrsoftware.org/isdl.php")
    print()
    print("2. Open Inno Setup Compiler")
    print()
    print("3. Open the script file:")
    print("   installer/HeavyDrops_Setup.iss")
    print()
    print("4. Click 'Build' > 'Compile' (or press Ctrl+F9)")
    print()
    print("5. The installer will be created at:")
    print("   installer/output/HeavyDrops_Transcoder_v1.1_Setup.exe")
    print()
    print("=" * 60)
    print("ALTERNATIVE: Quick portable package")
    print("=" * 60)
    print()
    print("If you just want to distribute without installer:")
    print("  - The executable is at: dist/HeavyDrops_Transcoder.exe")
    print("  - Users need to install FFmpeg manually")
    print()


def main():
    print()
    print("╔════════════════════════════════════════════════════════════╗")
    print("║       HeavyDrops Transcoder - Build Script                 ║")
    print("╚════════════════════════════════════════════════════════════╝")
    print()

    # Check PyInstaller
    if not check_pyinstaller():
        print("ERROR: PyInstaller is not installed!")
        print("Install it with: pip install pyinstaller")
        return 1

    # Build executable
    if not build_executable():
        return 1

    # Prepare installer files
    if not prepare_installer_files():
        return 1

    # Print instructions
    print_inno_setup_instructions()

    print("BUILD COMPLETE!")
    print()

    return 0


if __name__ == '__main__':
    sys.exit(main())
