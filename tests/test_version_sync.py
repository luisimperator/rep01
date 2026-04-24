"""Tests for version sync — single source of truth from pyproject.toml."""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

ROOT = Path(__file__).parent.parent


def test_pyproject_has_version():
    content = (ROOT / "pyproject.toml").read_text()
    match = re.search(r'^version\s*=\s*"(\d+\.\d+\.\d+)"', content, re.MULTILINE)
    assert match, "pyproject.toml must have a version field"


def test_gui_version_matches_pyproject():
    pyproject = (ROOT / "pyproject.toml").read_text()
    version = re.search(r'^version\s*=\s*"([^"]+)"', pyproject, re.MULTILINE).group(1)

    gui = (ROOT / "transcoder_gui.py").read_text()
    gui_match = re.search(r'^VERSION\s*=\s*"([^"]+)"', gui, re.MULTILINE)
    assert gui_match, "transcoder_gui.py must have VERSION constant"
    assert gui_match.group(1) == version, (
        f"transcoder_gui.py VERSION={gui_match.group(1)} != pyproject.toml {version}"
    )


def test_installer_ps1_version_matches():
    pyproject = (ROOT / "pyproject.toml").read_text()
    version = re.search(r'^version\s*=\s*"([^"]+)"', pyproject, re.MULTILINE).group(1)

    ps1_path = ROOT / "installer" / "install.ps1"
    if not ps1_path.exists():
        return  # Skip if installer not present
    ps1 = ps1_path.read_text()
    assert f"v{version}" in ps1, f"install.ps1 should contain v{version}"


def test_iss_version_matches():
    pyproject = (ROOT / "pyproject.toml").read_text()
    version = re.search(r'^version\s*=\s*"([^"]+)"', pyproject, re.MULTILINE).group(1)

    iss_path = ROOT / "installer" / "HeavyDrops_Setup.iss"
    if not iss_path.exists():
        return  # Skip if not present
    iss = iss_path.read_text()
    match = re.search(r'#define MyAppVersion\s*"([^"]+)"', iss)
    assert match, "HeavyDrops_Setup.iss must have MyAppVersion"
    assert match.group(1) == version, (
        f"HeavyDrops_Setup.iss version={match.group(1)} != pyproject.toml {version}"
    )


def test_sync_version_script_exists():
    assert (ROOT / "sync_version.py").exists(), "sync_version.py must exist"


def test_sync_version_check_mode():
    """sync_version.py should report all files in sync."""
    from sync_version import check_sync
    out_of_sync = check_sync()
    assert out_of_sync == [], f"Files out of sync: {out_of_sync}"
