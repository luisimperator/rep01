#!/usr/bin/env python3
"""
Sync version from pyproject.toml to all project files.

Single source of truth: pyproject.toml -> version = "X.Y.Z"
Updates: transcoder_gui.py, install.ps1, HeavyDrops_Setup.iss

Usage:
    python sync_version.py          # Check if all files are in sync
    python sync_version.py --fix    # Update all files to match pyproject.toml
    python sync_version.py --set 5.5.0  # Set new version everywhere
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent


def get_version_from_pyproject() -> str:
    content = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
    if not match:
        raise ValueError("Version not found in pyproject.toml")
    return match.group(1)


def set_version_in_pyproject(version: str) -> bool:
    path = ROOT / "pyproject.toml"
    content = path.read_text(encoding="utf-8")
    new_content = re.sub(
        r'^(version\s*=\s*")[^"]+"',
        rf'\g<1>{version}"',
        content,
        flags=re.MULTILINE,
    )
    if content != new_content:
        path.write_text(new_content, encoding="utf-8")
        return True
    return False


# Files to sync: (path, regex_pattern, replacement_template)
SYNC_TARGETS = [
    (
        "transcoder_gui.py",
        r'^(VERSION\s*=\s*")[^"]+"',
        r'\g<1>{version}"',
    ),
    (
        "installer/install.ps1",
        r"v\d+\.\d+\.\d+",
        "v{version}",
    ),
    (
        "installer/HeavyDrops_Setup.iss",
        r'(#define MyAppVersion\s*")[^"]+"',
        r'\g<1>{version}"',
    ),
]


def check_sync() -> list:
    """Return list of (file, current_version) for files out of sync."""
    canonical = get_version_from_pyproject()
    out_of_sync = []

    for rel_path, pattern, _ in SYNC_TARGETS:
        path = ROOT / rel_path
        if not path.exists():
            continue
        content = path.read_text(encoding="utf-8")
        matches = re.findall(r"\d+\.\d+\.\d+", content)
        versions_found = set(matches)
        if canonical not in versions_found or len(versions_found) > 1:
            out_of_sync.append((rel_path, versions_found, canonical))

    return out_of_sync


def sync_all(version: str) -> list:
    """Update all files to match the given version. Returns list of updated files."""
    updated = []
    for rel_path, pattern, template in SYNC_TARGETS:
        path = ROOT / rel_path
        if not path.exists():
            continue
        content = path.read_text(encoding="utf-8")
        replacement = template.format(version=version)
        new_content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
        if content != new_content:
            path.write_text(new_content, encoding="utf-8")
            updated.append(rel_path)
    return updated


def main():
    if "--set" in sys.argv:
        idx = sys.argv.index("--set")
        if idx + 1 >= len(sys.argv):
            print("Usage: sync_version.py --set X.Y.Z")
            sys.exit(1)
        new_version = sys.argv[idx + 1]
        if not re.match(r"^\d+\.\d+\.\d+$", new_version):
            print(f"Invalid version format: {new_version} (expected X.Y.Z)")
            sys.exit(1)
        set_version_in_pyproject(new_version)
        updated = sync_all(new_version)
        print(f"Version set to {new_version}")
        if updated:
            print(f"Updated: {', '.join(updated)}")
        else:
            print("All files already at this version")
        sys.exit(0)

    canonical = get_version_from_pyproject()

    if "--fix" in sys.argv:
        updated = sync_all(canonical)
        if updated:
            print(f"Synced to v{canonical}: {', '.join(updated)}")
        else:
            print(f"All files already at v{canonical}")
        sys.exit(0)

    # Default: check mode
    out_of_sync = check_sync()
    if not out_of_sync:
        print(f"All files in sync at v{canonical}")
        sys.exit(0)
    else:
        print(f"Version mismatch! pyproject.toml = v{canonical}")
        for rel_path, found, expected in out_of_sync:
            print(f"  {rel_path}: found {found}")
        print(f"\nRun: python sync_version.py --fix")
        sys.exit(1)


if __name__ == "__main__":
    main()
