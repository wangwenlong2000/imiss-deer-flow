#!/usr/bin/env python3
"""
Check dependencies for the analyze-video skill.

This skill requires only ffmpeg and ffprobe as external binaries.
No third-party Python packages are needed -- only stdlib is used.

Exit codes:
    0 = all dependencies already satisfied
    1 = dependencies were installed (re-run may be needed)
    2 = installation failed / missing required tools
"""
import subprocess
import shutil
import sys


def check_system_deps():
    """Return list of missing system tools with install hints."""
    missing = []
    for tool in ("ffmpeg", "ffprobe"):
        if not shutil.which(tool):
            if sys.platform == "darwin":
                hint = "brew install ffmpeg"
            elif sys.platform == "win32":
                hint = "download from https://ffmpeg.org/download.html and add to PATH"
            else:
                hint = "sudo apt-get install -y ffmpeg"
            missing.append((tool, hint))
    return missing


def main():
    installed_something = False

    # Check Python is available
    if not shutil.which("python3") and not shutil.which("python"):
        print("ERROR: Python 3 is required but not found.", file=sys.stderr)
        sys.exit(2)

    # System deps (ffmpeg + ffprobe -- required)
    missing_sys = check_system_deps()
    if missing_sys:
        for name, hint in missing_sys:
            print(f"Missing system tool: {name}")
            print(f"  Install with: {hint}")
        # Try auto-install on supported platforms
        for name, hint in missing_sys:
            if sys.platform == "darwin" and shutil.which("brew"):
                print(f"Attempting: {hint}")
                result = subprocess.run(hint.split(), capture_output=True, text=True)
            elif sys.platform == "linux" and shutil.which("apt-get"):
                print(f"Attempting: {hint}")
                result = subprocess.run(hint.split(), capture_output=True, text=True)
            else:
                print(f"Cannot auto-install {name}. Please install manually: {hint}")
                sys.exit(2)
            if result.returncode != 0:
                print(f"Auto-install of {name} failed. Install manually: {hint}",
                      file=sys.stderr)
                sys.exit(2)
            else:
                installed_something = True

    # Verify after install attempt
    still_missing = check_system_deps()
    if still_missing:
        for name, hint in still_missing:
            print(f"ERROR: {name} still not found after install attempt.", file=sys.stderr)
            print(f"  Install manually: {hint}", file=sys.stderr)
        sys.exit(2)

    # No Python packages needed -- this skill uses only stdlib + ffmpeg/ffprobe
    if installed_something:
        print("Dependencies installed successfully.")
        sys.exit(1)  # 1 = installed something
    else:
        print("All dependencies already satisfied.")
        sys.exit(0)  # 0 = already good


if __name__ == "__main__":
    main()
