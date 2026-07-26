#!/usr/bin/env python3
"""
Check dependencies for the analyze-video skill.

This skill requires only ffmpeg and ffprobe as external binaries.
No third-party Python packages are needed -- only stdlib is used.

Exit codes:
    0 = all dependencies already satisfied
    2 = missing required tools
"""
import json
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
    # Check Python is available
    if not shutil.which("python3") and not shutil.which("python"):
        print(json.dumps({
            "status": "failed",
            "error_code": "PYTHON_MISSING",
            "message": "Python 3 is required but not found.",
            "missing": ["python3"],
            "retryable": False,
        }))
        sys.exit(2)

    # System deps (ffmpeg + ffprobe -- required)
    missing_sys = check_system_deps()
    if missing_sys:
        print(json.dumps({
            "status": "failed",
            "error_code": "SYSTEM_DEPENDENCY_MISSING",
            "message": "Missing required system tools. Install them in the sandbox image or host runtime before running analyze-video.",
            "missing": [
                {"tool": name, "install_hint": hint}
                for name, hint in missing_sys
            ],
            "retryable": False,
        }))
        sys.exit(2)

    # No Python packages needed -- this skill uses only stdlib + ffmpeg/ffprobe
    print(json.dumps({
        "status": "success",
        "message": "All dependencies already satisfied.",
        "tools": ["ffmpeg", "ffprobe"],
    }))
    sys.exit(0)


if __name__ == "__main__":
    main()
