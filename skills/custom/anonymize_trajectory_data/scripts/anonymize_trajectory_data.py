#!/usr/bin/env python3
from __future__ import annotations

import runpy
import sys
from pathlib import Path


def main() -> int:
    script = (
        Path(__file__).resolve().parents[2]
        / "citybench-rag-search"
        / "skill"
        / "scripts"
        / "desensitize_trajectory.py"
    )
    if not script.exists():
        print(f"Missing upstream privacy script: {script}", file=sys.stderr)
        return 1
    sys.argv[0] = str(script)
    runpy.run_path(str(script), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
