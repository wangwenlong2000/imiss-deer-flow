from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.run_skill import run_skill_cli


def main(skill_name: str) -> int:
    return run_skill_cli(default_skill=skill_name)

