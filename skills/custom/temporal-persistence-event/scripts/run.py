from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.skills.base import SkillContext
from src.skills.event_detection.temporal_persistence_event import TemporalPersistenceEventSkill


def load_structured(path: str | None, inline_json: str | None = None) -> dict[str, Any]:
    if inline_json:
        return json.loads(inline_json)
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    if source.suffix.lower() in {".yaml", ".yml"}:
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    return data or {}


def write_output(result: dict[str, Any], output_path: str | None) -> None:
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    print(text)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the temporal-persistence-event video monitoring skill.")
    parser.add_argument("--input", help="Input JSON/YAML file for this skill.")
    parser.add_argument("--input-json", help="Inline JSON input. Useful for DeerFlow/bash tool calls.")
    parser.add_argument("--config", help="Context config JSON/YAML file.")
    parser.add_argument("--output", help="Optional path to write the result JSON.")
    parser.add_argument("--trace-id", default="temporal-persistence-event-run")
    parser.add_argument("--tenant-id", default="default")
    args = parser.parse_args()

    config = load_structured(args.config)
    input_data = load_structured(args.input, args.input_json)
    context = SkillContext(
        trace_id=args.trace_id,
        tenant_id=args.tenant_id,
        config=config,
        registry=None,
    )

    result = TemporalPersistenceEventSkill().run(input_data, context)
    write_output(result, args.output)
    return 0 if result.get("status") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
