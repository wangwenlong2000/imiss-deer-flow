import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


class BaseDetector:
    violation_type = ""

    def __init__(self, use_llm: bool = False, llm_adapter: Any = None) -> None:
        self.use_llm = use_llm
        self.llm_adapter = llm_adapter

    def detect(self, sample: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    def build_text_view(self, sample: Dict[str, Any]) -> str:
        text = sample.get("content_text")
        if isinstance(text, str) and text.strip():
            return text

        values = []
        for _, value in self.iter_fields(sample):
            if isinstance(value, (str, int, float, bool)):
                values.append(str(value))
        return "\n".join(values)

    def build_field_view(self, sample: Dict[str, Any]) -> Dict[str, Any]:
        features = sample.get("features")
        return features if isinstance(features, dict) else {}

    def iter_fields(self, sample: Dict[str, Any]) -> Iterable[Tuple[str, Any]]:
        features = self.build_field_view(sample)
        for key, value in features.items():
            yield key, value

    def iter_search_items(self, sample: Dict[str, Any]) -> Iterable[Tuple[str, str]]:
        text = self.build_text_view(sample)
        if text:
            yield "content_text", text
        for key, value in self.iter_fields(sample):
            if isinstance(value, (str, int, float, bool)):
                yield key, str(value)

    def make_location(
        self,
        field_path: str,
        risk_type: str,
        text: str,
        matched_rule_id: str,
        start: int = None,
        end: int = None,
    ) -> Dict[str, Any]:
        location = {
            "field_path": field_path,
            "risk_type": risk_type,
            "text": text,
            "matched_rule_id": matched_rule_id,
        }
        if start is not None:
            location["start"] = start
        if end is not None:
            location["end"] = end
        return location

    def make_result(
        self,
        sample: Dict[str, Any],
        is_hit: bool,
        confidence: float,
        risk_locations: List[Dict[str, Any]],
        matched_rules: List[str],
        reason: str,
        suggested_action: List[str],
    ) -> Dict[str, Any]:
        return {
            "data_id": sample.get("data_id"),
            "sample_id": sample.get("sample_id"),
            "data_type": sample.get("data_type"),
            "gate": sample.get("gate") or sample.get("trigger_gate"),
            "violation_type": self.violation_type,
            "is_hit": bool(is_hit),
            "confidence": round(float(confidence), 4),
            "risk_locations": risk_locations,
            "matched_rules": sorted(set(matched_rules)),
            "reason": reason,
            "suggested_action": suggested_action,
        }

    def no_hit(self, sample: Dict[str, Any], reason: str = "No rule hit.") -> Dict[str, Any]:
        return self.make_result(sample, False, 0.0, [], [], reason, ["allow"])


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def find_regex(pattern: str, text: str, flags: int = re.IGNORECASE) -> List[re.Match]:
    return list(re.finditer(pattern, text or "", flags))

