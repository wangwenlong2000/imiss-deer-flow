#!/usr/bin/env python3
"""Blur or mask sensitive regions across a whole video and write a masked video file.

`privacy-masking` handles a single evidence image. This skill handles the video
itself, so a redacted clip can be released externally. Regions must be supplied
by the caller: this skill does not detect faces, plates, or text, and it never
infers identity or reads plate numbers.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:
    yaml = None

VERSION = "1.0.0"
SKILL = "video-privacy-masking"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "deerflow_config.json"
SUPPORTED_METHODS = {"gaussian_blur", "pixelate", "solid"}


class SkillInputError(Exception):
    """Input file could not be loaded; reported through the standard failure contract."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def argv_output() -> str | None:
    argv = sys.argv
    if "--output" in argv:
        index = argv.index("--output")
        if index + 1 < len(argv):
            return argv[index + 1]
    return None


def load_structured(path: str | None) -> Any:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        raise SkillInputError("INPUT_NOT_FOUND", f"Input file not found: {path}")
    if source.is_dir():
        raise SkillInputError("INPUT_NOT_FOUND", f"Input path is a directory, not a file: {path}")
    try:
        text = source.read_text(encoding="utf-8")
    except OSError as exc:
        raise SkillInputError("INPUT_UNREADABLE", f"Could not read input file {path}: {exc}") from exc
    if source.suffix.lower() in {".yaml", ".yml"}:
        if yaml is None:
            raise SkillInputError("INPUT_INVALID_YAML", "PyYAML is required to read YAML files")
        try:
            return yaml.safe_load(text) or {}
        except Exception as exc:  # noqa: BLE001 - yaml raises library specific errors
            raise SkillInputError("INPUT_INVALID_YAML", f"Invalid YAML in {path}: {exc}") from exc
    if not text.strip():
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SkillInputError("INPUT_INVALID_JSON", f"Invalid JSON in {path}: {exc}") from exc


def load_config(path: str | None) -> dict[str, Any]:
    data = load_structured(path)
    return data if isinstance(data, dict) else {}


def load_list(path: str | None, key: str) -> list[Any]:
    data = load_structured(path)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        value = data.get(key)
        if value is None and isinstance(data.get("data"), dict):
            value = data["data"].get(key)
        return value if isinstance(value, list) else []
    return []


def load_input(path: str | None) -> dict[str, Any]:
    data = load_structured(path)
    return data if isinstance(data, dict) else {}


def input_value(input_data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    nested = input_data.get("data") if isinstance(input_data.get("data"), dict) else {}
    for key in keys:
        if input_data.get(key) is not None:
            return input_data[key]
        if nested.get(key) is not None:
            return nested[key]
    return default


def input_list(input_data: dict[str, Any], key: str) -> list[Any]:
    value = input_value(input_data, key, default=[])
    return value if isinstance(value, list) else []


def success(skill: str, data: dict[str, Any], confidence: float = 1.0) -> dict[str, Any]:
    return {"skill": skill, "version": VERSION, "status": "success", "confidence": round(float(confidence), 4), "data": data}


def failed(skill: str, code: str, message: str, retryable: bool = False, detail: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"skill": skill, "version": VERSION, "status": "failed", "error_code": code, "message": message, "retryable": retryable, "detail": detail or {}}


def emit(result: dict[str, Any], output: str | None) -> int:
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if result.get("status") == "success" else 1


def normalize_regions(regions: list[Any]) -> tuple[list[dict[str, Any]], str]:
    """Validate caller-supplied regions and normalize them to int pixel boxes."""
    if not regions:
        return [], "sensitive_regions with explicit bbox coordinates is required; this skill does not auto-detect faces or plates"
    normalized: list[dict[str, Any]] = []
    for index, region in enumerate(regions):
        bbox = region.get("bbox", region) if isinstance(region, dict) else region
        if not isinstance(bbox, list) or len(bbox) != 4:
            return [], f"sensitive_regions[{index}] must provide bbox as [x1, y1, x2, y2]"
        try:
            x1, y1, x2, y2 = [float(v) for v in bbox]
        except (TypeError, ValueError):
            return [], f"sensitive_regions[{index}].bbox values must be numeric"
        if x2 <= x1 or y2 <= y1:
            return [], f"sensitive_regions[{index}].bbox must satisfy x2 > x1 and y2 > y1"
        entry: dict[str, Any] = {
            "type": region.get("type") if isinstance(region, dict) else None,
            "bbox": [int(x1), int(y1), int(x2), int(y2)],
            "width": int(x2 - x1),
            "height": int(y2 - y1),
        }
        if isinstance(region, dict):
            for key in ("start_seconds", "end_seconds"):
                if region.get(key) is not None:
                    try:
                        entry[key] = float(region[key])
                    except (TypeError, ValueError):
                        return [], f"sensitive_regions[{index}].{key} must be numeric seconds"
            if entry.get("start_seconds") is not None and entry.get("end_seconds") is not None and entry["end_seconds"] <= entry["start_seconds"]:
                return [], f"sensitive_regions[{index}] requires end_seconds > start_seconds"
        normalized.append(entry)
    return normalized, ""


def build_filter_complex(regions: list[dict[str, Any]], method: str, strength: int) -> tuple[str, str]:
    """Crop each region, blur/pixelate/fill it, and overlay it back onto the frame.

    Regions carrying start_seconds/end_seconds are only overlaid inside that
    window, so a plate that is only sensitive for part of the clip stays visible
    elsewhere.
    """
    def enable_expr(region: dict[str, Any]) -> str:
        start, end = region.get("start_seconds"), region.get("end_seconds")
        if start is None and end is None:
            return ""
        lo = start if start is not None else 0
        # 逗号在 filtergraph 里是滤镜分隔符，enable 表达式里的逗号必须转义，
        # 否则 between(t,2,8) 会被拆成三个滤镜。
        expr = rf"gte(t\,{lo})" if end is None else rf"between(t\,{lo}\,{end})"
        return f":enable='{expr}'"

    # solid 只是把区域涂黑，drawbox 可以直接作用在主流上，不需要 split/overlay。
    if method == "solid":
        chain = []
        for region in regions:
            x1, y1, _, _ = region["bbox"]
            chain.append(
                f"drawbox=x={x1}:y={y1}:w={region['width']}:h={region['height']}"
                f":color=black@1.0:t=fill{enable_expr(region)}"
            )
        return f"[0:v]{','.join(chain)}[vout]", "vout"

    # 模糊/马赛克需要把区域裁出来单独处理再贴回去。每个已命名的中间流在
    # filtergraph 里只能被消费一次，所以每一轮都要先 split 出两份：
    # 一份作底图，一份用来裁剪。
    parts: list[str] = []
    current = "0:v"
    for index, region in enumerate(regions):
        x1, y1, _, _ = region["bbox"]
        width, height = region["width"], region["height"]
        base, src = f"base{index}", f"src{index}"
        parts.append(f"[{current}]split=2[{base}][{src}]")
        if method == "gaussian_blur":
            effect = f"gblur=sigma={max(1, strength)}"
        else:  # pixelate
            block = max(2, strength // 2)
            small_w, small_h = max(1, width // block), max(1, height // block)
            effect = f"scale={small_w}:{small_h}:flags=neighbor,scale={width}:{height}:flags=neighbor"
        parts.append(f"[{src}]crop={width}:{height}:{x1}:{y1},{effect}[mask{index}]")
        label = f"v{index}"
        parts.append(f"[{base}][mask{index}]overlay={x1}:{y1}{enable_expr(region)}[{label}]")
        current = label
    return ";".join(parts), current


def probe_duration(ffprobe: str, path: Path) -> float | None:
    proc = subprocess.run(
        [ffprobe, "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", str(path)],
        capture_output=True,
        text=True,
    )
    try:
        return round(float(proc.stdout.strip()), 3)
    except (TypeError, ValueError):
        return None


def sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Mask sensitive regions across a video and write a redacted video file.")
    parser.add_argument("--input", help="Optional JSON payload; CLI flags override matching fields.")
    parser.add_argument("--video-uri", "--uri", dest="uri")
    parser.add_argument("--sensitive-regions-json")
    parser.add_argument("--method", help="gaussian_blur (default), pixelate, or solid")
    parser.add_argument("--strength", type=int, help="Blur sigma / pixelate block scale (default 20)")
    parser.add_argument("--output-video", help="Masked video path. Defaults to <input>_masked<suffix>.")
    parser.add_argument("--disabled", action="store_true")
    parser.add_argument("--config")
    parser.add_argument("--output")
    args = parser.parse_args()

    input_data = load_input(args.input)
    config_path = args.config or (str(DEFAULT_CONFIG_PATH) if DEFAULT_CONFIG_PATH.is_file() else None)
    config = load_config(config_path)
    privacy_config = config.get("privacy_masking", {}) if isinstance(config.get("privacy_masking"), dict) else {}

    uri = args.uri or input_value(input_data, "video_uri", "uri", "raw_segment_uri")
    if not uri:
        return emit(failed(SKILL, "MISSING_URI", "--video-uri or input.video_uri is required"), args.output)

    enabled = not args.disabled and not bool(input_value(input_data, "disabled", default=False)) and privacy_config.get("enabled", True)
    if not enabled:
        return emit(success(SKILL, {"video_uri": uri, "privacy_masked": False, "masked_regions": []}), args.output)

    source = Path(str(uri).removeprefix("file://"))
    if not source.exists():
        return emit(failed(SKILL, "VIDEO_NOT_FOUND", f"Video file not found: {source}"), args.output)

    raw_regions = load_list(args.sensitive_regions_json, "sensitive_regions") if args.sensitive_regions_json else input_list(input_data, "sensitive_regions")
    regions, message = normalize_regions(raw_regions)
    if message:
        return emit(failed(SKILL, "MISSING_SENSITIVE_REGIONS", message), args.output)

    method = (args.method or input_value(input_data, "method") or privacy_config.get("method") or "gaussian_blur").strip()
    if method not in SUPPORTED_METHODS:
        return emit(
            failed(SKILL, "UNSUPPORTED_METHOD", f"method must be one of {sorted(SUPPORTED_METHODS)}, got {method!r}"),
            args.output,
        )
    strength = args.strength or int(input_value(input_data, "strength", default=0) or 0) or 20

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return emit(failed(SKILL, "FFMPEG_MISSING", "ffmpeg is required to write a masked video"), args.output)
    ffprobe = shutil.which("ffprobe")

    target = Path(args.output_video or input_value(input_data, "output_video") or source.with_name(f"{source.stem}_masked{source.suffix}"))
    target.parent.mkdir(parents=True, exist_ok=True)

    filter_complex, final_label = build_filter_complex(regions, method, strength)
    command = [
        ffmpeg, "-y", "-i", str(source),
        "-filter_complex", filter_complex,
        "-map", f"[{final_label}]",
        # 原视频可能没有音轨，用 ? 让映射变成可选，避免无声视频直接失败。
        "-map", "0:a?", "-c:a", "copy",
        str(target),
    ]
    proc = subprocess.run(command, capture_output=True, text=True)
    if proc.returncode != 0 or not target.exists():
        return emit(
            failed(
                SKILL,
                "MASKING_FAILED",
                f"ffmpeg failed to write masked video: {proc.stderr.strip()[-400:]}",
                detail={"command": " ".join(command)},
            ),
            args.output,
        )

    data = {
        "video_uri": str(target),
        "source_video_uri": str(source),
        "privacy_masked": True,
        "masked_regions": regions,
        "method": method,
        "strength": strength,
        "duration_seconds": probe_duration(ffprobe, target) if ffprobe else None,
        "file_size_bytes": target.stat().st_size,
        "sha256": sha256_file(target),
        "audio_preserved": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    return emit(success(SKILL, data), args.output)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SkillInputError as exc:
        raise SystemExit(
            emit(
                {
                    "skill": SKILL,
                    "version": VERSION,
                    "status": "failed",
                    "error_code": exc.code,
                    "message": exc.message,
                    "retryable": False,
                    "detail": {},
                },
                argv_output(),
            )
        )
