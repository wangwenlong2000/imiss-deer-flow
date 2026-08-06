"""System-level Skill input/output adapter tool."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Literal

import jsonschema
from langchain.tools import InjectedToolCallId, ToolRuntime, tool
from langgraph.typing import ContextT

from deerflow.agents.thread_state import ThreadState
from deerflow.skills.loader import get_skills_root_path, load_skills


def _schema_path(name: str) -> Path:
    return get_skills_root_path() / name


def _load_schema(name: str) -> dict[str, Any]:
    return json.loads(_schema_path(name).read_text(encoding="utf-8"))


def _validate_schema(payload: dict[str, Any], schema_name: str) -> None:
    jsonschema.validate(payload, _load_schema(schema_name))


def _json_response(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _skill_lookup() -> dict[str, Any]:
    skills = load_skills(enabled_only=True)
    lookup: dict[str, Any] = {}
    for skill in skills:
        lookup[skill.name] = skill
        if skill.skill_path:
            lookup[skill.skill_path] = skill
    return lookup


def _container_base_path() -> str:
    try:
        from deerflow.config import get_app_config

        return get_app_config().skills.container_path
    except Exception:
        return "/mnt/skills"


def _state_allowed_skills(runtime: ToolRuntime[ContextT, ThreadState]) -> set[str] | None:
    state = runtime.state or {}
    routing_context = state.get("routing_context") or {}
    if not isinstance(routing_context, dict):
        routing_context = {}

    values = (
        routing_context.get("allowed_skills")
        or routing_context.get("global_selected_skills")
        or state.get("final_scope_skill_ids")
    )
    if not isinstance(values, list):
        return None

    allowed = {item for item in values if isinstance(item, str) and item}
    return allowed or None


def _normalize_input_envelope(
    *,
    runtime: ToolRuntime[ContextT, ThreadState],
    skill_name: str,
    skill: Any,
    capability: str | None,
    input_envelope: dict[str, Any] | None,
) -> dict[str, Any]:
    envelope = dict(input_envelope or {})
    routing = dict(envelope.get("routing") or {})

    request_id = envelope.get("request_id") or str(uuid.uuid4())
    scenario = envelope.get("scenario") or _infer_scenario(skill)
    resolved_capability = capability or envelope.get("capability") or "skill_invocation"

    query = envelope.get("query")
    if not isinstance(query, dict):
        query = {}
    if not query.get("raw"):
        query["raw"] = _last_user_text(runtime) or f"Invoke {skill_name}"

    inputs = envelope.get("inputs")
    if not isinstance(inputs, dict):
        inputs = {"parameters": {}}
    elif not inputs:
        inputs = {"parameters": {}}

    routing.setdefault("selected_skill", skill_name)
    allowed_from_state = _state_allowed_skills(runtime)
    if allowed_from_state:
        routing.setdefault("allowed_skills", sorted(allowed_from_state))
    routing.setdefault("selection_owner", "lead_agent")

    normalized = {
        "schema_version": envelope.get("schema_version") or "1.0",
        "request_id": request_id,
        "skill_name": skill_name,
        "scenario": scenario,
        "capability": resolved_capability,
        "query": query,
        "inputs": inputs,
    }
    for key in ("context", "budget", "legacy"):
        if key in envelope:
            normalized[key] = envelope[key]
    normalized["routing"] = routing
    return normalized


def _infer_scenario(skill: Any) -> str:
    if skill.category == "public":
        return "public"

    card_path = skill.skill_dir / "router_card.json"
    if card_path.exists():
        try:
            card = json.loads(card_path.read_text(encoding="utf-8"))
            scope = card.get("scope") if isinstance(card, dict) else {}
            if isinstance(scope, dict):
                scenes = scope.get("scenes") or scope.get("scene")
                if isinstance(scenes, list) and scenes:
                    return str(scenes[0])
                if isinstance(scenes, str) and scenes:
                    return scenes
        except Exception:
            pass

    parts = skill.relative_path.parts
    return parts[0] if parts else "custom"


def _last_user_text(runtime: ToolRuntime[ContextT, ThreadState]) -> str | None:
    messages = (runtime.state or {}).get("messages") or []
    for message in reversed(messages):
        if getattr(message, "type", None) != "human":
            continue
        content = getattr(message, "content", "")
        if isinstance(content, str) and content.strip():
            return content.strip()
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text" and isinstance(item.get("text"), str):
                    parts.append(item["text"])
            if parts:
                return "\n".join(parts).strip()
    return None


def _legacy_invocation_package(envelope: dict[str, Any], skill: Any) -> dict[str, Any]:
    legacy = envelope.get("legacy") if isinstance(envelope.get("legacy"), dict) else {}
    inputs = envelope.get("inputs") if isinstance(envelope.get("inputs"), dict) else {}
    files = inputs.get("files") if isinstance(inputs.get("files"), list) else []
    data_sources = inputs.get("data_sources") if isinstance(inputs.get("data_sources"), list) else []
    parameters = inputs.get("parameters") if isinstance(inputs.get("parameters"), dict) else {}

    selected_data_sources = list(legacy.get("selected_data_sources") or [])
    for file_item in files:
        if isinstance(file_item, dict) and file_item.get("path"):
            selected_data_sources.append(str(file_item["path"]))
    for source in data_sources:
        if isinstance(source, dict):
            uri = source.get("uri") or source.get("source_id")
            if uri:
                selected_data_sources.append(str(uri))

    return {
        "skill_dir": str(skill.skill_dir),
        "skill_file": str(skill.skill_file),
        "container_skill_file": skill.get_container_file_path(_container_base_path()),
        "selected_data_sources": selected_data_sources,
        "argv": legacy.get("argv") or [],
        "env": legacy.get("env") or {},
        "parameters": parameters,
        "working_directory": legacy.get("working_directory") or str(skill.skill_dir),
        "adapter_note": (
            "Load the returned container_skill_file, follow its SKILL.md workflow, "
            "and use selected_data_sources/parameters as the legacy inputs."
        ),
    }


def _wrap_skill_result(
    *,
    envelope: dict[str, Any],
    skill_name: str,
    legacy_output: Any,
    legacy_artifacts: list[str] | None,
    status: str,
) -> dict[str, Any]:
    output_text = legacy_output if isinstance(legacy_output, str) else json.dumps(legacy_output, ensure_ascii=False)
    display_text = output_text.strip()
    overview = display_text or "Skill execution completed."
    if len(overview) > 1200:
        overview = overview[:1200] + "..."

    artifacts = [
        {
            "artifact_id": f"a-{idx:03d}",
            "type": _artifact_type(path),
            "title": Path(path).name or path,
            "uri": path,
        }
        for idx, path in enumerate(legacy_artifacts or [], start=1)
    ]

    result = {
        "schema_version": "1.0",
        "request_id": envelope.get("request_id") or str(uuid.uuid4()),
        "skill_name": skill_name,
        "scenario": envelope.get("scenario") or "unknown",
        "capability": envelope.get("capability") or "skill_invocation",
        "status": status,
        "result": {
            "display_text": display_text,
            "summary": {
                "title": f"{skill_name} result",
                "overview": overview,
            },
            "findings": [],
            "evidence": [
                {
                    "evidence_id": "e-001",
                    "type": "text",
                    "title": "Legacy skill output",
                    "data": output_text,
                }
            ] if output_text else [],
            "artifacts": artifacts,
        },
        "diagnostics": {
            "warnings": [],
            "data_quality": {},
            "provenance": [{"source": "invoke_skill.output_adapter"}],
            "runtime": {"ended_at": datetime.now(UTC).isoformat()},
        },
        "errors": [],
    }
    _validate_schema(result, "skill_result.schema.json")
    return result


def _artifact_type(path: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix in {".md", ".txt", ".pdf", ".docx"}:
        return "report"
    if suffix in {".png", ".jpg", ".jpeg", ".webp", ".tif", ".tiff"}:
        return "image"
    if suffix in {".json", ".jsonl"}:
        return "json"
    if suffix in {".csv", ".xlsx", ".xls", ".parquet"}:
        return "table"
    return "file"


@tool("invoke_skill", parse_docstring=True)
def invoke_skill_tool(
    runtime: ToolRuntime[ContextT, ThreadState],
    skill_name: str,
    tool_call_id: Annotated[str, InjectedToolCallId],
    mode: Literal["prepare", "wrap_output"] = "prepare",
    input_envelope: dict[str, Any] | str | None = None,
    capability: str | None = None,
    legacy_output: str | dict[str, Any] | list[Any] | None = None,
    legacy_artifacts: list[str] | None = None,
    status: Literal["success", "partial_success", "failed"] = "success",
) -> str:
    """Adapt a selected Skill call to the unified Skill input/output contracts.

    Use this tool after choosing a concrete Skill from the available skills.
    It is the system-level adapter boundary between LeadAgent and legacy Skill implementations.

    In `prepare` mode, this tool validates and normalizes `SkillInputEnvelope`, checks that
    `skill_name` is allowed in the current routing scope, locates the Skill file, and returns
    a legacy invocation package. After this, load the returned `container_skill_file` and follow
    the Skill workflow using the returned selected data sources and parameters.

    In `wrap_output` mode, pass the same `input_envelope` plus the legacy execution result in
    `legacy_output`; the tool wraps it as a validated `SkillResult`.

    Args:
        skill_name: Concrete selected Skill id. Do not pass the whole scene candidate set.
        mode: `prepare` before executing the Skill, or `wrap_output` after legacy execution.
        input_envelope: SkillInputEnvelope fields. Missing common fields are filled from runtime state.
        capability: Optional concrete capability for this call.
        legacy_output: Existing Skill output to wrap as SkillResult in `wrap_output` mode.
        legacy_artifacts: Optional output file paths produced by the legacy Skill.
        status: SkillResult status used in `wrap_output` mode.
    """
    del tool_call_id  # Injected for traceability by LangGraph; no direct use needed here.

    lookup = _skill_lookup()
    skill = lookup.get(skill_name)
    if skill is None:
        return _json_response({
            "status": "error",
            "error": {
                "code": "UNKNOWN_SKILL",
                "message": f"Skill '{skill_name}' is not installed or enabled.",
            },
        })

    allowed = _state_allowed_skills(runtime)
    is_public_skill = getattr(skill, "category", None) == "public"

    if (
        allowed is not None
        and not is_public_skill
        and skill_name not in allowed
        and skill.skill_path not in allowed
    ):
        return _json_response({
            "status": "error",
            "error": {
                "code": "SKILL_NOT_ALLOWED",
                "message": f"Skill '{skill_name}' is not in the current routing scope.",
                "allowed_skills": sorted(allowed),
            },
        })

    try:
        if isinstance(input_envelope, str):
            try:
                parsed_input_envelope = json.loads(input_envelope)
            except json.JSONDecodeError as exc:
                return _json_response({
                    "status": "error",
                    "error": {
                        "code": "INVALID_SKILL_INPUT_ENVELOPE",
                        "message": f"input_envelope must be a dictionary or valid JSON object string: {exc}",
                    },
                })

            if not isinstance(parsed_input_envelope, dict):
                return _json_response({
                    "status": "error",
                    "error": {
                        "code": "INVALID_SKILL_INPUT_ENVELOPE",
                        "message": "input_envelope JSON string must decode to an object/dictionary.",
                    },
                })

            input_envelope = parsed_input_envelope

        envelope = _normalize_input_envelope(
            runtime=runtime,
            skill_name=skill.name,
            skill=skill,
            capability=capability,
            input_envelope=input_envelope,
        )
        _validate_schema(envelope, "skill_input_envelope.schema.json")
    except Exception as exc:
        return _json_response({
            "status": "error",
            "error": {
                "code": "INVALID_SKILL_INPUT_ENVELOPE",
                "message": str(exc),
            },
        })

    if mode == "wrap_output":
        try:
            skill_result = _wrap_skill_result(
                envelope=envelope,
                skill_name=skill.name,
                legacy_output=legacy_output,
                legacy_artifacts=legacy_artifacts,
                status=status,
            )
        except Exception as exc:
            return _json_response({
                "status": "error",
                "error": {
                    "code": "INVALID_SKILL_RESULT",
                    "message": str(exc),
                },
            })
        return _json_response({"status": "wrapped", "skill_result": skill_result})

    return _json_response({
        "status": "prepared",
        "skill_name": skill.name,
        "input_envelope": envelope,
        "legacy_invocation": _legacy_invocation_package(envelope, skill),
        "next_steps": [
            "Read container_skill_file before executing the Skill workflow.",
            "Use selected_data_sources and parameters as the legacy inputs.",
            "After execution, call invoke_skill again with mode='wrap_output' to produce SkillResult.",
        ],
    })
