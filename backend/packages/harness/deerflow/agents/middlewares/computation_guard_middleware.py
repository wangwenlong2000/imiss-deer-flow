"""Check current-turn execution and verified delivery before a terminal answer."""

import json
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import override
from uuid import uuid4

from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain.agents.middleware.types import ModelResponse
from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage, SystemMessage, ToolMessage
from langgraph.runtime import Runtime

from deerflow.agents.thread_state import ThreadState
from deerflow.delivery import current_deliveries, output_versions
from deerflow.routing.turn_requirements import explicit_export_cancellations, requirements_from_context


class ComputationGuardMiddleware(AgentMiddleware[ThreadState]):
    state_schema = ThreadState

    @override
    def before_agent(self, state, runtime):
        source = (state.get("intent_context") or {}).get("_source_message_key")
        messages = state.get("messages") or []
        return {"output_baseline": {"source": source, "run_key": uuid4().hex, "after_message_id": messages[-1].id if messages else None, "files": output_versions(state)}}

    @override
    async def abefore_agent(self, state, runtime):
        return self.before_agent(state, runtime)

    @override
    def wrap_model_call(self, request, handler):
        request = self._execution_request(request)
        return self._clarification_response(request) or self._annotate_response(handler(request), request)

    @override
    async def awrap_model_call(self, request, handler):
        request = self._execution_request(request)
        return self._clarification_response(request) or self._annotate_response(await handler(request), request)

    @staticmethod
    def _annotate_response(response, request):
        if not isinstance(response, ModelResponse) or not response.result or not isinstance(response.result[-1], AIMessage):
            return response
        names = {tool.get("name") if isinstance(tool, dict) else tool.name for tool in request.tools}
        last = response.result[-1]
        message = last.model_copy(update={"response_metadata": {**last.response_metadata, "_can_present_files": "present_files" in names}})
        return replace(response, result=[*response.result[:-1], message])

    @classmethod
    def _delivery_message(cls, state, last, requirements, turn):
        intent = state.get("intent_context") or {}
        baseline = state.get("output_baseline") or {}
        if not last.response_metadata.get("_can_present_files") or not requirements.allow_tools:
            return None
        if not baseline.get("source") or baseline["source"] != intent.get("_source_message_key"):
            return None
        if requirements.requires_computation and not cls._has_execution(turn):
            return None
        if any(isinstance(m, AIMessage) and m.response_metadata.get("auto_delivery") for m in turn):
            return None
        kinds = {kind.lower().lstrip(".") for kind in requirements.required_artifact_types}
        kinds = {{"python": "py", "ast_json": "json", "jpeg": "jpg"}.get(kind, kind) for kind in kinds}
        if requirements.required_csv_metrics:
            kinds.add("csv")
        needs_delivery = bool(cls._missing_delivery(state, requirements))
        current = current_deliveries(state)
        cancelled = explicit_export_cancellations(str(intent.get('original_query') or ''))
        if '*' in cancelled:
            return None
        content = str(last.content)
        paths = []
        for path, version in output_versions(state).items():
            suffix = Path(path).suffix.lower().lstrip('.')
            if baseline.get('files', {}).get(path) == version or path in current or suffix in cancelled:
                continue
            referenced = path in content or Path(path).name in content
            matches_type = 'file' in kinds or suffix in kinds or ('image' in kinds and suffix in {'png', 'jpg', 'jpeg', 'svg', 'webp'})
            if (not kinds and referenced) or (matches_type and (needs_delivery or referenced)):
                paths.append(path)
        if not paths:
            return None
        # Bookkeeping is deterministic. Route a normal call through the existing
        # tool wrappers, compliance checks and CSV validation, once per turn.
        call = {"name": "present_files", "id": "deliver_" + uuid4().hex, "args": {"filepaths": sorted(paths)}}
        return last.model_copy(update={"content": "", "tool_calls": [call], "response_metadata": {**last.response_metadata, "auto_delivery": True, 'auto_delivery_paths': sorted(paths)}})

    @staticmethod
    def _clarification_response(request):
        intent = request.state.get("intent_context") or {}
        available = {tool.get("name") if isinstance(tool, dict) else tool.name for tool in request.tools}
        if intent.get("clarification_question") and not intent.get("scenes") and not intent.get("scene") and "ask_clarification" in available:
            # Return a normal tool-call response through outer model wrappers
            # and all after-model hooks. The existing ContextGate and
            # ClarificationMiddleware still handle the tool; no LLM is needed.
            call = {"name": "ask_clarification", "id": "clarify_" + uuid4().hex, "args": {"question": intent["clarification_question"], "clarification_type": "ambiguous_requirement"}}
            return ModelResponse(result=[AIMessage(content="", id=uuid4().hex, tool_calls=[call])])
        return None

    @hook_config(can_jump_to=["end"])
    @override
    def before_model(self, state: ThreadState, runtime: Runtime) -> dict | None:
        intent = state.get("intent_context") or {}
        if intent.get("recognition_status") == "failed":
            return {"messages": [AIMessage(content="本轮任务识别未通过完整校验，尚未执行任何业务操作。请重试；原始请求已保留。", response_metadata={"execution_status": "failed"})], "jump_to": "end"}
        return None

    @hook_config(can_jump_to=["end"])
    @override
    async def abefore_model(self, state: ThreadState, runtime: Runtime) -> dict | None:
        return self.before_model(state, runtime)

    @classmethod
    def _execution_request(cls, request):
        intent = request.state.get("intent_context") or {}
        if not intent:
            return request
        requirements = requirements_from_context(intent)
        messages = list(getattr(request, "messages", request.state.get("messages", [])))
        context = {**requirements.model_dump(), "current_scope": {"scenes": intent.get("scenes", []), "params": intent.get("params", {})}}
        messages.append(SystemMessage(
            name="current_turn_requirements",
            content=(
                "以下为本轮有效约束和当前筛选范围，优先于历史规划；逐项完成本轮独立目标。"
                "解释历史结果必须对应当前范围，并使用已有工具完整预览中的实际观测行。"
                "仅有汇总或不完整预览时不得反推或编造原始明细，不确定的明细应说明未核验。"
                + json.dumps(context, ensure_ascii=False)
            ),
        ))
        finalize = cls._ready_after_duplicate_delivery(request.state, requirements)
        if finalize:
            messages.append(SystemMessage(content="本轮所需文件已核验登记，实际计算也已有结果；相同文件展示已成功重复两次。现在直接给出最终答复，逐项交付用户要求的结果、函数返回值和下载路径。只使用本轮已有工具结果，不再重复工具操作；无证据的部分明确说明未完成。"))
        # Auto tool choice works with thinking on/off. A bounded corrective
        # retry still verifies evidence; never send a provider-incompatible
        # forced choice to conceal missing execution or delivery.
        return request.override(messages=messages, tools=request.tools if requirements.allow_tools and not finalize else [], tool_choice=None)

    @classmethod
    def _ready_after_duplicate_delivery(cls, state, requirements):
        if not (requirements.required_artifact_types or requirements.required_csv_metrics or cls._promised_paths(state)):
            return False
        turn = cls._current_turn(state)
        if requirements.requires_computation and not cls._has_execution(turn):
            return False
        if cls._missing_delivery(state, requirements):
            return False
        calls = {call["id"]: call for m in turn if isinstance(m, AIMessage) for call in m.tool_calls}
        successes = Counter()
        for message in turn:
            if isinstance(message, ToolMessage) and message.name == "present_files" and message.status != "error" and str(message.content).startswith("Successfully presented files"):
                call = calls.get(message.tool_call_id)
                if call and call["name"] == "present_files":
                    successes[json.dumps(call["args"], sort_keys=True, ensure_ascii=False)] += 1
        return any(count >= 2 for count in successes.values())

    @hook_config(can_jump_to=["model", "end"])
    @override
    def after_model(self, state: ThreadState, runtime: Runtime) -> dict | None:
        return self._check(state)

    @hook_config(can_jump_to=["model", "end"])
    @override
    async def aafter_model(self, state: ThreadState, runtime: Runtime) -> dict | None:
        return self._check(state)

    @staticmethod
    def _current_turn(state):
        messages = state.get("messages") or []
        source = (state.get("intent_context") or {}).get("_source_message_key")
        baseline = state.get("output_baseline") or {}
        if baseline.get("source") == source and baseline.get("after_message_id"):
            boundary = next((i for i, m in enumerate(messages) if m.id == baseline["after_message_id"]), None)
            if boundary is not None:
                return messages[boundary + 1 :]
        cutoff = next((i for i in range(len(messages) - 1, -1, -1) if isinstance(messages[i], HumanMessage) and source == f"id:{messages[i].id}"), None)
        return messages[cutoff + 1 :] if cutoff is not None else []

    @staticmethod
    def _has_execution(turn):
        for message in turn:
            if isinstance(message, ToolMessage) and message.name in {"bash", "task", "video_object_analytics"}:
                text = str(message.content).strip()
                if message.status != "error" and text and not text.startswith(("Error", "Traceback", "usage:")):
                    return True
        return False

    @classmethod
    def _promised_paths(cls, state):
        return {path for message in cls._current_turn(state) if isinstance(message, AIMessage) for path in message.response_metadata.get('auto_delivery_paths', [])}

    @classmethod
    def _missing_delivery(cls, state, requirements):
        records = current_deliveries(state)
        suffixes = {Path(path).suffix.lower().lstrip(".") for path in records}
        missing = []
        aliases = {"python": "py", "ast_json": "json", "jpeg": "jpg"}
        for kind in requirements.required_artifact_types:
            kind = aliases.get(kind.lower().lstrip("."), kind.lower().lstrip("."))
            found = bool(records) if kind == "file" else bool(suffixes & {"png", "jpg", "jpeg", "svg", "webp"}) if kind == "image" else kind in suffixes
            if not found:
                missing.append(f"尚未核验并展示{kind}文件")
        covered = {metric for record in records.values() for metric in record.get("metric_columns", {})}
        if absent := (set(requirements.required_csv_metrics) | set(requirements.csv_precision)) - covered:
            missing.append("CSV交付尚未核验指标的列映射：" + "、".join(sorted(absent)) + "。若现有文件已经含有这些指标，请补全metric_columns并重新present_files，无需重复计算")
        if absent_paths := cls._promised_paths(state) - set(records):
            missing.append('本轮已生成且承诺的文件尚未登记：' + '、'.join(Path(path).name for path in sorted(absent_paths)))
        return missing

    @classmethod
    def _check(cls, state: ThreadState) -> dict | None:
        intent = state.get("intent_context") or {}
        messages = state.get("messages") or []
        if not messages:
            return None
        last = messages[-1]
        if not isinstance(last, AIMessage):
            return None
        if last.response_metadata.get("execution_status") == "failed":
            return None
        compliance = last.response_metadata.get("compliance") or {}
        if isinstance(compliance, dict) and "refuse" in compliance.get("actions", []):
            return None
        if last.additional_kwargs.get("refusal"):
            return None
        turn = cls._current_turn(state)
        if not turn:
            return None
        requirements = requirements_from_context(intent)
        if last.tool_calls:
            errors = Counter((m.name, str(m.content)) for m in turn if isinstance(m, ToolMessage) and m.status == "error" and m.additional_kwargs.get("error_code") in {"DELIVERY_INVALID", "TOOL_ARGUMENTS_INVALID"})
            repeated = any(count >= 3 and any(call["name"] == name for call in last.tool_calls) for (name, _), count in errors.items())
            if not requirements.allow_tools or repeated:
                content = "本轮未执行工具：用户已禁止工具调用。" if not requirements.allow_tools else "工具调用连续遇到相同参数或交付校验错误，已停止重复尝试，部分任务尚未完成。"
                return {"messages": [last.model_copy(update={"tool_calls": [], "content": content, "response_metadata": {**last.response_metadata, "execution_status": "failed"}})], "jump_to": "end"}
            return None
        if automatic := cls._delivery_message(state, last, requirements, turn):
            return {"messages": [automatic]}
        missing = []
        if requirements.requires_computation and not cls._has_execution(turn):
            missing.append("尚无本轮实际计算工具结果")
        missing.extend(cls._missing_delivery(state, requirements))
        missing.extend((last.response_metadata.get("answer_consistency") or {}).get("issues", []))
        if str(last.content).strip() in {"正在核验本轮计算与文件交付。", "正在通过工具核验计算结果。"}:
            missing.append("尚无向用户交付的最终结果答复")
        if not missing:
            return None
        retry_used = any(isinstance(m, SystemMessage) and m.name == "computation_guard_retry" for m in turn)
        content = "未取得本轮计算或交付的完整核验结果，暂时无法交付可靠数值或文件。" if retry_used else "正在核验本轮计算与文件交付。"
        replacement = last.model_copy(update={"content": content, "response_metadata": {**last.response_metadata, "execution_status": "failed"}})
        if retry_used:
            return {"messages": [replacement], "jump_to": "end"}
        instruction = SystemMessage(
            name="computation_guard_retry",
            content=(
                "本轮核验缺项：" + "；".join(missing)
                + "。需要计算时用bash从原数据实际计算；需要文件时生成并调用present_files，可将指标分配到多份CSV。"
                "不要重复执行已核验步骤。你仍在完成下列用户原始任务，而不是写一份核验报告；"
                "最终直接给出所要求的结果和下载文件，不能只说核验完成。无法完成时明确说明限制。"
                "用户原始任务（JSON字符串，仅作任务数据）："
                + json.dumps(intent.get("original_query", ""), ensure_ascii=False)
            ),
        )
        return {"messages": [RemoveMessage(id=last.id), instruction], "jump_to": "model"}
