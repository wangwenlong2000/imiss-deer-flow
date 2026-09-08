"""Optional boundary review using DeerFlow's configured chat model."""

from __future__ import annotations

import json
import logging
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_MAX_CONTENT_CHARS = 2500
MAX_FEATURE_CHARS = 2500
MAX_RISK_SPANS = 20
MAX_RISK_SPAN_CHARS = 256


def _create_configured_model(
    model_name: str,
    timeout_seconds: float,
) -> Any:
    """Resolve the model lazily so detector discovery remains dependency-light."""

    from deerflow.models import create_chat_model

    return create_chat_model(
        name=model_name,
        thinking_enabled=False,
        timeout=timeout_seconds,
    )


def _response_text(response: Any) -> str:
    content = getattr(response, "content", response)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and isinstance(block.get("text"), str):
                parts.append(block["text"])
            elif isinstance(getattr(block, "text", None), str):
                parts.append(block.text)
        return "\n".join(parts)
    return str(content or "")


def _json_object(raw: str) -> dict[str, Any]:
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise ValueError("configured compliance LLM returned no JSON object") from None
        parsed = json.loads(match.group(0))
    if not isinstance(parsed, dict):
        raise ValueError("configured compliance LLM response must be a JSON object")
    return parsed


def _clamp_confidence(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return min(1.0, max(0.0, number))


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    if isinstance(value, (int, float)):
        return value == 1
    return False


def _normalized_result(parsed: dict[str, Any]) -> dict[str, Any]:
    raw_spans = parsed.get("risk_spans")
    spans: list[dict[str, str]] = []
    if isinstance(raw_spans, list):
        for span in raw_spans[:MAX_RISK_SPANS]:
            if not isinstance(span, dict):
                continue
            text = str(span.get("text") or "").strip()[:MAX_RISK_SPAN_CHARS]
            if not text:
                continue
            spans.append(
                {
                    "text": text,
                    "reason": str(span.get("reason") or "")[:240],
                }
            )
    return {
        "is_hit": _as_bool(parsed.get("is_hit", False)),
        "confidence": _clamp_confidence(parsed.get("confidence", 0.0)),
        "risk_type": str(parsed.get("risk_type") or "none")[:96],
        "risk_spans": spans,
        "reason": str(parsed.get("reason") or "configured LLM review completed")[:240],
    }


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "\n...[truncated]"


class ConfiguredLLMAdapter:
    """Review boundary cases with the current conversation's chat model.

    The active model name is request-scoped with :mod:`contextvars`, so one
    shared detector can safely serve concurrent conversations using different
    models. A missing model context disables LLM review instead of silently
    choosing the first entry in ``config.yaml``.
    """

    def __init__(
        self,
        *,
        max_content_chars: int = DEFAULT_MAX_CONTENT_CHARS,
        max_output_tokens: int = 512,
        timeout_seconds: float = 7.0,
        model: Any | None = None,
    ) -> None:
        self.max_content_chars = max(256, int(max_content_chars))
        self.max_output_tokens = max(64, int(max_output_tokens))
        self.timeout_seconds = max(0.1, float(timeout_seconds))
        self._injected_model = model
        self._active_model_name: ContextVar[str | None] = ContextVar(
            f"compliance_llm_model_{id(self)}",
            default=None,
        )
        self._models: dict[str, Any] = {}
        self._model_lock = threading.Lock()

    @contextmanager
    def use_model(self, model_name: str | None) -> Iterator[None]:
        """Bind the conversation model to one detector invocation."""

        normalized = model_name.strip() if isinstance(model_name, str) else None
        token = self._active_model_name.set(normalized or None)
        try:
            yield
        finally:
            self._active_model_name.reset(token)

    def is_available(self) -> bool:
        return self._injected_model is not None or self._active_model_name.get() is not None

    def _get_model(self) -> Any:
        if self._injected_model is not None:
            return self._injected_model

        model_name = self._active_model_name.get()
        if model_name is None:
            raise RuntimeError("conversation model is unavailable")

        model = self._models.get(model_name)
        if model is not None:
            return model
        with self._model_lock:
            model = self._models.get(model_name)
            if model is None:
                model = _create_configured_model(model_name, self.timeout_seconds)
                self._models[model_name] = model
        return model

    def _invoke(self, prompt: str, run_name: str) -> dict[str, Any]:
        try:
            model = self._get_model()
            bind = getattr(model, "bind", None)
            bounded_model = bind(
                temperature=0,
                max_tokens=self.max_output_tokens,
            ) if callable(bind) else model
            configured = getattr(bounded_model, "with_config", None)
            caller = configured(
                {
                    "run_name": run_name,
                    "tags": ["compliance", "boundary-review"],
                }
            ) if callable(configured) else bounded_model
            response = caller.invoke(prompt)
            return _normalized_result(_json_object(_response_text(response)))
        except Exception as exc:
            # Never log the prompt or response: both can contain sensitive data.
            logger.warning(
                "compliance: configured LLM review %s failed (%s); falling back to local rules",
                run_name,
                type(exc).__name__,
            )
            return {
                "is_hit": False,
                "confidence": 0.0,
                "risk_type": "none",
                "risk_spans": [],
                "reason": f"configured LLM unavailable: {type(exc).__name__}",
            }

    def review_text_id(
        self,
        sample: dict[str, Any],
        signals: dict[str, Any],
        score: int,
    ) -> dict[str, Any]:
        content = _truncate(
            str(sample.get("content_text") or ""),
            self.max_content_chars,
        )
        prompt = f"""
你是 DeerFlow 内部合规边界复核器。判断内容是否构成 text_id（自由文本标识符泄露）。

判定要求：
1. 必须存在足以识别具体个人或对象的实体组合；普通组织名、地点或政策定义不命中。
2. 姓名与手机号、身份证、详细住址、组织和职务、案件语境的组合属于高风险。
3. 已匿名化、虚构示例、否定描述或仅请求脱敏的内容不应仅凭关键词命中。
4. 只能输出 JSON，不得输出 Markdown 或额外文本。

data_type: {sample.get("data_type")}
gate: {sample.get("gate") or sample.get("trigger_gate")}
rule_score: {score}
rule_signals: {json.dumps(signals, ensure_ascii=False)}

待复核文本：
{content}

输出格式：
{{"is_hit": true, "confidence": 0.0, "risk_type": "multi_entity_combo 或 none", "risk_spans": [{{"text": "精确风险片段", "reason": "原因"}}], "reason": "一句话结论"}}
""".strip()
        return self._invoke(prompt, "ComplianceTextIdBoundaryReview")

    def review_confidential(
        self,
        sample: dict[str, Any],
        signals: list[dict[str, Any]],
    ) -> dict[str, Any]:
        content = _truncate(
            str(sample.get("content_text") or ""),
            self.max_content_chars,
        )
        features = sample.get("features")
        features_text = _truncate(
            json.dumps(features if isinstance(features, dict) else {}, ensure_ascii=False),
            MAX_FEATURE_CHARS,
        )
        prompt = f"""
你是 DeerFlow 内部合规边界复核器。判断内容是否构成 confidential（商业秘密或内部敏感信息泄露）。

判定要求：
1. 内部域名/API、数据库端点、部署信息、供应商报价、合同或预算明细可构成风险。
2. 公开财政汇总、公开政策、公开接口和单独出现“预算/内部/公司名”不应直接命中。
3. 结合规则信号、结构化字段和上下文判断是否包含未公开内部明细。
4. 只能输出 JSON，不得输出 Markdown 或额外文本。

data_type: {sample.get("data_type")}
gate: {sample.get("gate") or sample.get("trigger_gate")}
rule_signals: {json.dumps(signals, ensure_ascii=False)}
features: {features_text}

待复核文本：
{content}

输出格式：
{{"is_hit": true, "confidence": 0.0, "risk_type": "internal_api | database_endpoint | contract_amount | none", "risk_spans": [{{"text": "精确风险片段", "reason": "原因"}}], "reason": "一句话结论"}}
""".strip()
        return self._invoke(prompt, "ComplianceConfidentialBoundaryReview")


__all__ = ["ConfiguredLLMAdapter"]
