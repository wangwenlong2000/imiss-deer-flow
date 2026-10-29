"""Check explicit numerical comparisons against verified current CSV results.

This is a bounded arithmetic check, not a scene classifier or a semantic judge.
Unknown/ambiguous comparisons are left alone rather than guessed.
"""

import csv
import re
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from itertools import islice
from pathlib import Path

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelResponse
from langchain_core.messages import AIMessage

from deerflow.delivery import OUTPUT_PREFIX, current_deliveries
from deerflow.routing.turn_requirements import requirements_from_context

RELATIONS = {
    "不高于": lambda a, b: a <= b, "不大于": lambda a, b: a <= b,
    "不低于": lambda a, b: a >= b, "不小于": lambda a, b: a >= b,
    "高于": lambda a, b: a > b, "大于": lambda a, b: a > b,
    "多于": lambda a, b: a > b, "超过": lambda a, b: a > b,
    "低于": lambda a, b: a < b, "小于": lambda a, b: a < b,
    "少于": lambda a, b: a < b, "等于": lambda a, b: a == b,
}


def _number(value):
    try:
        number = Decimal(value.strip().removesuffix("%").replace(",", ""))
        return number if number.is_finite() else None
    except (InvalidOperation, AttributeError):
        return None


def _label(value):
    return re.sub(r"\s+", "", value).casefold()


def comparison_issues(state, text):
    """Only contradict a claim when every matching metric proves it false."""
    outputs = (state.get("thread_data") or {}).get("outputs_path")
    if not outputs:
        return []
    issues = []
    for virtual, delivery in current_deliveries(state).items():
        if not virtual.lower().endswith(".csv"):
            continue
        path = Path(outputs) / virtual.removeprefix(OUTPUT_PREFIX)
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(islice(reader, 101))
            headers = reader.fieldnames or []
        if not 2 <= len(rows) <= 100:
            continue
        metric_names = {column: {_label(column)} for column in headers}
        for name, column in delivery.get('metric_columns', {}).items():
            if column in metric_names:
                metric_names[column].add(_label(name))
        # A categorical key must identify exactly one row. Duplicate labels
        # (e.g. road/day detail) cannot establish an unambiguous comparison.
        keys = [column for column in headers if all(row.get(column) and _number(row[column]) is None for row in rows)
                and len({row[column] for row in rows}) == len(rows)]
        for key in keys:
            entities = {row[key]: row for row in rows}
            names = "|".join(re.escape(name) for name in sorted(entities, key=len, reverse=True))
            operators = "|".join(sorted(RELATIONS, key=len, reverse=True))
            pattern = re.compile(rf"(?P<left>{names})(?:的)?(?P<metrics>[^。！？；\n]{{1,60}}?)(?P<op>{operators})(?P<right>{names})")
            for match in pattern.finditer(text):
                left, right = entities[match['left']], entities[match['right']]
                metrics = re.sub(r"(?:均|都)$", "", match['metrics']).strip()
                for metric in re.split(r"以及|[、和与及，,]", metrics):
                    metric = _label(metric.strip().removeprefix('的'))
                    if not metric:
                        continue
                    candidates = [column for column, aliases in metric_names.items() if any(metric in alias or alias in metric for alias in aliases)]
                    values = [(column, _number(left.get(column)), _number(right.get(column))) for column in candidates]
                    # If “流量” could mean total or mean and they disagree,
                    # that ambiguity is not evidence that the answer is wrong.
                    if not values or any(a is None or b is None for _, a, b in values):
                        continue
                    if any(RELATIONS[match['op']](a, b) for _, a, b in values):
                        continue
                    facts = '；'.join(f"{column}：{match['left']}={a}，{match['right']}={b}" for column, a, b in values)
                    issues.append(f"答复中的“{match.group(0)}”与本轮已核验CSV {path.name}矛盾。实际{facts}")
    return list(dict.fromkeys(issues))[:4]


class AnswerConsistencyMiddleware(AgentMiddleware):
    @staticmethod
    def _checked(request, response):
        if not isinstance(response, ModelResponse) or not response.result:
            return response
        last = response.result[-1]
        if not isinstance(last, AIMessage) or last.tool_calls or not isinstance(last.content, str):
            return response
        compliance = last.response_metadata.get("compliance") or {}
        if last.additional_kwargs.get("refusal") or last.response_metadata.get("execution_status") == "failed" or isinstance(compliance, dict) and "refuse" in compliance.get("actions", []):
            return response
        if not requirements_from_context(request.state.get("intent_context")).requires_computation:
            return response
        issues = comparison_issues(request.state, last.content)
        # Never rewrite the answer here. The existing guard uses its bounded
        # correction, and the outer OutputGate still processes the final text.
        meta = {**last.response_metadata, "answer_consistency": {"method": "verified_csv_comparison", "status": "inconsistent" if issues else "no_proven_contradiction", "issues": issues}}
        message = last.model_copy(update={"response_metadata": meta})
        return replace(response, result=[*response.result[:-1], message])

    def wrap_model_call(self, request, handler):
        return self._checked(request, handler(request))

    async def awrap_model_call(self, request, handler):
        return self._checked(request, await handler(request))
