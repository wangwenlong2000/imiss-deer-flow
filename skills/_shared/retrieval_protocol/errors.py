"""标准化错误码与建议处理动作 —— schema v1.1 新增（task.md §4 errors 字段）。

SkillResult 的 ``errors[]`` 不再是无结构 list。每条错误必须是一个对象,带:

- ``code``    标准化错误码,见 :data:`ERROR_CODES`;
- ``message`` 面向人/日志的中文说明;
- ``action``  Planner / DeerFlow 拿到失败结果时的建议处理动作,见 :data:`ERROR_ACTIONS`;
- ``retryable`` 是否可幂等重试(bool);
- ``detail``  可选,任意结构化补充信息。

设计原则:
- **错误码与 status 联动** —— ``status="success"`` 时 errors 必须为空;
  ``status="partial"``/``"error"`` 时 errors 必须至少一条。
- **action 是建议而非硬性** —— Planner 可结合上下文覆盖,但缺省应遵循。
- 各 RAG 不能私造错误码;新错误类型必须先登记到本模块。

本模块仅依赖 Python 标准库。
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# 错误码登记
# ---------------------------------------------------------------------------

# 各错误码对应的「缺省建议动作」。Planner 可在 envelope.context 里覆盖,
# 但若未覆盖,DeerFlow 应当遵循此默认表。
ERROR_CODES: dict[str, str] = {
    # 上游/索引层
    "E_INDEX_MISSING":      "degrade_to_local",   # 检索索引不存在或未构建
    "E_UPSTREAM_FAIL":      "retry_with_backoff", # 上游 ES/Milvus/外部 RAG 报错
    "E_TIMEOUT":            "retry_with_backoff", # 请求超时
    "E_RATE_LIMIT":         "retry_with_backoff", # 触发限流
    # 请求层
    "E_BAD_QUERY":          "report_to_user",     # query 为空/无法解析/语法错误
    "E_UNSUPPORTED_FILTER": "report_to_user",     # filter 字段不被该 RAG 支持
    "E_PERMISSION_DENIED":  "abort",              # 鉴权/合规拒绝
    # 预算层(配合 v1.1 budget 字段)
    "E_OUT_OF_BUDGET":      "report_to_user",     # 命中数/token 远超 budget,已截断
    "E_PARTIAL_RESULT":     "report_to_user",     # 仅返回部分桶/分片结果
    # 兜底
    "E_INTERNAL":           "abort",              # 协议库内部断言失败
}

# 建议动作枚举 —— 全局只 4 种,避免 Planner 出现长长的 switch。
ERROR_ACTIONS: frozenset[str] = frozenset({
    "retry_with_backoff",  # 间隔抖动重试,默认 3 次
    "degrade_to_local",    # 切到 demo/local 模式,继续走
    "report_to_user",      # 把 message 透传给用户,不中断会话
    "abort",               # 中断当前 retrieve,后续 plan 重新规划
})

# status 标识结果完整性,与 errors 联动。
STATUS_SUCCESS = "success"
STATUS_PARTIAL = "partial"
STATUS_ERROR = "error"
ERROR_STATUSES: frozenset[str] = frozenset({STATUS_PARTIAL, STATUS_ERROR})


# ---------------------------------------------------------------------------
# 构造器
# ---------------------------------------------------------------------------


def build_error(
    *,
    code: str,
    message: str,
    action: str | None = None,
    retryable: bool | None = None,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """构造一条标准化 error 对象。

    ``action`` 不给则按 :data:`ERROR_CODES` 缺省值填;``retryable`` 不给则按
    ``action == "retry_with_backoff"`` 推断。
    """
    if code not in ERROR_CODES:
        raise ValueError(f"unknown error code {code!r}; expected one of {sorted(ERROR_CODES)}")
    if not isinstance(message, str) or not message.strip():
        raise ValueError("error.message must be a non-empty string")

    resolved_action = action if action is not None else ERROR_CODES[code]
    if resolved_action not in ERROR_ACTIONS:
        raise ValueError(
            f"unknown error action {resolved_action!r}; expected one of {sorted(ERROR_ACTIONS)}"
        )
    resolved_retryable = (
        retryable if retryable is not None else resolved_action == "retry_with_backoff"
    )

    error: dict[str, Any] = {
        "code": code,
        "message": message,
        "action": resolved_action,
        "retryable": bool(resolved_retryable),
    }
    if detail is not None:
        if not isinstance(detail, dict):
            raise ValueError("error.detail must be a dict if given")
        error["detail"] = dict(detail)
    return error


def derive_status(errors: list[dict[str, Any]] | None, *, partial_if_any_evidence: bool = False) -> str:
    """根据 ``errors`` 推断 SkillResult.status。

    - errors 为空 → ``success``
    - errors 非空 + ``partial_if_any_evidence=True``(调用方明确有部分命中) → ``partial``
    - errors 非空 + 否则 → ``error``
    """
    if not errors:
        return STATUS_SUCCESS
    return STATUS_PARTIAL if partial_if_any_evidence else STATUS_ERROR


# ---------------------------------------------------------------------------
# 校验
# ---------------------------------------------------------------------------


def validate_error(error: Any, *, path: str = "error") -> list[str]:
    """校验单条 error 对象;返回错误说明列表,空列表表示合规。"""
    if not isinstance(error, dict):
        return [f"[errors] {path} must be an object"]
    issues: list[str] = []
    code = error.get("code")
    if code not in ERROR_CODES:
        issues.append(f"[errors] {path}.code {code!r} is not a registered error code")
    message = error.get("message")
    if not isinstance(message, str) or not message.strip():
        issues.append(f"[errors] {path}.message must be a non-empty string")
    action = error.get("action")
    if action not in ERROR_ACTIONS:
        issues.append(
            f"[errors] {path}.action {action!r} must be one of {sorted(ERROR_ACTIONS)}"
        )
    retryable = error.get("retryable")
    if not isinstance(retryable, bool):
        issues.append(f"[errors] {path}.retryable must be a bool")
    if "detail" in error and not isinstance(error["detail"], dict):
        issues.append(f"[errors] {path}.detail must be an object if present")
    return issues


def validate_errors_block(
    errors: Any,
    *,
    status: str,
    path: str = "errors",
) -> list[str]:
    """校验整段 ``errors[]`` 与 ``status`` 的联动语义。

    - errors 必须是 list;
    - status=success 时 errors 必须为空;
    - status=partial/error 时 errors 必须至少一条;
    - 每条 error 形状合法。
    """
    if errors is None:
        errors = []
    if not isinstance(errors, list):
        return [f"[errors] {path} must be a list"]

    issues: list[str] = []
    if status == STATUS_SUCCESS and errors:
        issues.append(f"[errors] status='success' forbids non-empty {path}")
    if status in ERROR_STATUSES and not errors:
        issues.append(f"[errors] status={status!r} requires at least one {path}[*] entry")
    for index, error in enumerate(errors):
        issues.extend(validate_error(error, path=f"{path}[{index}]"))
    return issues
