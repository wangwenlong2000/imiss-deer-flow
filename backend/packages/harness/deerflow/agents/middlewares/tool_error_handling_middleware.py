"""Tool error handling middleware and shared runtime middleware builders."""

import logging
from collections.abc import Awaitable, Callable
from typing import Any, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.errors import GraphBubbleUp
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

logger = logging.getLogger(__name__)

_MISSING_TOOL_CALL_ID = "missing_tool_call_id"


class ComplianceStartupError(RuntimeError):
    """Raised when the compliance preflight fails and ``strict_startup`` is set.

    Distinct from a generic exception so the guarded builder can let it through
    instead of degrading to "run without gates" — which would make the option a
    no-op.
    """


class ToolErrorHandlingMiddleware(AgentMiddleware[AgentState]):
    """Convert tool exceptions into error ToolMessages so the run can continue."""

    def _build_error_message(self, request: ToolCallRequest, exc: Exception) -> ToolMessage:
        tool_name = str(request.tool_call.get("name") or "unknown_tool")
        tool_call_id = str(request.tool_call.get("id") or _MISSING_TOOL_CALL_ID)
        detail = str(exc).strip() or exc.__class__.__name__
        if len(detail) > 500:
            detail = detail[:497] + "..."

        content = f"Error: Tool '{tool_name}' failed with {exc.__class__.__name__}: {detail}. Continue with available context, or choose an alternative tool."
        return ToolMessage(
            content=content,
            tool_call_id=tool_call_id,
            name=tool_name,
            status="error",
        )

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        try:
            return handler(request)
        except GraphBubbleUp:
            # Preserve LangGraph control-flow signals (interrupt/pause/resume).
            raise
        except Exception as exc:
            logger.exception("Tool execution failed (sync): name=%s id=%s", request.tool_call.get("name"), request.tool_call.get("id"))
            return self._build_error_message(request, exc)

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        try:
            return await handler(request)
        except GraphBubbleUp:
            # Preserve LangGraph control-flow signals (interrupt/pause/resume).
            raise
        except Exception as exc:
            logger.exception("Tool execution failed (async): name=%s id=%s", request.tool_call.get("name"), request.tool_call.get("id"))
            return self._build_error_message(request, exc)


def _build_runtime_middlewares(
    *,
    include_uploads: bool,
    include_dangling_tool_call_patch: bool,
    lazy_init: bool = True,
    model_name: str | None = None,
) -> list[AgentMiddleware]:
    """Build shared base middlewares for agent execution."""
    from deerflow.agents.middlewares.thread_data_middleware import ThreadDataMiddleware
    from deerflow.sandbox.middleware import SandboxMiddleware

    middlewares: list[AgentMiddleware] = [
        ThreadDataMiddleware(lazy_init=lazy_init),
        SandboxMiddleware(lazy_init=lazy_init),
    ]

    if include_uploads:
        from deerflow.agents.middlewares.uploads_middleware import UploadsMiddleware

        middlewares.insert(1, UploadsMiddleware())

    if include_dangling_tool_call_patch:
        from deerflow.agents.middlewares.dangling_tool_call_middleware import DanglingToolCallMiddleware

        middlewares.append(DanglingToolCallMiddleware())

    middlewares.append(ToolErrorHandlingMiddleware())

    # The context and output gates go at the very FRONT of the list. Two
    # orderings depend on it:
    #
    #   wrap_tool_call — composes first-in-list = outermost. ToolErrorHandling
    #     (appended just above, hence innermost) converts any exception into an
    #     error ToolMessage. A compliance gate inside it would have detection
    #     failures silently downgraded to "tool failed, carry on" — a gate that
    #     fails open without saying so.
    #
    #   after_model — runs in REVERSE list order, so front-of-list means the
    #     OutputGate is the LAST component to rewrite the AIMessage, and nothing
    #     downstream can reintroduce the violating text.
    #
    # The INPUT gate is deliberately not here: it uses before_agent, which runs
    # in FORWARD order, and it needs IntentRecognitionMiddleware's result to make
    # the "sensitive entity + high-risk intent" judgement. It is appended after
    # IntentRecognitionMiddleware in lead_agent/agent.py instead.
    #
    # test_compliance_gates.py asserts all of these positions rather than
    # trusting this comment to stay true.
    middlewares[:0] = build_compliance_flow_middlewares(model_name=model_name)
    return middlewares


def build_compliance_flow_middlewares(*, model_name: str | None = None) -> list[AgentMiddleware]:
    """Context and output gate middlewares, or none when compliance is off.

    Imports are deliberately local and guarded: compliance is disabled by
    default, and a problem in this subsystem must not stop agents being built.
    """
    try:
        from deerflow.config.compliance_config import get_compliance_config

        config = get_compliance_config()
        if not config.enabled:
            return []

        if not _compliance_preflight_passed(config):
            return []

        from deerflow.agents.middlewares.compliance_context_gate_middleware import ComplianceContextGateMiddleware
        from deerflow.agents.middlewares.compliance_output_gate_middleware import ComplianceOutputGateMiddleware

        gates: list[AgentMiddleware] = []
        if config.gates.output.enabled:
            gates.append(ComplianceOutputGateMiddleware(model_name=model_name))
        if config.gates.context.enabled:
            gates.append(ComplianceContextGateMiddleware(model_name=model_name))
        return gates
    except ComplianceStartupError:
        # strict_startup was requested: the deployment has declared that running
        # unprotected is worse than not running. Must not be swallowed below.
        raise
    except Exception:
        logger.exception("compliance: failed to build gate middlewares; agent will run WITHOUT compliance context/output gates")
        return []


#: Preflight runs once per process, not once per agent build.
_preflight_ok: bool | None = None


def _compliance_preflight_passed(config: Any) -> bool:
    """Run the startup canary once; decide whether gates may mount.

    An *installation* failure (missing model weights, unreadable policy matrix)
    must not present as "refuse every request". Without this check it would:
    the engine builds lazily on the request path, and with ``fail_mode: closed``
    every raise becomes a refusal. See ``deerflow.compliance.preflight``.

    Raises only when ``strict_startup`` is set — the deployment has then declared
    that running unprotected is worse than not running.
    """
    global _preflight_ok
    if _preflight_ok is not None:
        return _preflight_ok

    from deerflow.compliance.preflight import run_preflight

    result = run_preflight()
    if result.ok:
        logger.info("compliance: %s", result.describe())
        _preflight_ok = True
        return True

    logger.critical(
        "compliance: %s\ncompliance gates will NOT be mounted — traffic is UNPROTECTED. "
        "Most likely cause: `models/compliance/` or `config/compliance/` is not present "
        "(model weights are gitignored; run `make compliance-assets`, and in Docker check "
        "that both directories are mounted into the container).",
        result.describe(),
    )
    _preflight_ok = False
    if getattr(config, "strict_startup", False):
        raise ComplianceStartupError(f"compliance strict_startup is set and the preflight failed: {result.describe()}")
    return False


def reset_compliance_preflight() -> None:
    """Clear the cached preflight verdict (tests, config reload)."""
    global _preflight_ok
    _preflight_ok = None


def build_compliance_input_gate_middlewares(*, model_name: str | None = None) -> list[AgentMiddleware]:
    """Input gate middleware, mounted after IntentRecognitionMiddleware.

    Separate from the flow gates because ``before_agent`` runs in forward list
    order: the input gate has to come *after* intent recognition to combine
    "sensitive entity" with "high-risk intent" (guide §9.7).
    """
    try:
        from deerflow.config.compliance_config import get_compliance_config

        config = get_compliance_config()
        if not config.enabled or not config.gates.input.enabled:
            return []

        from deerflow.agents.middlewares.compliance_input_gate_middleware import ComplianceInputGateMiddleware

        return [ComplianceInputGateMiddleware(model_name=model_name)]
    except Exception:
        logger.exception("compliance: failed to build the input gate middleware; agent will run WITHOUT it")
        return []


def build_lead_runtime_middlewares(*, lazy_init: bool = True, model_name: str | None = None) -> list[AgentMiddleware]:
    """Middlewares shared by lead agent runtime before lead-only middlewares."""
    return _build_runtime_middlewares(
        include_uploads=True,
        include_dangling_tool_call_patch=True,
        lazy_init=lazy_init,
        model_name=model_name,
    )


def build_subagent_runtime_middlewares(*, lazy_init: bool = True, model_name: str | None = None) -> list[AgentMiddleware]:
    """Middlewares shared by subagent runtime before subagent-only middlewares."""
    return _build_runtime_middlewares(
        include_uploads=False,
        include_dangling_tool_call_patch=False,
        lazy_init=lazy_init,
        model_name=model_name,
    )
