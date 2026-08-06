"""Tests for local JSONL run-history middleware."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from langchain_core.messages import AIMessage, HumanMessage

from deerflow.agents.middlewares.run_history_middleware import RunHistoryMiddleware


class TestRunHistoryMiddleware:
    def test_after_agent_writes_thread_jsonl_when_enabled(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DEERFLOW_RUN_EVENT_LOG_ENABLED", "1")
        monkeypatch.setenv("DEERFLOW_RUN_EVENT_LOG_DIR", str(tmp_path))

        middleware = RunHistoryMiddleware()
        runtime = MagicMock()
        runtime.context = {
            "thread_id": "thread-1",
            "agent_name": "default",
            "thinking_enabled": True,
        }
        state = {
            "messages": [
                HumanMessage(content="帮我分析这个问题"),
                AIMessage(content="这是最终答案"),
            ],
            "artifacts": ["/mnt/user-data/outputs/report.md"],
            "title": "分析报告",
        }

        assert middleware.after_agent(state, runtime) is None

        log_path = tmp_path / "thread-1.jsonl"
        assert log_path.exists()
        records = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
        assert records[-1]["event"] == "agent.run.final"
        assert records[-1]["payload"]["response_text"] == "这是最终答案"
        assert records[-1]["payload"]["artifacts"] == ["/mnt/user-data/outputs/report.md"]
        assert records[-1]["payload"]["context"]["agent_name"] == "default"

    def test_after_agent_skips_when_thread_id_missing(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DEERFLOW_RUN_EVENT_LOG_ENABLED", "1")
        monkeypatch.setenv("DEERFLOW_RUN_EVENT_LOG_DIR", str(tmp_path))

        middleware = RunHistoryMiddleware()
        runtime = MagicMock()
        runtime.context = {}

        assert middleware.after_agent({"messages": [HumanMessage(content="Q")]}, runtime) is None
        assert not list(tmp_path.glob("*.jsonl"))