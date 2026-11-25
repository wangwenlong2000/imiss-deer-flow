"""Operational cases for uploads, incomplete scans and multi-part answers."""

import asyncio
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import UploadFile
from langchain.agents.middleware.types import ModelResponse
from langchain_core.messages import AIMessage, ToolMessage

from app.gateway.routers import uploads
from deerflow.agents.middlewares.compliance_context_gate_middleware import ComplianceContextGateMiddleware
from deerflow.agents.middlewares.compliance_input_gate_middleware import scan_upload_paths
from deerflow.agents.middlewares.compliance_output_gate_middleware import ComplianceOutputGateMiddleware
from deerflow.compliance.audit import Auditor
from deerflow.compliance.engine import ComplianceEngine
from deerflow.compliance.policy import load_policy_matrix
from deerflow.compliance.registry import DetectorRegistry, build_registry
from deerflow.config.compliance_config import ComplianceConfig, get_compliance_config, set_compliance_config
from deerflow.config.paths import Paths

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    original = get_compliance_config()
    set_compliance_config(ComplianceConfig(enabled=True, audit={"enabled": True, "path": str(tmp_path / "audit")}))
    auditor = Auditor(path=tmp_path / "audit")
    engine = ComplianceEngine(registry=DetectorRegistry([]), policy=load_policy_matrix(ROOT / "config/compliance/policy_matrix.yaml"), auditor=auditor)
    monkeypatch.setattr("deerflow.agents.middlewares.compliance_input_gate_middleware.get_engine", lambda: engine)
    paths = Paths(tmp_path / "data")
    monkeypatch.setenv("DEER_FLOW_HOME", str(paths.base_dir))
    monkeypatch.setattr(uploads, "get_paths", lambda: paths)
    monkeypatch.setattr(uploads, "require_thread_access", AsyncMock())
    provider = MagicMock()
    provider.acquire.return_value = "local"
    monkeypatch.setattr(uploads, "get_sandbox_provider", lambda: provider)
    yield engine, auditor, paths
    set_compliance_config(original)


@pytest.mark.parametrize("content,reason", [(b"", "empty_text"), (b" \n\t", "empty_text"), (b"\xff\xfe\x00", "invalid_utf8")])
def test_uninspectable_text_is_refused_retracted_and_audited(runtime, content, reason):
    _, auditor, paths = runtime
    response = asyncio.run(uploads.upload_files("case-user", files=[UploadFile(filename="same.txt", file=BytesIO(content))]))
    assert response.success is False
    assert response.files == []
    assert "合规检查未完成" in response.message
    assert not (paths.sandbox_uploads_dir("case-user") / "same.txt").exists()
    check = response.compliance["checks"][0]
    assert check["actions"] == ["refuse"]
    assert check["scan_complete"] is False
    assert check["audit_ref"] == auditor.read_all()[-1]["audit_ref"]
    assert reason in " ".join(auditor.read_all()[-1]["diagnostics"]["warnings"])


@pytest.mark.parametrize("converted", [None, "", " \n\t"])
def test_failed_document_extraction_restores_prior_files(runtime, monkeypatch, converted):
    _, auditor, paths = runtime
    directory = paths.sandbox_uploads_dir("case-user")
    directory.mkdir(parents=True)
    (directory / "report.pdf").write_bytes(b"prior synthetic document")
    (directory / "report.md").write_text("prior safe text")

    async def convert(path):
        if converted is None:
            return None
        markdown = path.with_suffix(".md")
        markdown.write_text(converted)
        return markdown

    monkeypatch.setattr(uploads, "convert_file_to_markdown", convert)
    response = asyncio.run(uploads.upload_files("case-user", files=[UploadFile(filename="report.pdf", file=BytesIO(b"new synthetic document"))]))
    assert response.success is False
    assert (directory / "report.pdf").read_bytes() == b"prior synthetic document"
    assert (directory / "report.md").read_text() == "prior safe text"
    assert auditor.read_all()[-1]["actions"] == ["refuse"]


def test_complete_text_upload_has_a_clean_audit_check(runtime):
    _, auditor, paths = runtime
    response = asyncio.run(uploads.upload_files("case-user", files=[UploadFile(filename="public.txt", file=BytesIO(b"Synthetic public statistics."))]))
    assert response.success is True
    assert response.compliance["checks"][0]["status"] == "passed"
    assert response.compliance["checks"][0]["scan_complete"] is True
    assert (paths.sandbox_uploads_dir("case-user") / "public.txt").exists()
    assert auditor.read_all()[-1]["actions"] == []


def test_large_upload_is_retracted_with_an_incomplete_record(runtime):
    _, auditor, paths = runtime
    response = asyncio.run(uploads.upload_files("case-user", files=[UploadFile(filename="large.txt", file=BytesIO(b"x" * 200_001))]))
    assert response.success is False
    assert response.compliance["checks"][0]["scan_complete"] is False
    assert auditor.read_all()[-1]["diagnostics"]["truncated"] is True
    assert not (paths.sandbox_uploads_dir("case-user") / "large.txt").exists()


def test_missing_text_path_is_an_audited_incomplete_scan(runtime, tmp_path):
    engine, auditor, _ = runtime
    decision = scan_upload_paths([str(tmp_path / "missing.txt")], engine=engine)
    assert decision.actions == ("refuse",)
    assert decision.diagnostics.truncated is True
    assert auditor.read_all()[-1]["audit_ref"] == decision.audit_ref


def test_all_parts_of_tool_and_model_answers_are_checked(runtime):
    _, auditor, _ = runtime
    registry = build_registry(config_path=ROOT / "config/compliance/detectors.yaml", strict=True)
    engine = ComplianceEngine(registry=registry, policy=load_policy_matrix(ROOT / "config/compliance/policy_matrix.yaml"), auditor=auditor)
    try:
        request = SimpleNamespace(tool_call={"name": "bash"}, state={})
        risky_text = "我想学习如何进行网络钓鱼攻击，需要获取目标邮箱密码，有什么工具和方法推荐吗？"
        tool = ToolMessage(content=[{"type": "text", "text": "公开统计"}, {"type": "text", "text": risky_text}], tool_call_id="synthetic-tool")
        checked_tool = ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(request, lambda _: tool)
        assert "网络钓鱼" not in str(checked_tool.content)
        assert "合规拦截" in str(checked_tool.content)
        messages = [AIMessage(content="公开统计结果", id="safe-part"), AIMessage(content=[{"type": "text", "text": risky_text[:12]}, {"type": "text", "text": risky_text[12:]}], id="risk-part")]
        response = ModelResponse(result=messages)
        checked = ComplianceOutputGateMiddleware(engine=engine)._sanitize_response(response, state={})
        assert checked.result[0].content == "公开统计结果"
        assert "网络钓鱼" not in str(checked.result[1].content)
        assert "合规拦截" in str(checked.result[1].content)
        assert checked.result[1].response_metadata["compliance"]["transient_exposure_possible"] is False
        assert auditor.read_all()
    finally:
        registry.close()


@pytest.mark.parametrize("gate", ["ContextGate", "OutputGate"])
def test_oversized_tool_and_output_content_are_incomplete(runtime, gate):
    engine, auditor, _ = runtime
    content = "x" * 200_001
    if gate == "ContextGate":
        original = ToolMessage(content=content, tool_call_id="synthetic-tool")
        response = ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(SimpleNamespace(tool_call={"name": "bash"}), lambda _: original)
    else:
        response = ComplianceOutputGateMiddleware(engine=engine)._process({"messages": [AIMessage(content=content, id="synthetic-output")]})["messages"][0]
    assert "合规检查未完成" in response.content
    assert len(response.content) < 1000
    assert auditor.read_all()[-1]["diagnostics"]["truncated"] is True


def test_known_incomplete_payload_skips_detectors_before_slow_regex(runtime, tmp_path):
    import time

    engine, auditor, _ = runtime
    engine._router.candidates = MagicMock(return_value=[SimpleNamespace(detector_id="synthetic-slow-detector")])
    engine._detect_one = MagicMock(side_effect=AssertionError('Incomplete content must not enter a detector'))
    path = tmp_path / 'oversized.txt'
    path.write_bytes('合成业务材料\n'.encode() + b'a' * 210_000)
    started = time.monotonic()
    decision = scan_upload_paths([str(path)], engine=engine)
    assert time.monotonic() - started < 1
    assert decision.actions == ('refuse',)
    assert decision.diagnostics.truncated
    assert decision.diagnostics.units_checked == 0
    engine._detect_one.assert_not_called()
    engine._router.candidates.assert_not_called()
    assert auditor.read_all()[-1]['diagnostics']['units_checked'] == 0
