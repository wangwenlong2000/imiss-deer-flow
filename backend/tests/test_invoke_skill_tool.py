import json
import importlib
from pathlib import Path
from types import SimpleNamespace

invoke_module = importlib.import_module("deerflow.tools.builtins.invoke_skill_tool")


class FakeSkill:
    name = "network-traffic-analysis"
    category = "custom"
    relative_path = Path("network-traffic-analysis")
    skill_path = "network-traffic-analysis"
    skill_dir = Path("/tmp/skills/custom/network-traffic-analysis")
    skill_file = Path("/tmp/skills/custom/network-traffic-analysis/SKILL.md")

    def get_container_file_path(self, container_base_path="/mnt/skills"):
        return f"{container_base_path}/custom/network-traffic-analysis/SKILL.md"


def _runtime():
    return SimpleNamespace(
        state={"routing_context": {"allowed_skills": ["network-traffic-analysis"]}, "messages": []},
        context={"thread_id": "thread-1"},
    )


def test_invoke_skill_prepare_builds_legacy_invocation(monkeypatch):
    monkeypatch.setattr(invoke_module, "_skill_lookup", lambda: {"network-traffic-analysis": FakeSkill()})

    response = json.loads(
        invoke_module.invoke_skill_tool.func(
            runtime=_runtime(),
            skill_name="network-traffic-analysis",
            tool_call_id="tool-1",
            input_envelope={
                "query": {"raw": "分析 pcap"},
                "inputs": {"files": [{"path": "/mnt/user-data/uploads/a.pcap", "input_type": "pcap"}]},
                "budget": {"max_evidence_count": 3, "max_token_estimate": 1000},
            },
        )
    )

    assert response["status"] == "prepared"
    assert response["input_envelope"]["routing"]["selected_skill"] == "network-traffic-analysis"
    assert response["legacy_invocation"]["container_skill_file"] == "/mnt/skills/custom/network-traffic-analysis/SKILL.md"
    assert response["legacy_invocation"]["selected_data_sources"] == ["/mnt/user-data/uploads/a.pcap"]


def test_invoke_skill_wrap_output_returns_skill_result(monkeypatch):
    monkeypatch.setattr(invoke_module, "_skill_lookup", lambda: {"network-traffic-analysis": FakeSkill()})

    response = json.loads(
        invoke_module.invoke_skill_tool.func(
            runtime=_runtime(),
            skill_name="network-traffic-analysis",
            tool_call_id="tool-2",
            mode="wrap_output",
            input_envelope={
                "request_id": "req-1",
                "scenario": "network_traffic",
                "capability": "pcap_analysis",
                "query": {"raw": "分析 pcap"},
                "inputs": {"files": [{"path": "/mnt/user-data/uploads/a.pcap"}]},
            },
            legacy_output="分析完成，未发现高危异常。",
            legacy_artifacts=["/mnt/user-data/outputs/report.md"],
        )
    )

    skill_result = response["skill_result"]
    assert response["status"] == "wrapped"
    assert skill_result["schema_version"] == "1.0"
    assert skill_result["request_id"] == "req-1"
    assert skill_result["result"]["display_text"] == "分析完成，未发现高危异常。"
    assert skill_result["result"]["artifacts"][0]["uri"] == "/mnt/user-data/outputs/report.md"
