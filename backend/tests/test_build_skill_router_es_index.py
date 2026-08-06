"""Tests for SkillRouter ES indexing document construction."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "scripts"))

from build_skill_router_es_index import _build_embedding_text, _build_es_document


def _card() -> dict:
    return {
        "identity": {
            "id": "policy-report",
            "name": "policy-report",
            "description": "Analyze policy reports and governance ledgers.",
        },
        "scope": {
            "scenes": ["policy_regulation"],
            "is_public": False,
            "task_types": ["policy_analysis"],
            "input_types": ["xlsx", "text"],
            "output_types": ["policy_analysis_report"],
        },
        "routing": {
            "routing_text": "Full routing text. 不适合使用：商标注册。",
            "positive_triggers": ["分析政策台账"],
            "negative_triggers": ["商标注册", "专利侵权"],
            "keywords": ["政策", "台账"],
            "anti_keywords": ["商标", "专利"],
        },
        "execution": {
            "can_run_standalone": True,
            "can_compose_with": ["data-analysis"],
            "required_tools": ["read_file"],
            "optional_tools": ["write_file"],
        },
        "routing_policy": {
            "priority": 90,
            "prefer_when": ["需要政策执行分析"],
            "defer_when": ["只需要商标注册咨询"],
        },
        "source": {"generated_at": "2026-01-01T00:00:00+00:00"},
        "embedding": {"model": "test-model", "es_index": "test-index"},
    }


def test_embedding_text_is_positive_only():
    text = _build_embedding_text(_card())

    assert "分析政策台账" in text
    assert "政策" in text
    assert "data-analysis" in text
    assert "商标注册" not in text
    assert "专利侵权" not in text
    assert "只需要商标注册咨询" not in text


def test_es_document_keeps_boundaries_outside_embedding_text():
    doc = _build_es_document(_card(), [0.1, 0.2])

    assert doc["embedding_text"] == _build_embedding_text(_card())
    assert doc["embedding_vector"] == [0.1, 0.2]
    assert doc["negative_triggers"] == ["商标注册", "专利侵权"]
    assert doc["anti_keywords"] == ["商标", "专利"]
    assert doc["defer_when"] == ["只需要商标注册咨询"]
    assert "商标注册" not in doc["embedding_text"]
