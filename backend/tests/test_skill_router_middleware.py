"""Tests for SkillRouterMiddleware routing components.

Covers:
- should_route skip and pass cases
- query segmentation rules
- resolver ignores public skills because they are injected separately
- pick_primary with dict and SelectedSkill inputs

Run with:
    PYTHONPATH=backend/packages/harness python3 backend/tests/test_skill_router_middleware.py
"""

from __future__ import annotations

import importlib.util
import typing
from types import SimpleNamespace

from deerflow.routing.query_segmenter import should_route, segment_query
from deerflow.routing.resolver import resolve, pick_primary
from deerflow.routing.schema import SelectedSkill

if not hasattr(typing, "Self"):
    typing.Self = typing.Any

_middleware_spec = importlib.util.spec_from_file_location(
    "_skill_router_middleware_under_test",
    "packages/harness/deerflow/agents/middlewares/skill_router_middleware.py",
)
_middleware_module = importlib.util.module_from_spec(_middleware_spec)
assert _middleware_spec.loader is not None
_middleware_spec.loader.exec_module(_middleware_module)
SkillRouterMiddleware = _middleware_module.SkillRouterMiddleware


# ---------------------------------------------------------------------------
# should_route
# ---------------------------------------------------------------------------

class TestShouldRoute:
    def test_skip_greeting(self):
        assert should_route("你好") is False

    def test_skip_are_you_there(self):
        assert should_route("在吗") is False

    def test_skip_thanks(self):
        assert should_route("谢谢") is False

    def test_skip_ok(self):
        assert should_route("ok") is False

    def test_skip_who_are_you(self):
        assert should_route("你是谁") is False

    def test_skip_introduce_yourself(self):
        assert should_route("介绍一下自己") is False

    def test_skip_empty(self):
        assert should_route("") is False

    def test_route_when_file_present(self):
        assert should_route("帮我看看", [{"filename": "traffic.pcap"}]) is True

    def test_route_when_file_ref(self):
        assert should_route("这个文件有什么问题？") is True

    def test_route_analysis_intent(self):
        assert should_route("帮我分析这个数据") is True

    def test_route_statistics_intent(self):
        assert should_route("做个统计并画图") is True

    def test_route_compliance_intent(self):
        assert should_route("判断这个台账是否合规") is True


# ---------------------------------------------------------------------------
# segment_query
# ---------------------------------------------------------------------------

class TestSegmentQuery:
    def test_pcap_segment(self):
        segs = segment_query("帮我分析这个 pcap 文件有没有异常通信")
        assert len(segs) >= 1
        scenes = {s["scene"] for s in segs}
        assert "network_traffic" in scenes

    def test_policy_segment(self):
        segs = segment_query("查一下相关法律条文并判断这个台账是否合规")
        scenes = {s["scene"] for s in segs}
        assert "policy_regulation" in scenes

    def test_generic_data_analysis_has_no_business_scene(self):
        segs = segment_query("上传 Excel，帮我做统计并画图")
        assert len(segs) == 1
        assert segs[0]["scene"] is None

    def test_unknown_segment(self):
        segs = segment_query("今天天气怎么样")
        assert len(segs) == 1
        assert segs[0]["scene"] is None

    def test_scene_hint_overrides_keyword_rules(self):
        segs = segment_query(
            "场景：交通流量。场景ID：road_traffic。用户原始问题：统计交通流量",
            scene_hint="road_traffic",
        )
        assert len(segs) == 1
        assert segs[0]["scene"] == "road_traffic"

    def test_multi_segment(self):
        segs = segment_query("帮我分析 pcap 文件异常，并查一下相关法律条文")
        scenes = {s["scene"] for s in segs}
        assert "network_traffic" in scenes
        assert "policy_regulation" in scenes

    def test_scene_hints_create_multiple_segments(self):
        segs = segment_query(
            "分析这个 pcap 文件中的异常通信，并判断这份合同的违约责任和争议解决条款风险",
            scene_hints=["network_traffic", "policy_regulation"],
        )
        assert len(segs) == 2
        assert [s["scene"] for s in segs] == ["network_traffic", "policy_regulation"]

    def test_empty_string(self):
        assert segment_query("") == []


# ---------------------------------------------------------------------------
# resolver
# ---------------------------------------------------------------------------

class TestResolve:
    def _candidate(self, skill_id, is_public=False, score=0.8, scenes=None):
        return {
            "skill_id": skill_id,
            "is_public": is_public,
            "score": score,
            "scenes": scenes or [],
        }

    def test_single_primary(self):
        result = resolve("test", [self._candidate("network-traffic-analysis", score=0.91)])
        assert len(result) == 1
        assert result[0]["id"] == "network-traffic-analysis"
        assert result[0]["role"] == "primary"

    def test_public_skills_are_ignored_by_resolver(self):
        """Public skills are injected separately, not selected by routing."""
        candidates = [
            self._candidate("data-analysis", is_public=True, score=0.85),
            self._candidate("chart-visualization", is_public=True, score=0.80),
            self._candidate("data-analysis-2", is_public=True, score=0.75),
        ]
        result = resolve("test", candidates)
        assert result == []

    def test_min_score_filter(self):
        """Skills below min_score should be excluded."""
        candidates = [
            self._candidate("good-skill", score=0.90),
            self._candidate("bad-skill", score=0.50),
        ]
        result = resolve("test", candidates)
        ids = [r["id"] for r in result]
        assert "good-skill" in ids
        assert "bad-skill" not in ids

    def test_empty_input(self):
        assert resolve("test", []) == []

    def test_scene_promotes_custom_skill(self):
        """When scene is known, non-public matching skill becomes primary."""
        candidates = [
            self._candidate("data-analysis", is_public=True, score=0.92, scenes=["public"]),
            self._candidate("find-skills", is_public=True, score=0.88, scenes=["public"]),
            self._candidate("law-regulations-rag", is_public=False, score=0.85, scenes=["policy_regulation"]),
        ]
        result = resolve("test", candidates, scene="policy_regulation")
        assert len(result) >= 1
        assert result[0]["id"] == "law-regulations-rag"
        assert result[0]["role"] == "primary"

    def test_scene_no_match_falls_back(self):
        """If no candidate matches the scene, fall back to normal scoring."""
        candidates = [
            self._candidate("data-analysis", is_public=True, score=0.90, scenes=["public"]),
        ]
        result = resolve("分析这个 Excel 表格", candidates, scene="network_traffic")
        assert result == []

    def test_scene_primary_not_duplicated(self):
        """Primary selected via scene match should not appear again."""
        candidates = [
            self._candidate("law-regulations-rag", is_public=False, score=0.95, scenes=["policy_regulation"]),
            self._candidate("data-analysis", is_public=True, score=0.90, scenes=["public"]),
        ]
        result = resolve("test", candidates, scene="policy_regulation")
        ids = [r["id"] for r in result]
        assert ids.count("law-regulations-rag") == 1

    def test_public_skills_do_not_displace_domain_skills(self):
        """High-scoring public skills are ignored by domain skill resolution."""
        candidates = [
            self._candidate("law-local-retrieval", is_public=False, score=0.93, scenes=["policy_regulation"]),
            self._candidate("find-skills", is_public=True, score=0.91, scenes=["public"]),
            self._candidate("chinese-policy-analysis", is_public=False, score=0.88, scenes=["policy_regulation"]),
            self._candidate("skill-creator", is_public=True, score=0.87, scenes=["public"]),
            self._candidate("data-analysis", is_public=True, score=0.34, scenes=["public"]),
            self._candidate("chart-visualization", is_public=True, score=0.33, scenes=["public"]),
        ]
        result = resolve("生成政策专项报告", candidates, scene="policy_regulation")
        ids = [r["id"] for r in result]

        assert ids == [
            "law-local-retrieval",
            "chinese-policy-analysis",
        ]

    def test_public_skills_are_not_global_minimum(self):
        """Public skills are not selected even when no domain support asks for them."""
        candidates = [
            self._candidate("china-legal-consultation", is_public=False, score=0.90, scenes=["policy_regulation"]),
            self._candidate("data-analysis", is_public=True, score=0.35, scenes=["public"]),
            self._candidate("chart-visualization", is_public=True, score=0.31, scenes=["public"]),
        ]
        result = resolve("咨询消防处罚程序是否合法", candidates, scene="policy_regulation")
        ids = [r["id"] for r in result]

        assert ids == ["china-legal-consultation"]

    def test_negative_router_boundaries_filter_candidates(self):
        """Cards whose anti-keywords match the query should not be selected."""
        candidates = [
            self._candidate("condition-based-screening", score=3.9, scenes=["phone_network"]),
            {
                **self._candidate("topn-high-risk-discovery", score=3.5, scenes=["phone_network"]),
                "anti_keywords": ["筛选"],
            },
        ]
        result = resolve("请筛选出夜间通话异常的重点号码", candidates, scene="phone_network")
        ids = [r["id"] for r in result]

        assert ids == ["condition-based-screening"]

    def test_supporting_skills_must_be_close_to_primary(self):
        """A broad same-scene candidate should not be selected just because it clears the floor."""
        candidates = [
            self._candidate("condition-based-screening", score=3.9, scenes=["phone_network"]),
            self._candidate("group-risk-analysis", score=2.3, scenes=["phone_network"]),
        ]
        result = resolve("请筛选出夜间通话异常的重点号码", candidates, scene="phone_network")
        ids = [r["id"] for r in result]

        assert ids == ["condition-based-screening"]

    def test_explicit_compound_task_keeps_low_scoring_supporting_skills(self):
        """Explicit subtasks should survive the supporting score ratio gate."""
        query = "分析视频中是否发生事件，若发生则截取该帧并对人脸打码脱敏，最终输出打码后的证据截图"
        candidates = [
            {
                **self._candidate("privacy-masking", score=8.0, scenes=["video_surveillance"]),
                "positive_triggers": ["人脸打码脱敏"],
                "keywords": ["打码后的证据截图"],
            },
            {
                **self._candidate("analyze-video", score=1.7, scenes=["video_surveillance"]),
                "keywords": ["是否发生事件"],
            },
            {
                **self._candidate("evidence-snapshot", score=-0.1, scenes=["video_surveillance"]),
                "positive_triggers": ["若发生则截取该帧"],
                "keywords": ["证据截图"],
            },
            {
                **self._candidate("object-detection", score=5.0, scenes=["video_surveillance"]),
                "anti_keywords": ["证据截图"],
            },
        ]

        result = resolve(query, candidates, scene="video_surveillance")
        ids = [r["id"] for r in result]

        assert ids == ["privacy-masking", "analyze-video", "evidence-snapshot"]


class TestPickPrimary:
    def test_returns_primary(self):
        skills = [
            SelectedSkill(id="a", role="supporting", score=0.9),
            SelectedSkill(id="b", role="primary", score=0.8),
        ]
        result = pick_primary(skills)
        assert result is not None
        assert result["id"] == "b"

    def test_fallback_to_highest_score(self):
        skills = [
            SelectedSkill(id="a", role="supporting", score=0.9),
            SelectedSkill(id="b", role="supporting", score=0.6),
        ]
        result = pick_primary(skills)
        assert result is not None
        assert result["id"] == "a"

    def test_empty_returns_none(self):
        assert pick_primary([]) is None


# ---------------------------------------------------------------------------
# SkillRouterMiddleware custom candidate filtering
# ---------------------------------------------------------------------------

class _FakeElasticStore:
    def __init__(self, candidates):
        self.candidates = candidates
        self.calls = []

    def search(self, **kwargs):
        self.calls.append(kwargs)
        return self.candidates


class TestSkillRouterCandidateFiltering:
    def _middleware(self, candidates, top_k=8):
        middleware = object.__new__(SkillRouterMiddleware)
        middleware.es_store = _FakeElasticStore(candidates)
        middleware.top_k = top_k
        return middleware

    def test_embedding_query_includes_scene_when_available(self):
        query = SkillRouterMiddleware._build_embedding_query(
            "分析这个 pcap 文件中的异常通信",
            "network_traffic",
        )

        assert query == "Scene: network_traffic\nTask: 分析这个 pcap 文件中的异常通信"

    def test_embedding_query_uses_task_only_without_scene(self):
        query = SkillRouterMiddleware._build_embedding_query("  做个统计并画图  ", None)

        assert query == "做个统计并画图"

    def test_scene_search_filters_to_custom_scene_candidates(self):
        middleware = self._middleware([
            {"skill_id": "network-traffic-analysis", "is_public": False},
            {"skill_id": "data-analysis", "is_public": True},
        ])

        candidates = middleware._search_candidates_for_segment(
            query_vec=[0.1, 0.2],
            scene="network_traffic",
            filters={"enabled": True},
        )

        assert [c["skill_id"] for c in candidates] == ["network-traffic-analysis"]
        assert middleware.es_store.calls[0]["filters"] == {
            "enabled": True,
            "is_public": False,
            "scenes": "network_traffic",
        }

    def test_global_search_filters_to_custom_candidates(self):
        middleware = self._middleware([
            {"skill_id": "custom-report", "is_public": False},
            {"skill_id": "find-skills", "is_public": True},
        ])

        candidates = middleware._search_candidates_for_segment(
            query_vec=[0.1, 0.2],
            scene=None,
            filters={"enabled": True},
        )

        assert [c["skill_id"] for c in candidates] == ["custom-report"]
        assert middleware.es_store.calls[0]["filters"] == {
            "enabled": True,
            "is_public": False,
        }

    def test_scene_rerank_uses_scene_candidates_without_embedding(self, tmp_path, monkeypatch):
        skill_dir = tmp_path / "program-snippet-skill"
        skill_dir.mkdir()
        (skill_dir / "router_card.json").write_text(
            """
            {
              "identity": {"id": "code-to-ast-new", "name": "code-to-ast-new", "description": "AST parser"},
              "scope": {"scenes": ["program_snippet"], "task_types": ["ast_generation"], "input_types": ["source_code"], "output_types": ["ast_json"], "is_public": true},
              "routing": {"routing_text": "AST parser", "positive_triggers": ["AST"], "negative_triggers": [], "keywords": ["AST"], "anti_keywords": []},
              "execution": {"required_tools": ["python"], "optional_tools": [], "can_compose_with": []}
            }
            """,
            encoding="utf-8",
        )
        fake_skill = SimpleNamespace(name="code-to-ast-new", description="AST parser", skill_dir=skill_dir)
        monkeypatch.setattr(_middleware_module, "load_custom_skills", lambda enabled_only=True: [fake_skill])

        class FailingEmbeddingClient:
            def embed_text(self, _text):
                raise AssertionError("embedding should not be called in scene_rerank mode")

        middleware = object.__new__(SkillRouterMiddleware)
        middleware.mode = "scene_rerank"
        middleware.scene_prefilter_enabled = True
        middleware.fallback_global_when_no_scene = True
        middleware.embedding_client = FailingEmbeddingClient()

        candidates = middleware._candidate_skills_for_segment(
            segment_text="把代码转成 AST",
            scene="program_snippet",
            base_scope_set=set(),
        )

        assert [candidate["skill_id"] for candidate in candidates] == ["code-to-ast-new"]
        assert candidates[0]["required_tools"] == ["python"]

    def test_scene_rerank_respects_base_scope(self, tmp_path, monkeypatch):
        skill_dir = tmp_path / "program-snippet-skill"
        skill_dir.mkdir()
        (skill_dir / "router_card.json").write_text(
            '{"identity":{"id":"code-to-ast-new","name":"code-to-ast-new","description":"AST parser"},"scope":{"scenes":["program_snippet"]}}',
            encoding="utf-8",
        )
        fake_skill = SimpleNamespace(name="code-to-ast-new", description="AST parser", skill_dir=skill_dir)
        monkeypatch.setattr(_middleware_module, "load_custom_skills", lambda enabled_only=True: [fake_skill])

        middleware = object.__new__(SkillRouterMiddleware)
        middleware.mode = "scene_rerank"
        middleware.scene_prefilter_enabled = True
        middleware.fallback_global_when_no_scene = True

        candidates = middleware._candidate_skills_for_segment(
            segment_text="把代码转成 AST",
            scene="program_snippet",
            base_scope_set={"other-skill"},
        )

        assert candidates == []

    def test_scene_rerank_can_disable_global_fallback_without_scene(self):
        middleware = object.__new__(SkillRouterMiddleware)
        middleware.mode = "scene_rerank"
        middleware.scene_prefilter_enabled = True
        middleware.fallback_global_when_no_scene = False

        candidates = middleware._candidate_skills_for_segment(
            segment_text="分析代码",
            scene=None,
            base_scope_set=set(),
        )

        assert candidates == []


# ---------------------------------------------------------------------------
# Acceptance criteria from Engineer D spec
# ---------------------------------------------------------------------------

class TestAcceptanceCriteria:
    """Verify the three acceptance examples from the work package."""

    def test_pcap_query(self):
        """帮我分析这个 pcap 文件有没有异常通信 → network-traffic-analysis"""
        # Verify should_route
        assert should_route("帮我分析这个 pcap 文件有没有异常通信") is True
        # Verify segmentation
        segs = segment_query("帮我分析这个 pcap 文件有没有异常通信")
        assert any(s["scene"] == "network_traffic" for s in segs)

    def test_law_query(self):
        """查一下相关法律条文并判断这个台账是否合规 → law-regulations-rag"""
        assert should_route("查一下相关法律条文并判断这个台账是否合规") is True
        segs = segment_query("查一下相关法律条文并判断这个台账是否合规")
        assert any(s["scene"] == "policy_regulation" for s in segs)

    def test_greeting_skipped(self):
        """你好 → trigger=false"""
        assert should_route("你好") is False


if __name__ == "__main__":
    import sys
    passed = 0
    failed = 0

    def _run(cls):
        global passed, failed
        for name in dir(cls):
            if name.startswith("test_"):
                try:
                    getattr(cls(), name)()
                    passed += 1
                    print(f"  PASS {name}")
                except Exception as e:
                    failed += 1
                    print(f"  FAIL {name}: {e}")

    for test_cls in [TestShouldRoute, TestSegmentQuery, TestResolve, TestPickPrimary, TestAcceptanceCriteria]:
        print(f"{test_cls.__name__}:")
        _run(test_cls)

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
