"""Tests for SkillRouterMiddleware routing components.

Covers:
- should_route skip and pass cases
- query segmentation rules
- resolver public-skill cap and scoring
- pick_primary with dict and SelectedSkill inputs

Run with:
    PYTHONPATH=backend/packages/harness python3 backend/tests/test_skill_router_middleware.py
"""

from __future__ import annotations

from deerflow.routing.query_segmenter import should_route, segment_query
from deerflow.routing.resolver import resolve, pick_primary
from deerflow.routing.schema import RoutingContext, SceneTask, SelectedSkill


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

    def test_data_analysis_segment(self):
        segs = segment_query("上传 Excel，帮我做统计并画图")
        scenes = {s["scene"] for s in segs}
        assert "data_analysis" in scenes

    def test_unknown_segment(self):
        segs = segment_query("今天天气怎么样")
        assert len(segs) == 1
        assert segs[0]["scene"] is None

    def test_multi_segment(self):
        segs = segment_query("帮我分析 pcap 文件异常，并查一下相关法律条文")
        scenes = {s["scene"] for s in segs}
        assert "network_traffic" in scenes
        assert "policy_regulation" in scenes

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

    def test_public_skill_cap(self):
        """At most 2 public skills should be selected."""
        candidates = [
            self._candidate("data-analysis", is_public=True, score=0.85),
            self._candidate("chart-visualization", is_public=True, score=0.80),
            self._candidate("data-analysis-2", is_public=True, score=0.75),
        ]
        result = resolve("test", candidates)
        pub_in_result = 0
        for r in result:
            for c in candidates:
                if r["id"] == c["skill_id"] and c["is_public"]:
                    pub_in_result += 1
        assert pub_in_result <= 2

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
        result = resolve("test", candidates, scene="network_traffic")
        assert len(result) >= 1
        assert result[0]["id"] == "data-analysis"
        assert result[0]["role"] == "primary"

    def test_scene_primary_not_duplicated(self):
        """Primary selected via scene match should not appear again."""
        candidates = [
            self._candidate("law-regulations-rag", is_public=False, score=0.95, scenes=["policy_regulation"]),
            self._candidate("data-analysis", is_public=True, score=0.90, scenes=["public"]),
        ]
        result = resolve("test", candidates, scene="policy_regulation")
        ids = [r["id"] for r in result]
        assert ids.count("law-regulations-rag") == 1


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
