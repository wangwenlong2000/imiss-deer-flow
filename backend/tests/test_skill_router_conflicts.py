"""Tests for scripts/check_skill_router_conflicts.py.

Covers:
- Jaccard and cosine similarity calculations
- Single conflict detection dimensions
- Overlap score thresholds (ready / pending_review / conflict)
- Negative trigger gap detection
- All-skills pairwise check

Run with:
    python3 backend/tests/test_skill_router_conflicts.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from check_skill_router_conflicts import (
    check_single_conflict,
    cosine_sim,
    jaccard,
)


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

class TestJaccard:
    def test_identical_sets(self):
        assert jaccard(["a", "b"], ["b", "a"]) == 1.0

    def test_disjoint_sets(self):
        assert jaccard(["a"], ["b"]) == 0.0

    def test_partial_overlap(self):
        assert abs(jaccard(["a", "b", "c"], ["c", "d", "e"]) - 1.0 / 5.0) < 1e-9

    def test_empty_sets(self):
        assert jaccard([], []) == 0.0

    def test_one_empty_one_not(self):
        assert jaccard([], ["a"]) == 0.0


class TestCosineSim:
    def test_identical_vectors(self):
        assert abs(cosine_sim([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) - 1.0) < 1e-9

    def test_orthogonal_vectors(self):
        assert abs(cosine_sim([1.0, 0.0], [0.0, 1.0])) < 1e-9

    def test_empty_vectors(self):
        assert cosine_sim([], []) == 0.0

    def test_zero_vector(self):
        assert cosine_sim([0.0, 0.0], [1.0, 2.0]) == 0.0


# ---------------------------------------------------------------------------
# Conflict detection
# ---------------------------------------------------------------------------

def _make_card(skill_id, **overrides):
    base = {
        "identity": {"id": skill_id, "name": skill_id, "description": f"Desc for {skill_id}"},
        "scope": {
            "scenes": ["test_scene"],
            "is_public": False,
            "task_types": [],
            "input_types": [],
            "output_types": [],
        },
        "routing": {
            "routing_text": f"Desc for {skill_id}",
            "positive_triggers": [],
            "negative_triggers": [],
            "keywords": [],
            "anti_keywords": [],
        },
        "execution": {
            "required_tools": [],
            "optional_tools": [],
            "allowed_file_patterns": [],
            "can_run_standalone": True,
            "can_compose_with": [],
        },
        "routing_policy": {"priority": 50, "conflict_group": skill_id},
        "source": {"skill_dir": f"custom/{skill_id}", "skill_md_path": f"custom/{skill_id}/SKILL.md"},
        "embedding": {"model": "test", "text_hash": "sha256:aaa", "es_index": "test", "es_doc_id": skill_id},
    }

    def deep_merge(target, src):
        for k, v in src.items():
            if k in target and isinstance(target[k], dict) and isinstance(v, dict):
                deep_merge(target[k], v)
            else:
                target[k] = v

    deep_merge(base, overrides)
    return base


class TestSingleConflictNoEmbedding:
    """Test conflict detection without embedding (set-based only)."""

    def test_identical_skills_high_overlap(self):
        a = _make_card("skill-a",
            scope={"scenes": ["scene_x"], "task_types": ["task_x", "task_y"], "input_types": ["type_x"], "output_types": ["out_x"]},
            routing={"positive_triggers": ["do X analysis"], "negative_triggers": []},
            execution={"required_tools": ["read_file", "bash"]},
        )
        b = _make_card("skill-a",
            scope={"scenes": ["scene_x"], "task_types": ["task_x", "task_y"], "input_types": ["type_x"], "output_types": ["out_x"]},
            routing={"positive_triggers": ["do X analysis"], "negative_triggers": []},
            execution={"required_tools": ["read_file", "bash"]},
        )
        result = check_single_conflict(a, b)
        # scene=1, task=1, input=1, output=1, pos=1, tools=1, routing_sim=0
        # score = 0.20 + 0.25 + 0.10 + 0.10 + 0.15 + 0.05 = 0.85
        assert result["overlap_score"] >= 0.70
        assert result["existing_skill_id"] == "skill-a"

    def test_completely_different_skills(self):
        a = _make_card("skill-a", scope={"scenes": ["scene_a"], "task_types": ["task_a"], "input_types": ["type_a"]})
        b = _make_card("skill-b", scope={"scenes": ["scene_b"], "task_types": ["task_b"], "input_types": ["type_b"]})
        result = check_single_conflict(a, b)
        assert result["overlap_score"] < 0.70
        assert result["status"] == "ready"

    def test_partial_overlap_pending_review(self):
        a = _make_card("skill-a",
            scope={"scenes": ["scene_a", "shared"], "task_types": ["task_a", "shared_task"], "input_types": ["type_a"]},
        )
        b = _make_card("skill-b",
            scope={"scenes": ["scene_b", "shared"], "task_types": ["task_b", "shared_task"], "input_types": ["type_b"]},
        )
        result = check_single_conflict(a, b)
        # scene_jaccard = 1/3, task_jaccard = 1/3, input_jaccard = 0
        # score = 0.20*(1/3) + 0.25*(1/3) = 0.15 => should be ready
        assert result["status"] in ("ready", "pending_review")

    def test_negative_trigger_gap(self):
        # Target shares positive triggers with another skill but has no
        # negative triggers — that's a detection gap.
        a = _make_card("skill-a",
            scope={"scenes": ["shared"], "task_types": ["shared"]},
            routing={"positive_triggers": ["do X analysis"], "negative_triggers": []},
        )
        b = _make_card("skill-b",
            scope={"scenes": ["other"], "task_types": ["other"]},
            routing={"positive_triggers": ["do X analysis"], "negative_triggers": ["do Y"]},
        )
        result = check_single_conflict(a, b)
        assert result["detail"]["negative_triggers_gap"] is True

    def test_overlap_dimensions_reported(self):
        a = _make_card("skill-a",
            scope={"scenes": ["net"], "task_types": ["pcap"], "input_types": ["pcap"]},
        )
        b = _make_card("skill-b",
            scope={"scenes": ["net"], "task_types": ["pcap"], "input_types": ["pcap"]},
        )
        result = check_single_conflict(a, b)
        assert "scenes" in result["overlap_dimensions"]
        assert "task_types" in result["overlap_dimensions"]
        assert "input_types" in result["overlap_dimensions"]
