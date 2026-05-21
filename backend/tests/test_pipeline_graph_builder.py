from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deerflow.pipeline import DeerFlowOrchestrator, PipelineGraphBuilder


def test_build_dag_matches_expected_business_pipeline() -> None:
    calls = [
        {"caller": "main", "callee": "cv2.VideoCapture"},
        {"caller": "main", "callee": "model.predict"},
    ]
    dataflow = [["frame", "model.predict"], ["results", "tracker.update"]]
    labeled_nodes = [
        {"name": "cv2.VideoCapture", "type": "Camera"},
        {"name": "model.predict", "type": "Detection"},
        {"name": "tracker.update", "type": "Tracking"},
    ]

    result = PipelineGraphBuilder().build_dag(calls, dataflow, labeled_nodes)

    assert result == {
        "pipeline": {
            "nodes": ["Camera", "Detection", "Tracking"],
            "edges": [["Camera", "Detection"], ["Detection", "Tracking"]],
        }
    }


def test_build_dag_deduplicates_edges_and_removes_self_loops() -> None:
    calls = [
        {"caller": "camera.open", "callee": "model.predict"},
        {"caller": "camera.open", "callee": "model.predict"},
        {"caller": "model.predict", "callee": "model.postprocess"},
    ]
    labeled_nodes = [
        {"name": "camera.open", "type": "Camera"},
        {"name": "model.predict", "type": "Detection"},
        {"name": "model.postprocess", "type": "Detection"},
    ]

    result = PipelineGraphBuilder().build_dag(calls, [], labeled_nodes)

    assert result["pipeline"]["nodes"] == ["Camera", "Detection"]
    assert result["pipeline"]["edges"] == [["Camera", "Detection"]]


def test_build_dag_breaks_cycles_to_export_strict_dag() -> None:
    calls = [["camera.open", "model.predict"], ["model.predict", "camera.open"]]
    labeled_nodes = [
        {"name": "camera.open", "type": "Camera"},
        {"name": "model.predict", "type": "Detection"},
    ]

    result = PipelineGraphBuilder().build_dag(calls, [], labeled_nodes)

    assert result["pipeline"]["nodes"] == ["Camera", "Detection"]
    assert result["pipeline"]["edges"] == [["Camera", "Detection"]]


def test_orchestrator_appends_pipeline_graph_builder_step() -> None:
    class FakeSplitter:
        def split(self, raw_code: str) -> list[str]:
            return [raw_code]

    class FakeExtractor:
        def extract(self, chunks: list[Any]) -> Mapping[str, Any]:
            return {
                "calls": [{"caller": "main", "callee": "cv2.VideoCapture"}, {"caller": "main", "callee": "model.predict"}],
                "dataflow": [["results", "tracker.update"]],
            }

    class FakeLabeler:
        def label(self, *args: Any, **kwargs: Any) -> list[Mapping[str, Any]]:
            return [
                {"name": "cv2.VideoCapture", "type": "Camera"},
                {"name": "model.predict", "type": "Detection"},
                {"name": "tracker.update", "type": "Tracking"},
            ]

    result = DeerFlowOrchestrator(
        code_splitter=FakeSplitter(),
        ast_extractor=FakeExtractor(),
        semantic_labeler=FakeLabeler(),
    ).process_raw_code("print('demo')")

    assert result["pipeline"] == {
        "nodes": ["Camera", "Detection", "Tracking"],
        "edges": [["Camera", "Detection"], ["Detection", "Tracking"]],
    }
