from __future__ import annotations

import networkx as nx

from deerflow.pipeline import CloudLLMReasoner


def test_serialize_for_llm_keeps_only_types_and_edges() -> None:
    graph = nx.DiGraph()
    graph.add_node("camera.open", type="Camera", source_code="should not leak")
    graph.add_node("model.predict", type="Detection", params={"large": "object"})
    graph.add_edge("camera.open", "model.predict", payload="ignored")

    serialized = CloudLLMReasoner()._serialize_for_llm(graph)

    assert serialized == "NodeTypes:\nCamera\nDetection\nEdges:\nCamera -> Detection"
    assert "source_code" not in serialized
    assert "payload" not in serialized


def test_reason_diagnoses_video_pipeline_without_cloud_api_key() -> None:
    graph = nx.DiGraph()
    graph.add_edge("Camera", "Detection")

    result = CloudLLMReasoner().reason(graph)

    assert result == {
        "pipeline_type": "视频目标检测流水线",
        "missing": ["Decode", "Preprocess", "Tracking", "Storage"],
        "anomalies": ["逻辑断层: Camera 与 Detection 之间缺少 Decode 解码模块"],
    }


def test_reason_detects_reverse_dependency_and_storage_gap() -> None:
    graph = nx.DiGraph()
    graph.add_edge("Storage", "Detection")

    result = CloudLLMReasoner().reason(graph)

    assert result["pipeline_type"] == "数据采集与存储"
    assert result["missing"] == ["Tracking"]
    assert result["anomalies"] == [
        "反向依赖: Storage 不应流向 Detection",
        "逻辑断层: Detection 与 Storage 之间没有结果落库路径",
    ]


def test_reason_detects_disconnected_camera_and_detection() -> None:
    graph = nx.DiGraph()
    graph.add_node("Camera")
    graph.add_node("Detection")

    result = CloudLLMReasoner().reason(graph)

    assert result["pipeline_type"] == "视频目标检测流水线"
    assert "逻辑断层: Camera 与 Detection 之间没有数据流转路径" in result["anomalies"]
