"""Business pipeline graph construction utilities."""

from __future__ import annotations

from typing import Any

from deerflow.pipeline.graph_builder import PipelineGraphBuilder
from deerflow.pipeline.orchestrator import DeerFlowOrchestrator
from deerflow.pipeline.reasoner import CloudLLMReasoner, PipelineDiagnosis

__all__ = [
    "CloudLLMReasoner",
    "ComponentPackage",
    "ComponentPackager",
    "CodeProcessingPipeline",
    "DeerFlowOrchestrator",
    "PipelineDiagnosis",
    "PipelineGraphBuilder",
]


def __getattr__(name: str) -> Any:
    if name in {"ComponentPackage", "ComponentPackager"}:
        from deerflow.pipeline.component_packager import ComponentPackage, ComponentPackager

        return {
            "ComponentPackage": ComponentPackage,
            "ComponentPackager": ComponentPackager,
        }[name]
    if name == "CodeProcessingPipeline":
        from deerflow.pipeline.code_processing_pipeline import CodeProcessingPipeline

        return CodeProcessingPipeline
    raise AttributeError(f"module 'deerflow.pipeline' has no attribute {name!r}")
