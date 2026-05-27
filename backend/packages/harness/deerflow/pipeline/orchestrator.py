"""Orchestrate code-to-business-DAG pipeline steps."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, TypedDict, runtime_checkable

from deerflow.pipeline.graph_builder import PipelineGraphBuilder, PipelineJson


class ExtractionResult(TypedDict, total=False):
    calls: list[Any]
    call_graph: list[Any]
    dataflow: list[Any]
    data_flow: list[Any]


class OrchestratorResult(TypedDict):
    chunks: list[Any]
    calls: list[Any]
    dataflow: list[Any]
    labeled_nodes: list[Mapping[str, Any]]
    pipeline: dict[str, list[str] | list[list[str]]]


@runtime_checkable
class CodeSplitterLike(Protocol):
    """Adapter protocol for the code-splitter-adapter skill."""

    def split(self, raw_code: str) -> list[Any]: ...


@runtime_checkable
class ASTExtractorLike(Protocol):
    """Adapter protocol for the code-to-ast-new and dataflow-extractor skills."""

    def extract(self, chunks: list[Any]) -> Mapping[str, Any]: ...


@runtime_checkable
class SemanticLabelerLike(Protocol):
    """Adapter protocol for the code-semantic-labeler skill."""

    def label(self, *args: Any, **kwargs: Any) -> list[Mapping[str, Any]]: ...


class DeerFlowOrchestrator:
    """Run DeerFlow Agentic RAG code analysis from raw code to business DAG."""

    def __init__(
        self,
        *,
        code_splitter: CodeSplitterLike,
        ast_extractor: ASTExtractorLike,
        semantic_labeler: SemanticLabelerLike,
        graph_builder: PipelineGraphBuilder | None = None,
    ) -> None:
        self.code_splitter = code_splitter
        self.ast_extractor = ast_extractor
        self.semantic_labeler = semantic_labeler
        self.graph_builder = graph_builder or PipelineGraphBuilder()

    def process_raw_code(self, raw_code: str) -> OrchestratorResult:
        """Execute Step 1-4 and return intermediate outputs plus final pipeline."""

        # Step 1: CodeSplitter 负责将原始代码切分为前置技能可处理的代码块。
        chunks = self.code_splitter.split(raw_code)

        # Step 2: ASTExtractor 汇总 code-to-ast-new 与 dataflow-extractor 的结构化输出。
        extraction = self.ast_extractor.extract(chunks)
        calls = self._as_list(extraction.get("calls", extraction.get("call_graph", [])))
        dataflow = self._as_list(extraction.get("dataflow", extraction.get("data_flow", [])))

        # Step 3: SemanticLabelingPipeline 将代码节点映射成业务语义 Type。
        labeled_nodes = self._run_semantic_labeler(chunks, calls, dataflow)

        # Step 4: PipelineGraphBuilder 将 Call Graph、Dataflow 和 Semantic Labels 融合成 DAG。
        pipeline_json: PipelineJson = self.graph_builder.build_dag(calls, dataflow, labeled_nodes)

        return {
            "chunks": chunks,
            "calls": calls,
            "dataflow": dataflow,
            "labeled_nodes": labeled_nodes,
            "pipeline": pipeline_json["pipeline"],
        }

    def _run_semantic_labeler(
        self,
        chunks: list[Any],
        calls: list[Any],
        dataflow: list[Any],
    ) -> list[Mapping[str, Any]]:
        try:
            labels = self.semantic_labeler.label(chunks=chunks, calls=calls, dataflow=dataflow)
        except TypeError:
            labels = self.semantic_labeler.label(chunks)
        return [label for label in labels if isinstance(label, Mapping)]

    def _as_list(self, value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, Sequence) and not isinstance(value, str | bytes):
            return list(value)
        return []
