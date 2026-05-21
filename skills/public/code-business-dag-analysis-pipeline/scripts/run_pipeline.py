"""Run the full code-to-business-DAG analysis pipeline for Python code.

This script wires together these skills in order:
code-to-ast-new -> code-splitter-adapter -> dataflow-extractor ->
code-semantic-labeler -> pipeline-graph-builder -> pipeline-reasoner ->
standalone Markdown analysis report.
"""

from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import networkx as nx


STEP_ORDER = [
    "code-to-ast-new",
    "code-splitter-adapter",
    "dataflow-extractor",
    "code-semantic-labeler",
    "pipeline-graph-builder",
    "pipeline-reasoner",
    "analysis-report",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _skill_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _step(name: str, status: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "status": status, **extra}


def _error_result(
    *,
    failed_step: str,
    error: str,
    steps: list[dict[str, Any]],
    language: str = "python",
    partial: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "status": "error",
        "failed_step": failed_step,
        "error": error,
        "language": language,
        "steps": steps,
        "chunks": [],
        "calls": [],
        "dataflow": [],
        "labeled_nodes": [],
        "pipeline": {"nodes": [], "edges": []},
        "diagnosis": {
            "pipeline_type": "unknown",
            "missing": [],
            "anomalies": ["pipeline did not finish; full business diagnosis is unavailable"],
        },
        "report_path": "",
        "report_format": "markdown",
    }
    if partial:
        payload.update(partial)
    return payload


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _call_name(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    if isinstance(node, ast.Call):
        return _call_name(node.func)
    return ""


def _extract_calls_and_nodes(raw_code: str) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    tree = ast.parse(raw_code)
    calls: list[dict[str, str]] = []
    nodes: list[dict[str, Any]] = []

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.scope: list[str] = ["module"]

        def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
            self.scope.append(node.name)
            self.generic_visit(node)
            self.scope.pop()

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
            self.visit_FunctionDef(node)

        def visit_Call(self, node: ast.Call) -> Any:
            name = _call_name(node.func)
            if name:
                caller = self.scope[-1]
                calls.append({"caller": caller, "callee": name})
                nodes.append(
                    {
                        "name": name,
                        "node_type": "Call",
                        "parent_name": caller,
                        "line_number": getattr(node, "lineno", 0),
                    }
                )
            self.generic_visit(node)

    Visitor().visit(tree)

    preferred_entrypoints = ("run_pipeline", "main", "process", "run", "pipeline")
    callers = {call["caller"] for call in calls}
    entrypoint = next((name for name in preferred_entrypoints if name in callers), None)
    if entrypoint:
        calls = [call for call in calls if call["caller"] == entrypoint]
        call_nodes = [node for node in nodes if node.get("parent_name") == entrypoint]
        return calls, call_nodes

    return calls, nodes


def _run_code_to_ast(raw_code: str, skills_dir: Path) -> dict[str, Any]:
    module = _load_module("code_to_ast_convert", skills_dir / "code-to-ast-new" / "scripts" / "convert.py")
    result = module.parse_python(raw_code)
    if isinstance(result, dict) and "error" in result:
        raise ValueError(str(result["error"]))
    return result


def _run_code_splitter(raw_code: str, skills_dir: Path) -> list[Any]:
    module = _load_module("code_splitter_adapter", skills_dir / "code-splitter-adapter" / "code_splitter.py")
    try:
        splitter = module.get_code_splitter(strategy="langchain", chunk_size=1000, chunk_overlap=120)
        return splitter.split_code(code=raw_code, language="python", metadata={"language": "python"})
    except Exception as exc:
        return [
            {
                "content": raw_code,
                "metadata": {
                    "language": "python",
                    "chunk_index": 0,
                    "fallback_reason": str(exc),
                },
            }
        ]


def _run_dataflow(raw_code: str, skills_dir: Path) -> list[Any]:
    module = _load_module("dataflow_extractor", skills_dir / "dataflow-extractor" / "dataflow_extractor.py")
    result = module.ASTDataflowExtractor().extract(raw_code)
    return result.get("dataflow", []) if isinstance(result, dict) else []


def _run_semantic_labeler(
    *,
    raw_code: str,
    ast_result: dict[str, Any],
    call_nodes: list[dict[str, Any]],
    skills_dir: Path,
) -> list[dict[str, Any]]:
    module = _load_module("semantic_labeler", skills_dir / "code-semantic-labeler" / "semantic_labeler.py")
    rules_path = skills_dir / "code-semantic-labeler" / "ontology_rules.json"
    rules = json.loads(rules_path.read_text(encoding="utf-8")) if rules_path.exists() else {}
    rules.update(
        {
            "cv2.VideoCapture": "Camera",
            "VideoCapture": "Camera",
            "capture.read": "Camera",
            "detector.predict": "Detection",
            "model.predict": "Detection",
            "tracker.update": "Tracking",
            "save_tracks": "Storage",
            "file.write": "Storage",
            "open": "Storage",
        }
    )
    pipeline = module.SemanticLabelingPipeline([module.RuleBasedLabeler(rules)])
    labels = pipeline.process_nodes(call_nodes, code=raw_code, language="python", full_ast=ast_result)
    return [label for label in labels if isinstance(label, dict) and label.get("type") != "Unknown"]


def _build_business_graph(pipeline: dict[str, Any]) -> nx.DiGraph:
    graph = nx.DiGraph()
    for node in pipeline.get("nodes", []):
        graph.add_node(node, type=node)
    for edge in pipeline.get("edges", []):
        if isinstance(edge, list | tuple) and len(edge) >= 2:
            graph.add_edge(edge[0], edge[1])
    return graph


def run_pipeline(raw_code: str) -> dict[str, Any]:
    if not raw_code.strip():
        return _error_result(failed_step="language-check", error="raw_code is empty", steps=[])

    steps: list[dict[str, Any]] = []
    skills_dir = _repo_root() / "skills" / "public"
    backend_path = _repo_root() / "backend" / "packages" / "harness"
    if str(backend_path) not in sys.path:
        sys.path.insert(0, str(backend_path))

    try:
        ast.parse(raw_code)
    except SyntaxError as exc:
        return _error_result(failed_step="language-check", error=str(exc), steps=[])

    try:
        ast_result = _run_code_to_ast(raw_code, skills_dir)
        calls, call_nodes = _extract_calls_and_nodes(raw_code)
        steps.append(_step("code-to-ast-new", "success"))
    except Exception as exc:
        return _error_result(failed_step="code-to-ast-new", error=str(exc), steps=steps)

    try:
        chunks = _run_code_splitter(raw_code, skills_dir)
        steps.append(_step("code-splitter-adapter", "success", language="python"))
    except Exception as exc:
        return _error_result(
            failed_step="code-splitter-adapter",
            error=str(exc),
            steps=steps,
            partial={"calls": calls},
        )

    try:
        dataflow = _run_dataflow(raw_code, skills_dir)
        steps.append(_step("dataflow-extractor", "success"))
    except Exception as exc:
        return _error_result(
            failed_step="dataflow-extractor",
            error=str(exc),
            steps=steps,
            partial={"chunks": chunks, "calls": calls},
        )

    try:
        labeled_nodes = _run_semantic_labeler(
            raw_code=raw_code,
            ast_result=ast_result,
            call_nodes=call_nodes,
            skills_dir=skills_dir,
        )
        steps.append(_step("code-semantic-labeler", "success"))
    except Exception as exc:
        return _error_result(
            failed_step="code-semantic-labeler",
            error=str(exc),
            steps=steps,
            partial={"chunks": chunks, "calls": calls, "dataflow": dataflow},
        )

    try:
        from deerflow.pipeline import CloudLLMReasoner, PipelineGraphBuilder

        graph_result = PipelineGraphBuilder().build_dag(calls, dataflow, labeled_nodes)
        pipeline = graph_result["pipeline"]
        steps.append(_step("pipeline-graph-builder", "success"))
    except Exception as exc:
        return _error_result(
            failed_step="pipeline-graph-builder",
            error=str(exc),
            steps=steps,
            partial={
                "chunks": chunks,
                "calls": calls,
                "dataflow": dataflow,
                "labeled_nodes": labeled_nodes,
            },
        )

    try:
        diagnosis = CloudLLMReasoner().reason(_build_business_graph(pipeline))
        steps.append(_step("pipeline-reasoner", "success"))
    except Exception as exc:
        return _error_result(
            failed_step="pipeline-reasoner",
            error=str(exc),
            steps=steps,
            partial={
                "chunks": chunks,
                "calls": calls,
                "dataflow": dataflow,
                "labeled_nodes": labeled_nodes,
                "pipeline": pipeline,
            },
        )

    result: dict[str, Any] = {
        "status": "success",
        "language": "python",
        "steps": steps,
        "chunks": chunks,
        "calls": calls,
        "dataflow": dataflow,
        "labeled_nodes": labeled_nodes,
        "pipeline": pipeline,
        "diagnosis": diagnosis,
        "report_path": "",
        "report_format": "markdown",
    }

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the code-business-DAG analysis pipeline.")
    parser.add_argument("--file", help="Python source file to analyze.")
    parser.add_argument("--code", help="Python source code string to analyze.")
    parser.add_argument("--output", help="Optional output JSON path.")
    parser.add_argument("--report-output", help="Optional standalone Markdown report path.")
    args = parser.parse_args()

    if args.file:
        raw_code = Path(args.file).read_text(encoding="utf-8")
    elif args.code:
        raw_code = args.code
    else:
        raise SystemExit("Either --file or --code is required.")

    result = run_pipeline(raw_code)

    if args.report_output:
        report_path = Path(args.report_output)
    elif args.output:
        report_path = Path(args.output).with_suffix(".md")
    else:
        report_path = Path("code_business_dag_report.md")

    if report_path is not None:
        try:
            report_module = _load_module("code_business_dag_report", _skill_root() / "scripts" / "render_report.py")
            report_text = report_module.render_report(result)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(report_text, encoding="utf-8")
            result["report_path"] = str(report_path)
            result["report_format"] = "markdown"
            result["steps"].append(_step("analysis-report", "success", output=str(report_path)))
        except Exception as exc:
            result["steps"].append(_step("analysis-report", "error", error=str(exc)))

    output = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output, encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    main()
