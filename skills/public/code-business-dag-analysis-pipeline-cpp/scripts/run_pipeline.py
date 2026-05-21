"""Run the code-to-business-DAG analysis pipeline for C++ code.

This is a copied and C++-adapted version of the Python pipeline. The original
`code-business-dag-analysis-pipeline` directory is intentionally left unchanged.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import Any

import networkx as nx


STEP_ORDER = [
    "code-to-ast-new",
    "code-splitter-adapter",
    "cpp-dataflow-extractor",
    "code-semantic-labeler",
    "pipeline-graph-builder",
    "pipeline-reasoner",
    "analysis-report",
]

CPP_KEYWORDS = {
    "alignas",
    "alignof",
    "asm",
    "auto",
    "bool",
    "break",
    "case",
    "catch",
    "char",
    "class",
    "const",
    "constexpr",
    "continue",
    "decltype",
    "default",
    "delete",
    "do",
    "double",
    "else",
    "enum",
    "explicit",
    "extern",
    "false",
    "float",
    "for",
    "friend",
    "goto",
    "if",
    "inline",
    "int",
    "long",
    "namespace",
    "new",
    "noexcept",
    "nullptr",
    "operator",
    "private",
    "protected",
    "public",
    "register",
    "return",
    "short",
    "signed",
    "sizeof",
    "static",
    "static_cast",
    "struct",
    "switch",
    "template",
    "this",
    "throw",
    "true",
    "try",
    "typedef",
    "typename",
    "union",
    "unsigned",
    "using",
    "virtual",
    "void",
    "volatile",
    "while",
}

CPP_SEMANTIC_RULES = {
    "cv::VideoCapture": "Camera",
    "VideoCapture": "Camera",
    "capture.read": "Camera",
    "cap.read": "Camera",
    "camera.read": "Camera",
    "imread": "Camera",
    "cv::imread": "Camera",
    "decode": "Decode",
    "decodeFrame": "Decode",
    "resize": "Preprocess",
    "cv::resize": "Preprocess",
    "normalize": "Preprocess",
    "preprocess": "Preprocess",
    "cvtColor": "Preprocess",
    "cv::cvtColor": "Preprocess",
    "detect": "Detection",
    "detector.detect": "Detection",
    "detector.predict": "Detection",
    "model.predict": "Detection",
    "infer": "Detection",
    "predict": "Detection",
    "YOLO": "Detection",
    "Detector": "Detection",
    "tracker.update": "Tracking",
    "track": "Tracking",
    "updateTracks": "Tracking",
    "Tracker": "Tracking",
    "save": "Storage",
    "saveTracks": "Storage",
    "write": "Storage",
    "writer.write": "Storage",
    "std::ofstream": "Storage",
    "ofstream": "Storage",
    "db.save": "Storage",
    "Database": "Storage",
    "std::cout": "Output",
    "cout": "Output",
    "render": "Output",
    "display": "Output",
    "imshow": "Output",
    "cv::imshow": "Output",
    "std::sort": "DataProcessing",
    "sort": "DataProcessing",
    "std::vector": "DataProcessing",
    "vector": "DataProcessing",
}


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
    partial: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "status": "error",
        "failed_step": failed_step,
        "error": error,
        "language": "cpp",
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


def _looks_like_cpp(raw_code: str) -> bool:
    cpp_markers = (
        "#include",
        "std::",
        "::",
        "->",
        "int main(",
        "class ",
        "struct ",
        "template<",
        "template <",
        "cv::",
    )
    return any(marker in raw_code for marker in cpp_markers)


def _strip_comments(raw_code: str) -> str:
    without_block = re.sub(r"/\*.*?\*/", "", raw_code, flags=re.S)
    return re.sub(r"//.*", "", without_block)


def _line_number(raw_code: str, offset: int) -> int:
    return raw_code.count("\n", 0, max(offset, 0)) + 1


def _line_text(raw_code: str, line_number: int) -> str:
    lines = raw_code.splitlines()
    if 1 <= line_number <= len(lines):
        return lines[line_number - 1].strip()
    return ""


def _normalize_call_name(name: str) -> str:
    name = name.strip()
    name = name.replace("->", ".")
    name = re.sub(r"\s+", "", name)
    if name.startswith("std::"):
        return name
    if name.startswith("cv::"):
        return name
    return name


def _infer_semantic_type(name: str) -> str:
    lowered = name.casefold()
    for pattern, label in CPP_SEMANTIC_RULES.items():
        if name == pattern or pattern in name or pattern.casefold() in lowered:
            return label

    if any(token in lowered for token in ("camera", "capture", "framegrabber")):
        return "Camera"
    if any(token in lowered for token in ("decode", "demux")):
        return "Decode"
    if any(token in lowered for token in ("preprocess", "resize", "normalize", "transform", "filter")):
        return "Preprocess"
    if any(token in lowered for token in ("detect", "detector", "predict", "infer", "yolo", "model")):
        return "Detection"
    if any(token in lowered for token in ("track", "tracker")):
        return "Tracking"
    if any(token in lowered for token in ("save", "store", "storage", "database", "db", "ofstream", "write")):
        return "Storage"
    if any(token in lowered for token in ("cout", "display", "render", "imshow", "output", "report")):
        return "Output"
    if any(token in lowered for token in ("sort", "find", "transform", "accumulate", "vector", "map", "queue")):
        return "DataProcessing"
    return "General"


def _extract_variable_types(raw_code: str) -> dict[str, str]:
    code = _strip_comments(raw_code)
    var_types: dict[str, str] = {}
    declaration_pattern = re.compile(
        r"(?P<type>(?:std::|cv::)?[A-Za-z_]\w*(?:::[A-Za-z_]\w*)?(?:\s*<[^;=(){}]+>)?)"
        r"\s+(?P<name>[A-Za-z_]\w*)\s*(?:[=({;])"
    )
    for match in declaration_pattern.finditer(code):
        declared_type = match.group("type").strip()
        name = match.group("name")
        if declared_type in CPP_KEYWORDS or name in CPP_KEYWORDS:
            continue
        var_types[name] = declared_type
    return var_types


def _call_name_from_match(raw_name: str, variable_types: dict[str, str]) -> str:
    name = _normalize_call_name(raw_name)
    if name in variable_types:
        return variable_types[name]
    if "." in name:
        receiver, method = name.split(".", 1)
        receiver_type = variable_types.get(receiver, "")
        receiver_label = _infer_semantic_type(receiver_type)
        if receiver_label != "General":
            return f"{receiver}.{method}"
        return name
    return name


def _extract_cpp_call_nodes(raw_code: str) -> list[dict[str, Any]]:
    code = _strip_comments(raw_code)
    variable_types = _extract_variable_types(raw_code)
    call_pattern = re.compile(
        r"(?<![\w:])(?P<name>(?:[A-Za-z_]\w*::)*[A-Za-z_]\w*|[A-Za-z_]\w*(?:\.|->)[A-Za-z_]\w*)\s*\("
    )
    nodes: list[dict[str, Any]] = []
    seen_at_offset: set[tuple[int, str]] = set()

    for match in call_pattern.finditer(code):
        raw_name = match.group("name")
        base_name = raw_name.split("::")[-1].split(".")[-1].split("->")[-1]
        if base_name in CPP_KEYWORDS:
            continue
        line_number = _line_number(code, match.start())
        snippet = _line_text(raw_code, line_number)
        if raw_name == base_name and "{" in snippet and ")" in snippet and raw_name not in variable_types:
            continue
        name = _call_name_from_match(raw_name, variable_types)
        key = (match.start(), name)
        if key in seen_at_offset:
            continue
        seen_at_offset.add(key)
        nodes.append(
            {
                "name": name,
                "node_type": "CallExpression",
                "parent_name": "translation_unit",
                "line_number": line_number,
                "code_snippet": _line_text(raw_code, line_number),
            }
        )

    stream_pattern = re.compile(r"\b(?P<name>std::cout|cout|std::cerr|cerr|std::ofstream|ofstream)\b")
    for match in stream_pattern.finditer(code):
        name = _normalize_call_name(match.group("name"))
        line_number = _line_number(code, match.start())
        nodes.append(
            {
                "name": name,
                "node_type": "StreamOrStorageExpression",
                "parent_name": "translation_unit",
                "line_number": line_number,
                "code_snippet": _line_text(raw_code, line_number),
            }
        )

    nodes.sort(key=lambda item: (int(item.get("line_number") or 0), str(item.get("name") or "")))
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[int, str]] = set()
    for node in nodes:
        key = (int(node.get("line_number") or 0), str(node.get("name") or ""))
        if key not in seen:
            seen.add(key)
            deduped.append(node)
    return deduped


def _extract_calls_and_nodes(raw_code: str) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    call_nodes = _extract_cpp_call_nodes(raw_code)
    calls = [
        {"caller": str(source["name"]), "callee": str(target["name"])}
        for source, target in zip(call_nodes, call_nodes[1:], strict=False)
        if source.get("name") and target.get("name")
    ]
    return calls, call_nodes


def _extract_cpp_dataflow(raw_code: str, call_nodes: list[dict[str, Any]]) -> list[Any]:
    dataflow: list[Any] = []
    code = _strip_comments(raw_code)
    assignments = re.finditer(
        r"(?P<target>[A-Za-z_]\w*)\s*=\s*(?P<source>[A-Za-z_]\w*(?:::[A-Za-z_]\w*)?(?:\.|->)?[A-Za-z_]\w*)\s*\(",
        code,
    )
    for match in assignments:
        dataflow.append([_normalize_call_name(match.group("source")), match.group("target")])

    unique: list[Any] = []
    seen: set[tuple[str, str]] = set()
    for edge in dataflow:
        if isinstance(edge, list | tuple) and len(edge) >= 2:
            key = (str(edge[0]), str(edge[1]))
            if key not in seen:
                seen.add(key)
                unique.append([key[0], key[1]])
    return unique


def _run_code_to_ast(raw_code: str, skills_dir: Path) -> dict[str, Any]:
    module = _load_module("code_to_ast_convert", skills_dir / "code-to-ast-new" / "scripts" / "convert.py")
    result = module.parse_cpp(raw_code)
    if isinstance(result, dict) and "error" not in result:
        return result
    return {
        "type": "translation_unit",
        "language": "cpp",
        "parser": "regex-fallback",
        "warning": result.get("error") if isinstance(result, dict) else "tree-sitter C++ parser unavailable",
        "text": raw_code,
    }


def _run_code_splitter(raw_code: str, skills_dir: Path) -> list[Any]:
    module = _load_module("code_splitter_adapter", skills_dir / "code-splitter-adapter" / "code_splitter.py")
    try:
        splitter = module.get_code_splitter(strategy="langchain", chunk_size=1000, chunk_overlap=120)
        return splitter.split_code(code=raw_code, language="cpp", metadata={"language": "cpp"})
    except Exception as exc:
        return [
            {
                "content": raw_code,
                "metadata": {
                    "language": "cpp",
                    "chunk_index": 0,
                    "fallback_reason": str(exc),
                },
            }
        ]


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
    rules.update(CPP_SEMANTIC_RULES)
    pipeline = module.SemanticLabelingPipeline([module.RuleBasedLabeler(rules)])
    labels = pipeline.process_nodes(call_nodes, code=raw_code, language="cpp", full_ast=ast_result)

    enriched: list[dict[str, Any]] = []
    for node, label in zip(call_nodes, labels, strict=False):
        item = dict(label) if isinstance(label, dict) else dict(node)
        if item.get("type") in (None, "Unknown", "General"):
            item["type"] = _infer_semantic_type(str(item.get("name") or ""))
        if item.get("type") != "General":
            enriched.append(item)
    return enriched


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
    if not _looks_like_cpp(raw_code):
        return _error_result(
            failed_step="language-check",
            error="input does not look like C++ code",
            steps=[],
        )

    steps: list[dict[str, Any]] = []
    skills_dir = _repo_root() / "skills" / "public"
    backend_path = _repo_root() / "backend" / "packages" / "harness"
    if str(backend_path) not in sys.path:
        sys.path.insert(0, str(backend_path))

    try:
        ast_result = _run_code_to_ast(raw_code, skills_dir)
        calls, call_nodes = _extract_calls_and_nodes(raw_code)
        steps.append(_step("code-to-ast-new", "success", language="cpp", parser=ast_result.get("parser", "tree-sitter-cpp")))
    except Exception as exc:
        return _error_result(failed_step="code-to-ast-new", error=str(exc), steps=steps)

    try:
        chunks = _run_code_splitter(raw_code, skills_dir)
        steps.append(_step("code-splitter-adapter", "success", language="cpp"))
    except Exception as exc:
        return _error_result(failed_step="code-splitter-adapter", error=str(exc), steps=steps, partial={"calls": calls})

    try:
        dataflow = _extract_cpp_dataflow(raw_code, call_nodes)
        steps.append(_step("cpp-dataflow-extractor", "success"))
    except Exception as exc:
        return _error_result(
            failed_step="cpp-dataflow-extractor",
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
        steps.append(_step("code-semantic-labeler", "success", language="cpp"))
    except Exception as exc:
        return _error_result(
            failed_step="code-semantic-labeler",
            error=str(exc),
            steps=steps,
            partial={"chunks": chunks, "calls": calls, "dataflow": dataflow},
        )

    try:
        from deerflow.pipeline import CloudLLMReasoner, PipelineGraphBuilder

        # Keep C++ dataflow in the JSON/report, but build the business DAG from
        # the ordered call sequence. Sparse C++ assignment edges often point to
        # intermediate variables that are intentionally unlabeled; feeding them
        # into the generic graph builder can create artificial wraparound edges.
        graph_result = PipelineGraphBuilder().build_dag(calls, [], labeled_nodes)
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

    return {
        "status": "success",
        "language": "cpp",
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the C++ code-business-DAG analysis pipeline.")
    parser.add_argument("--file", help="C++ source file to analyze.")
    parser.add_argument("--code", help="C++ source code string to analyze.")
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
        report_path = Path("cpp_code_business_dag_report.md")

    try:
        report_module = _load_module("cpp_code_business_dag_report", _skill_root() / "scripts" / "render_report.py")
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
