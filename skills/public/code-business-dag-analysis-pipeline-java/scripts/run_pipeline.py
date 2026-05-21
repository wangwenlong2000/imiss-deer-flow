"""Run the code-to-business-DAG analysis pipeline for Java code.

This is a copied and Java-adapted version of the Python pipeline. The original
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
    "java-dataflow-extractor",
    "code-semantic-labeler",
    "pipeline-graph-builder",
    "pipeline-reasoner",
    "analysis-report",
]

JAVA_KEYWORDS = {
    "abstract",
    "assert",
    "boolean",
    "break",
    "byte",
    "case",
    "catch",
    "char",
    "class",
    "const",
    "continue",
    "default",
    "do",
    "double",
    "else",
    "enum",
    "extends",
    "final",
    "finally",
    "float",
    "for",
    "goto",
    "if",
    "implements",
    "import",
    "instanceof",
    "int",
    "interface",
    "long",
    "native",
    "new",
    "package",
    "private",
    "protected",
    "public",
    "return",
    "short",
    "static",
    "strictfp",
    "super",
    "switch",
    "synchronized",
    "this",
    "throw",
    "throws",
    "transient",
    "try",
    "void",
    "volatile",
    "while",
}

JAVA_SEMANTIC_RULES = {
    "VideoCapture": "Camera",
    "camera.read": "Camera",
    "capture.read": "Camera",
    "cap.read": "Camera",
    "FrameGrabber": "Camera",
    "Imgcodecs.imread": "Camera",
    "imread": "Camera",
    "decode": "Decode",
    "decodeFrame": "Decode",
    "resize": "Preprocess",
    "Imgproc.resize": "Preprocess",
    "normalize": "Preprocess",
    "preprocess": "Preprocess",
    "cvtColor": "Preprocess",
    "Imgproc.cvtColor": "Preprocess",
    "detect": "Detection",
    "detector.detect": "Detection",
    "detector.predict": "Detection",
    "model.predict": "Detection",
    "infer": "Detection",
    "predict": "Detection",
    "Detector": "Detection",
    "tracker.update": "Tracking",
    "track": "Tracking",
    "updateTracks": "Tracking",
    "Tracker": "Tracking",
    "save": "Storage",
    "saveTracks": "Storage",
    "repository.save": "Storage",
    "storage.save": "Storage",
    "db.save": "Storage",
    "writer.write": "Storage",
    "Files.write": "Storage",
    "FileWriter": "Storage",
    "System.out.println": "Output",
    "println": "Output",
    "render": "Output",
    "display": "Output",
    "imshow": "Output",
    "Collections.sort": "DataProcessing",
    "stream": "DataProcessing",
    "map": "DataProcessing",
    "filter": "DataProcessing",
    "collect": "DataProcessing",
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
        "language": "java",
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


def _looks_like_java(raw_code: str) -> bool:
    java_markers = (
        "public class ",
        "class ",
        "interface ",
        "enum ",
        "public static void main",
        "package ",
        "import java.",
        "System.out.",
        "new ",
    )
    return any(marker in raw_code for marker in java_markers)


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
    return re.sub(r"\s+", "", name.strip())


def _infer_semantic_type(name: str) -> str:
    lowered = name.casefold()
    for pattern, label in JAVA_SEMANTIC_RULES.items():
        if name == pattern or pattern in name or pattern.casefold() in lowered:
            return label

    if any(token in lowered for token in ("camera", "capture", "framegrabber", "imread")):
        return "Camera"
    if any(token in lowered for token in ("decode", "demux")):
        return "Decode"
    if any(token in lowered for token in ("preprocess", "resize", "normalize", "transform", "filter", "cvtcolor")):
        return "Preprocess"
    if any(token in lowered for token in ("detect", "detector", "predict", "infer", "model")):
        return "Detection"
    if any(token in lowered for token in ("track", "tracker")):
        return "Tracking"
    if any(token in lowered for token in ("save", "store", "storage", "repository", "database", "db", "write", "file")):
        return "Storage"
    if any(token in lowered for token in ("println", "display", "render", "imshow", "output", "report")):
        return "Output"
    if any(token in lowered for token in ("sort", "stream", "map", "filter", "collect", "list", "queue")):
        return "DataProcessing"
    return "General"


def _extract_variable_types(raw_code: str) -> dict[str, str]:
    code = _strip_comments(raw_code)
    var_types: dict[str, str] = {}
    declaration_pattern = re.compile(
        r"(?:final\s+)?(?P<type>[A-Z][A-Za-z_]\w*(?:<[^;=(){}]+>)?(?:\[\])?)"
        r"\s+(?P<name>[a-zA-Z_]\w*)\s*(?:[=;,)])"
    )
    for match in declaration_pattern.finditer(code):
        declared_type = match.group("type").strip()
        name = match.group("name")
        if declared_type in JAVA_KEYWORDS or name in JAVA_KEYWORDS:
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


def _is_method_declaration(line: str, raw_name: str) -> bool:
    if re.match(rf"\s*(?:public|private|protected)\s+{re.escape(raw_name)}\s*\(", line):
        return True
    if "{" not in line:
        return False
    declaration_prefixes = ("public ", "private ", "protected ", "static ", "final ", "synchronized ", "void ")
    return any(prefix in line for prefix in declaration_prefixes) and re.search(rf"\b{re.escape(raw_name)}\s*\(", line) is not None


def _extract_java_call_nodes(raw_code: str) -> list[dict[str, Any]]:
    code = _strip_comments(raw_code)
    variable_types = _extract_variable_types(raw_code)
    call_pattern = re.compile(
        r"(?<![\w.])(?P<name>[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*\("
    )
    nodes: list[dict[str, Any]] = []

    for match in call_pattern.finditer(code):
        raw_name = match.group("name")
        base_name = raw_name.rsplit(".", 1)[-1]
        if base_name in JAVA_KEYWORDS:
            continue
        line_number = _line_number(code, match.start())
        snippet = _line_text(raw_code, line_number)
        if _is_method_declaration(snippet, raw_name):
            continue
        name = _call_name_from_match(raw_name, variable_types)
        nodes.append(
            {
                "name": name,
                "node_type": "MethodInvocation",
                "parent_name": "compilation_unit",
                "line_number": line_number,
                "code_snippet": snippet,
            }
        )

    stream_pattern = re.compile(r"\b(?P<name>System\.out\.println|System\.err\.println|Files\.write|FileWriter)\b")
    for match in stream_pattern.finditer(code):
        name = _normalize_call_name(match.group("name"))
        line_number = _line_number(code, match.start())
        nodes.append(
            {
                "name": name,
                "node_type": "OutputOrStorageExpression",
                "parent_name": "compilation_unit",
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
    call_nodes = _extract_java_call_nodes(raw_code)
    calls = [
        {"caller": str(source["name"]), "callee": str(target["name"])}
        for source, target in zip(call_nodes, call_nodes[1:], strict=False)
        if source.get("name") and target.get("name")
    ]
    return calls, call_nodes


def _extract_java_dataflow(raw_code: str) -> list[Any]:
    dataflow: list[Any] = []
    code = _strip_comments(raw_code)
    assignments = re.finditer(
        r"(?:[A-Z][A-Za-z_]\w*(?:<[^;=(){}]+>)?\s+)?(?P<target>[A-Za-z_]\w*)\s*="
        r"\s*(?:new\s+)?(?P<source>[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*\(",
        code,
    )
    for match in assignments:
        dataflow.append([_normalize_call_name(match.group("source")), match.group("target")])

    unique: list[Any] = []
    seen: set[tuple[str, str]] = set()
    for edge in dataflow:
        key = (str(edge[0]), str(edge[1]))
        if key not in seen:
            seen.add(key)
            unique.append([key[0], key[1]])
    return unique


def _run_code_to_ast(raw_code: str, skills_dir: Path) -> dict[str, Any]:
    module = _load_module("code_to_ast_convert", skills_dir / "code-to-ast-new" / "scripts" / "convert.py")
    result = module.parse_java(raw_code)
    if isinstance(result, dict) and "error" not in result:
        return result
    return {
        "type": "compilation_unit",
        "language": "java",
        "parser": "regex-fallback",
        "warning": result.get("error") if isinstance(result, dict) else "Java parser unavailable",
        "text": raw_code,
    }


def _run_code_splitter(raw_code: str, skills_dir: Path) -> list[Any]:
    chunks: list[dict[str, Any]] = []
    method_pattern = re.compile(
        r"(?m)^\s*(?:public|private|protected)?\s*(?:static\s+)?(?:final\s+)?"
        r"(?:[\w<>\[\], ?]+\s+)?(?P<name>[A-Za-z_]\w*)\s*\([^;{}]*\)\s*\{"
    )
    matches = list(method_pattern.finditer(raw_code))
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(raw_code)
        content = raw_code[start:end].strip()
        if content:
            chunks.append(
                {
                    "content": content,
                    "metadata": {
                        "language": "java",
                        "chunk_index": index,
                        "name": match.group("name"),
                        "strategy": "java-regex-method",
                    },
                }
            )
    if chunks:
        return chunks
    return [
        {
            "content": raw_code,
            "metadata": {
                "language": "java",
                "chunk_index": 0,
                "strategy": "single-chunk-fallback",
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
    rules.update(JAVA_SEMANTIC_RULES)
    pipeline = module.SemanticLabelingPipeline([module.RuleBasedLabeler(rules)])
    labels = pipeline.process_nodes(call_nodes, code=raw_code, language="java", full_ast=ast_result)

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
    if not _looks_like_java(raw_code):
        return _error_result(
            failed_step="language-check",
            error="input does not look like Java code",
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
        steps.append(_step("code-to-ast-new", "success", language="java", parser=ast_result.get("parser", "java")))
    except Exception as exc:
        return _error_result(failed_step="code-to-ast-new", error=str(exc), steps=steps)

    try:
        chunks = _run_code_splitter(raw_code, skills_dir)
        steps.append(_step("code-splitter-adapter", "success", language="java"))
    except Exception as exc:
        return _error_result(failed_step="code-splitter-adapter", error=str(exc), steps=steps, partial={"calls": calls})

    try:
        dataflow = _extract_java_dataflow(raw_code)
        steps.append(_step("java-dataflow-extractor", "success"))
    except Exception as exc:
        return _error_result(
            failed_step="java-dataflow-extractor",
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
        steps.append(_step("code-semantic-labeler", "success", language="java"))
    except Exception as exc:
        return _error_result(
            failed_step="code-semantic-labeler",
            error=str(exc),
            steps=steps,
            partial={"chunks": chunks, "calls": calls, "dataflow": dataflow},
        )

    try:
        from deerflow.pipeline import CloudLLMReasoner, PipelineGraphBuilder

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
        "language": "java",
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
    parser = argparse.ArgumentParser(description="Run the Java code-business-DAG analysis pipeline.")
    parser.add_argument("--file", help="Java source file to analyze.")
    parser.add_argument("--code", help="Java source code string to analyze.")
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
        report_path = Path("java_code_business_dag_report.md")

    try:
        report_module = _load_module("java_code_business_dag_report", _skill_root() / "scripts" / "render_report.py")
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
