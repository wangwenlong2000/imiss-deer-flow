"""Render a standalone Markdown report from a C++ code-business-DAG result."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _bullet_list(values: list[Any], empty_text: str, *, limit: int | None = None) -> str:
    items = values[:limit] if limit is not None else values
    if not items:
        return f"- {empty_text}"
    lines = [f"- `{item}`" if isinstance(item, str) else f"- `{json.dumps(item, ensure_ascii=False)}`" for item in items]
    if limit is not None and len(values) > limit:
        lines.append(f"- 其余 {len(values) - limit} 项已省略，可在 JSON 中查看完整结果。")
    return "\n".join(lines)


def _table(headers: list[str], rows: list[list[Any]], empty_text: str) -> str:
    if not rows:
        return empty_text
    header_line = "| " + " | ".join(headers) + " |"
    sep_line = "| " + " | ".join("---" for _ in headers) + " |"
    row_lines = ["| " + " | ".join(str(cell).replace("\n", " ") for cell in row) + " |" for row in rows]
    return "\n".join([header_line, sep_line, *row_lines])


def _mermaid_id(value: Any, index: int) -> str:
    raw = "".join(ch if str(ch).isalnum() else "_" for ch in str(value))
    raw = raw.strip("_") or f"node_{index}"
    if raw[0].isdigit():
        raw = f"node_{raw}"
    return raw


def build_mermaid(pipeline: dict[str, Any]) -> str:
    nodes = _as_list(pipeline.get("nodes"))
    edges = _as_list(pipeline.get("edges"))
    if not nodes:
        return "graph TD\n  empty[未发现业务节点]"

    node_ids = {str(node): _mermaid_id(node, index) for index, node in enumerate(nodes)}
    lines = ["graph TD"]
    for node in nodes:
        label = str(node).replace('"', "'")
        lines.append(f'  {node_ids[str(node)]}["{label}"]')
    for edge in edges:
        if isinstance(edge, list | tuple) and len(edge) >= 2:
            source = str(edge[0])
            target = str(edge[1])
            if source in node_ids and target in node_ids:
                lines.append(f"  {node_ids[source]} --> {node_ids[target]}")
    return "\n".join(lines)


def _summarize_chunks(chunks: list[Any]) -> str:
    rows: list[list[Any]] = []
    for index, chunk in enumerate(chunks[:12]):
        if isinstance(chunk, dict):
            content = str(chunk.get("content") or chunk.get("code") or "")
            metadata = _as_dict(chunk.get("metadata"))
            rows.append([index, len(content), metadata.get("chunk_index", index), content[:90]])
        else:
            rows.append([index, len(str(chunk)), index, str(chunk)[:90]])
    return _table(["序号", "字符数", "chunk_index", "内容预览"], rows, "未产生代码切分结果。")


def _summarize_labeled_nodes(labeled_nodes: list[Any]) -> str:
    rows: list[list[Any]] = []
    for item in labeled_nodes:
        if isinstance(item, dict):
            rows.append([item.get("name", ""), item.get("type", ""), item.get("line_number", ""), item.get("code_snippet", "")])
    return _table(["代码节点", "业务语义", "行号", "代码片段"], rows, "未产生语义标注节点。")


def _json_block(value: Any) -> str:
    return "```json\n" + json.dumps(value, ensure_ascii=False, indent=2) + "\n```"


def render_report(result: dict[str, Any]) -> str:
    diagnosis = _as_dict(result.get("diagnosis"))
    pipeline = _as_dict(result.get("pipeline"))
    nodes = _as_list(pipeline.get("nodes"))
    edges = _as_list(pipeline.get("edges"))
    chunks = _as_list(result.get("chunks"))
    calls = _as_list(result.get("calls"))
    dataflow = _as_list(result.get("dataflow"))
    labeled_nodes = _as_list(result.get("labeled_nodes"))
    missing = _as_list(diagnosis.get("missing"))
    anomalies = _as_list(diagnosis.get("anomalies"))
    pipeline_type = diagnosis.get("pipeline_type") or "unknown"

    return "\n".join(
        [
            "# C++ 代码业务 DAG 分析报告",
            "",
            "## 1. 摘要",
            "",
            f"本报告基于 `code-business-dag-analysis-pipeline-cpp` 对输入 C++ 代码的静态分析结果生成。当前识别出的流水线类型为 `{pipeline_type}`。",
            "",
            f"本次分析识别出 {len(nodes)} 个业务节点、{len(edges)} 条业务依赖边、{len(chunks)} 个代码片段、{len(calls)} 条调用关系、{len(dataflow)} 条数据流关系和 {len(labeled_nodes)} 个语义标注节点。",
            "",
            "## 2. 业务 DAG Mermaid 图",
            "",
            "```mermaid",
            build_mermaid(pipeline),
            "```",
            "",
            "## 3. AST 与调用关系",
            "",
            "C++ 版本优先调用 `code-to-ast-new` 的 C++ 解析能力；如果当前环境缺少 `tree-sitter-cpp`，会降级为轻量静态扫描，继续提取调用序列和业务节点。",
            "",
            _bullet_list(calls, "未发现调用关系。", limit=20),
            "",
            "## 4. 代码切分结果",
            "",
            _summarize_chunks(chunks),
            "",
            "## 5. C++ 数据流结果",
            "",
            _bullet_list(dataflow, "未发现数据流关系。", limit=30),
            "",
            "## 6. 语义标注结果",
            "",
            _summarize_labeled_nodes(labeled_nodes),
            "",
            "## 7. 诊断结果",
            "",
            f"- 流水线类型：`{pipeline_type}`",
            "",
            "缺失模块：",
            "",
            _bullet_list(missing, "未发现明确缺失模块。"),
            "",
            "逻辑异常：",
            "",
            _bullet_list(anomalies, "未发现明确逻辑异常。"),
            "",
            "## 8. 附录：关键 JSON",
            "",
            "### pipeline",
            "",
            _json_block(pipeline),
            "",
            "### diagnosis",
            "",
            _json_block(diagnosis),
            "",
            "### labeled_nodes",
            "",
            _json_block(labeled_nodes[:40]),
            "",
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Render a standalone Markdown report from a C++ pipeline result JSON.")
    parser.add_argument("--input", "-i", required=True, help="Path to pipeline result JSON.")
    parser.add_argument("--output", "-o", required=True, help="Path for the Markdown report.")
    args = parser.parse_args()

    result_path = Path(args.input)
    result = json.loads(result_path.read_text(encoding="utf-8"))
    report = render_report(result)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
