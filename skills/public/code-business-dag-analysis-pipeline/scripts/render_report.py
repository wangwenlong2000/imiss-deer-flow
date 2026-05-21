"""Render a standalone Markdown report from a code-business-DAG pipeline result."""

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
    row_lines = ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
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

    node_ids: dict[str, str] = {str(node): _mermaid_id(node, index) for index, node in enumerate(nodes)}
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
            rows.append([index, len(content), metadata.get("chunk_index", index), content[:80].replace("\n", " ")])
        else:
            rows.append([index, len(str(chunk)), index, str(chunk)[:80].replace("\n", " ")])
    return _table(["序号", "字符数", "chunk_index", "内容预览"], rows, "未产生代码切分结果。")


def _summarize_labeled_nodes(labeled_nodes: list[Any]) -> str:
    rows: list[list[Any]] = []
    for item in labeled_nodes:
        if isinstance(item, dict):
            rows.append(
                [
                    item.get("name", ""),
                    item.get("type", ""),
                    item.get("parent_name", ""),
                    item.get("line_number", ""),
                ]
            )
    return _table(["代码节点", "业务语义", "所属函数", "行号"], rows, "未产生语义标注节点。")


def _count_types(labeled_nodes: list[Any]) -> str:
    counts: dict[str, int] = {}
    for item in labeled_nodes:
        if isinstance(item, dict):
            semantic_type = str(item.get("type") or "Unknown")
            counts[semantic_type] = counts.get(semantic_type, 0) + 1
    rows = [[key, value] for key, value in sorted(counts.items())]
    return _table(["业务语义", "节点数量"], rows, "未产生可统计的语义类型。")


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
    mermaid = build_mermaid(pipeline)

    return "\n".join(
        [
            "# 代码业务 DAG 分析报告",
            "",
            "## 1. 摘要",
            "",
            f"本报告基于 `code-business-dag-analysis-pipeline` 对输入 Python 代码的静态分析结果生成。分析链路将源码解析为 AST，切分为可处理片段，抽取数据流，进行业务语义标注，再构建业务 DAG，并使用本地图推理规则给出诊断。当前识别出的流水线类型为 `{pipeline_type}`。",
            "",
            f"本次分析共识别出 {len(nodes)} 个业务节点、{len(edges)} 条业务依赖边、{len(chunks)} 个代码片段、{len(calls)} 条调用关系、{len(dataflow)} 条数据流关系和 {len(labeled_nodes)} 个语义标注节点。报告后续章节会逐个说明这些结果的含义，而不是只给出运行概况。",
            "",
            "## 2. 业务 DAG Mermaid 图",
            "",
            "下面是业务 DAG 的 Mermaid 图。报告不使用文字箭头链路描述 DAG，业务流程以图形方式呈现。",
            "",
            "```mermaid",
            mermaid,
            "```",
            "",
            "## 3. code-to-ast-new 结果分析",
            "",
            f"`code-to-ast-new` 的主要作用是将源码转成可遍历的 AST 结构，并为调用关系、节点抽取和后续语义标注提供结构基础。本次从 AST 中整理出 {len(calls)} 条调用关系。调用关系反映了入口函数或主要流程中涉及的关键函数、方法与对象行为。",
            "",
            "典型调用关系样例：",
            "",
            _bullet_list(calls, "未发现调用关系。", limit=12),
            "",
            "这些调用关系不是最终业务 DAG，而是后续语义标注和构图的原始材料。业务 DAG 会进一步把代码级调用归并为 `Camera`、`Detection`、`Tracking`、`Storage` 等业务语义节点。",
            "",
            "## 4. code-splitter-adapter 结果分析",
            "",
            f"`code-splitter-adapter` 将源码拆分为较小的代码片段，便于后续分析在较稳定的上下文窗口内处理代码。本次得到 {len(chunks)} 个 chunk。chunk 数量较多通常说明代码包含多个类、函数或较长的业务逻辑；chunk 数量较少则说明当前输入更接近单一脚本或单一入口。",
            "",
            _summarize_chunks(chunks),
            "",
            "切分结果主要用于辅助定位代码范围和降低后续分析复杂度。若某个关键函数没有进入 chunk，后续的数据流和语义标注可能会出现漏报。",
            "",
            "## 5. dataflow-extractor 数据流分析",
            "",
            f"`dataflow-extractor` 从赋值、函数调用、返回值和变量传递中提取数据移动关系。本次得到 {len(dataflow)} 条数据流边。数据流用于判断业务阶段之间是否真的存在数据传递，而不仅仅是存在代码调用。",
            "",
            "关键数据流样例：",
            "",
            _bullet_list(dataflow, "未发现数据流关系。", limit=20),
            "",
            "如果数据流边数量明显少于预期，常见原因包括：代码中大量使用对象内部状态、动态属性、容器字段，或数据通过外部服务间接传递。这类情况需要结合调用图和语义标注一起判断。",
            "",
            "## 6. code-semantic-labeler 语义标注分析",
            "",
            f"`code-semantic-labeler` 将代码节点映射为业务语义类型。本次得到 {len(labeled_nodes)} 个语义节点。语义标注是从代码图走向业务 DAG 的关键步骤，因为最终业务节点来自这些标签，而不是直接来自变量名或函数名。",
            "",
            _summarize_labeled_nodes(labeled_nodes),
            "",
            "语义类型分布：",
            "",
            _count_types(labeled_nodes),
            "",
            "若出现 `Unknown`、`General` 或明显不符合业务含义的标签，需要优先检查本体规则、节点命名和 `_init_llm` 的配置路径。语义标注错误会直接影响 DAG 节点类型和 reasoner 的诊断结果。",
            "",
            "## 7. pipeline-graph-builder 业务 DAG 分析",
            "",
            f"`pipeline-graph-builder` 将调用关系、数据流和语义标签融合为业务级 DAG。本次输出 {len(nodes)} 个业务节点和 {len(edges)} 条业务依赖边。构图阶段会去重、合并同类型节点，并尽量把代码级关系提升为业务阶段关系。",
            "",
            "业务节点：",
            "",
            _bullet_list(nodes, "未生成业务节点。"),
            "",
            "业务边的完整结构已进入附录 JSON。报告正文使用 Mermaid 图展示 DAG，避免用文字箭头替代图形表达。",
            "",
            "## 8. pipeline-reasoner 诊断结果",
            "",
            f"`pipeline-reasoner` 使用 `CloudLLMReasoner` 的本地图推理规则分析业务 DAG。当前诊断出的流水线类型是 `{pipeline_type}`。该类型通常由业务节点组合和有向依赖关系共同决定。",
            "",
            "缺失模块：",
            "",
            _bullet_list(missing, "未发现明确缺失模块。"),
            "",
            "逻辑异常：",
            "",
            _bullet_list(anomalies, "未发现明确逻辑异常。"),
            "",
            "缺失模块和逻辑异常需要结合业务场景理解。例如视频分析流水线中，如果从采集阶段直接进入检测阶段，reasoner 往往会提示缺少解码或预处理模块，因为真实生产链路通常需要帧解码、尺寸归一化、颜色空间转换、去噪或张量化等步骤。",
            "",
            "## 9. 缺失模块影响分析",
            "",
            "缺失模块不一定意味着代码无法运行，它更常表示业务流水线抽象层面存在不完整之处。若缺少 `Decode`，视频源、网络流或文件流到帧数据之间的转换过程没有被显式表达；若缺少 `Preprocess`，模型输入前的数据清洗、归一化、缩放或格式转换没有被纳入业务 DAG。",
            "",
            "这些缺口会影响后续维护和平台化接入：监控系统难以定位某一阶段失败，性能优化缺少可观测边界，模型替换时也不容易确认输入输出契约。因此建议把关键隐式步骤补成独立函数、类或适配器，并让语义标注规则能识别它们。",
            "",
            "## 10. 逻辑异常解释",
            "",
            "逻辑异常通常来自三类情况：第一，真实业务顺序不完整；第二，代码调用顺序和数据流方向不一致；第三，语义标注把某些节点归到了错误业务阶段。当前异常应优先按业务链路检查，而不是仅按代码能否执行判断。",
            "",
            "如果 DAG 中出现反向依赖、孤立节点或关键阶段之间没有路径，需要回看 `dataflow-extractor` 的边和 `code-semantic-labeler` 的标签是否能对齐。若调用图有边但数据流无边，说明代码有行为调用，但数据传递可能是隐式发生的。",
            "",
            "## 11. 工程改进建议",
            "",
            "1. 将隐式业务阶段显式化，例如增加 `decode_frame`、`preprocess_frame`、`normalize_input` 等函数或类。",
            "2. 为 `code-semantic-labeler` 补充更细的本体规则，让 `Decode`、`Preprocess`、`Analysis`、`Alert`、`Output` 等阶段更容易被识别。",
            "3. 对关键数据对象保持清晰命名，例如 `frame`、`decoded_frame`、`preprocessed_frame`、`detections`、`tracks`、`events`。",
            "4. 在入口函数中保持业务阶段顺序清楚，减少跨对象隐式副作用，方便调用图和数据流图对齐。",
            "5. 对生产级流水线，建议为每个业务阶段记录输入输出契约，便于自动构图、诊断和可观测性建设。",
            "",
            "## 12. 附录：关键 JSON 片段",
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
            _json_block(labeled_nodes[:30]),
            "",
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Render a standalone Markdown report from a pipeline result JSON.")
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
