#!/usr/bin/env python3
"""Extract Router Cards from SKILL.md files.

Scans skills/ public and custom directories, parses SKILL.md frontmatter,
and generates router_card.json files per router_card.schema.json.

Usage:
    python scripts/extract_router_cards.py [--skills-root PATH]
"""

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


SCHEMA_VERSION = "1.0.0"
GENERATOR_VERSION = "0.1.0"
EMBEDDING_MODEL = "SkillRouter-Embedding-0.6B"


# ---------------------------------------------------------------------------
# Scene / task / type inference maps (manually curated per skill id)
# ---------------------------------------------------------------------------

# Each entry: {
#   "scenes": [...], "is_public": bool,
#   "task_types": [...], "input_types": [...], "output_types": [...],
#   "routing": {positive_triggers, negative_triggers, keywords, anti_keywords},
#   "execution": {required_tools, optional_tools, allowed_file_patterns,
#                 can_run_standalone, can_compose_with},
#   "routing_policy": {priority, conflict_group, prefer_when, defer_when}
# }

CUSTOM_SKILL_PROFILES = {
    "network-traffic-analysis": {
        "scenes": ["network_traffic"],
        "is_public": False,
        "task_types": [
            "pcap_parse", "protocol_analysis", "anomaly_detect",
            "domain_analysis", "security_report",
        ],
        "input_types": ["pcap", "pcapng", "cap", "csv"],
        "output_types": ["flow_csv", "domain_list", "anomaly_findings", "security_report"],
        "routing": {
            "positive_triggers": [
                "分析 pcap 文件中的异常通信",
                "识别网络流量中的可疑域名",
                "统计 DNS、HTTP、TLS 和 TCP 会话",
            ],
            "negative_triggers": [
                "分析整治台账合规风险",
                "检索政策法规条款",
                "分析车辆时空轨迹",
                "预测交通路网流量",
            ],
            "keywords": [
                "pcap", "pcapng", "流量", "DNS", "HTTP", "TLS",
                "异常通信", "可疑域名",
            ],
            "anti_keywords": ["法规", "政策", "台账", "轨迹", "交通流量"],
        },
        "execution": {
            "required_tools": ["read_file", "bash", "tshark"],
            "optional_tools": ["write_file", "chart_generator"],
            "allowed_file_patterns": ["*.pcap", "*.pcapng", "*.cap", "*.csv"],
            "can_run_standalone": True,
            "can_compose_with": ["data-analysis", "chart-visualization"],
        },
        "routing_policy": {
            "priority": 90,
            "conflict_group": "network_traffic_analysis",
            "prefer_when": [
                "用户上传 pcap/pcapng/cap 文件",
                "用户要求分析异常通信、协议行为、可疑域名或安全事件",
            ],
            "defer_when": [
                "任务只是通用 CSV 统计，应优先使用 data-analysis",
                "任务只是生成图表，应优先使用 chart-visualization",
            ],
        },
    },
    "law-regulations-rag": {
        "scenes": ["policy_regulation"],
        "is_public": False,
        "task_types": [
            "law_retrieval", "policy_retrieval",
            "legal_basis_mapping", "compliance_reference",
        ],
        "input_types": ["text", "docx", "pdf", "xlsx"],
        "output_types": ["law_articles", "policy_references", "legal_basis_mapping"],
        "routing": {
            "positive_triggers": [
                "查一下相关法律条文",
                "检索这个问题对应的政策依据",
                "判断台账处置措施有没有法规依据",
            ],
            "negative_triggers": [
                "分析 pcap 文件中的异常通信",
                "识别 DNS 可疑域名",
                "分析车辆轨迹异常停留",
                "预测道路交通流量",
            ],
            "keywords": ["法规", "政策", "条款", "法律", "依据", "合规", "台账", "通知"],
            "anti_keywords": ["pcap", "DNS", "HTTP", "轨迹", "交通流量"],
        },
        "execution": {
            "required_tools": ["read_file", "law_search"],
            "optional_tools": ["write_file"],
            "allowed_file_patterns": ["*.txt", "*.docx", "*.pdf", "*.xlsx"],
            "can_run_standalone": True,
            "can_compose_with": ["data-analysis", "chart-visualization"],
        },
        "routing_policy": {
            "priority": 90,
            "conflict_group": "policy_regulation_retrieval",
            "prefer_when": [
                "用户明确要求查询法规、政策条款或合规依据",
                "任务需要为台账或通知匹配法律政策依据",
            ],
            "defer_when": [
                "任务只是解析 Excel 表格，应优先使用 data-analysis",
                "任务只是绘制图表，应优先使用 chart-visualization",
            ],
        },
    },
    "location-matcher": {
        "scenes": ["street_view_image"],
        "is_public": False,
        "task_types": [
            "street_view_location_matching",
            "image_to_location",
            "place_description_search",
            "geocoding",
            "reverse_geocoding",
        ],
        "input_types": ["image", "jpg", "jpeg", "png", "text", "address", "coordinates"],
        "output_types": ["matched_location", "address", "latitude_longitude", "street_view_evidence"],
        "routing": {
            "positive_triggers": [
                "根据街景照片判断拍摄地点",
                "通过建筑物、道路、商铺招牌或地标描述查找位置",
                "验证某个地址或经纬度是否存在街景图像",
                "根据图片中的建筑、路口或街道特征匹配真实地址",
            ],
            "negative_triggers": [
                "分析 pcap 网络流量文件",
                "预测道路交通流量",
                "检索政策法规条款",
                "处理程序代码片段",
            ],
            "keywords": [
                "街景", "街景图像", "街景照片", "照片在哪里拍的", "拍摄位置",
                "地址", "经纬度", "地标", "建筑物", "道路", "路口", "商铺招牌",
                "location", "street view", "geocoding", "reverse geocoding",
            ],
            "anti_keywords": ["pcap", "网络流量", "政策法规", "程序片段", "代码片段", "交通流量预测"],
        },
        "execution": {
            "required_tools": ["bash", "view_image"],
            "optional_tools": ["read_file", "write_file"],
            "allowed_file_patterns": ["*.jpg", "*.jpeg", "*.png", "*.webp", "*.txt", "*.json"],
            "can_run_standalone": True,
            "can_compose_with": ["data-analysis", "chart-visualization"],
        },
        "routing_policy": {
            "priority": 88,
            "conflict_group": "street_view_location_matching",
            "prefer_when": [
                "用户上传或引用街景、道路、建筑、门店、路口相关图片并询问地点",
                "用户根据文字描述、地标、招牌、道路信息或坐标查找街景位置",
            ],
            "defer_when": [
                "用户只是做通用图像生成时优先使用 image-generation",
                "用户要求识别占道、违停等城市治理问题但不需要定位地点时可使用其他街景图像分析 skill",
            ],
        },
    },
    "road-traffic-analysis": {
        "scenes": ["road_traffic"],
        "is_public": False,
        "task_types": [
            "realtime_traffic_query",
            "route_planning",
            "road_weather_query",
            "geocoding",
            "around_traffic_query",
            "csv_traffic_analysis",
            "traffic_forecasting",
            "traffic_anomaly_detection",
            "traffic_report_rag",
        ],
        "input_types": ["text", "address", "coordinates", "csv", "xlsx", "json"],
        "output_types": [
            "traffic_status",
            "route_plan",
            "forecast_result",
            "anomaly_findings",
            "traffic_report_evidence",
        ],
        "routing": {
            "positive_triggers": [
                "查询道路实时拥堵、路况、路线规划或周边交通",
                "分析道路交通流量 CSV 数据、历史趋势、峰谷规律或拥堵情况",
                "预测未来交通流量或检测异常交通流量",
                "检索西安交通年报中的拥堵、运行趋势或治理背景资料",
            ],
            "negative_triggers": [
                "分析 pcap 网络通信流量",
                "通过街景照片定位地点",
                "检索政策法规条款",
                "处理程序代码片段",
            ],
            "keywords": [
                "道路交通", "交通流量", "路况", "拥堵", "堵车", "实时交通",
                "路线规划", "周边交通", "交通预测", "异常流量", "车流量",
                "西安交通", "交通年报", "road traffic", "traffic flow",
            ],
            "anti_keywords": ["pcap", "DNS", "HTTP", "街景", "照片定位", "政策法规", "程序片段"],
        },
        "execution": {
            "required_tools": ["bash", "read_file"],
            "optional_tools": ["write_file", "chart_generator"],
            "allowed_file_patterns": ["*.csv", "*.xlsx", "*.json", "*.txt", "*.md"],
            "can_run_standalone": True,
            "can_compose_with": ["data-analysis", "chart-visualization"],
        },
        "routing_policy": {
            "priority": 90,
            "conflict_group": "road_traffic_analysis",
            "prefer_when": [
                "用户询问道路路况、拥堵、路线、周边交通、交通流量分析、预测或异常检测",
                "用户要求使用本地或上传的交通 CSV 数据做统计、趋势、预测或异常分析",
            ],
            "defer_when": [
                "用户说的网络流量是 pcap、DNS、HTTP、TLS 等通信流量时优先使用 network-traffic-analysis",
                "用户通过街景图像、建筑、招牌或道路照片找地点时优先使用 location-matcher",
            ],
        },
    },
}


def _program_snippet_profile(
    *,
    task_types: list[str],
    output_types: list[str],
    positive_triggers: list[str],
    keywords: list[str],
    priority: int = 82,
    can_compose_with: list[str] | None = None,
) -> dict:
    return {
        "scenes": ["program_snippet"],
        "is_public": False,
        "task_types": task_types,
        "input_types": ["code_snippet", "source_code", "python", "java", "cpp", "text"],
        "output_types": output_types,
        "routing": {
            "positive_triggers": positive_triggers,
            "negative_triggers": [
                "分析网络流量 pcap 文件",
                "查询道路交通流量",
                "检索政策法规条款",
                "处理时空轨迹数据",
            ],
            "keywords": keywords,
            "anti_keywords": ["pcap", "交通流量", "政策法规", "时空轨迹"],
        },
        "execution": {
            "required_tools": ["read_file", "bash"],
            "optional_tools": ["write_file"],
            "allowed_file_patterns": ["*.py", "*.java", "*.cpp", "*.cc", "*.cxx", "*.h", "*.hpp", "*.js", "*.ts", "*.txt", "*.md"],
            "can_run_standalone": True,
            "can_compose_with": can_compose_with or [],
        },
        "routing_policy": {
            "priority": priority,
            "conflict_group": "program_snippet_processing",
            "prefer_when": [
                "用户提供代码片段、函数、类或源码文件并要求静态分析或加工",
                "任务目标是 AST、格式校验、标准化、语义标签、IO、调用图、数据流、文档、示例或组件封装",
            ],
            "defer_when": [
                "用户要求代码安全合规扫描时优先使用 opengrep-compliance",
                "用户只是在本仓库检索源码上下文时优先使用 code-rag",
            ],
        },
    }


CUSTOM_SKILL_PROFILES.update({
    "code-processing-pipeline": _program_snippet_profile(
        task_types=["code_pipeline", "code_normalization", "semantic_tagging", "io_extraction", "component_packaging"],
        output_types=["component_json", "component_code", "metadata", "interface"],
        positive_triggers=[
            "端到端处理代码片段并生成组件",
            "把程序片段加工成带 metadata、接口描述、文档和示例的组件",
            "串联格式校验、标准化、语义路由、IO 提取、文档生成和组件封装",
        ],
        keywords=["程序片段", "代码片段", "CodeProcessingPipeline", "组件封装", "metadata", "接口描述", "process_snippet"],
        priority=92,
        can_compose_with=[
            "code-format-validation",
            "code-normalization",
            "code-intent-routing",
            "code-io-extraction",
            "component-docstring-generation",
            "component-example-generation",
        ],
    ),
    "code-format-validation": _program_snippet_profile(
        task_types=["code_format_validation", "syntax_check"],
        output_types=["validation_result", "syntax_error"],
        positive_triggers=["校验代码片段格式", "检查 Python 代码语法是否合法", "判断代码是否包含顶层函数或类"],
        keywords=["格式校验", "语法检查", "valid", "syntax", "code-format-validation"],
    ),
    "code-normalization": _program_snippet_profile(
        task_types=["code_normalization", "wrap_snippet_as_function"],
        output_types=["normalized_code"],
        positive_triggers=["将裸代码片段标准化为函数", "把程序片段包装成函数定义", "normalize_code"],
        keywords=["代码标准化", "裸代码", "函数定义", "normalize_code", "generated_function"],
    ),
    "code-intent-routing": _program_snippet_profile(
        task_types=["code_intent_routing", "semantic_intent_classification"],
        output_types=["code_intent", "semantic_tags"],
        positive_triggers=["识别代码片段的语义意图", "判断代码是 HTTP、数据处理、图像处理还是硬件 IO", "为程序片段生成 intent 和 tags"],
        keywords=["语义意图", "CodeSemanticRouter", "intent", "tags", "HTTP", "硬件 IO"],
    ),
    "code-metadata-tagging": _program_snippet_profile(
        task_types=["code_metadata_tagging", "semantic_labeling"],
        output_types=["metadata_tags"],
        positive_triggers=["为代码片段生成 metadata 标签", "给程序片段打语义标签", "生成可检索可过滤的代码标签"],
        keywords=["metadata", "标签", "语义标签", "code-metadata-tagging"],
    ),
    "code-io-extraction": _program_snippet_profile(
        task_types=["code_io_extraction", "interface_inference"],
        output_types=["input_variables", "output_variables", "interface"],
        positive_triggers=["从代码片段提取输入输出变量", "分析函数接口", "生成组件接口描述"],
        keywords=["输入输出", "IO 提取", "extract_io", "interface", "input variables", "output variables"],
    ),
    "component-docstring-generation": _program_snippet_profile(
        task_types=["docstring_generation", "component_documentation"],
        output_types=["docstring"],
        positive_triggers=["为代码组件生成 docstring", "根据接口和代码生成组件文档", "生成 Args 和 Returns 说明"],
        keywords=["docstring", "组件文档", "Args", "Returns", "documentation"],
    ),
    "component-example-generation": _program_snippet_profile(
        task_types=["usage_example_generation", "component_example"],
        output_types=["usage_example"],
        positive_triggers=["为代码组件生成调用示例", "生成 usage example", "根据组件接口生成示例代码"],
        keywords=["示例生成", "usage example", "调用示例", "example"],
    ),
    "code-to-ast-new": _program_snippet_profile(
        task_types=["ast_parse", "code_structure_extraction"],
        output_types=["ast_json", "code_structure"],
        positive_triggers=["把代码片段转换为 AST", "解析程序片段结构", "提取函数类和语法树"],
        keywords=["AST", "语法树", "代码结构", "code-to-ast"],
    ),
    "cpp-to-ast": _program_snippet_profile(
        task_types=["cpp_ast_parse", "cpp_structure_extraction"],
        output_types=["ast_json", "cpp_structure"],
        positive_triggers=["解析 C++ 代码片段 AST", "提取 C++ 函数类结构", "分析 cpp 语法树"],
        keywords=["C++", "cpp", "AST", "语法树"],
    ),
    "call-graph-extractor": _program_snippet_profile(
        task_types=["call_graph_extraction", "static_call_analysis"],
        output_types=["call_graph", "call_edges"],
        positive_triggers=["提取代码片段调用关系", "生成函数调用图", "分析调用者和被调用者"],
        keywords=["调用图", "调用关系", "call graph", "caller", "callee"],
    ),
    "dataflow-extractor": _program_snippet_profile(
        task_types=["dataflow_extraction", "static_dataflow_analysis"],
        output_types=["dataflow_edges", "variable_flow"],
        positive_triggers=["提取代码片段数据流", "分析变量流转关系", "生成 dataflow edges"],
        keywords=["数据流", "变量流转", "dataflow", "赋值", "return"],
    ),
    "business-flow-renderer": _program_snippet_profile(
        task_types=["business_flow_rendering", "mermaid_flowchart"],
        output_types=["mermaid", "business_flowchart"],
        positive_triggers=["将业务代码片段转换为流程图", "把 if else 业务逻辑渲染为 Mermaid", "生成非技术人员可读的业务流程图"],
        keywords=["业务流程图", "Mermaid", "if-else", "工单流转", "流程图"],
    ),
    "code-splitter-adapter": _program_snippet_profile(
        task_types=["code_splitting", "chunking"],
        output_types=["code_chunks", "chunk_metadata"],
        positive_triggers=["切分源码为代码片段", "对长代码做 chunk", "生成代码片段 metadata"],
        keywords=["代码切分", "chunk", "代码片段", "splitter"],
    ),
    "code-semantic-labeler": _program_snippet_profile(
        task_types=["semantic_labeling", "business_semantic_labeling"],
        output_types=["semantic_labels", "labeled_nodes"],
        positive_triggers=["给代码节点生成业务语义标签", "为代码片段做语义标注", "标注业务节点"],
        keywords=["语义标注", "业务语义", "semantic label", "labeled nodes"],
    ),
    "code-business-dag-analysis-pipeline": _program_snippet_profile(
        task_types=["business_dag_analysis", "python_code_dag"],
        output_types=["business_dag", "analysis_report"],
        positive_triggers=["分析 Python 代码业务 DAG", "识别业务节点和依赖边", "生成代码业务分析报告"],
        keywords=["业务 DAG", "业务依赖", "Python", "代码业务分析"],
        priority=88,
    ),
    "code-business-dag-analysis-pipeline-java": _program_snippet_profile(
        task_types=["business_dag_analysis", "java_code_dag"],
        output_types=["business_dag", "analysis_report"],
        positive_triggers=["分析 Java 代码业务 DAG", "识别 Java 业务节点和依赖边", "生成 Java 代码业务分析报告"],
        keywords=["业务 DAG", "Java", "业务依赖", "代码业务分析"],
        priority=88,
    ),
    "code-business-dag-analysis-pipeline-cpp": _program_snippet_profile(
        task_types=["business_dag_analysis", "cpp_code_dag"],
        output_types=["business_dag", "analysis_report"],
        positive_triggers=["分析 C++ 代码业务 DAG", "识别 C++ 业务节点和依赖边", "生成 C++ 代码业务分析报告"],
        keywords=["业务 DAG", "C++", "cpp", "业务依赖", "代码业务分析"],
        priority=88,
    ),
    "code-rag": _program_snippet_profile(
        task_types=["code_retrieval", "source_snippet_search"],
        output_types=["code_search_results", "line_numbered_snippets"],
        positive_triggers=["检索本地代码片段", "搜索源码符号和实现位置", "返回带行号的代码上下文"],
        keywords=["code_search", "代码检索", "源码检索", "snippet", "行号"],
        priority=78,
    ),
})

PUBLIC_SKILL_DEFAULTS = {
    "data-analysis": {
        "scenes": ["public"],
        "is_public": True,
        "task_types": ["data_exploration", "sql_query", "statistical_summary", "pivot_table"],
        "input_types": ["xlsx", "xls", "csv"],
        "output_types": ["csv", "json", "markdown", "statistical_summary"],
        "routing": {
            "positive_triggers": [
                "分析这个 Excel 表格",
                "统计 CSV 中的数据",
                "对这个表格做汇总分析",
            ],
            "negative_triggers": [
                "生成图表",
                "画一个图",
            ],
            "keywords": ["Excel", "CSV", "DuckDB", "SQL", "统计表", "汇总", "数据透视"],
            "anti_keywords": ["可视化", "图表", "画图"],
        },
        "execution": {
            "required_tools": ["read_file", "bash"],
            "optional_tools": ["write_file"],
            "allowed_file_patterns": ["*.xlsx", "*.xls", "*.csv"],
            "can_run_standalone": True,
            "can_compose_with": ["chart-visualization"],
        },
        "routing_policy": {
            "priority": 50,
            "conflict_group": "public_data_processing",
            "prefer_when": [
                "用户上传 Excel/CSV 并要求数据分析、统计汇总、SQL 查询",
            ],
            "defer_when": [
                "任务只是生成图表，应优先使用 chart-visualization",
            ],
        },
    },
    "chart-visualization": {
        "scenes": ["public"],
        "is_public": True,
        "task_types": ["chart_generation", "data_visualization"],
        "input_types": ["json", "csv", "text"],
        "output_types": ["chart_image", "png"],
        "routing": {
            "positive_triggers": [
                "画一个柱状图",
                "可视化这些数据",
                "生成折线图",
            ],
            "negative_triggers": [
                "分析数据",
                "统计汇总",
                "SQL 查询",
            ],
            "keywords": ["图表", "可视化", "柱状图", "折线图", "饼图", "散点图", "heatmap"],
            "anti_keywords": ["SQL", "DuckDB", "数据透视"],
        },
        "execution": {
            "required_tools": ["bash", "write_file"],
            "optional_tools": ["read_file"],
            "allowed_file_patterns": ["*.json", "*.csv"],
            "can_run_standalone": True,
            "can_compose_with": ["data-analysis"],
        },
        "routing_policy": {
            "priority": 50,
            "conflict_group": "public_visualization",
            "prefer_when": [
                "用户明确要求生成图表、可视化数据",
            ],
            "defer_when": [
                "任务只是数据统计分析，应优先使用 data-analysis",
            ],
        },
    },
}

# Default fallback for skills without a curated profile
DEFAULT_PUBLIC_PROFILE = {
    "scenes": ["public"],
    "is_public": True,
    "task_types": [],
    "input_types": [],
    "output_types": [],
    "routing": {
        "positive_triggers": [],
        "negative_triggers": [],
        "keywords": [],
        "anti_keywords": [],
    },
    "execution": {
        "required_tools": ["read_file", "bash"],
        "optional_tools": ["write_file"],
        "allowed_file_patterns": [],
        "can_run_standalone": True,
        "can_compose_with": [],
    },
    "routing_policy": {
        "priority": 50,
        "conflict_group": "public_default",
        "prefer_when": [],
        "defer_when": [],
    },
}


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_frontmatter(content: str) -> tuple[dict, str]:
    """Split YAML frontmatter from body. Returns (frontmatter_dict, body_text)."""
    content = content.lstrip("﻿")
    if not content.startswith("---"):
        return {}, content
    parts = content.split("---", 2)
    if len(parts) < 3:
        return {}, content
    frontmatter_text = parts[1].strip()
    body = parts[2].strip()

    frontmatter = {}
    lines = frontmatter_text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if ":" not in line:
            i += 1
            continue
        key, _, value = line.partition(":")
        key = key.strip()
        value = value.strip()
        if value in {">", ">-", "|", "|-"}:
            block_lines: list[str] = []
            i += 1
            while i < len(lines) and (lines[i].startswith(" ") or lines[i].startswith("\t") or not lines[i].strip()):
                block_lines.append(lines[i].strip())
                i += 1
            frontmatter[key] = " ".join(part for part in block_lines if part).strip()
            continue
        frontmatter[key] = value.strip('"').strip("'")
        i += 1
    return frontmatter, body


def infer_description_from_body(body: str) -> str:
    """Infer a concise description from Markdown body when frontmatter lacks one."""
    for line in body.splitlines():
        match = re.match(r"\s*-\s*\*\*描述\*\*\s*[:：]\s*(.+?)\s*$", line)
        if match:
            return match.group(1).strip()
    for line in body.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and not stripped.startswith("```"):
            return stripped[:240]
    return ""


def make_routing_text(
    name: str,
    description: str,
    scenes: list[str],
    task_types: list[str],
    input_types: list[str],
    output_types: list[str],
    positive_triggers: list[str],
    negative_triggers: list[str],
) -> str:
    """Generate standardized routing_text for embedding."""
    lines = []
    lines.append(f"名称：{name}")
    lines.append(f"描述：{description}")
    if scenes:
        lines.append(f"适用场景：{', '.join(scenes)}。")
    if task_types:
        lines.append(f"适用任务：{', '.join(task_types)}。")
    if input_types:
        lines.append(f"输入类型：{', '.join(input_types)}。")
    if output_types:
        lines.append(f"输出类型：{', '.join(output_types)}。")
    if positive_triggers:
        lines.append(f"适合使用：{'；'.join(positive_triggers)}。")
    if negative_triggers:
        lines.append(f"不适合使用：{'；'.join(negative_triggers)}。")
    return "\n".join(lines)


def clean_body_content(body: str, max_length: int = 5000) -> str:
    """Strip excessive whitespace, limit length for Reranker input."""
    lines = body.splitlines()
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if stripped:
            cleaned.append(stripped)
    text = "\n".join(cleaned)
    if len(text) > max_length:
        text = text[:max_length] + "..."
    return text


def build_router_card(
    skill_id: str,
    skill_name: str,
    skill_description: str,
    body_content: str,
    skill_dir: Path,
    skill_md_path: str,
    profile: dict,
    es_index: str = "",
    existing_card: dict | None = None,
) -> dict:
    """Assemble a full Router Card dict.

    If existing_card is provided, preserve its detailed configurations
    (routing triggers, keywords, task_types, evaluation, etc.) and only
    update incrementally computed fields (source, embedding, body).
    """
    skill_md_hash = f"sha256:{sha256_hex(body_content)}"
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # If existing card has detailed config, preserve it
    if existing_card and isinstance(existing_card, dict):
        preserved_fields = {
            "identity": existing_card.get("identity", {}),
            "scope": existing_card.get("scope", {}),
            "routing": existing_card.get("routing", {}),
            "execution": existing_card.get("execution", {}),
            "routing_policy": existing_card.get("routing_policy", {}),
            "evaluation": existing_card.get("evaluation", {}),
        }
        # Only update name/description if they were empty or generic
        if not preserved_fields["identity"].get("name") or preserved_fields["identity"].get("name") == skill_id:
            preserved_fields["identity"]["name"] = skill_name
        if not preserved_fields["identity"].get("description"):
            preserved_fields["identity"]["description"] = skill_description
        # Always update id to match skill_id
        preserved_fields["identity"]["id"] = skill_id
        # Update body content (always fresh)
        preserved_fields["body"] = {
            "source": "SKILL.md",
            "content": clean_body_content(body_content),
        }
        # Update source metadata
        preserved_fields["source"] = {
            "skill_dir": str(skill_dir.relative_to(skill_dir.anchor) if skill_dir.anchor else skill_dir),
            "skill_md_path": skill_md_path,
            "skill_md_hash": skill_md_hash,
            "generated_at": generated_at,
            "generator_version": GENERATOR_VERSION,
        }
        # Update embedding metadata
        r = preserved_fields.get("routing", {})
        routing_text = r.get("routing_text") or make_routing_text(
            name=skill_name,
            description=skill_description,
            scenes=preserved_fields.get("scope", {}).get("scenes", []),
            task_types=preserved_fields.get("scope", {}).get("task_types", []),
            input_types=preserved_fields.get("scope", {}).get("input_types", []),
            output_types=preserved_fields.get("scope", {}).get("output_types", []),
            positive_triggers=r.get("positive_triggers", []),
            negative_triggers=r.get("negative_triggers", []),
        )
        preserved_fields["embedding"] = {
            "model": EMBEDDING_MODEL,
            "text_hash": f"sha256:{sha256_hex(routing_text)}",
            "es_index": es_index,
            "es_doc_id": skill_id,
        }
        preserved_fields["schema_version"] = existing_card.get("schema_version", SCHEMA_VERSION)
        return preserved_fields

    # Fallback: build new card from profile
    r = profile.get("routing", {})
    routing_text = make_routing_text(
        name=skill_name,
        description=skill_description,
        scenes=profile.get("scenes", []),
        task_types=profile.get("task_types", []),
        input_types=profile.get("input_types", []),
        output_types=profile.get("output_types", []),
        positive_triggers=r.get("positive_triggers", []),
        negative_triggers=r.get("negative_triggers", []),
    )
    routing_text_hash = f"sha256:{sha256_hex(routing_text)}"
    body_cleaned = clean_body_content(body_content)

    exec_info = profile.get("execution", {})
    policy = profile.get("routing_policy", {})

    card = {
        "schema_version": SCHEMA_VERSION,
        "identity": {
            "id": skill_id,
            "name": skill_name,
            "description": skill_description,
        },
        "scope": {
            "scenes": profile.get("scenes", []),
            "is_public": profile.get("is_public", False),
            "task_types": profile.get("task_types", []),
            "input_types": profile.get("input_types", []),
            "output_types": profile.get("output_types", []),
        },
        "routing": {
            "routing_text": routing_text,
            "positive_triggers": r.get("positive_triggers", []),
            "negative_triggers": r.get("negative_triggers", []),
            "keywords": r.get("keywords", []),
            "anti_keywords": r.get("anti_keywords", []),
        },
        "body": {
            "source": "SKILL.md",
            "content": body_cleaned,
        },
        "execution": {
            "required_tools": exec_info.get("required_tools", []),
            "optional_tools": exec_info.get("optional_tools", []),
            "allowed_file_patterns": exec_info.get("allowed_file_patterns", []),
            "can_run_standalone": exec_info.get("can_run_standalone", True),
            "can_compose_with": exec_info.get("can_compose_with", []),
        },
        "routing_policy": {
            "priority": policy.get("priority", 50),
            "conflict_group": policy.get("conflict_group", ""),
            "prefer_when": policy.get("prefer_when", []),
            "defer_when": policy.get("defer_when", []),
        },
        "source": {
            "skill_dir": str(skill_dir.relative_to(skill_dir.anchor) if skill_dir.anchor else skill_dir),
            "skill_md_path": skill_md_path,
            "skill_md_hash": skill_md_hash,
            "generated_at": generated_at,
            "generator_version": GENERATOR_VERSION,
        },
        "embedding": {
            "model": EMBEDDING_MODEL,
            "text_hash": routing_text_hash,
            "es_index": es_index,
            "es_doc_id": skill_id,
        },
    }
    return card


def find_skill_dirs(skills_root: Path) -> list[tuple[str, Path]]:
    """Return list of (skill_id, skill_dir) sorted by category then name.

    Recursively scans custom/ and public/ directories to find nested skills.
    A skill directory is identified by presence of SKILL.md file.
    skill_id is always the directory name (not the full relative path).
    """
    results = []
    seen_ids: set[str] = set()  # Track skill_ids to detect conflicts
    for category in ("custom", "public"):
        cat_dir = skills_root / category
        if not cat_dir.is_dir():
            continue
        # Recursively find all directories containing SKILL.md
        for skill_md in cat_dir.rglob("SKILL.md"):
            skill_dir = skill_md.parent
            # Use directory name as skill_id (consistent with ES index)
            skill_id = skill_dir.name
            if skill_id in seen_ids:
                print(f"WARN: duplicate skill_id '{skill_id}' at {skill_dir}, skipping", file=sys.stderr)
                continue
            seen_ids.add(skill_id)
            results.append((skill_id, skill_dir))
    # Sort by skill_id
    results.sort(key=lambda x: x[0])
    return results


def main():
    parser = argparse.ArgumentParser(description="Extract Router Cards from SKILL.md files")
    parser.add_argument(
        "--skills-root",
        default=str(Path(__file__).resolve().parent.parent / "skills"),
        help="Root directory containing skills/ (default: repo root/skills)",
    )
    args = parser.parse_args()

    skills_root = Path(args.skills_root).resolve()
    if not skills_root.is_dir():
        print(f"ERROR: Skills root {skills_root} does not exist", file=sys.stderr)
        sys.exit(1)

    # Resolve ES index from env
    es_index = os.environ.get("SKILL_ROUTER_ES_INDEX", "citybrain-skill-router-cards")

    skill_dirs = find_skill_dirs(skills_root)
    if not skill_dirs:
        print("No skills found.", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning {len(skill_dirs)} skills under {skills_root}")

    for skill_id, skill_dir in skill_dirs:
        skill_md_path_rel = str(skill_dir.relative_to(skills_root.parent)) + "/SKILL.md"
        skill_md_file = skill_dir / "SKILL.md"

        # Determine category
        is_custom = "custom" in str(skill_dir)
        category = "custom" if is_custom else "public"

        print(f"  [{category}] {skill_id} ...", end=" ", flush=True)

        raw_content = skill_md_file.read_text(encoding="utf-8")
        frontmatter, body = parse_frontmatter(raw_content)

        name = frontmatter.get("name", skill_id)
        description = frontmatter.get("description", "") or infer_description_from_body(body)

        # Pick profile
        if is_custom:
            profile = CUSTOM_SKILL_PROFILES.get(skill_id, {})
            if not profile:
                print(f"WARN: no curated profile for custom skill {skill_id}, using defaults", file=sys.stderr)
                profile = {
                    "scenes": [skill_id],
                    "is_public": False,
                    "task_types": [],
                    "input_types": [],
                    "output_types": [],
                    "routing": {"positive_triggers": [], "negative_triggers": [], "keywords": [], "anti_keywords": []},
                    "execution": {"required_tools": ["read_file", "bash"], "optional_tools": ["write_file"],
                                  "allowed_file_patterns": [], "can_run_standalone": True, "can_compose_with": []},
                    "routing_policy": {"priority": 70, "conflict_group": skill_id.replace("-", "_"),
                                       "prefer_when": [], "defer_when": []},
                }
        else:
            profile = PUBLIC_SKILL_DEFAULTS.get(skill_id, dict(DEFAULT_PUBLIC_PROFILE))

        card = build_router_card(
            skill_id=skill_id,
            skill_name=name,
            skill_description=description,
            body_content=body,
            skill_dir=skill_dir,
            skill_md_path=skill_md_path_rel,
            profile=profile,
            es_index=es_index,
        )

        card_path = skill_dir / "router_card.json"
        card_path.write_text(
            json.dumps(card, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"-> {card_path}")

    print(f"\nDone. Generated {len(skill_dirs)} Router Cards.")


if __name__ == "__main__":
    main()
