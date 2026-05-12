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
}

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
    for line in frontmatter_text.splitlines():
        if ":" in line:
            key, _, value = line.partition(":")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            frontmatter[key] = value
    return frontmatter, body


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
) -> dict:
    """Assemble a full Router Card dict."""
    skill_md_hash = f"sha256:{sha256_hex(body_content)}"
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # routing_text
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
    """Return list of (skill_id, skill_dir) sorted by category then name."""
    results = []
    for category in ("custom", "public"):
        cat_dir = skills_root / category
        if not cat_dir.is_dir():
            continue
        for d in sorted(cat_dir.iterdir()):
            if d.is_dir() and (d / "SKILL.md").exists():
                results.append((d.name, d))
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
        description = frontmatter.get("description", "")

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
