# -*- coding: utf-8 -*-

"""
YiGraph 风格轻量意图路由器

目的：
- 不直接依赖 LLM
- 先用规则把问题路由到合适的 graph skill
- 后面如果需要，再把它升级成 LLM router
"""

import re
from typing import Dict, List


def _normalize(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"\s+", "", text)
    return text


def route_question(question: str) -> Dict:
    q = _normalize(question)

    # 1. 两点如何关联、路径、桥接
    if any(k in q for k in ["路径", "怎么连", "怎么联系", "桥接", "中间号", "怎么到达", "关联路径"]):
        return {
            "recommended_skill": "association-path-analysis",
            "recommended_query_type": "path_query",
            "reason": "问题核心是两点之间如何连接，优先走路径分析。",
            "next_skills": ["subgraph-extraction-analysis", "overlap-analysis"],
        }

    # 2. 共同联系人 / 共同对端 / 共享关系
    if any(k in q for k in ["共同对端", "共同联系人", "共同邻居", "共同号码", "重合联系人", "有没有共同"]):
        return {
            "recommended_skill": "overlap-analysis",
            "recommended_query_type": "common_neighbor",
            "reason": "问题核心是两个节点的邻居集合重叠。",
            "next_skills": ["association-path-analysis", "subgraph-extraction-analysis"],
        }

    # 3. 局部关系圈 / 周围关系 / 几跳
    if any(k in q for k in ["局部图", "子图", "关系圈", "周围关系", "几跳", "邻居展开"]):
        return {
            "recommended_skill": "subgraph-extraction-analysis",
            "recommended_query_type": "subgraph",
            "reason": "问题核心是围绕中心节点抽局部关系图。",
            "next_skills": ["association-path-analysis", "overlap-analysis"],
        }

    # 4. 单号码画像
    if any(k in q for k in ["这个号码", "号码画像", "节点信息", "号码信息", "这个号什么情况"]):
        return {
            "recommended_skill": "graph-operator",
            "recommended_query_type": "node_lookup",
            "reason": "问题核心是查询单个节点属性。",
            "next_skills": ["subgraph-extraction-analysis", "association-path-analysis"],
        }

    # 5. 排名 / 统计 / topk
    if any(k in q for k in ["top", "排名", "统计", "数量", "最多", "最活跃", "度最高", "计数"]):
        return {
            "recommended_skill": "graph-operator",
            "recommended_query_type": "aggregation_query",
            "reason": "问题核心是聚合统计或排名。",
            "next_skills": ["subgraph-extraction-analysis"],
        }

    return {
        "recommended_skill": "graph-operator",
        "recommended_query_type": "neighbor_query",
        "reason": "无法明确识别时，默认走基础邻居探索。",
        "next_skills": ["association-path-analysis", "subgraph-extraction-analysis"],
    }


def route_batch(questions: List[str]) -> List[Dict]:
    return [route_question(q) for q in questions]


if __name__ == "__main__":
    samples = [
        "这两个号码怎么连起来的？",
        "这两个号码有没有共同联系人？",
        "帮我抽一下这个号码的两跳子图",
        "这个号码的节点信息是什么？",
        "谁的度最高？",
    ]
    for q in samples:
        print("=" * 60)
        print("Q:", q)
        print(route_question(q))
