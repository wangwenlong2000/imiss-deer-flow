"""测试 SkillRouter 篮选能否正确匹配任务对应的 Skill。

测试流程：
1. 模拟 ES Top-K 返回的场景内候选 Skills
2. 模拟 Reranker 的重排序
3. 验证 Resolver 能否根据场景约束选出正确的 primary skill

重点测试多 skill 场景的区分度：
- policy_regulation (19 skills): 合同审查、专利分析、判决书分析、法律检索等
- phone_network (16 skills): 单号分析、TopN发现、关联路径、团伙识别等
"""

import json
from pathlib import Path


def load_router_cards(scene: str) -> list[dict]:
    """加载指定场景的所有 router_card."""
    base_dir = Path("/home/wwl/imiss-deer-flow-main/skills/custom")

    scene_paths = {
        "policy_regulation": "policies-regulations",
        "phone_network": "phone-network-analysis",
        "video_surveillance": "video_surveillance",
        "remote_sensing_image": "remote_sensing_image",
    }

    path = base_dir / scene_paths.get(scene, scene)
    cards = []

    for skill_dir in path.iterdir():
        if skill_dir.is_dir():
            rc_path = skill_dir / "router_card.json"
            if rc_path.exists():
                with open(rc_path, 'r') as fp:
                    d = json.load(fp)
                # 只返回属于该场景的 skill
                if scene in d.get('scope', {}).get('scenes', []):
                    cards.append({
                        'skill_id': d['identity']['id'],
                        'name': d['identity']['name'],
                        'description': d['identity']['description'],
                        'keywords': d.get('routing', {}).get('keywords', []),
                        'anti_keywords': d.get('routing', {}).get('anti_keywords', []),
                        'positive_triggers': d.get('routing', {}).get('positive_triggers', []),
                        'routing_text': d.get('routing', {}).get('routing_text', ''),
                        'scenes': d['scope']['scenes'],
                    })

    return cards


def simulate_keyword_match(query: str, cards: list[dict]) -> list[dict]:
    """模拟关键词匹配得分计算."""
    scored = []
    query_lower = query.lower()

    for card in cards:
        score = 0.0
        matched_keywords = []

        # 正关键词匹配加分
        for kw in card['keywords']:
            if kw.lower() in query_lower:
                score += 1.0
                matched_keywords.append(kw)

        # 反关键词匹配扣分
        for akw in card['anti_keywords']:
            if akw.lower() in query_lower:
                score -= 0.5

        scored.append({
            'skill_id': card['skill_id'],
            'score': score,
            'matched_keywords': matched_keywords,
            'card': card,
        })

    # 按分数排序
    scored.sort(key=lambda x: x['score'], reverse=True)
    return scored


def run_skill_differentiation_tests():
    """运行 Skill 区分度测试."""

    test_cases = [
        # policy_regulation 场景测试
        {
            "scene": "policy_regulation",
            "query": "审查这份采购合同的违约责任条款是否合理",
            "expected_primary": "china-contract-review",
            "expected_keywords": ["合同审查", "违约责任"],
        },
        {
            "scene": "policy_regulation",
            "query": "分析这个发明专利的权利要求保护范围",
            "expected_primary": "patent-analysis",
            "expected_keywords": ["专利", "权利要求"],
        },
        {
            "scene": "policy_regulation",
            "query": "帮我检索关于合同违约金调整的法规依据",
            "expected_primary": "law-local-retrieval",
            "expected_keywords": ["法规依据", "检索"],
        },
        {
            "scene": "policy_regulation",
            "query": "分析这份一审判决书的判决结果和上诉可能性",
            "expected_primary": "litigation-analysis",
            "expected_keywords": ["判决书", "上诉"],
        },
        {
            "scene": "policy_regulation",
            "query": "帮我写一份法律备忘录整理这些法规材料",
            "expected_primary": "legal-briefing",
            "expected_keywords": ["备忘录", "整理"],
        },

        # phone_network 场景测试
        {
            "scene": "phone_network",
            "query": "分析这个号码的画像和共享设备风险特征",
            "expected_primary": "single-number-analysis",
            "expected_keywords": ["号码", "画像", "共享设备"],
        },
        {
            "scene": "phone_network",
            "query": "帮我找出 Top10 高风险号码",
            "expected_primary": "topn-high-risk-discovery",
            "expected_keywords": ["Top10", "高风险"],
        },
        {
            "scene": "phone_network",
            "query": "这两个号码之间有没有关联路径",
            "expected_primary": "association-path-analysis",
            "expected_keywords": ["关联路径", "两个号码"],
        },
        {
            "scene": "phone_network",
            "query": "识别这组号码是否存在团伙聚类",
            "expected_primary": "gang-cluster-analysis",
            "expected_keywords": ["团伙", "聚类"],
        },
        {
            "scene": "phone_network",
            "query": "筛选夜间通话异常的号码",
            "expected_primary": "condition-based-screening",
            "expected_keywords": ["筛选", "夜间"],
        },
        {
            "scene": "phone_network",
            "query": "概览一下这个电话网络数据集",
            "expected_primary": "dataset-overview-analysis",
            "expected_keywords": ["概览", "数据集"],
        },

        # video_surveillance 场景测试
        {
            "scene": "video_surveillance",
            "query": "分析监控录像中的人员聚集和异常停留事件",
            "expected_primary": "analyze-video",
            "expected_keywords": ["监控录像", "人员聚集"],
        },

        # remote_sensing_image 场景测试
        {
            "scene": "remote_sensing_image",
            "query": "检测这两期卫星遥感影像的建设用地变化",
            "expected_primary": "urban-change-detection",
            "expected_keywords": ["变化检测", "建设用地"],
        },
        {
            "scene": "remote_sensing_image",
            "query": "提取遥感影像中的建筑物轮廓",
            "expected_primary": "building-footprint-extraction",
            "expected_keywords": ["建筑物", "提取"],
        },
    ]

    print("=" * 80)
    print("SkillRouter 篮选能力测试 - 多 Skill 场景区分度验证")
    print("=" * 80)

    passed = 0
    failed = 0

    for tc in test_cases:
        cards = load_router_cards(tc['scene'])
        scored = simulate_keyword_match(tc['query'], cards)

        if scored:
            top = scored[0]
            is_correct = top['skill_id'] == tc['expected_primary']
            status = "✓" if is_correct else "✗"

            if is_correct:
                passed += 1
            else:
                failed += 1

            print(f"\n{status} [{tc['scene']}] {tc['query'][:50]}...")
            print(f"   预期: {tc['expected_primary']}")
            print(f"   实际: {top['skill_id']} (score={top['score']})")
            print(f"   匹配关键词: {top['matched_keywords']}")

            if not is_correct:
                # 显示前3个候选供分析
                print(f"   前3候选:")
                for i, s in enumerate(scored[:3]):
                    print(f"     {i+1}. {s['skill_id']} (score={s['score']}, keywords={s['matched_keywords'][:3]})")
        else:
            failed += 1
            print(f"\n✗ [{tc['scene']}] {tc['query'][:50]}...")
            print(f"   无候选 Skill")

    print("\n" + "=" * 80)
    print(f"测试结果: {passed} 通过, {failed} 失败")
    print("=" * 80)


if __name__ == "__main__":
    run_skill_differentiation_tests()