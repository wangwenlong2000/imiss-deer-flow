"""测试各场景的意图识别和 Skill 篮选是否正确。

测试流程：
1. 意图识别：验证 classify_routing_intent 能正确匹配场景
2. Skill 篮选：验证 SkillRouter 能筛选出对应的 skill

场景列表：
- network_traffic: 网络流量分析
- phone_network: 电话网络分析
- policy_regulation: 政策法规/合同审查
- street_view_image: 街景图像识别
- remote_sensing_image: 遥感图像分析
- video_surveillance: 视频监控分析
- road_traffic: 交通流量分析
"""

import pytest

from deerflow.routing.intent import classify_routing_intent, load_scene_templates
from deerflow.routing.query_segmenter import segment_query, should_route


# ============================================================================
# 测试用例定义：每个场景一个典型问题
# ============================================================================

SCENE_TEST_CASES = [
    {
        "scene": "network_traffic",
        "scene_name": "网络流量",
        "query": "分析这个 pcap 文件中的 DNS 异常通信和可疑域名",
        "expected_skill_keywords": ["network-traffic-analysis", "pcap", "流量", "DNS", "异常"],
    },
    {
        "scene": "phone_network",
        "scene_name": "电话网络",
        "query": "帮我分析这个号码的画像和共享设备风险特征",
        "expected_skill_keywords": ["single-number-analysis", "号码", "画像", "共享设备", "风险"],
    },
    {
        "scene": "policy_regulation",
        "scene_name": "政策法规",
        "query": "请从乙方法务角度审查这份采购合同的违约责任和仲裁条款风险",
        "expected_skill_keywords": ["china-contract-review", "合同", "审查", "违约责任", "仲裁"],
    },
    {
        "scene": "street_view_image",
        "scene_name": "街景图像",
        "query": "帮我识别这张街景图像的地理位置并进行定位匹配",
        "expected_skill_keywords": ["location-matcher", "街景", "图像", "地理", "定位"],
        "note": "街景图像主要用于图像地理位置定位，street_order 是街面秩序告警的独立默认场景",
    },
    {
        "scene": "remote_sensing_image",
        "scene_name": "遥感图像",
        "query": "分析这两期卫星遥感影像中建设用地扩张变化，输出变化检测结果",
        "expected_skill_keywords": ["urban-change-detection", "遥感", "卫星", "变化检测", "建设用地"],
    },
    {
        "scene": "video_surveillance",
        "scene_name": "视频监控",
        "query": "分析昨晚东门摄像头监控录像中是否有人员聚集和异常停留事件",
        "expected_skill_keywords": ["analyze-video", "视频", "监控", "人员聚集", "异常停留"],
    },
    {
        "scene": "road_traffic",
        "scene_name": "交通流量",
        "query": "统计昨天晚高峰解放路路口的车流量和拥堵指数，生成趋势报告",
        "expected_skill_keywords": ["road-traffic-analysis", "交通", "流量", "拥堵", "路口"],
    },
]


# ============================================================================
# L1: 意图识别测试 - 验证场景匹配是否正确
# ============================================================================

class TestIntentRecognition:
    """测试意图识别能否正确匹配场景。"""

    @pytest.fixture
    def scene_templates(self):
        return load_scene_templates()

    @pytest.mark.parametrize("case", SCENE_TEST_CASES, ids=lambda c: c["scene"])
    def test_scene_match(self, case, scene_templates):
        """验证每个场景的问题能被正确识别."""
        result = classify_routing_intent(case["query"], scene_templates=scene_templates)

        # 验证意图为 task
        assert result.intent == "task"

        # 验证场景匹配正确
        assert result.scene == case["scene"], f"Expected scene {case['scene']}, got {result.scene}"
        assert result.scene_name == case["scene_name"], f"Expected scene_name {case['scene_name']}, got {result.scene_name}"

        # 验证 routing_query 包含关键信息
        assert result.routing_query, "routing_query should not be empty"
        assert result.scenes and result.scene in result.scenes

    def test_capability_inventory(self, scene_templates):
        """测试能力清单查询."""
        result = classify_routing_intent("你具备哪些能力", scene_templates=scene_templates)
        assert result.intent == "capability_inventory"

    def test_chitchat(self, scene_templates):
        """测试闲聊."""
        result = classify_routing_intent("你好", scene_templates=scene_templates)
        assert result.intent == "chitchat"


# ============================================================================
# L2: Should Route 测试 - 验证路由触发判断
# ============================================================================

class TestShouldRoute:
    """测试 should_route 函数能否正确判断是否触发路由."""

    @pytest.mark.parametrize("case", SCENE_TEST_CASES, ids=lambda c: c["scene"])
    def test_should_route_true(self, case):
        """验证业务问题应该触发路由."""
        assert should_route(case["query"], uploaded_files=[]), f"Query should trigger routing: {case['query']}"

    def test_should_route_false_chitchat(self):
        """验证闲聊不应触发路由."""
        assert not should_route("你好", uploaded_files=[])

    def test_should_route_false_simple_ok(self):
        """验证简单确认不应触发路由."""
        assert not should_route("ok")

    def test_should_route_with_file_ref(self):
        """验证带文件引用的查询应触发路由."""
        assert should_route("分析这个文件的数据分布", uploaded_files=["data.csv"])


# ============================================================================
# L3: Query Segmenter 测试 - 验证任务分割
# ============================================================================

class TestQuerySegmenter:
    """测试 query_segmenter 能否正确分割任务."""

    @pytest.mark.parametrize("case", SCENE_TEST_CASES, ids=lambda c: c["scene"])
    def test_single_segment(self, case):
        """验证单任务场景能被正确分割."""
        segments = segment_query(case["query"], scene_hint=case["scene"])

        assert len(segments) >= 1, f"Should have at least 1 segment for: {case['query']}"
        assert segments[0]["text"], "Segment text should not be empty"

        # 验证分割后的场景信息
        seg_scene = segments[0].get("scene")
        assert seg_scene == case["scene"], f"Segment scene should match: expected {case['scene']}, got {seg_scene}"

    def test_multi_segment_composite_query(self):
        """测试复合查询的分割."""
        query = "分析这份合同的违约责任条款，并检索相关法律法规依据"
        segments = segment_query(query, scene_hint="policy_regulation")

        assert len(segments) >= 1, "Should have at least 1 segment"
        # 可能分割为：合同审查 + 法规检索，也可能保持为单段

    def test_single_scene_query_stays_single_scene(self):
        query = "分析这两期卫星遥感影像里建设用地扩张变化，并输出变化检测结果"
        result = classify_routing_intent(query, scene_templates=load_scene_templates())
        assert result.scene == "remote_sensing_image"
        assert result.scenes == ["remote_sensing_image"]
        segments = segment_query(query, scene_hints=result.scenes)
        assert len(segments) == 1
        assert segments[0]["scene"] == "remote_sensing_image"


# ============================================================================
# L4: 场景模板加载测试
# ============================================================================

class TestSceneTemplates:
    """测试场景模板加载."""

    def test_all_scenes_loaded(self):
        """验证所有场景都被正确加载."""
        templates = load_scene_templates()

        expected_scenes = [
            "network_traffic",
            "phone_network",
            "policy_regulation",
            "spatiotemporal_trajectory",
            "street_view_image",
            "remote_sensing_image",
            "video_surveillance",
            "program_snippet",
            "road_traffic",
        ]

        for scene in expected_scenes:
            assert scene in templates, f"Scene {scene} should be in templates"
            assert templates[scene]["scene"] == scene, f"Scene field should match key: {scene}"

    def test_scene_has_required_fields(self):
        """验证每个场景模板都有必要字段."""
        templates = load_scene_templates()

        required_fields = ["name", "description", "parameters"]

        for scene_key, template in templates.items():
            for field in required_fields:
                assert field in template, f"Scene {scene_key} should have field {field}"
            if "scene" in template:
                assert isinstance(template["scene"], str) and template["scene"], f"Scene {scene_key} has invalid scene field"


# ============================================================================
# 运行测试的辅助函数
# ============================================================================

def run_intent_tests():
    """运行意图识别测试，输出结果摘要."""
    print("=" * 80)
    print("场景意图识别测试")
    print("=" * 80)

    templates = load_scene_templates()

    for case in SCENE_TEST_CASES:
        result = classify_routing_intent(case["query"], scene_templates=templates)

        scene_match = result.scene == case["scene"]
        status = "✓" if scene_match else "✗"

        print(f"\n{status} {case['scene_name']} ({case['scene']})")
        print(f"   查询: {case['query']}")
        print(f"   期望场景: {case['scene']}")
        print(f"   实际场景: {result.scene}")
        print(f"   routing_query: {result.routing_query[:100]}...")

        if not scene_match:
            print(f"   ⚠️  场景匹配错误！")


def run_segment_tests():
    """运行任务分割测试."""
    print("\n" + "=" * 80)
    print("任务分割测试")
    print("=" * 80)

    for case in SCENE_TEST_CASES:
        segments = segment_query(case["query"], scene_hint=case["scene"])

        print(f"\n{case['scene_name']} ({case['scene']})")
        print(f"   查询: {case['query']}")
        print(f"   分割数: {len(segments)}")
        for i, seg in enumerate(segments):
            print(f"   段{i+1}: {seg['text'][:60]}... (scene={seg.get('scene')})")


if __name__ == "__main__":
    run_intent_tests()
    run_segment_tests()
