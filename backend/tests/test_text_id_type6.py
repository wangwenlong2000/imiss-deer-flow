"""Focused offline regression checks for the delivered text_id rule."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "packages/harness"))

from deerflow.compliance.detectors.delivered_rules.vendor.text_id import TextIdDetector


class TextIdType6Tests(unittest.TestCase):
    def test_chinese_adjacent_phone_and_explanation(self):
        result = TextIdDetector().detect({"content_text": "联系人高沐言，电话13600138024，负责失业登记补正。"})
        self.assertTrue(result["is_hit"])
        self.assertEqual(result["decision_source"], "pure_rule")
        relation = next(item for item in result["combination_scores"] if item["rule_id"] == "person-phone-relation")
        self.assertEqual(relation["score"], 7)
        self.assertEqual({entity["text"] for entity in relation["trigger_entities"]}, {"高沐言", "13600138024"})

    def test_generic_and_anonymized_boundaries(self):
        detector = TextIdDetector()
        examples = (
            "交通执法指南的联系人统一写为窗口值班人员，咨询方式为政务热线。",
            "财政年鉴备注写某单位负责预算审核，未出现具体人员信息。",
            "投诉人姓名已匿名化为张某，联系电话和详细住址均已删除。",
            "投诉人可拨打政务热线13800138000咨询案件复核流程，未列出个人姓名。",
        )
        for content in examples:
            with self.subTest(content=content):
                result = detector.detect({"content_text": content})
                self.assertFalse(result["is_hit"])
                self.assertEqual(result["decision_source"], "pure_rule")

    def test_llm_decision_is_separate_from_rule_evidence(self):
        class ReviewAdapter:
            def is_available(self):
                return True

            def review_text_id(self, sample, signals, score):
                return {"is_hit": True, "confidence": 0.9, "reason": "人工边界复核模拟", "risk_spans": []}

        result = TextIdDetector(use_llm=True, llm_adapter=ReviewAdapter()).detect(
            {"content_text": "边界文本", "sample_kind": "boundary"}
        )
        self.assertTrue(result["is_hit"])
        self.assertEqual(result["decision_source"], "llm_review")
        self.assertEqual(result["rule_score"], 0)


if __name__ == "__main__":
    unittest.main()
