"""planner / aggregator 公开 API 可从包顶层 import。"""

from __future__ import annotations

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))


class TestPublicAPI(unittest.TestCase):
    def test_planner_exports(self):
        import retrieval_protocol as rp
        for name in ("split_budget", "build_plan", "RetrievalTask", "Plan"):
            self.assertTrue(hasattr(rp, name), f"missing {name}")
            self.assertIn(name, rp.__all__, f"{name} not in __all__")

    def test_aggregator_exports(self):
        import retrieval_protocol as rp
        for name in ("Aggregator", "AggregatorHooks"):
            self.assertTrue(hasattr(rp, name), f"missing {name}")
            self.assertIn(name, rp.__all__, f"{name} not in __all__")


if __name__ == "__main__":
    unittest.main()
