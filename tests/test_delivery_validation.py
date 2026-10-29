import copy
import json
import unittest
from pathlib import Path

from send_daily_push import report_is_ready


class DeliveryValidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[1] / "snapshots" / "2026-10-29.json"
        cls.report = json.loads(path.read_text(encoding="utf-8"))

    def test_verified_official_snapshot_passes(self):
        self.assertEqual(report_is_ready(self.report, mode="minimal", expected_date="2026/10/29"), (True, "ok"))

    def test_missing_daily_value_is_not_treated_as_zero(self):
        report = copy.deepcopy(self.report)
        report["changeOverview"]["highLowAlignmentSummaryRows"][0]["callBuyTop5SpecificDay"] = None
        ready, reason = report_is_ready(report, mode="minimal", expected_date="2026/10/29")
        self.assertFalse(ready)
        self.assertIn("callBuyTop5SpecificDay", reason)

    def test_mismatched_source_value_is_blocked(self):
        report = copy.deepcopy(self.report)
        report["changeOverview"]["highLowAlignmentSummaryRows"][0]["futuresBuyTop5SpecificQty"] += 1
        ready, reason = report_is_ready(report, mode="minimal", expected_date="2026/10/29")
        self.assertFalse(ready)
        self.assertIn("勾稽不符", reason)

    def test_wrong_total_is_blocked(self):
        report = copy.deepcopy(self.report)
        report["changeOverview"]["highLowAlignmentSummaryRows"][0]["highPointFuturesShortTotal"] += 1
        ready, reason = report_is_ready(report, mode="minimal", expected_date="2026/10/29")
        self.assertFalse(ready)
        self.assertIn("highPointFuturesShortTotal", reason)

    def test_stale_summary_date_is_blocked(self):
        report = copy.deepcopy(self.report)
        report["changeOverview"]["highLowAlignmentSummaryRows"][0]["date"] = "2026/10/28"
        ready, _ = report_is_ready(report, mode="minimal", expected_date="2026/10/29")
        self.assertFalse(ready)

    def test_cards_must_match_summary_table(self):
        report = copy.deepcopy(self.report)
        report["changeOverview"]["highLowAlignmentRows"][0]["foreignFuturesBuyDay"] += 1
        ready, reason = report_is_ready(report, mode="minimal", expected_date="2026/10/29")
        self.assertFalse(ready)
        self.assertIn("圖卡與彙整表", reason)


if __name__ == "__main__":
    unittest.main()
