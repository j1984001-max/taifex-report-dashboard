import contextlib
import importlib.util
import io
import json
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import send_daily_push

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("daily_push_workflow", ROOT / ".github/scripts/daily_push_workflow.py")
workflow = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(workflow)


class DailyPushWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = json.loads((ROOT / "snapshots/2026-10-29.json").read_text(encoding="utf-8"))
        cls.delivery = {
            "date": "2026/10/29", "complete": True, "highLowTelegram": True,
            "highLowTelegramItemCount": 3, "highLowTelegramMessageIds": [4227, 4228, 4229],
            "detailTelegram": True, "email": True,
        }

    def test_target_is_stable_across_the_reported_midnight_boundary(self):
        for now in ("2026-10-29T23:59:35+08:00", "2026-10-30T00:00:02+08:00"):
            with self.subTest(now=now):
                self.assertEqual(workflow.latest_released_business_day(datetime.fromisoformat(now)), "2026/10/29")

    def test_release_cutoff_weekends_and_market_holidays(self):
        cases = {
            "2026-10-30T14:59:59+08:00": "2026/10/29",
            "2026-10-30T15:00:00+08:00": "2026/10/30",
            "2026-10-31T16:00:00+08:00": "2026/10/30",
            "2026-11-02T09:00:00+08:00": "2026/10/30",
            "2026-10-09T16:00:00+08:00": "2026/10/08",
            "2026-10-29T16:00:02+00:00": "2026/10/29",
        }
        for now, expected in cases.items():
            with self.subTest(now=now):
                self.assertEqual(workflow.latest_released_business_day(datetime.fromisoformat(now)), expected)

    def test_verification_uses_frozen_target_without_recomputing_today(self):
        with mock.patch.object(workflow, "latest_released_business_day", side_effect=AssertionError("must use frozen date")), \
             mock.patch.object(workflow, "fetch_json", side_effect=[self.report, self.delivery, self.report]) as fetch:
            result = workflow.verify_published("2026/10/29")
        self.assertEqual(result["verifiedSnapshot"], "2026/10/29")
        self.assertTrue(result["deliveryComplete"])
        self.assertEqual(fetch.call_count, 3)
        for call in fetch.call_args_list:
            self.assertNotIn("2026-10-30", call.args[0])

    def test_missing_delivery_still_fails_verification(self):
        with mock.patch.object(workflow, "fetch_json", side_effect=[self.report, {"date": "2026/10/29"}]):
            with self.assertRaisesRegex(RuntimeError, "delivery record incomplete"):
                workflow.verify_published("2026/10/29")

    def test_explicit_completed_target_does_not_resend(self):
        output = io.StringIO()
        with mock.patch("sys.argv", ["send_daily_push.py", "--date", "2026/10/29"]), \
             mock.patch.dict(send_daily_push.os.environ, {"SKIP_EXISTING_DAILY_PUSH": "1"}), \
             mock.patch.object(send_daily_push, "load_delivery_state", return_value=self.delivery), \
             mock.patch.object(send_daily_push, "load_telegram_token", side_effect=AssertionError("must not send")), \
             mock.patch.object(send_daily_push, "cached_report", side_effect=AssertionError("must not rebuild")), \
             contextlib.redirect_stdout(output):
            send_daily_push.main()
        result = json.loads(output.getvalue())
        self.assertEqual(result["date"], "2026/10/29")
        self.assertEqual(result["reason"], "daily_delivery_already_complete")


if __name__ == "__main__":
    unittest.main()
