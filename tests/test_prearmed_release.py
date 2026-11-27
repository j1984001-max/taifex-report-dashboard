import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / ".github/scripts"))
import arm_release as arm
import high_low_standby as standby


def moment(day=27, hour=16, minute=39, month=11):
    return datetime(2026, month, day, hour, minute, tzinfo=arm.TAIPEI)


def receipt(now):
    return {"date": now.strftime("%Y/%m/%d"), "highLowTelegram": True,
            "highLowTelegramMessageIds": [1, 2, 3], "highLowTelegramItemCount": 3,
            "highLowTelegramDeliveredAt": now.isoformat()}


class PrearmTests(unittest.TestCase):
    def test_target_is_next_session_and_runner_wakes_before_release(self):
        for day, target_day in [(26, 27), (27, 30)]:
            for hour in range(15, 19):
                for minute in (0, 57, 59):
                    now = moment(day, hour, minute)
                    plan = arm.release_plan(now, receipt(now))
                    wake = now + timedelta(minutes=plan["minutes"])
                    self.assertEqual(plan["date"], f"2026/11/{target_day}")
                    self.assertTrue(standby.prearmed_window(wake, plan["date"]))
                    self.assertLess(wake.hour, 15)

    def test_no_duplicate_from_later_noop_and_no_old_or_partial_receipt(self):
        now = moment()
        state = receipt(now)
        active = {"run_started_at": (now - timedelta(minutes=3)).isoformat(),
                  "updated_at": now.isoformat()}
        self.assertIsNotNone(arm.release_plan(now, state, active))
        late = now + timedelta(minutes=3)
        self.assertIsNone(arm.release_plan(late, state, dict(active, run_started_at=late.isoformat())))
        for bad in [dict(state, date="2026/11/26"), dict(state, highLowTelegramMessageIds=[True, 2, 3])]:
            self.assertIsNone(arm.release_plan(now, bad))

    def test_holiday_weekend_and_delayed_runner(self):
        now = moment(24, month=12)
        self.assertEqual(arm.release_plan(now, receipt(now))["date"], "2026/12/28")
        self.assertIsNone(arm.release_plan(moment(28), receipt(moment(28))))
        self.assertFalse(standby.prearmed_window(moment(), "2026/11/30"))
        self.assertFalse(standby.prearmed_window(moment(30, 17), "2026/11/30"))

    def test_optional_weekly_requires_receipt_if_present(self):
        now = moment()
        state = receipt(now)
        for weekly, expected in [({}, True), ({"week": "W48"}, False)]:
            snap = {"meta": {"date": state["date"]}, "changeOverview": {"weeklyQuickView": weekly}}
            with patch.object(standby, "request_json", side_effect=[state, snap]):
                self.assertEqual(standby.delivery_is_complete(state["date"]), expected)
        with patch.object(standby, "request_json", return_value=dict(state, weeklyTelegram=True, weeklyTelegramMessageId=4)) as fetch:
            self.assertTrue(standby.delivery_is_complete(state["date"]))
            fetch.assert_called_once()
