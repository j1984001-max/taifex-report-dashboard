import copy
import json
import unittest
from pathlib import Path
from unittest import mock

import server
import send_daily_push as push

ROOT = Path(__file__).resolve().parents[1]


class WeeklyQuickViewTest(unittest.TestCase):
    def report(self, date="2026-10-30"):
        return json.loads((ROOT / f"snapshots/{date}.json").read_text(encoding="utf-8"))

    def make_view(self, current, previous, baseline):
        return server.build_weekly_quick_view(
            current["meta"]["date"], current["tables"]["C"]["rows"], current["tables"]["D"]["specificRows"],
            previous["tables"]["C"]["rows"], previous["tables"]["D"]["specificRows"],
            baseline["tables"]["C"]["rows"], baseline["tables"]["D"]["specificRows"],
            previous["meta"]["date"], previous["meta"]["date"],
        )

    def valid_report(self):
        report, previous = self.report(), self.report("2026-10-29")
        view = self.make_view(report, previous, previous)
        report["changeOverview"]["weeklyQuickView"] = view
        for group in view["groups"]:
            if group["key"] == "futures":
                continue
            row = next(row for row in report["tables"]["D"]["specificRows"]
                       if row["contractType"] == "weekly" and row["optionSide"] == group["key"])
            for metric in group["metrics"]:
                row[metric["key"] + "SpecificCycle"] = metric["cycle"]
        return report

    def test_monthly_response_cannot_overwrite_weekly_baseline(self):
        weekly = {"contractType": "weekly", "optionSide": "call", "longTop10SpecificQty": 689}
        wrong = {**weekly, "longTop10SpecificQty": 826}
        monthly = {"contractType": "monthly", "optionSide": "call", "longTop10SpecificQty": 1008}
        result = server.large_option_cycle_map({"weekly": [weekly, monthly], "monthly": [wrong, monthly]})
        self.assertEqual(result[("weekly", "call")]["longTop10SpecificQty"], 689)
        self.assertEqual(result[("monthly", "call")], monthly)

    def test_friday_cycles_use_thursday_not_monthly_baseline(self):
        report = self.valid_report()
        self.assertEqual(push.weekly_view_is_ready(report), (True, "ok"))
        call = next(group for group in report["changeOverview"]["weeklyQuickView"]["groups"] if group["key"] == "call")
        top10 = next(item for item in call["metrics"] if item["key"] == "longTop10")
        self.assertEqual((top10["qty"], top10["previousQty"], top10["day"], top10["cycle"]), (2359, 689, 1670, 1670))

    def test_thursday_baseline_starts_at_zero_not_previous_month(self):
        current, previous = self.report("2026-10-29"), self.report("2026-10-28")
        view = self.make_view(current, previous, current)
        self.assertTrue(all(metric["cycle"] == 0 for group in view["groups"] for metric in group["metrics"]))
        self.assertTrue(any(metric["day"] != 0 for group in view["groups"] for metric in group["metrics"]))

    def test_missing_baseline_is_not_zero(self):
        report = self.valid_report()
        view = server.build_weekly_quick_view("2026/10/30", [], [], [], [], [], [], None, None)
        self.assertTrue(all(metric["cycle"] is None for group in view["groups"] for metric in group["metrics"]))
        report["changeOverview"]["weeklyQuickView"] = view
        self.assertFalse(push.weekly_view_is_ready(report)[0])

    def test_reject_stale_tampered_or_incomplete_weekly_view(self):
        for change in (lambda v: v.update(date="2026/10/29"),
                       lambda v: v.update(baselineDate="2026/10/22"),
                       lambda v: v["groups"][0]["metrics"][0].update(cycle=999),
                       lambda v: v["groups"].pop()):
            report = self.valid_report()
            change(report["changeOverview"]["weeklyQuickView"])
            self.assertFalse(push.weekly_view_is_ready(report)[0])

    def test_weekly_send_deduplicates_independently_without_resending_monthly(self):
        report = self.valid_report()
        state = {"highLowTelegram": True, "highLowTelegramItemCount": 3, "highLowTelegramMessageIds": [1, 2, 3]}
        original = copy.deepcopy(state)
        with mock.patch.object(push, "send_telegram_document", return_value={"result": {"message_id": 42}}) as send, \
             mock.patch.object(push, "save_delivery_state") as save, mock.patch.object(push, "publish_snapshot"):
            push.send_weekly_screenshot(report, state, "test-token", "test-chat", image=b"png")
            self.assertTrue(push.weekly_delivery_complete(state))
            for key, value in original.items():
                self.assertEqual(state[key], value)
            self.assertIsNone(push.send_weekly_screenshot(report, state, "test-token", "test-chat", image=b"png"))
            self.assertEqual(send.call_count, 1)
            self.assertEqual(save.call_count, 1)

    def test_bad_telegram_reply_does_not_record_weekly_delivery(self):
        for message_id in (None, True, 0, -1, "42"):
            state = {}
            with mock.patch.object(push, "send_telegram_document", return_value={"result": {"message_id": message_id}}), \
                 mock.patch.object(push, "save_delivery_state") as save:
                with self.assertRaises(RuntimeError):
                    push.send_weekly_screenshot(self.valid_report(), state, "test-token", "test-chat", image=b"png")
                save.assert_not_called()
            self.assertFalse(push.weekly_delivery_complete(state))


if __name__ == "__main__":
    unittest.main()
