import json
import copy
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import wantgoo_reconcile as reconcile

ROOT = Path(__file__).resolve().parents[1]


def html_table(headers, rows):
    return "<table><thead><tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr></thead><tbody>" + "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows
    ) + "</tbody></table>"


class WantGooParserTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reference = json.loads((ROOT / "tests/fixtures/wantgoo-2026-10-29.json").read_text(encoding="utf-8"))

    def futures_html(self):
        expected = self.reference["futures"]
        values = [f"{qty:,}({day:+,})" for qty, day in zip(expected["qty"], expected["day"])]
        return html_table(["日期", "前五大", "前十大", "前五特法", "前十特法"], [["2026/10/29", *values]])

    def options_html(self, contract="202611"):
        return html_table(["日期", "契約", "權別", "前五大", "前十大", "前五特法"], [
            ["2026/10/29", contract, "買權" if row["optionSide"] == "call" else "賣權",
             *[f"{qty:,}({day:+,})" for qty, day in zip(row["qty"], row["day"])], row["marketOi"]]
            for row in self.reference["options"] if row["contractType"] == ("weekly" if contract == "週契約" else "monthly")
        ])

    def test_futures_table_matches_independent_wantgoo_fixture(self):
        row = reconcile.parse_futures(self.futures_html(), "2026/10/29")
        self.assertEqual([row[key + "Qty"] for key in reconcile.METRICS], self.reference["futures"]["qty"])
        self.assertEqual([row[key + "Day"] for key in reconcile.METRICS], self.reference["futures"]["day"])

    def test_monthly_and_weekly_tables_have_distinct_contract_scopes(self):
        for kind, contract in (("monthly", "202611"), ("weekly", "週契約")):
            rows = reconcile.parse_options(self.options_html(contract), "2026/10/29", kind, "202611")
            for row in rows:
                expected = next(item for item in self.reference["options"]
                                if item["contractType"] == kind and item["optionSide"] == row["optionSide"])
                self.assertEqual([row[key + "Qty"] for key in reconcile.METRICS], expected["qty"])
                self.assertEqual([row[key + "Day"] for key in reconcile.METRICS], expected["day"])
                self.assertEqual(row["marketOi"], expected["marketOi"])

    def test_old_date_or_wrong_contract_never_becomes_today(self):
        with self.assertRaises(reconcile.SourceNotReady):
            reconcile.parse_futures(self.futures_html(), "2026/10/30")
        with self.assertRaises(reconcile.SourceNotReady):
            reconcile.parse_options(self.options_html(), "2026/10/29", "monthly", "202612")
        with self.assertRaises(reconcile.SourceNotReady):
            reconcile.parse_options(self.options_html("202611W2"), "2026/10/29", "weekly", "202611")

    def test_missing_change_is_not_zero_and_invalid_numbers_fail(self):
        self.assertEqual(reconcile.quantity_change("1,008()"), (1008, None))
        self.assertEqual(reconcile.quantity_change("0(+0)"), (0, 0))
        for value in ("缺資料", "--", "NaN", "1,000(錯誤)"):
            with self.subTest(value=value), self.assertRaises(reconcile.SourceNotReady):
                reconcile.quantity_change(value)

    def test_conflicting_duplicate_table_is_rejected(self):
        with self.assertRaises(reconcile.SourceNotReady):
            reconcile.parse_futures(self.futures_html() + self.futures_html().replace("83,647", "83,648"), "2026/10/29")

    def test_fingerprint_ignores_fetch_time_but_tracks_numeric_changes(self):
        original = {"date": "2026/10/29", "qty": 3, "fetchedAt": "a"}
        self.assertEqual(reconcile.fingerprint(original), reconcile.fingerprint({**original, "fetchedAt": "b"}))
        self.assertNotEqual(reconcile.fingerprint(original), reconcile.fingerprint({**original, "qty": 4}))


def report_reference(report):
    date = report["meta"]["date"]
    futures = next(row for row in report["tables"]["C"]["historyRows"] if row["date"] == date)
    options = [row for row in report["tables"]["D"]["largeTraderOptionHistoryRows"] if row["date"] == date]
    future_institutions = [row for row in report["tables"]["B"]["rows"] if row["product"] == "臺股期貨"]
    option_institutions = [row for row in report["tables"]["D"]["institutionHistoryRows"] if row["date"] == date]
    return {"date": date, "monthlyContract": report["tables"]["D"]["monthlyContract"],
            "futures": {"date": date, **{key: futures[key] for key in reconcile.LARGE_FIELDS}},
            "options": [{**row} for row in options],
            "futureInstitutions": [{"date": date, **row} for row in future_institutions],
            "optionInstitutions": [copy.deepcopy(row) for row in option_institutions], "sources": reconcile.URLS}


class WantGooCorrectionTest(unittest.TestCase):
    def setUp(self):
        self.report = json.loads((ROOT / "snapshots/2026-10-29.json").read_text(encoding="utf-8"))
        self.reference = report_reference(self.report)

    def test_exact_match_is_non_destructive(self):
        aligned, comparison = reconcile.align_report(self.report, self.reference)
        self.assertEqual(aligned, self.report)
        self.assertEqual(comparison["differences"], [])
        self.assertEqual(comparison["checkedCount"], 162)

    def test_futures_change_updates_source_cards_totals_and_keeps_original_immutable(self):
        expected = copy.deepcopy(self.reference)
        expected["futures"]["shortTop10SpecificDay"] += 1
        original = copy.deepcopy(self.report)
        aligned, comparison = reconcile.align_report(self.report, expected)
        self.assertEqual(len(comparison["differences"]), 1)
        self.assertEqual(self.report, original)
        before = original["changeOverview"]["highLowAlignmentSummaryRows"][0]
        after = aligned["changeOverview"]["highLowAlignmentSummaryRows"][0]
        self.assertEqual(after["highPointFuturesShortTotal"], before["highPointFuturesShortTotal"] + 1)
        self.assertEqual(after["futuresSellTop10SpecificCycle"], before["futuresSellTop10SpecificCycle"])
        self.assertEqual(after["futuresHigh"], before["futuresHigh"])

    def test_weekly_quantity_change_keeps_the_explicit_thursday_baseline(self):
        report = json.loads((ROOT / "snapshots/2026-10-30.json").read_text(encoding="utf-8"))
        reference = report_reference(report)
        source = next(row for row in reference["options"] if row["contractType"] == "weekly" and row["optionSide"] == "call")
        source["longTop10SpecificQty"] += 1
        source["longTop10SpecificDay"] += 1
        aligned, comparison = reconcile.align_report(report, reference)
        group = next(row for row in aligned["changeOverview"]["weeklyQuickView"]["groups"] if row["key"] == "call")
        metric = next(row for row in group["metrics"] if row["key"] == "longTop10")
        self.assertEqual((metric["qty"], metric["day"], metric["cycle"], metric["baselineQty"]), (2360, 1671, 1671, 689))
        self.assertEqual(len(comparison["differences"]), 2)

    def test_foreign_option_change_updates_both_history_and_raw_sources(self):
        reference = copy.deepcopy(self.reference)
        row = next(row for row in reference["optionInstitutions"] if row["institution"] == "外資" and row["optionSide"] == "call")
        row["oiShortQtyDay"] += 2
        aligned, _ = reconcile.align_report(self.report, reference)
        summary = aligned["changeOverview"]["highLowAlignmentSummaryRows"][0]
        self.assertEqual(summary["foreignCallSellDay"], row["oiShortQtyDay"])
        self.assertEqual(summary["foreignCallNetDay"], row["oiLongQtyDay"] - row["oiShortQtyDay"])

    def test_wrong_date_missing_institution_and_negative_positions_are_rejected(self):
        cases = [lambda r: r.update(date="2026/10/28"),
                 lambda r: r["optionInstitutions"].pop(),
                 lambda r: r["futures"].update(longTop5Qty=-1)]
        for mutate in cases:
            reference = copy.deepcopy(self.reference)
            mutate(reference)
            with self.assertRaises(reconcile.SourceNotReady):
                reconcile.align_report(self.report, reference)

    def test_unpublished_change_is_excluded_not_fabricated(self):
        self.reference["futures"]["longTop5Day"] = None
        aligned, comparison = reconcile.align_report(self.report, self.reference)
        self.assertEqual(aligned, self.report)
        self.assertEqual(comparison["checkedCount"], 161)
        self.assertEqual(comparison["skippedFields"], ["futures.longTop5Day"])

    def test_late_official_refresh_reapplies_verified_reference(self):
        self.reference["futures"]["longTop10SpecificDay"] += 1
        audit = {"date": "2026/10/29", "status": "corrected", "reference": self.reference,
                 "fingerprint": reconcile.fingerprint(self.reference), "summary": {"status": "corrected"}}
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "audit.json"
            path.write_text(json.dumps(audit), encoding="utf-8")
            with mock.patch.object(reconcile, "audit_path", return_value=path):
                result = reconcile.apply_saved_reference(self.report)
        self.assertEqual(reconcile.compare_reference(result, self.reference)["differences"], [])
        self.assertEqual(result["meta"]["wantgooReconciliation"]["status"], "corrected")

    def test_automatic_midnight_runs_skip_while_manual_dry_runs_remain_available(self):
        for text in ("2026-11-10T01:05:00+08:00", "2026-11-10T16:59:59+08:00", "2026-11-14T17:00:00+08:00"):
            self.assertFalse(reconcile.resolve_gate(None, False, True, datetime.fromisoformat(text))["run"])
        self.assertTrue(reconcile.resolve_gate(None, False, True, datetime.fromisoformat("2026-11-10T17:00:00+08:00"))["run"])
        self.assertTrue(reconcile.resolve_gate("2026/11/09", True, False, datetime.fromisoformat("2026-11-10T08:30:00+08:00"))["run"])

    def test_inconsistent_rankings_and_institution_amounts_never_publish(self):
        for mutate in (lambda r: r["futures"].update(longTop5Qty=9999999),
                       lambda r: r["optionInstitutions"][0].update(oiNetAmount=9999999)):
            reference = copy.deepcopy(self.reference)
            mutate(reference)
            with self.assertRaises(reconcile.SourceNotReady):
                reconcile.align_report(self.report, reference)

    def test_dry_run_cannot_publish_send_or_change_original_report(self):
        original = copy.deepcopy(self.report)
        with mock.patch.object(reconcile, "save_audit", side_effect=AssertionError("must not publish")), \
             mock.patch.object(reconcile, "notify_result", side_effect=AssertionError("must not send")):
            result = reconcile.reconcile_report(self.report, self.reference, dry_run=True)
        self.assertEqual(result["status"], "matched")
        self.assertFalse(result["telegramSent"])
        self.assertEqual(self.report, original)


class WantGooReportLoadTest(unittest.TestCase):
    def test_current_complete_snapshot_is_accepted(self):
        self.assertEqual(reconcile.load_current_report("2026/10/29")["meta"]["date"], "2026/10/29")

    def test_stale_snapshot_cannot_be_used_as_today(self):
        report = json.loads((ROOT / "snapshots/2026-10-29.json").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "snapshots").mkdir()
            (root / "snapshots/2026-10-30.json").write_text(json.dumps(report), encoding="utf-8")
            with mock.patch.object(reconcile, "ROOT", root), self.assertRaises(reconcile.SourceNotReady):
                reconcile.load_current_report("2026/10/30")

    def test_incomplete_current_weekly_view_is_rejected(self):
        with mock.patch("send_daily_push.weekly_view_is_ready", return_value=(False, "incomplete")), \
             self.assertRaises(reconcile.SourceNotReady):
            reconcile.load_current_report("2026/10/29")


class WantGooNotificationTest(unittest.TestCase):
    def test_completed_audit_skips_all_sends(self):
        audit = {"date": "2026/11/09", "fingerprint": "same", "status": "matched",
                 "notification": {"complete": True, "items": {"text": 42}}}
        with mock.patch.object(reconcile, "save_audit"), \
             mock.patch("send_daily_push.publish_snapshot"), \
             mock.patch("send_daily_push.load_telegram_token", side_effect=AssertionError("must not send")):
            reconcile.notify_result({}, copy.deepcopy(audit), audit)

    def test_partial_correction_resumes_only_unsent_images(self):
        audit = {"date": "2026/11/09", "fingerprint": "same", "status": "corrected",
                 "differences": [{"field": "test", "before": 1, "after": 2}],
                 "summary": {"checkedCount": 162, "skippedCount": 0}}
        shots = {key: b"png" for key in ("high_low_cards", "high_low_summary", "weekly_quick_view")}
        with mock.patch.object(reconcile, "save_audit"), mock.patch("send_daily_push.publish_snapshot"), \
             mock.patch("send_daily_push.load_telegram_token", return_value="test-token"), \
             mock.patch("send_daily_push.send_telegram_message", return_value={"result": {"message_id": 1}}) as send_text, \
             mock.patch("send_daily_push.capture_report_screenshots", return_value=shots), \
             mock.patch("send_daily_push.send_telegram_document", side_effect=[{"result": {"message_id": 2}}, TimeoutError("transient")]):
            with self.assertRaises(TimeoutError):
                reconcile.notify_result({}, audit, {})
            self.assertEqual(send_text.call_count, 1)
        previous = copy.deepcopy(audit)
        with mock.patch.object(reconcile, "save_audit"), mock.patch("send_daily_push.publish_snapshot"), \
             mock.patch("send_daily_push.load_telegram_token", return_value="test-token"), \
             mock.patch("send_daily_push.send_telegram_message", side_effect=AssertionError("text already delivered")), \
             mock.patch("send_daily_push.capture_report_screenshots", return_value=shots), \
             mock.patch("send_daily_push.send_telegram_document", side_effect=[{"result": {"message_id": 3}}, {"result": {"message_id": 4}}]) as send_image:
            reconcile.notify_result({}, audit, previous)
            self.assertEqual(send_image.call_count, 2)
        self.assertTrue(reconcile.audit_delivery_complete(audit, "2026/11/09"))

    def test_pending_read_cannot_downgrade_previously_verified_data(self):
        with mock.patch.object(reconcile, "load_audit", return_value={"status": "matched"}), \
             mock.patch.object(reconcile, "save_audit", side_effect=AssertionError("must preserve verified audit")), \
             mock.patch("send_daily_push.publish_snapshot", side_effect=AssertionError("must not publish")):
            reconcile.persist_pending("2026/11/09", "source temporarily unavailable", None)


if __name__ == "__main__":
    unittest.main()
