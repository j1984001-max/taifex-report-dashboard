import json
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
