import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class WantGooReferenceTest(unittest.TestCase):
    """Lock the 2026/10/28 snapshot to the values published by WantGoo."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.report = json.loads(
            (ROOT / "snapshots" / "2026-10-28.json").read_text(encoding="utf-8")
        )
        cls.summary = cls.report["changeOverview"]["highLowAlignmentSummaryRows"][0]

    def test_futures_large_trader_values(self) -> None:
        row = self.summary
        self.assertEqual(row["futuresBuyTop5SpecificQty"] - row["futuresSellTop5SpecificQty"], 32727)
        self.assertEqual(row["futuresBuyTop5SpecificDay"] - row["futuresSellTop5SpecificDay"], 3250)
        self.assertEqual(row["futuresBuyTop10SpecificQty"] - row["futuresSellTop10SpecificQty"], 19903)
        self.assertEqual(row["futuresBuyTop10SpecificDay"] - row["futuresSellTop10SpecificDay"], 6178)

    def test_option_large_trader_values(self) -> None:
        expected = {
            "callBuyTop5SpecificQty": 2032,
            "callSellTop5SpecificQty": 2190,
            "putBuyTop5SpecificQty": 1580,
            "putSellTop5SpecificQty": 3438,
            "callBuyTop10SpecificQty": 2032,
            "callSellTop10SpecificQty": 3248,
            "putBuyTop10SpecificQty": 2229,
            "putSellTop10SpecificQty": 4838,
            "callBuyTop5SpecificDay": 1399,
            "callSellTop5SpecificDay": 82,
            "putBuyTop5SpecificDay": 26,
            "putSellTop5SpecificDay": 49,
            "callBuyTop10SpecificDay": 1000,
            "callSellTop10SpecificDay": 485,
            "putBuyTop10SpecificDay": 395,
            "putSellTop10SpecificDay": 56,
        }
        self.assertEqual({key: self.summary[key] for key in expected}, expected)

    def test_institutional_futures_net_open_interest(self) -> None:
        rows = {
            row["institution"]: row
            for row in self.report["tables"]["B"]["rows"]
            if row.get("product") == "臺股期貨"
        }
        self.assertEqual(rows["外資"]["oiNetQty"], -84917)
        self.assertEqual(rows["投信"]["oiNetQty"], 87179)
        self.assertEqual(rows["自營商"]["oiNetQty"], 483)


class WantGooOctober29ReferenceTest(unittest.TestCase):
    def test_same_date_and_maturity_match_128_published_values(self):
        reference = json.loads((ROOT / "tests/fixtures/wantgoo-2026-10-29.json").read_text(encoding="utf-8"))
        report = json.loads((ROOT / "snapshots/2026-10-29.json").read_text(encoding="utf-8"))
        self.assertEqual(report["meta"]["date"], reference["date"])
        checked = 0

        def check(actual, expected):
            nonlocal checked
            self.assertEqual(actual, expected)
            checked += 1

        futures = next(row for row in report["tables"]["C"]["historyRows"] if row["date"] == reference["date"])
        for suffix, values in (("Qty", reference["futures"]["qty"]), ("Day", reference["futures"]["day"])):
            for key, value in zip(reference["metricKeys"], values):
                check(futures[key + suffix], value)
        for index in range(4):
            buy, sell = reference["metricKeys"][index * 2:index * 2 + 2]
            check(futures[buy + "Qty"] - futures[sell + "Qty"], reference["futures"]["net"][index])
            check(futures[buy + "Day"] - futures[sell + "Day"], reference["futures"]["netDay"][index])
        for expected in reference["options"]:
            row = next(row for row in report["tables"]["D"]["largeTraderOptionHistoryRows"]
                       if row["date"] == reference["date"] and row["contractType"] == expected["contractType"]
                       and row["optionSide"] == expected["optionSide"])
            for suffix, values in (("Qty", expected["qty"]), ("Day", expected["day"])):
                for key, value in zip(reference["metricKeys"], values):
                    check(row[key + suffix], value)
            check(row["marketOi"], expected["marketOi"])
        for expected in reference["institutionalFutures"]:
            row = next(row for row in report["tables"]["B"]["rows"]
                       if row["product"] == "臺股期貨" and row["institution"] == expected["institution"])
            check(row["oiNetQty"], expected["net"])
            check(row["dayChangeOiNetQty"], expected["day"])
        for expected in reference["institutionalOptions"]:
            row = next(row for row in report["tables"]["D"]["institutionHistoryRows"]
                       if row["date"] == reference["date"] and row["institution"] == expected["institution"]
                       and row["optionSide"] == expected["optionSide"])
            for key, value in zip(reference["institutionOptionKeys"], expected["values"]):
                check(row[key], value)
        self.assertEqual(checked, 128)


if __name__ == "__main__":
    unittest.main()
