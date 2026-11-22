import unittest

import server
import send_daily_push


class SettlementRolloverTest(unittest.TestCase):
    def setUp(self):
        # Official TX daily-market rows for the November 18 settlement day.
        self.expiring = ["TX", "202611", "40950", "41071", "39385", "40025", "▲14", "▲0.03%",
                         "48144", "36931", "85075", "-", "13631", "40023", "40024", "50440", "37694"]
        self.active = ["TX", "202612", "41073", "41205", "39523", "40312", "▲205", "▲0.51%",
                       "8927", "74871", "83798", "40332", "123675", "40291", "40358", "50560", "27382"]

    def test_settlement_day_uses_the_contract_with_a_published_settlement(self):
        row = server.parse_tx_reference([["契約"], self.expiring, self.active])
        self.assertEqual((row["contract"], row["settlement"], row["high"], row["low"]),
                         ("202612", 40332, 41205, 39523))

    def test_normal_day_keeps_near_month(self):
        self.expiring[11] = "41478"
        self.assertEqual(server.parse_tx_reference([["契約"], self.expiring, self.active])["contract"], "202611")

    def test_unpublished_settlements_do_not_become_zero_price(self):
        for missing in ("", "-", "--", "0"):
            with self.subTest(missing=missing):
                self.active[11] = missing
                with self.assertRaises(ValueError):
                    server.parse_tx_reference([["契約"], self.expiring, self.active])

    def test_rollover_notification_explains_the_reset_not_zero_trading(self):
        report = {"meta": {"date": "2026/11/18", "reportUrl": "https://example.com"},
                  "changeOverview": {"highLowAlignmentRows": [{"date": "2026/11/18", "largeTraderContract": "202612"}]}}
        text = send_daily_push.build_high_low_focus_overview(report)
        self.assertIn("換月基準 202612", text)
        self.assertIn("非當日無交易", text)


if __name__ == "__main__":
    unittest.main()
