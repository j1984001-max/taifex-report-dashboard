import unittest
from unittest import mock

import server


class OptionHistoryTest(unittest.TestCase):
    def test_oldest_displayed_day_has_a_previous_day_baseline(self):
        current = {"institution": "外資", "optionSide": "call", "oiLongQty": 120, "oiShortQty": 90, "oiNetQty": 30}
        previous = {"institution": "外資", "optionSide": "call", "oiLongQty": 100, "oiShortQty": 90, "oiNetQty": 10}
        series = [{"date": "2026/10/29", "value": [current]}, {"date": "2026/10/28", "value": [previous]}]
        with mock.patch("server.fetch_business_day_series", return_value=series) as fetch:
            rows = server.build_institution_option_history_rows("2026/10/29", count=1)
        self.assertEqual(fetch.call_args.kwargs["count"], 2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["oiLongQtyDay"], 20)
        self.assertEqual(rows[0]["oiShortQtyDay"], 0)

    def test_missing_option_delta_stays_missing(self):
        row = {"date": "2026/10/29", "institution": "外資", "optionSide": "call", "oiLongQtyDay": None, "oiShortQtyDay": None}
        result = server.build_high_low_specific_alignment_rows([{"date": "2026/10/29"}], [], [], [], [row])[0]
        self.assertIsNone(result["foreignCallBuyDay"])
        self.assertIsNone(result["foreignCallSellDay"])
        self.assertIsNone(result["foreignCallNetDay"])


if __name__ == "__main__":
    unittest.main()
