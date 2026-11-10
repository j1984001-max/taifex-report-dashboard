import unittest

import server


class OiReleaseDateTest(unittest.TestCase):
    def test_yesterdays_optional_oi_table_does_not_relabel_today(self):
        table = [["日期", "本日", "前一日", "前值", "增減"], ["2026/11/09", "100", "2026/11/06", "95", "5"]]
        row = server.parse_oi_change(table, "2026/11/10")
        self.assertEqual(row["date"], "2026/11/10")
        self.assertEqual(row["sourceDate"], "2026/11/09")
        self.assertTrue(row["missing"])
        self.assertIsNone(row["currentOi"])
        self.assertIsNone(row["previousDate"])
        self.assertIsNone(row["change"])

    def test_current_official_oi_values_are_preserved(self):
        table = [[], ["2026/11/10", "100", "2026/11/09", "95", "5"]]
        row = server.parse_oi_change(table, "2026/11/10")
        self.assertFalse(row["missing"])
        self.assertEqual((row["currentOi"], row["previousOi"], row["change"]), (100, 95, 5))

    def test_unpublished_or_empty_table_stays_missing_not_zero(self):
        for table in ([], [[]], [[], ["無資料"]]):
            with self.subTest(table=table):
                row = server.parse_oi_change(table, "2026/11/10")
                self.assertEqual(row["date"], "2026/11/10")
                self.assertIsNone(row["currentOi"])
                self.assertIsNone(row["change"])


if __name__ == "__main__":
    unittest.main()
