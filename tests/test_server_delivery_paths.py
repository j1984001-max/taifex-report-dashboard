import json
import unittest
from unittest import mock

import server


class _Response:
    def __init__(self, payload: dict) -> None:
        self.body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def read(self) -> bytes:
        return self.body


class ServerDeliveryPathTest(unittest.TestCase):
    def setUp(self) -> None:
        server.REMOTE_SNAPSHOT_CACHE_MEMORY.clear()

    def test_latest_and_fallback_reports_are_not_browser_cached(self) -> None:
        latest = server.latest_business_day()
        self.assertEqual(server.report_cache_control({"meta": {"date": latest}}), "no-store")
        self.assertEqual(
            server.report_cache_control(
                {"meta": {"date": server.previous_business_day(latest), "fallbackReason": "not_ready"}}
            ),
            "no-store",
        )
        self.assertIn(
            "max-age=86400",
            server.report_cache_control({"meta": {"date": server.previous_business_day(latest)}}),
        )

    def test_remote_snapshot_is_validated_and_cached(self) -> None:
        report_date = "2026/10/29"
        payload = {
            "meta": {"date": report_date},
            "importantDates": {},
            "changeOverview": {"highLowAlignmentSummaryRows": [{"date": report_date}]},
            "tables": {
                "B": {"rows": [{"product": "臺股期貨", "oiLongQty": 1, "oiShortQty": 2, "oiNetQty": -1}]},
                "D": {"rows": [{"oiLongQty": 1, "oiShortQty": 2, "oiNetQty": -1}]},
            },
        }
        with mock.patch("server.urllib.request.urlopen", return_value=_Response(payload)) as urlopen:
            first = server.load_remote_snapshot(report_date, "https://example.test")
            second = server.load_remote_snapshot(report_date, "https://example.test")

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertEqual(first[0]["meta"]["reportUrl"], "https://example.test")
        self.assertEqual(urlopen.call_count, 1)


if __name__ == "__main__":
    unittest.main()
