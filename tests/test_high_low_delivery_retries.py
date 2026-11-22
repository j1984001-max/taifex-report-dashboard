import contextlib
import io
import unittest
from unittest import mock

import send_daily_push as push


class HighLowDeliveryRetryTest(unittest.TestCase):
    report = {"meta": {"date": "2026/11/18"}}
    shots = {"high_low_cards": b"png-cards", "high_low_summary": b"png-summary"}

    def test_retry_resumes_only_the_unsent_image(self):
        state = {}
        with mock.patch.object(push, "capture_report_screenshots", return_value=self.shots), \
             mock.patch.object(push, "save_delivery_state"), mock.patch.object(push, "publish_snapshot") as publish, \
             mock.patch.object(push, "send_telegram_message", return_value={"result": {"message_id": 1}}), \
             mock.patch.object(push, "send_telegram_document", side_effect=[{"result": {"message_id": 2}}, TimeoutError("transient")]):
            with self.assertRaises(TimeoutError):
                push.send_high_low_bundle(self.report, state, "test-token", "owner", ["text"])
            self.assertEqual(publish.call_count, 2)
        self.assertFalse(push.high_low_delivery_complete(state))
        self.assertEqual(state["highLowTelegramItems"], {"text_0": 1, "cards": 2})
        with mock.patch.object(push, "capture_report_screenshots", return_value=self.shots), \
             mock.patch.object(push, "save_delivery_state"), mock.patch.object(push, "publish_snapshot"), \
             mock.patch.object(push, "send_telegram_message", side_effect=AssertionError("text already delivered")), \
             mock.patch.object(push, "send_telegram_document", return_value={"result": {"message_id": 3}}) as send:
            push.send_high_low_bundle(self.report, state, "test-token", "owner", ["text"])
        self.assertEqual(send.call_count, 1)
        self.assertEqual(send.call_args.kwargs["data"], b"png-summary")
        self.assertEqual(state["highLowTelegramMessageIds"], [1, 2, 3])
        self.assertTrue(push.high_low_delivery_complete(state))

    def test_invalid_receipt_does_not_mark_an_item_delivered(self):
        state = {}
        with mock.patch.object(push, "capture_report_screenshots", return_value=self.shots), \
             mock.patch.object(push, "save_delivery_state") as save, \
             mock.patch.object(push, "send_telegram_message", return_value={"result": {"message_id": 0}}):
            with self.assertRaises(RuntimeError):
                push.send_high_low_bundle(self.report, state, "test-token", "owner", ["text"])
        save.assert_not_called()
        self.assertFalse(push.high_low_delivery_complete(state))

    def test_completed_legacy_bundle_does_not_render_or_resend(self):
        state = {"highLowTelegram": True, "highLowTelegramItemCount": 3, "highLowTelegramMessageIds": [1, 2, 3]}
        with mock.patch.object(push, "capture_report_screenshots", side_effect=AssertionError("already delivered")):
            self.assertEqual(push.send_high_low_bundle(self.report, state, "test-token", "owner", ["text"]), [])

    def test_automatic_build_is_pinned_to_the_expected_date_not_web_fallback(self):
        with mock.patch("sys.argv", ["send_daily_push.py", "--snapshot-only", "--max-retries", "0"]), \
             mock.patch.object(push, "latest_business_day", return_value="2026/11/20"), \
             mock.patch.object(push, "taifex_source_is_ready", return_value=(True, "ok")), \
             mock.patch.object(push, "cached_report", side_effect=ValueError("source unavailable")) as build, \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError):
                push.main()
        self.assertEqual(build.call_count, 1)
        self.assertEqual(build.call_args.args[0], "2026/11/20")


if __name__ == "__main__":
    unittest.main()
