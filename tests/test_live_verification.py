import contextlib
import importlib.util
import io
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("verify_high_low", ROOT / ".github/scripts/verify_high_low.py")
verify = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify)


class LiveVerificationTest(unittest.TestCase):
    def test_transient_official_timeout_retries_without_falling_back(self):
        report = {"meta": {"date": "2026/10/30"}}
        with mock.patch.object(verify, "build_report", side_effect=[TimeoutError("15s"), report]) as build, \
             mock.patch.object(verify, "report_is_ready", return_value=(True, "ok")), \
             mock.patch.object(verify, "weekly_view_is_ready", return_value=(True, "ok")), \
             mock.patch.object(verify.time, "sleep") as sleep, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(verify.build_verified_report("2026/10/30"), report)
        self.assertEqual(build.call_count, 2)
        sleep.assert_called_once_with(5)
        self.assertTrue(all(call.args == ("2026/10/30",) for call in build.call_args_list))

    def test_persistent_incomplete_weekly_data_still_fails(self):
        with mock.patch.object(verify, "build_report", return_value={}), \
             mock.patch.object(verify, "report_is_ready", return_value=(True, "ok")), \
             mock.patch.object(verify, "weekly_view_is_ready", return_value=(False, "weekly missing")), \
             mock.patch.object(verify.time, "sleep"), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, "weekly missing"):
                verify.build_verified_report("2026/10/30")


if __name__ == "__main__":
    unittest.main()
