import importlib.util
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock


PATH = Path(__file__).resolve().parents[1] / ".github/scripts/check_taifex_readiness.py"
SPEC = importlib.util.spec_from_file_location("readiness_gate", PATH)
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


class ReadinessGateTest(unittest.TestCase):
    def run_gate(self, hour):
        with mock.patch("sys.argv", ["check", "--date", "2026/10/29", "--attempts", "1"]), \
             mock.patch.object(gate, "taifex_source_is_ready", return_value=(False, "not_ready")), \
             mock.patch.object(gate, "datetime") as clock, \
             mock.patch.dict(gate.os.environ, {"GITHUB_OUTPUT": ""}):
            clock.now.return_value = datetime(2026, 10, 29, hour, 50)
            gate.main()

    def test_not_ready_during_release_window_is_a_normal_defer(self):
        self.run_gate(15)

    def test_not_ready_after_final_window_is_a_failure_for_alerting(self):
        with self.assertRaises(SystemExit) as result:
            self.run_gate(18)
        self.assertEqual(result.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
