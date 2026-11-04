import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


class ScreenshotWorkflowTest(unittest.TestCase):
    def test_browser_jobs_use_the_supported_pinned_runner(self):
        browser_jobs = []
        for path in sorted((ROOT / ".github/workflows").glob("*.yml")):
            workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
            for name, job in workflow.get("jobs", {}).items():
                steps = job.get("steps", [])
                if not any("playwright install" in step.get("run", "") for step in steps):
                    continue
                browser_jobs.append((path.name, name))
                with self.subTest(workflow=path.name, job=name):
                    self.assertEqual(job["runs-on"], "ubuntu-24.04")
                    for step in steps:
                        if step.get("uses", "").startswith("actions/cache@"):
                            key = step.get("with", {}).get("key", "")
                            if "playwright" in key:
                                self.assertIn("ubuntu-24.04", key)
                                self.assertIn("runner.arch", key)
        self.assertEqual(len(browser_jobs), 8)

    def test_completed_daily_deliveries_skip_browser_setup_and_cache(self):
        for name in ("daily-push.yml", "daily-push-watchdog.yml"):
            workflow = yaml.safe_load((ROOT / ".github/workflows" / name).read_text(encoding="utf-8"))
            for job in workflow["jobs"].values():
                steps = {step.get("name"): step for step in job.get("steps", [])}
                for step_name in ("Cache Playwright browsers", "Install screenshot dependencies"):
                    with self.subTest(workflow=name, step=step_name):
                        self.assertEqual(steps[step_name]["if"], "steps.target.outputs.complete != 'true'")


if __name__ == "__main__":
    unittest.main()
