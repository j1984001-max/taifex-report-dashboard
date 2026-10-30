#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from server import build_report, latest_business_day
from send_daily_push import capture_report_screenshots, report_is_ready, weekly_view_is_ready


def build_verified_report(report_date: str) -> dict:
    for attempt in range(3):
        try:
            report = build_report(report_date, fast_build=True)
            ready, reason = report_is_ready(report, mode="minimal", expected_date=report_date)
            if not ready:
                raise RuntimeError(reason)
            weekly_ready, weekly_reason = weekly_view_is_ready(report)
            if not weekly_ready:
                raise RuntimeError(weekly_reason)
            return report
        except (OSError, ValueError, RuntimeError) as exc:
            print(json.dumps({"date": report_date, "verificationBuildAttempt": attempt + 1,
                              "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False), flush=True)
            if attempt == 2:
                raise
            time.sleep(5)
    raise RuntimeError("Verification retry exhausted")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify official high-low data and render screenshots without sending.")
    parser.add_argument("--date", default="")
    parser.add_argument("--output", default="validation-artifacts")
    args = parser.parse_args()
    report_date = args.date.strip() or latest_business_day()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    report = build_verified_report(report_date)
    build_seconds = round(time.monotonic() - started, 2)
    print(json.dumps({"date": report_date, "buildSeconds": build_seconds, "dataValidated": True}), flush=True)

    screenshots = capture_report_screenshots(report_date, report=report, include_detail_sections=False, include_weekly=True)
    required = ("high_low_cards", "high_low_summary", "weekly_quick_view")
    for key in required:
        data = screenshots.get(key)
        if not data:
            raise RuntimeError(f"Screenshot missing: {key}")
        (output / f"{key}.png").write_bytes(data)
    result = {
        "date": report_date,
        "buildSeconds": build_seconds,
        "totalSeconds": round(time.monotonic() - started, 2),
        "screenshots": {key: len(screenshots[key]) for key in required},
        "currentRow": report["changeOverview"]["highLowAlignmentSummaryRows"][0],
        "weeklyQuickView": report["changeOverview"]["weeklyQuickView"],
        "telegramSent": False,
    }
    (output / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
