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
from send_daily_push import capture_report_screenshots, report_is_ready


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify official high-low data and render screenshots without sending.")
    parser.add_argument("--date", default="")
    parser.add_argument("--output", default="validation-artifacts")
    args = parser.parse_args()
    report_date = args.date.strip() or latest_business_day()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    report = build_report(report_date, fast_build=True)
    ready, reason = report_is_ready(report, mode="minimal", expected_date=report_date)
    if not ready:
        raise RuntimeError(reason)
    build_seconds = round(time.monotonic() - started, 2)
    print(json.dumps({"date": report_date, "buildSeconds": build_seconds, "dataValidated": True}), flush=True)

    screenshots = capture_report_screenshots(report_date, report=report, include_detail_sections=False)
    required = ("high_low_cards", "high_low_summary")
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
        "telegramSent": False,
    }
    (output / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
