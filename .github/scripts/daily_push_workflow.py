#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, time as datetime_time, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from server import PUBLIC_BASE_URL, TW_TZ, latest_business_day
from send_daily_push import high_low_delivery_complete, load_delivery_state, report_is_ready


def latest_released_business_day(now: datetime | None = None) -> str:
    current = now or datetime.now(TW_TZ)
    current = current.replace(tzinfo=TW_TZ) if current.tzinfo is None else current.astimezone(TW_TZ)
    if current.time() < datetime_time(15, 0):
        current -= timedelta(days=1)
    return latest_business_day(current)


def resolve_target() -> dict:
    report_date = latest_released_business_day()
    delivery = load_delivery_state(report_date)
    complete = delivery.get("date") == report_date and delivery.get("complete") is True and high_low_delivery_complete(delivery)
    if complete:
        try:
            report = json.loads((ROOT / "snapshots" / f"{report_date.replace('/', '-')}.json").read_text(encoding="utf-8"))
            complete, _ = report_is_ready(report, mode="minimal", expected_date=report_date)
        except (OSError, ValueError, TypeError):
            complete = False
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with Path(output_path).open("a", encoding="utf-8") as output:
            output.write(f"date={report_date}\n")
            output.write(f"complete={'true' if complete else 'false'}\n")
    return {"date": report_date, "alreadyComplete": bool(complete)}


def fetch_json(url: str) -> dict:
    for attempt in range(3):
        request = urllib.request.Request(
            url,
            headers={"Cache-Control": "no-cache", "Pragma": "no-cache", "User-Agent": "taifex-daily-verifier"},
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return json.loads(response.read().decode("utf-8"))
        except (OSError, ValueError):
            if attempt == 2:
                raise
            time.sleep(2)
    raise RuntimeError("Snapshot fetch retry exhausted")


def verify_published(report_date: str) -> dict:
    datetime.strptime(report_date, "%Y/%m/%d")
    repo = os.environ.get("GITHUB_REPOSITORY", "j1984001-max/taifex-report-dashboard")
    slug = report_date.replace("/", "-")
    raw_base = f"https://raw.githubusercontent.com/{repo}/main/snapshots/{slug}"
    cache_bust = time.time_ns()
    report = fetch_json(f"{raw_base}.json?verify={cache_bust}")
    ready, reason = report_is_ready(report, mode="minimal", expected_date=report_date)
    if not ready:
        raise RuntimeError(f"Published snapshot {report_date} invalid: {reason}")
    rows = report["changeOverview"]["highLowAlignmentSummaryRows"]
    required = (
        "futuresBuyTop5SpecificCycle", "futuresSellTop5SpecificCycle",
        "futuresBuyTop10SpecificCycle", "futuresSellTop10SpecificCycle",
        "foreignFuturesBuyCycle", "foreignFuturesSellCycle",
    )
    for row in rows[:5]:
        for field in required:
            if row.get(field) is None:
                raise RuntimeError(f"Published snapshot {row.get('date')} missing {field}")
    delivery = fetch_json(f"{raw_base}.delivery.json?verify={cache_bust}")
    if delivery.get("date") != report_date or delivery.get("complete") is not True or not high_low_delivery_complete(delivery):
        raise RuntimeError(f"Published delivery record incomplete for {report_date}")

    site_warning = None
    try:
        api = fetch_json(f"{PUBLIC_BASE_URL}/api/report?{urllib.parse.urlencode({'date': report_date})}")
        if api.get("meta", {}).get("date") != report_date:
            site_warning = f"expected {report_date}, got {api.get('meta', {}).get('date')}"
    except (OSError, ValueError) as exc:
        site_warning = f"{type(exc).__name__}: {exc}"
    return {
        "verifiedSnapshot": report_date,
        "telegramMessageIds": delivery["highLowTelegramMessageIds"],
        "deliveryComplete": True,
        "siteWarning": site_warning,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve and verify one frozen TAIFEX daily delivery target.")
    parser.add_argument("command", choices=("target", "verify"))
    parser.add_argument("--date")
    args = parser.parse_args()
    if args.command == "verify" and not args.date:
        parser.error("verify requires the date resolved before the push")
    result = resolve_target() if args.command == "target" else verify_published(args.date)
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
