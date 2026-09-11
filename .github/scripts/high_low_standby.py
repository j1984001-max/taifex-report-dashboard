#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, time as datetime_time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


REPO = os.environ.get("GITHUB_REPOSITORY", "j1984001-max/taifex-report-dashboard")
WORKFLOW_ID = "high-low-fast-push.yml"
TAIPEI = ZoneInfo("Asia/Taipei")
ACTIVE_RUN_STATES = {"queued", "in_progress", "waiting", "requested", "pending"}
HOLIDAYS = {
    "2026/01/01", "2026/02/16", "2026/02/17", "2026/02/18", "2026/02/19",
    "2026/02/20", "2026/02/27", "2026/04/03", "2026/04/06", "2026/05/01",
    "2026/06/19", "2026/09/25", "2026/10/09",
}


def request_json(url: str, *, token: str = "", data: dict[str, object] | None = None) -> object:
    headers = {
        "Accept": "application/vnd.github+json",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "User-Agent": "taifex-high-low-standby",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    encoded = None
    if data is not None:
        headers["Content-Type"] = "application/json"
        encoded = json.dumps(data).encode("utf-8")
    request = urllib.request.Request(url, data=encoded, headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read()
    return json.loads(body.decode("utf-8")) if body else {}


def is_business_day(now: datetime) -> bool:
    return now.weekday() < 5 and now.strftime("%Y/%m/%d") not in HOLIDAYS


def cloudflare_cron_is_degraded() -> bool:
    try:
        payload = request_json("https://www.cloudflarestatus.com/api/v2/incidents/unresolved.json")
        incidents = payload.get("incidents", []) if isinstance(payload, dict) else []
        return any(
            "cron trigger" in str(incident.get("name", "")).lower()
            and incident.get("status") not in {"resolved", "completed"}
            for incident in incidents
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Cloudflare status unavailable; enabling standby: {type(exc).__name__}: {exc}", flush=True)
        return True


def delivery_is_complete(report_date: str) -> bool:
    slug = report_date.replace("/", "-")
    url = (
        f"https://raw.githubusercontent.com/{REPO}/main/snapshots/{slug}.delivery.json"
        f"?cache_bust={time.time_ns()}"
    )
    try:
        payload = request_json(url)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise
    return bool(
        isinstance(payload, dict)
        and payload.get("date") == report_date
        and payload.get("highLowTelegram") is True
    )


def fast_push_is_running(token: str) -> bool:
    payload = request_json(
        f"https://api.github.com/repos/{REPO}/actions/workflows/{WORKFLOW_ID}/runs"
        "?branch=main&per_page=10",
        token=token,
    )
    runs = payload.get("workflow_runs", []) if isinstance(payload, dict) else []
    return any(run.get("status") in ACTIVE_RUN_STATES for run in runs)


def dispatch_fast_push(token: str, report_date: str, reason: str) -> None:
    request_json(
        f"https://api.github.com/repos/{REPO}/actions/workflows/{WORKFLOW_ID}/dispatches",
        token=token,
        data={
            "ref": "main",
            "inputs": {
                "trigger_reason": reason,
                "report_date": report_date,
            },
        },
    )
    print(json.dumps({"dispatched": report_date, "reason": reason}, ensure_ascii=False), flush=True)


def sleep_until(target: datetime, report_date: str) -> bool:
    while True:
        now = datetime.now(TAIPEI)
        if now >= target:
            return False
        if delivery_is_complete(report_date):
            print(json.dumps({"alreadyDelivered": report_date, "whileWaiting": True}), flush=True)
            return True
        remaining = max(1, int((target - now).total_seconds()))
        print(
            json.dumps(
                {
                    "waitingUntil": target.isoformat(timespec="seconds"),
                    "remainingMinutes": round(remaining / 60, 1),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        time.sleep(min(300, remaining))


def source_is_ready(report_date: str) -> tuple[bool, str]:
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root))
    from send_daily_push import taifex_source_is_ready

    return taifex_source_is_ready(report_date)


def run_release_watch(token: str, report_date: str, now: datetime) -> None:
    target = datetime.combine(now.date(), datetime_time(15, 1), tzinfo=TAIPEI)
    deadline = datetime.combine(now.date(), datetime_time(15, 41), tzinfo=TAIPEI)

    if sleep_until(target, report_date):
        return

    last_dispatch: datetime | None = None
    while True:
        now = datetime.now(TAIPEI)
        if delivery_is_complete(report_date):
            print(json.dumps({"delivered": report_date, "observedAt": now.isoformat()}), flush=True)
            return

        ready, reason = source_is_ready(report_date)
        print(
            json.dumps(
                {
                    "date": report_date,
                    "sourceReady": ready,
                    "reason": reason,
                    "checkedAt": now.isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

        cooldown_over = last_dispatch is None or now - last_dispatch >= timedelta(minutes=8)
        if ready and cooldown_over and not fast_push_is_running(token):
            try:
                dispatch_fast_push(token, report_date, "github-standby-source-ready")
                last_dispatch = now
            except Exception as exc:  # noqa: BLE001
                print(f"Dispatch failed; will retry: {type(exc).__name__}: {exc}", flush=True)

        if now >= deadline:
            print(json.dumps({"standbyDeadlineReached": report_date}), flush=True)
            return
        time.sleep(120)


def main() -> None:
    token = os.environ.get("GH_TOKEN", "").strip()
    if not token:
        raise RuntimeError("GH_TOKEN is required")

    now = datetime.now(TAIPEI)
    report_date = now.strftime("%Y/%m/%d")
    if not is_business_day(now):
        print(json.dumps({"skipped": True, "reason": "non_business_day", "date": report_date}), flush=True)
        return

    if os.environ.get("STANDBY_SMOKE_TEST") == "1":
        dispatch_fast_push(token, report_date, "github-standby-smoke-test")
        return

    if delivery_is_complete(report_date):
        print(json.dumps({"skipped": True, "reason": "already_delivered", "date": report_date}), flush=True)
        return

    if not cloudflare_cron_is_degraded():
        print(
            json.dumps(
                {"skipped": True, "reason": "cloudflare_cron_operational", "date": report_date},
                ensure_ascii=False,
            ),
            flush=True,
        )
        return

    print(json.dumps({"standbyActive": True, "date": report_date}), flush=True)
    run_release_watch(token, report_date, now)


if __name__ == "__main__":
    main()
