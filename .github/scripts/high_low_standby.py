#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, time as datetime_time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


REPO = os.environ.get("GITHUB_REPOSITORY", "j1984001-max/taifex-report-dashboard")
WORKFLOW_ID = "high-low-fast-push.yml"
PUBLIC_DASHBOARD_URL = os.environ.get(
    "PUBLIC_DASHBOARD_URL",
    "https://taifex-report-dashboard.onrender.com",
).rstrip("/")
TAIPEI = ZoneInfo("Asia/Taipei")
ACTIVE_RUN_STATES = {"queued", "in_progress", "waiting", "requested", "pending"}
MIN_HIGH_LOW_TELEGRAM_ITEMS = 3
HOLIDAYS = {
    "2026/01/01", "2026/02/16", "2026/02/17", "2026/02/18", "2026/02/19",
    "2026/02/20", "2026/02/27", "2026/04/03", "2026/04/06", "2026/05/01",
    "2026/06/19", "2026/09/25", "2026/09/28", "2026/10/09", "2026/10/26", "2026/12/25",
    # TAIFEX 2027 market calendar (1150003597), including settlement-only weekdays.
    "2027/01/01", "2027/02/02", "2027/02/03", "2027/02/04", "2027/02/05",
    "2027/02/08", "2027/02/09", "2027/02/10", "2027/03/01", "2027/04/05",
    "2027/04/06", "2027/04/30", "2027/06/09", "2027/09/15", "2027/09/28",
    "2027/10/11", "2027/10/25", "2027/12/24",
}
LAST_DASHBOARD_WARM_AT: datetime | None = None


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


def prearmed_window(now: datetime, target_date: str) -> bool:
    return (now.strftime("%Y/%m/%d") == target_date and is_business_day(now)
            and datetime_time(13, 45) <= now.time() < datetime_time(16, 31))


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
    if not isinstance(payload, dict):
        return False
    message_ids = payload.get("highLowTelegramMessageIds")
    core_complete = bool(
        payload.get("date") == report_date
        and payload.get("highLowTelegram") is True
        and isinstance(message_ids, list)
        and len(message_ids) >= MIN_HIGH_LOW_TELEGRAM_ITEMS
        and payload.get("highLowTelegramItemCount") == len(message_ids)
        and all(type(message_id) is int and message_id > 0 for message_id in message_ids)
    )
    if not core_complete:
        return False
    if (payload.get("weeklyTelegram") is True and type(payload.get("weeklyTelegramMessageId")) is int
            and payload["weeklyTelegramMessageId"] > 0):
        return True
    snapshot = request_json(f"https://raw.githubusercontent.com/{REPO}/main/snapshots/{slug}.json"
                            f"?cache_bust={time.time_ns()}")
    return (snapshot.get("meta", {}).get("date") == report_date
            and not snapshot.get("changeOverview", {}).get("weeklyQuickView"))


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


def maybe_warm_public_dashboard(report_date: str) -> None:
    global LAST_DASHBOARD_WARM_AT
    now = datetime.now(TAIPEI)
    if now.time() < datetime_time(14, 40):
        return
    if LAST_DASHBOARD_WARM_AT and now - LAST_DASHBOARD_WARM_AT < timedelta(minutes=4):
        return
    LAST_DASHBOARD_WARM_AT = now
    url = f"{PUBLIC_DASHBOARD_URL}/api/report?{urllib.parse.urlencode({'date': report_date})}"
    request = urllib.request.Request(
        url,
        method="HEAD",
        headers={"Cache-Control": "no-cache", "User-Agent": "taifex-dashboard-prewarm"},
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status = response.status
        print(
            json.dumps(
                {
                    "dashboardWarm": True,
                    "status": status,
                    "seconds": round(time.monotonic() - started, 2),
                    "checkedAt": now.isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Dashboard warm-up failed; will retry: {type(exc).__name__}: {exc}", flush=True)


def keep_dashboard_warm_until(target: datetime, report_date: str) -> None:
    while datetime.now(TAIPEI) < target:
        maybe_warm_public_dashboard(report_date)
        remaining = (target - datetime.now(TAIPEI)).total_seconds()
        if remaining > 0:
            time.sleep(min(240, remaining))


def sleep_until(target: datetime, report_date: str) -> None:
    while True:
        now = datetime.now(TAIPEI)
        if now >= target:
            return
        maybe_warm_public_dashboard(report_date)
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
    target = datetime.combine(now.date(), datetime_time(15, 0), tzinfo=TAIPEI)
    cutoff = datetime_time(16, 31) if os.environ.get("STANDBY_TARGET_DATE") else datetime_time(18, 55)
    deadline = datetime.combine(now.date(), cutoff, tzinfo=TAIPEI)

    sleep_until(target, report_date)

    last_dispatch: datetime | None = None
    while True:
        now = datetime.now(TAIPEI)
        if now >= deadline:
            raise RuntimeError(f"No verified delivery before standby deadline: {report_date}")
        try:
            if delivery_is_complete(report_date):
                print(json.dumps({"delivered": report_date, "observedAt": now.isoformat()}), flush=True)
                return
            if fast_push_is_running(token):
                time.sleep(30)
                continue
            ready, reason = source_is_ready(report_date)
        except (OSError, ValueError) as exc:
            print(f"Transient release check error: {type(exc).__name__}; retrying", flush=True)
            time.sleep(30)
            continue
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

        cooldown_over = last_dispatch is None or now - last_dispatch >= timedelta(minutes=2)
        if ready and cooldown_over and datetime.now(TAIPEI) < deadline:
            try:
                dispatch_fast_push(token, report_date, "github-standby-source-ready")
                last_dispatch = now
            except Exception as exc:  # noqa: BLE001
                print(f"Dispatch failed; will retry: {type(exc).__name__}: {exc}", flush=True)

        time.sleep(30)


def main() -> None:
    token = os.environ.get("GH_TOKEN", "").strip()
    if not token:
        raise RuntimeError("GH_TOKEN is required")

    now = datetime.now(TAIPEI)
    report_date = now.strftime("%Y/%m/%d")
    pinned_date = os.environ.get("STANDBY_TARGET_DATE")
    if pinned_date and not prearmed_window(now, pinned_date):
        raise RuntimeError(f"Prearmed runner outside target window: {pinned_date}, now {now.isoformat()}")
    if not is_business_day(now):
        print(json.dumps({"skipped": True, "reason": "non_business_day", "date": report_date}), flush=True)
        return

    if now.time() >= datetime_time(18, 55) or now.hour < 10:
        print("Outside standby window; no dispatch.")
        return

    if os.environ.get("STANDBY_SMOKE_TEST") == "1":
        print(json.dumps({"smokeTest": True, "date": report_date, "dispatched": False}), flush=True)
        return

    try:
        complete = delivery_is_complete(report_date)
    except (OSError, ValueError):
        complete = False  # The watch checks the receipt again before dispatch.
    if complete:
        print(json.dumps({"skipped": True, "reason": "already_delivered", "date": report_date}), flush=True)
        if not pinned_date and datetime_time(14, 40) <= now.time() < datetime_time(16, 31):
            deadline = datetime.combine(now.date(), datetime_time(16, 31), tzinfo=TAIPEI)
            keep_dashboard_warm_until(deadline, report_date)
        return

    print(
        json.dumps(
            {
                "standbyActive": True,
                "date": report_date,
                "cloudflareCronDegraded": cloudflare_cron_is_degraded(),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    run_release_watch(token, report_date, now)


if __name__ == "__main__":
    main()
