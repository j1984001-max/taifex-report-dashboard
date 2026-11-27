"""Plan the next release from a proven fast-push receipt, without using cron."""
from __future__ import annotations

import json
import os
from datetime import datetime, time, timedelta
from pathlib import Path

from high_low_standby import REPO, TAIPEI, is_business_day, request_json


def release_plan(now, delivery, upstream=None):
    now = now.astimezone(TAIPEI)
    today = now.strftime("%Y/%m/%d")
    if not is_business_day(now) or not 15 <= now.hour < 19:
        return None
    ids = delivery.get("highLowTelegramMessageIds")
    if not (delivery.get("date") == today and delivery.get("highLowTelegram") is True
            and isinstance(ids, list) and len(ids) >= 3
            and all(type(i) is int and i > 0 for i in ids)
            and delivery.get("highLowTelegramItemCount") == len(ids)):
        return None
    sent = datetime.fromisoformat(delivery["highLowTelegramDeliveredAt"]).astimezone(TAIPEI)
    if sent.date() != now.date() or sent > now:
        return None
    if upstream:
        start, end = (datetime.fromisoformat(upstream[k].replace("Z", "+00:00"))
                      for k in ("run_started_at", "updated_at"))
        if not start <= sent <= end:
            return None  # A later successful no-op must not arm another timer.
    day = now + timedelta(days=1)
    while not is_business_day(day):
        day += timedelta(days=1)
    target = datetime.combine(day.date(), time(14, 58), tzinfo=TAIPEI)
    # Whole-hour timers are installed once by an administrator, never by CI.
    hours = int((target - now).total_seconds() // 3600)
    return {"date": day.strftime("%Y/%m/%d"), "environment": f"taifex-wake-{hours}h",
            "minutes": hours * 60}


def main():
    now = datetime.now(TAIPEI)
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    path = Path("snapshots") / f"{now:%Y-%m-%d}.delivery.json"
    plan = release_plan(now, json.loads(path.read_text()) if path.exists() else {},
                        event.get("workflow_run"))
    if not plan:
        print("No new same-day delivery to arm from.")
        return
    # GitHub silently creates unprotected environments referenced by name.
    # Fail before entering a job unless the expected wait rule really exists.
    config = request_json(f"https://api.github.com/repos/{REPO}/environments/{plan['environment']}",
                          token=os.environ["GH_TOKEN"])
    if not any(r.get("type") == "wait_timer" and r.get("wait_timer") == plan["minutes"]
               for r in config.get("protection_rules", [])):
        raise RuntimeError(f"Missing wait timer: {plan['environment']}")
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as out:
        print(f"date={plan['date']}\nenvironment={plan['environment']}", file=out)
    print(json.dumps(plan))


if __name__ == "__main__":
    main()
