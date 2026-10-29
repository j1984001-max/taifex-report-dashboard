#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, time as datetime_time
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from send_daily_push import taifex_source_is_ready


def main() -> None:
    parser = argparse.ArgumentParser(description="Check whether the required TAIFEX tables are complete.")
    parser.add_argument("--date", required=True, help="Expected report date in YYYY/MM/DD.")
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=5)
    args = parser.parse_args()

    for attempt in range(max(1, args.attempts)):
        try:
            ready, reason = taifex_source_is_ready(args.date)
        except Exception as exc:  # noqa: BLE001
            ready = False
            reason = f"readiness_check_failed: {type(exc).__name__}: {exc}"
        print(json.dumps({
            "date": args.date,
            "sourceReady": ready,
            "reason": reason,
            "attempt": attempt + 1,
        }, ensure_ascii=False), flush=True)
        if ready:
            break
        if attempt < args.attempts - 1:
            time.sleep(max(0, args.retry_delay))

    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        clean_reason = str(reason).replace("\r", " ").replace("\n", " ")
        with Path(output_path).open("a", encoding="utf-8") as output:
            output.write(f"ready={'true' if ready else 'false'}\n")
            output.write(f"reason={clean_reason}\n")

    if not ready and datetime.now(ZoneInfo("Asia/Taipei")).time() >= datetime_time(17, 45):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
