#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Commit and push updated TAIFEX snapshots.")
    parser.add_argument("--date", help="Snapshot date in YYYY-MM-DD for commit message context.")
    args = parser.parse_args()

    if args.date:
        candidates = [
            ROOT / "snapshots" / f"{args.date}.json",
            ROOT / "snapshots" / f"{args.date}.pdf",
            ROOT / "snapshots" / f"{args.date}.delivery.json",
        ]
        targets = [str(path.relative_to(ROOT)) for path in candidates if path.exists()]
    else:
        targets = ["snapshots"]

    status = run(["git", "status", "--short", "--", *targets])
    if not status:
        print("no_snapshot_changes")
        return

    run(["git", "add", "--", *targets])
    date_text = args.date or "latest"
    subprocess.run(
        ["git", "commit", "-m", f"Update TAIFEX snapshot {date_text}"],
        cwd=ROOT,
        check=True,
    )
    run(["git", "push", "origin", "main"])
    print(f"pushed_snapshot={date_text}")


if __name__ == "__main__":
    main()
