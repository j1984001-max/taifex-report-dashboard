"""One-time admin setup. Only creates missing, secret-free TAIFEX wait timers."""
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timedelta

from high_low_standby import REPO, TAIPEI, is_business_day


def gh(*args):
    return json.loads(subprocess.check_output(["gh", "api", *args], text=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="create missing timers after inspection")
    args = parser.parse_args()
    if gh(f"repos/{REPO}")["private"]:
        raise SystemExit("Expected a public repository with free environment timers.")
    start = datetime.now(TAIPEI).date()
    end = start + timedelta(days=366)
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    trading = [d for d in days if is_business_day(d)]
    gaps = {(b - a).days for a, b in zip(trading, trading[1:])}
    hours = sorted({24 * gap - offset for gap in gaps for offset in range(1, 6)})
    path = f"repos/{REPO}/environments"
    pages = gh("--paginate", "--slurp", path + "?per_page=100")
    existing = {v["name"]: v for page in pages for v in page["environments"]}
    for h in hours:
        name = f"taifex-wake-{h}h"
        config = existing.get(name)
        if config is None and args.apply:
            config = gh("--method", "PUT", f"{path}/{name}", "-F", f"wait_timer={60*h}")
        if config is not None and not any(r.get("wait_timer") == 60*h
                                          for r in config.get("protection_rules", [])):
            raise RuntimeError(f"Unexpected protection on {name}; not modifying it.")
        print(name, "verified" if config else "missing", flush=True)


if __name__ == "__main__":
    main()
