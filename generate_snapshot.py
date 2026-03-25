#!/usr/bin/env python3
from __future__ import annotations

import argparse

from server import PUBLIC_BASE_URL, build_report_pdf, cached_report, save_snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate and persist a daily TAIFEX report snapshot.")
    parser.add_argument("--date", help="Target report date in YYYY/MM/DD. Defaults to latest available business day.")
    args = parser.parse_args()

    report_date = args.date
    report_url = f"{PUBLIC_BASE_URL}/?date={report_date}" if report_date else PUBLIC_BASE_URL
    report, _ = cached_report(report_date, report_url, force_refresh=True)
    pdf_data = build_report_pdf(report)
    save_snapshot(report["meta"]["date"], report, pdf_data)
    print(f"snapshot_date={report['meta']['date']}")


if __name__ == "__main__":
    main()
