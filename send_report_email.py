#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import smtplib
import ssl
import sys
import urllib.parse
import urllib.request
from email.message import EmailMessage


def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=120) as response:
        return json.load(response)


def fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as response:
        return response.read()


def build_email_body(report: dict, pdf_url: str) -> str:
    lines = [
        f"日期：{report['meta']['date']}",
        f"完整網頁：{report['meta'].get('reportUrl', '')}",
        f"PDF 下載：{pdf_url}",
        "",
        report["email"],
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Send TAIFEX report email with PDF attachment.")
    parser.add_argument("--base-url", default=os.environ.get("PUBLIC_BASE_URL", "https://taifex-report-dashboard.onrender.com"), help="Dashboard base URL")
    parser.add_argument("--date", default="", help="Report date in YYYY/MM/DD")
    parser.add_argument("--to", default=os.environ.get("GMAIL_TO", ""))
    parser.add_argument("--from-addr", default=os.environ.get("GMAIL_USER", ""))
    parser.add_argument("--app-password", default=os.environ.get("GMAIL_APP_PASSWORD", ""))
    args = parser.parse_args()

    if not args.to or not args.from_addr or not args.app_password:
        print("missing email settings: GMAIL_TO / GMAIL_USER / GMAIL_APP_PASSWORD", file=sys.stderr)
        return 2

    query = f"?date={urllib.parse.quote(args.date)}" if args.date else ""
    report_url = f"{args.base_url}/api/report{query}"
    pdf_url = f"{args.base_url}/api/report.pdf{query}"
    report = fetch_json(report_url)
    pdf_bytes = fetch_bytes(pdf_url)

    msg = EmailMessage()
    msg["From"] = args.from_addr
    msg["To"] = args.to
    msg["Subject"] = f"{report['meta']['date']} 台指期貨 / 選擇權籌碼完整報告"
    msg.set_content(build_email_body(report, pdf_url))
    filename = f"{report['meta']['date'].replace('/', '-')}-taifex-report.pdf"
    msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename=filename)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context, timeout=30) as smtp:
        smtp.login(args.from_addr, args.app_password)
        smtp.send_message(msg)

    print(f"sent {filename} to {args.to}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
