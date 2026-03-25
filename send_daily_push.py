#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import ssl
import smtplib
import urllib.parse
import urllib.request
from email.message import EmailMessage
from pathlib import Path

from server import PUBLIC_BASE_URL, build_report_pdf, cached_report, save_snapshot


TELEGRAM_LIMIT = 3500
DEFAULT_TELEGRAM_CHAT_ID = "7154157141"


def parse_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def load_telegram_token() -> str:
    config_path = Path.home() / ".openclaw" / "openclaw.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    return config["channels"]["telegram"]["botToken"]


def split_telegram_text(text: str, limit: int = TELEGRAM_LIMIT) -> list[str]:
    chunks: list[str] = []
    remaining = text.strip()
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        cut = remaining.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = limit
        chunks.append(remaining[:cut].strip())
        remaining = remaining[cut:].strip()
    return chunks


def send_telegram_message(bot_token: str, chat_id: str, text: str) -> dict[str, object]:
    payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def build_quick_overview(report: dict[str, object]) -> str:
    meta = report["meta"]
    overview = report["changeOverview"]
    lines = [
        f"{meta['date']} 台指籌碼速覽",
        f"完整網頁：{meta['reportUrl']}",
        f"PDF：{PUBLIC_BASE_URL}/api/report.pdf?date={meta['date']}",
        "",
        "期貨差異變動速覽",
    ]
    lines.extend(f"- {item}" for item in overview.get("highlights", []))
    if overview.get("largeTraderSummary"):
        lines.extend(["", "大額交易人前五大 / 前十大"])
        lines.extend(f"- {item}" for item in overview["largeTraderSummary"])
    return "\n".join(lines)


def send_email(report: dict[str, object], pdf_data: bytes) -> str:
    env = parse_dotenv(Path.home() / "yt_digest" / ".env")
    user = env["GMAIL_USER"]
    password = env["GMAIL_APP_PASSWORD"]
    to_addr = env["GMAIL_TO"]
    report_date = report["meta"]["date"]
    page_url = report["meta"]["reportUrl"]
    pdf_url = f"{PUBLIC_BASE_URL}/api/report.pdf?date={report_date}"

    msg = EmailMessage()
    msg["Subject"] = f"{report_date} 台指期貨 / 選擇權籌碼完整報告"
    msg["From"] = user
    msg["To"] = to_addr
    msg.set_content(f"{report['email']}\n\n完整網頁：{page_url}\nPDF 下載：{pdf_url}\n")
    msg.add_attachment(
        pdf_data,
        maintype="application",
        subtype="pdf",
        filename=f"{report_date.replace('/', '-')}-taifex-report.pdf",
    )

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
        smtp.login(user, password)
        smtp.send_message(msg)
    return to_addr


def main() -> None:
    parser = argparse.ArgumentParser(description="Send the daily TAIFEX snapshot, Telegram summary, and email.")
    parser.add_argument("--date", help="Target report date in YYYY/MM/DD. Defaults to latest available business day.")
    parser.add_argument("--chat-id", default=DEFAULT_TELEGRAM_CHAT_ID, help="Telegram chat id.")
    args = parser.parse_args()

    requested_date = args.date
    report_url = f"{PUBLIC_BASE_URL}/?date={requested_date}" if requested_date else PUBLIC_BASE_URL
    report, _ = cached_report(requested_date, report_url, force_refresh=True)
    pdf_data = build_report_pdf(report)
    save_snapshot(report["meta"]["date"], report, pdf_data)

    token = load_telegram_token()
    quick_overview = build_quick_overview(report)
    full_messages = split_telegram_text(report["telegram"])

    results = []
    results.append(send_telegram_message(token, args.chat_id, quick_overview))
    for message in full_messages:
        results.append(send_telegram_message(token, args.chat_id, message))

    email_to = send_email(report, pdf_data)

    print(json.dumps({
        "date": report["meta"]["date"],
        "telegramMessages": len(results),
        "telegramMessageIds": [item.get("result", {}).get("message_id") for item in results],
        "emailTo": email_to,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
