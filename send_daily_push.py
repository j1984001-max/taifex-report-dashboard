#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import ssl
import smtplib
import subprocess
import time
import urllib.parse
import urllib.request
import uuid
from email.message import EmailMessage
from pathlib import Path

from server import PUBLIC_BASE_URL, build_report_pdf, build_telegram_important_date_lines, cached_report, save_snapshot


TELEGRAM_LIMIT = 3500
DEFAULT_TELEGRAM_CHAT_ID = "7154157141"
DEFAULT_RETRY_DELAY_SECONDS = 300
DEFAULT_MAX_RETRIES = 2


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


def clean_secret(value: str | None, *, strip_all_spaces: bool = False) -> str:
    cleaned = (value or "").replace("\xa0", " ").strip()
    if strip_all_spaces:
        cleaned = "".join(cleaned.split())
    return cleaned


def load_telegram_token() -> str:
    env_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if env_token:
        return env_token
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


def _multipart_form_data(fields: dict[str, str], files: list[dict[str, object]]) -> tuple[bytes, str]:
    boundary = f"----openclawcodex{uuid.uuid4().hex}"
    lines: list[bytes] = []

    def add_line(line: str) -> None:
        lines.append(line.encode("utf-8"))

    for name, value in fields.items():
        add_line(f"--{boundary}\r\n")
        add_line(f'Content-Disposition: form-data; name="{name}"\r\n\r\n')
        add_line(f"{value}\r\n")

    for f in files:
        field_name = f["field_name"]
        filename = f["filename"]
        content_type = f.get("content_type", "application/octet-stream")
        data = f["data"]
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("file data must be bytes")
        add_line(f"--{boundary}\r\n")
        add_line(
            f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'
        )
        add_line(f"Content-Type: {content_type}\r\n\r\n")
        lines.append(bytes(data))
        add_line("\r\n")

    add_line(f"--{boundary}--\r\n")
    body = b"".join(lines)
    return body, boundary


def send_telegram_document(bot_token: str, chat_id: str, *, caption: str, filename: str, data: bytes) -> dict[str, object]:
    body, boundary = _multipart_form_data(
        {"chat_id": chat_id, "caption": caption},
        [
            {
                "field_name": "document",
                "filename": filename,
                "content_type": "image/png",
                "data": data,
            }
        ],
    )
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/sendDocument",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def capture_d_section_screenshots(report_date: str) -> dict[str, bytes]:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return {}

    url = f"{PUBLIC_BASE_URL}/?date={urllib.parse.quote(report_date)}"
    screenshots: dict[str, bytes] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch()
        # Higher pixel density makes small table text readable in Telegram.
        page = browser.new_page(viewport={"width": 1400, "height": 2200}, device_scale_factor=2)
        page.goto(url, wait_until="networkidle", timeout=120_000)

        d_section = page.locator("section.section-card").filter(has=page.locator("h2", has_text="D. 三大法人選擇權分契約詳細版")).first
        d_section.wait_for(timeout=60_000)

        # 1) 三大法人買賣權未平倉（外資/自營商/投信卡片區）
        cards_container = d_section.locator("div.space-y-4").first
        if cards_container.count() > 0:
            screenshots["d_institutions"] = cards_container.screenshot(type="png")

        # 2) 特定法人表（「選擇權特定法人詳細版」區塊）
        heading = d_section.locator("h4", has_text="選擇權特定法人詳細版").first
        if heading.count() > 0:
            block = heading.locator("xpath=ancestor::div[1]")
            screenshots["d_specific"] = block.screenshot(type="png")

        browser.close()

    return screenshots


def build_quick_overview(report: dict[str, object]) -> str:
    meta = report["meta"]
    overview = report["changeOverview"]
    lines = [
        f"{meta['date']} 台指籌碼速覽",
        f"完整網頁：{meta['reportUrl']}",
        f"PDF：{PUBLIC_BASE_URL}/api/report.pdf?date={meta['date']}",
    ]

    if overview.get("urgentHighlights"):
        lines.extend(["", "三個營業日內重要日期"])
        lines.extend(f"- {item}" for item in overview["urgentHighlights"])

    futures_highlights = overview.get("futuresOverviewHighlights") or overview.get("highlights") or []
    if futures_highlights:
        lines.extend(["", "期貨差異變動速覽"])
        lines.extend(f"- {item}" for item in futures_highlights)

    large_highlights = overview.get("largeTraderOverviewHighlights") or overview.get("largeTraderSummary") or []
    if large_highlights:
        lines.extend(["", "大額交易人前五大 / 前十大"])
        lines.extend(f"- {item}" for item in large_highlights)

    option_highlights = overview.get("optionOverviewHighlights") or overview.get("optionHighlights") or []
    if option_highlights:
        lines.extend(["", "選擇權分契約速覽"])
        lines.extend(f"- {item}" for item in option_highlights)

    prediction = overview.get("prediction") or {}
    if prediction:
        lines.extend(["", "預測分析"])
        summary = prediction.get("summary")
        psychology = prediction.get("psychology")
        reasons = prediction.get("reasons") or []
        if summary:
            lines.append(f"- {summary}")
        if psychology:
            lines.append(f"- {psychology}")
        lines.extend(f"- 理由：{item}" for item in reasons)

    warning = build_important_date_warning(report)
    if warning:
        lines.extend(["", "重要日期提醒", warning])
    return "\n".join(lines)


def build_important_date_warning(report: dict[str, object]) -> str:
    section = report.get("importantDates", {})
    rows = section.get("rows", [])
    stale = [
        row for row in rows
        if row.get("sourceTitle") == "BLS" and row.get("status") not in {"官方排程"}
    ]
    if not stale:
        return ""
    titles = "、".join(row.get("title", "") for row in stale)
    return f"注意：{titles} 的 BLS 年度排程尚未更新，需補新年度官方日期。"


def send_email(report: dict[str, object], pdf_data: bytes) -> str:
    env = parse_dotenv(Path.home() / "yt_digest" / ".env")
    user = clean_secret(os.environ.get("GMAIL_USER") or env["GMAIL_USER"])
    password = clean_secret(os.environ.get("GMAIL_APP_PASSWORD") or env["GMAIL_APP_PASSWORD"], strip_all_spaces=True)
    to_addr = clean_secret(os.environ.get("GMAIL_TO") or env["GMAIL_TO"])
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


def publish_snapshot(report_date: str) -> str:
    result = subprocess.run(
        ["python3", "publish_snapshot.py", "--date", report_date.replace("/", "-")],
        cwd=Path(__file__).resolve().parent,
        capture_output=True,
        text=True,
        check=True,
    )
    return (result.stdout or "").strip()


def report_is_ready(report: dict[str, object], *, mode: str = "full") -> tuple[bool, str]:
    tables = report.get("tables", {})
    for key in ["A", "B", "C", "D"]:
        rows = tables.get(key, {}).get("rows", [])
        if not rows:
            return False, f"{key} 缺少 rows"
    if mode == "full":
        charts = tables.get("E", {}).get("charts", [])
        if len(charts) < 3:
            return False, "E 支撐壓力圖不足 3 張"
        g_rows = tables.get("G", {}).get("rows", [])
        if not g_rows:
            return False, "G PCR 無資料"
    return True, "ok"


def main() -> None:
    parser = argparse.ArgumentParser(description="Send the daily TAIFEX snapshot, Telegram summary, and email.")
    parser.add_argument("--date", help="Target report date in YYYY/MM/DD. Defaults to latest available business day.")
    parser.add_argument("--chat-id", default=os.environ.get("TELEGRAM_CHAT_ID", DEFAULT_TELEGRAM_CHAT_ID), help="Telegram chat id.")
    parser.add_argument("--quick-only", action="store_true", help="Only send the quick overview and D-section screenshots (no full text, no email).")
    parser.add_argument("--max-retries", type=int, default=int(os.environ.get("REPORT_MAX_RETRIES", str(DEFAULT_MAX_RETRIES))), help="Max retries when the report is not ready.")
    parser.add_argument("--retry-delay", type=int, default=int(os.environ.get("REPORT_RETRY_DELAY_SECONDS", str(DEFAULT_RETRY_DELAY_SECONDS))), help="Retry delay in seconds when the report is not ready.")
    args = parser.parse_args()

    requested_date = args.date
    report_url = f"{PUBLIC_BASE_URL}/?date={requested_date}" if requested_date else PUBLIC_BASE_URL
    attempts = args.max_retries + 1
    last_reason = "unknown"
    report = None
    for index in range(attempts):
        report, _ = cached_report(requested_date, report_url, force_refresh=True)
        # Only block send when core tables are missing. E/G delays happen after 15:00.
        ready, last_reason = report_is_ready(report, mode="minimal")
        if ready:
            break
        if index < attempts - 1:
            time.sleep(args.retry_delay)
    if report is None:
        raise RuntimeError("無法建立報表")
    ready, last_reason = report_is_ready(report, mode="minimal")
    if not ready:
        raise RuntimeError(f"核心表格仍未完整：{last_reason}")

    pdf_data = build_report_pdf(report)
    save_snapshot(report["meta"]["date"], report, pdf_data)

    token = load_telegram_token()
    quick_overview = build_quick_overview(report)
    full_messages = split_telegram_text(report["telegram"])

    results = []
    results.append(send_telegram_message(token, args.chat_id, quick_overview))

    # Attach D-section screenshots right after quick overview.
    shots = capture_d_section_screenshots(report["meta"]["date"])
    if shots.get("d_institutions"):
        results.append(
            send_telegram_document(
                token,
                args.chat_id,
                caption=f"D. 三大法人買賣權未平倉（{report['meta']['date']}）",
                filename=f"d-institutions-{report['meta']['date'].replace('/', '-')}.png",
                data=shots["d_institutions"],
            )
        )
    if shots.get("d_specific"):
        results.append(
            send_telegram_document(
                token,
                args.chat_id,
                caption=f"D. 選擇權特定法人表（{report['meta']['date']}）",
                filename=f"d-specific-{report['meta']['date'].replace('/', '-')}.png",
                data=shots["d_specific"],
            )
        )

    if args.quick_only:
        print(json.dumps({
            "date": report["meta"]["date"],
            "telegramMessages": len(results),
            "telegramMessageIds": [item.get("result", {}).get("message_id") for item in results],
            "quickOnly": True,
        }, ensure_ascii=False))
        return

    # Full report is optional; if not ready, still send the simplified full message.
    full_ready, _ = report_is_ready(report, mode="full")
    for message in full_messages:
        results.append(send_telegram_message(token, args.chat_id, message))

    email_to = send_email(report, pdf_data)
    publish_result = publish_snapshot(report["meta"]["date"])

    print(json.dumps({
        "date": report["meta"]["date"],
        "telegramMessages": len(results),
        "telegramMessageIds": [item.get("result", {}).get("message_id") for item in results],
        "emailTo": email_to,
        "publishResult": publish_result,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
