#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import json
import os
import re
import threading
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from html.parser import HTMLParser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


ROOT = Path(__file__).resolve().parent
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8000"))
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://taifex-report-dashboard.onrender.com").rstrip("/")
CACHE_DIR = ROOT / ".cache"
SNAPSHOT_DIR = ROOT / "snapshots"
LATEST_REPORT_CACHE_TTL = int(os.environ.get("LATEST_REPORT_CACHE_TTL", "21600"))
HISTORICAL_REPORT_CACHE_TTL = int(os.environ.get("HISTORICAL_REPORT_CACHE_TTL", "604800"))
TAIFEX = "https://www.taifex.com.tw"
BQ888 = "https://www.bq888.taifex.com.tw"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Codex dashboard fetcher)",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}

TARGET_FUTURES_PRODUCTS = {"臺股期貨", "小型臺指期貨", "微型臺指期貨", "電子期貨", "金融期貨"}
TARGET_OPTION_PRODUCT = "臺指選擇權"
TARGET_LARGE_TRADER = "臺股期貨(TX+MTX/4+TMF/20)"

pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))

CACHE_DIR.mkdir(exist_ok=True)
SNAPSHOT_DIR.mkdir(exist_ok=True)
REPORT_CACHE_LOCK = threading.Lock()
REPORT_CACHE_MEMORY: dict[str, dict[str, Any]] = {}


@dataclass
class Cell:
    text: str
    rowspan: int
    colspan: int


class TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[Cell]]] = []
        self._stack: list[dict[str, Any]] = []
        self._in_cell = False
        self._data: list[str] = []
        self._attrs: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {key: value or "" for key, value in attrs}
        if tag == "table":
            self._stack.append({"rows": [], "current": None})
        elif tag == "tr" and self._stack:
            self._stack[-1]["current"] = []
        elif tag in {"td", "th"} and self._stack and self._stack[-1]["current"] is not None:
            self._in_cell = True
            self._data = []
            self._attrs = attrs_dict

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._data.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._in_cell:
            text = normalize_text(" ".join(self._data))
            rowspan = int(self._attrs.get("rowspan", "1") or "1")
            colspan = int(self._attrs.get("colspan", "1") or "1")
            self._stack[-1]["current"].append(Cell(text, rowspan, colspan))
            self._in_cell = False
        elif tag == "tr" and self._stack and self._stack[-1]["current"] is not None:
            self._stack[-1]["rows"].append(self._stack[-1]["current"])
            self._stack[-1]["current"] = None
        elif tag == "table" and self._stack:
            self.tables.append(self._stack.pop()["rows"])


def normalize_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def latest_business_day(today: datetime | None = None) -> str:
    current = today or datetime.now()
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current.strftime("%Y/%m/%d")


def previous_business_day(date_text: str) -> str:
    current = datetime.strptime(date_text, "%Y/%m/%d") - timedelta(days=1)
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current.strftime("%Y/%m/%d")


def next_business_day(date_text: str) -> str:
    current = datetime.strptime(date_text, "%Y/%m/%d") + timedelta(days=1)
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current.strftime("%Y/%m/%d")


def cache_key(report_date: str, report_url: str) -> str:
    safe_date = report_date.replace("/", "-")
    safe_url = re.sub(r"[^A-Za-z0-9]+", "_", report_url).strip("_") or "default"
    return f"{safe_date}-{safe_url}"


def cache_ttl_for_date(report_date: str) -> int:
    return LATEST_REPORT_CACHE_TTL if report_date == latest_business_day() else HISTORICAL_REPORT_CACHE_TTL


def cache_paths(key: str) -> tuple[Path, Path]:
    return CACHE_DIR / f"{key}.json", CACHE_DIR / f"{key}.pdf"


def snapshot_paths(report_date: str) -> tuple[Path, Path]:
    safe_date = report_date.replace("/", "-")
    return SNAPSHOT_DIR / f"{safe_date}.json", SNAPSHOT_DIR / f"{safe_date}.pdf"


def load_snapshot(report_date: str, report_url: str) -> tuple[dict[str, Any], bytes | None] | None:
    json_path, pdf_path = snapshot_paths(report_date)
    if not json_path.exists():
        return None
    try:
        report = json.loads(json_path.read_text(encoding="utf-8"))
        report["meta"]["reportUrl"] = report_url
        pdf_data = pdf_path.read_bytes() if pdf_path.exists() else None
        return report, pdf_data
    except Exception:
        return None


def save_snapshot(report_date: str, report: dict[str, Any], pdf_data: bytes | None = None) -> None:
    json_path, pdf_path = snapshot_paths(report_date)
    json_path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
    if pdf_data is not None:
        pdf_path.write_bytes(pdf_data)


def load_cached_report(key: str, ttl: int) -> tuple[dict[str, Any], bytes | None] | None:
    now = datetime.now().timestamp()
    with REPORT_CACHE_LOCK:
        cached = REPORT_CACHE_MEMORY.get(key)
        if cached and now - cached["created_at"] <= ttl:
            return cached["report"], cached.get("pdf")

    json_path, pdf_path = cache_paths(key)
    if not json_path.exists():
        return None
    age = now - json_path.stat().st_mtime
    if age > ttl:
        return None

    try:
        report = json.loads(json_path.read_text(encoding="utf-8"))
        pdf_data = pdf_path.read_bytes() if pdf_path.exists() else None
    except Exception:
        return None

    with REPORT_CACHE_LOCK:
        REPORT_CACHE_MEMORY[key] = {"created_at": now, "report": report, "pdf": pdf_data}
    return report, pdf_data


def save_cached_report(key: str, report: dict[str, Any], pdf_data: bytes | None = None) -> None:
    json_path, pdf_path = cache_paths(key)
    json_path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
    if pdf_data is not None:
        pdf_path.write_bytes(pdf_data)
    with REPORT_CACHE_LOCK:
        REPORT_CACHE_MEMORY[key] = {
            "created_at": datetime.now().timestamp(),
            "report": report,
            "pdf": pdf_data,
        }


def invalidate_cached_report(key: str) -> None:
    json_path, pdf_path = cache_paths(key)
    with REPORT_CACHE_LOCK:
        REPORT_CACHE_MEMORY.pop(key, None)
    if json_path.exists():
        json_path.unlink()
    if pdf_path.exists():
        pdf_path.unlink()


def cached_report(report_date: str | None, report_url: str, force_refresh: bool = False) -> tuple[dict[str, Any], str]:
    candidate_dates = [report_date] if report_date else []
    if not candidate_dates:
        candidate = latest_business_day()
        for _ in range(7):
            candidate_dates.append(candidate)
            candidate = previous_business_day(candidate)

    last_error: Exception | None = None
    for index, candidate_date in enumerate(candidate_dates):
        key = cache_key(candidate_date, report_url)
        ttl = cache_ttl_for_date(candidate_date)
        should_force = force_refresh and index == 0
        if not should_force:
            snapshot = load_snapshot(candidate_date, report_url)
            if snapshot:
                report, pdf_data = snapshot
                save_cached_report(key, report, pdf_data)
                return report, key
        if should_force:
            invalidate_cached_report(key)
        else:
            cached = load_cached_report(key, ttl)
            if cached:
                report, _ = cached
                return report, key
        try:
            report = build_report(candidate_date, report_url)
            save_cached_report(key, report)
            save_snapshot(candidate_date, report)
            return report, key
        except Exception as exc:
            last_error = exc
            if report_date:
                break
            continue

    raise last_error or ValueError("找不到可用報告資料")


def request_html(base: str, path: str, data: dict[str, str] | None = None) -> str:
    payload = urllib.parse.urlencode(data).encode() if data else None
    request = urllib.request.Request(f"{base}{path}", headers=HEADERS)
    with urllib.request.urlopen(request, data=payload, timeout=30) as response:
        return response.read().decode("utf-8", "ignore")


def request_bytes(base: str, path: str, data: dict[str, str] | None = None) -> bytes:
    payload = urllib.parse.urlencode(data).encode() if data else None
    request = urllib.request.Request(f"{base}{path}", headers=HEADERS)
    with urllib.request.urlopen(request, data=payload, timeout=30) as response:
        return response.read()


def parse_tables(html: str) -> list[list[list[str]]]:
    parser = TableParser()
    parser.feed(html)
    return [expand_table(table) for table in parser.tables]


def expand_table(rows: list[list[Cell]]) -> list[list[str]]:
    pending: dict[int, tuple[str, int]] = {}
    out: list[list[str]] = []
    max_cols = 0

    for source in rows:
        row: list[str] = []
        col = 0
        index = 0

        while index < len(source) or pending:
            if col in pending:
                text, remain = pending[col]
                row.append(text)
                if remain == 1:
                    del pending[col]
                else:
                    pending[col] = (text, remain - 1)
                col += 1
                continue

            if index >= len(source):
                break

            cell = source[index]
            for _ in range(cell.colspan):
                row.append(cell.text)
                if cell.rowspan > 1:
                    pending[col] = (cell.text, cell.rowspan - 1)
                col += 1
            index += 1

        out.append(row)
        max_cols = max(max_cols, len(row))

    for row in out:
        row.extend([""] * (max_cols - len(row)))
    return out


def find_table(tables: list[list[list[str]]], token: str) -> list[list[str]]:
    for table in tables:
        preview = " ".join(" ".join(row) for row in table[:8])
        if token in preview:
            return table
    raise ValueError(f"找不到表格：{token}")


def to_int(value: str) -> int:
    cleaned = (
        value.replace(",", "")
        .replace(" ", "")
        .replace("▲", "")
        .replace("▼", "")
        .replace("+", "")
    )
    if cleaned in {"", "-", "--"}:
        return 0
    return int(float(cleaned))


def to_float(value: str) -> float:
    cleaned = value.replace(",", "").replace(" ", "").replace("%", "")
    if cleaned in {"", "-", "--"}:
        return 0.0
    return float(cleaned)


def extract_page_date(html: str) -> str:
    for pattern in [r"日期：\s*(\d{4}/\d{2}/\d{2})", r"日期(\d{4}/\d{2}/\d{2})", r"(\d{4}/\d{2}/\d{2})"]:
        match = re.search(pattern, html)
        if match:
            return match.group(1)
    raise ValueError("無法辨識日期")


def parse_pair_number(value: str) -> int:
    match = re.match(r"\s*([0-9,.\-]+)", value)
    return to_int(match.group(1)) if match else 0


def parse_pair_percent(value: str) -> float:
    match = re.match(r"\s*([0-9,.\-]+)%?", value)
    return to_float(match.group(1)) if match else 0.0


def parse_dual_number(value: str) -> tuple[int, int | None]:
    parts = re.findall(r"[0-9][0-9,.\-]*", value)
    if not parts:
        return 0, None
    primary = to_int(parts[0])
    secondary = to_int(parts[1]) if len(parts) > 1 else None
    return primary, secondary


def parse_dual_percent(value: str) -> tuple[float, float | None]:
    parts = re.findall(r"[0-9][0-9,.\-]*", value)
    if not parts:
        return 0.0, None
    primary = to_float(parts[0])
    secondary = to_float(parts[1]) if len(parts) > 1 else None
    return primary, secondary


def parse_futures_contracts(table: list[list[str]]) -> list[dict[str, Any]]:
    rows = []
    for row in table[3:]:
        if len(row) < 15 or row[1] not in TARGET_FUTURES_PRODUCTS:
            continue
        rows.append(
            {
                "product": row[1],
                "institution": row[2],
                "tradeLongQty": to_int(row[3]),
                "tradeLongAmount": to_int(row[4]),
                "tradeShortQty": to_int(row[5]),
                "tradeShortAmount": to_int(row[6]),
                "tradeNetQty": to_int(row[7]),
                "tradeNetAmount": to_int(row[8]),
                "oiLongQty": to_int(row[9]),
                "oiLongAmount": to_int(row[10]),
                "oiShortQty": to_int(row[11]),
                "oiShortAmount": to_int(row[12]),
                "oiNetQty": to_int(row[13]),
                "oiNetAmount": to_int(row[14]),
            }
        )
    return rows


def fetch_futures_history_rows(report_date: str, count: int = 5) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    current = datetime.strptime(report_date, "%Y/%m/%d") - timedelta(days=1)
    checked = 0

    while len(history) < count and checked < 20:
        date_text = current.strftime("%Y/%m/%d")
        try:
            html = request_html(
                TAIFEX,
                "/cht/3/futContractsDateExcel",
                {"queryType": "1", "queryDate": date_text, "commodityId": ""},
            )
            table = find_table(parse_tables(html), "臺股期貨")
            rows = parse_futures_contracts(table)
            if rows:
                history.append({"date": date_text, "rows": rows})
        except Exception:
            pass
        current -= timedelta(days=1)
        checked += 1
    return history


def fetch_futures_rows_for_date(report_date: str) -> list[dict[str, Any]]:
    html = request_html(
        TAIFEX,
        "/cht/3/futContractsDateExcel",
        {"queryType": "1", "queryDate": report_date, "commodityId": ""},
    )
    table = find_table(parse_tables(html), "臺股期貨")
    return parse_futures_contracts(table)


def cycle_start_thursday(report_date: str) -> str:
    current = datetime.strptime(report_date, "%Y/%m/%d")
    thursday = current - timedelta(days=(current.weekday() - 3) % 7)
    return thursday.strftime("%Y/%m/%d")


def third_wednesday(year: int, month: int) -> str:
    current = datetime(year, month, 1)
    while current.weekday() != 2:
        current += timedelta(days=1)
    current += timedelta(days=14)
    return current.strftime("%Y/%m/%d")


def monthly_cycle_start(monthly_code: str) -> str:
    year = int(monthly_code[:4])
    month = int(monthly_code[4:])
    if month == 1:
        prev_year = year - 1
        prev_month = 12
    else:
        prev_year = year
        prev_month = month - 1
    settlement_day = third_wednesday(prev_year, prev_month)
    return next_business_day(settlement_day)


def enrich_futures_with_history(
    current_rows: list[dict[str, Any]],
    history: list[dict[str, Any]],
    cycle_start_rows: list[dict[str, Any]],
    cycle_start_date: str,
) -> list[dict[str, Any]]:
    previous_map = {
        (row["product"], row["institution"]): row
        for row in (history[0]["rows"] if history else [])
    }
    five_day_map = {
        (row["product"], row["institution"]): row
        for row in (history[min(len(history), 5) - 1]["rows"] if history else [])
    }
    cycle_start_map = {
        (row["product"], row["institution"]): row
        for row in cycle_start_rows
    }
    previous_date = history[0]["date"] if history else None
    five_day_date = history[min(len(history), 5) - 1]["date"] if history else None

    enriched = []
    for row in current_rows:
        prev = previous_map.get((row["product"], row["institution"]))
        five = five_day_map.get((row["product"], row["institution"]))
        cycle = cycle_start_map.get((row["product"], row["institution"]))
        prev_oi_long = prev["oiLongQty"] if prev else None
        prev_oi_short = prev["oiShortQty"] if prev else None
        prev_oi_net = prev["oiNetQty"] if prev else None
        five_oi_long = five["oiLongQty"] if five else None
        five_oi_short = five["oiShortQty"] if five else None
        five_oi_net = five["oiNetQty"] if five else None
        cycle_oi_long = cycle["oiLongQty"] if cycle else None
        cycle_oi_short = cycle["oiShortQty"] if cycle else None
        cycle_oi_net = cycle["oiNetQty"] if cycle else None
        enriched.append(
            {
                **row,
                "previousDate": previous_date,
                "previousOiLongQty": prev_oi_long,
                "previousOiShortQty": prev_oi_short,
                "previousOiNetQty": prev_oi_net,
                "dayChangeOiLongQty": None if prev_oi_long is None else row["oiLongQty"] - prev_oi_long,
                "dayChangeOiShortQty": None if prev_oi_short is None else row["oiShortQty"] - prev_oi_short,
                "dayChangeOiNetQty": None if prev_oi_net is None else row["oiNetQty"] - prev_oi_net,
                "fiveDayDate": five_day_date,
                "fiveDayOiLongQty": five_oi_long,
                "fiveDayOiShortQty": five_oi_short,
                "fiveDayOiNetQty": five_oi_net,
                "fiveDayChangeOiLongQty": None if five_oi_long is None else row["oiLongQty"] - five_oi_long,
                "fiveDayChangeOiShortQty": None if five_oi_short is None else row["oiShortQty"] - five_oi_short,
                "fiveDayChangeOiNetQty": None if five_oi_net is None else row["oiNetQty"] - five_oi_net,
                "cycleStartDate": cycle_start_date,
                "cycleStartOiLongQty": cycle_oi_long,
                "cycleStartOiShortQty": cycle_oi_short,
                "cycleStartOiNetQty": cycle_oi_net,
                "cycleChangeOiLongQty": None if cycle_oi_long is None else row["oiLongQty"] - cycle_oi_long,
                "cycleChangeOiShortQty": None if cycle_oi_short is None else row["oiShortQty"] - cycle_oi_short,
                "cycleChangeOiNetQty": None if cycle_oi_net is None else row["oiNetQty"] - cycle_oi_net,
            }
        )
    return enriched


def build_futures_category_analysis(rows: list[dict[str, Any]]) -> list[str]:
    categories = ["臺股期貨", "電子期貨", "金融期貨", "小型臺指期貨", "微型臺指期貨"]
    analysis: list[str] = []
    for product in categories:
        product_rows = [row for row in rows if row["product"] == product]
        if not product_rows:
            continue
        day_total = sum((row["dayChangeOiNetQty"] or 0) for row in product_rows)
        cycle_total = sum((row["cycleChangeOiNetQty"] or 0) for row in product_rows)
        institution_bits = "；".join(
            (
                f"{row['institution']}：單日多方 {format_signed(row['dayChangeOiLongQty'])}、空方 {format_signed(row['dayChangeOiShortQty'])}、淨額 {format_signed(row['dayChangeOiNetQty'])}"
                f"；自 {row['cycleStartDate']} 起累積多方 {format_signed(row['cycleChangeOiLongQty'])}、空方 {format_signed(row['cycleChangeOiShortQty'])}、淨額 {format_signed(row['cycleChangeOiNetQty'])}"
            )
            for row in product_rows
        )
        analysis.append(
            f"{product}：三大法人未平倉淨額單日合計變動 {format_signed(day_total)} 口，自 {product_rows[0]['cycleStartDate']} 起累積變動 {format_signed(cycle_total)} 口；{institution_bits}。"
        )
    return analysis


def build_futures_delta_overview(rows: list[dict[str, Any]]) -> dict[str, Any]:
    categories = ["臺股期貨", "電子期貨", "金融期貨", "小型臺指期貨", "微型臺指期貨"]
    items = []
    for product in categories:
        product_rows = [row for row in rows if row["product"] == product]
        if not product_rows:
            continue
        day_total = sum((row["dayChangeOiNetQty"] or 0) for row in product_rows)
        cycle_total = sum((row["cycleChangeOiNetQty"] or 0) for row in product_rows)
        items.append(
            {
                "product": product,
                "dayTotal": day_total,
                "cycleTotal": cycle_total,
                "cycleStartDate": product_rows[0]["cycleStartDate"],
                "institutions": [
                    {
                        "institution": row["institution"],
                        "dayLongChange": row["dayChangeOiLongQty"],
                        "dayShortChange": row["dayChangeOiShortQty"],
                        "dayNetChange": row["dayChangeOiNetQty"],
                        "cycleLongChange": row["cycleChangeOiLongQty"],
                        "cycleShortChange": row["cycleChangeOiShortQty"],
                        "cycleNetChange": row["cycleChangeOiNetQty"],
                    }
                    for row in product_rows
                ],
            }
        )
    highlights = []
    for item in items:
        institution_text = "；".join(
            f"{inst['institution']}：單日 {format_signed(inst['dayNetChange'])}、累積 {format_signed(inst['cycleNetChange'])}"
            for inst in item["institutions"]
        )
        highlights.append(
            f"{item['product']}：單日淨額合計 {format_signed(item['dayTotal'])} 口，自 {item['cycleStartDate']} 起累積淨額合計 {format_signed(item['cycleTotal'])} 口；{institution_text}。"
        )
    return {"items": items, "highlights": highlights}


def parse_option_contracts(table: list[list[str]]) -> list[dict[str, Any]]:
    rows = []
    for row in table[3:]:
        if len(row) < 15 or row[1] != TARGET_OPTION_PRODUCT:
            continue
        rows.append(
            {
                "product": row[1],
                "institution": row[2],
                "tradeLongQty": to_int(row[3]),
                "tradeLongAmount": to_int(row[4]),
                "tradeShortQty": to_int(row[5]),
                "tradeShortAmount": to_int(row[6]),
                "tradeNetQty": to_int(row[7]),
                "tradeNetAmount": to_int(row[8]),
                "oiLongQty": to_int(row[9]),
                "oiLongAmount": to_int(row[10]),
                "oiShortQty": to_int(row[11]),
                "oiShortAmount": to_int(row[12]),
                "oiNetQty": to_int(row[13]),
                "oiNetAmount": to_int(row[14]),
            }
        )
    return rows


def build_total_summary(
    futures_contracts: list[dict[str, Any]],
    option_contracts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    futures_map = {row["institution"]: row for row in futures_contracts}
    options_map = {row["institution"]: row for row in option_contracts}
    summary = []
    for institution in ["外資", "投信", "自營商"]:
        futures_row = futures_map.get(institution)
        options_row = options_map.get(institution)
        summary.append(
            {
                "institution": institution,
                "futuresTradeNetQty": futures_row["tradeNetQty"] if futures_row else None,
                "futuresOiNetQty": futures_row["oiNetQty"] if futures_row else None,
                "optionsTradeNetQty": options_row["tradeNetQty"] if options_row else None,
                "optionsOiNetQty": options_row["oiNetQty"] if options_row else None,
                "combinedOiNetQty": (
                    (futures_row["oiNetQty"] if futures_row else 0)
                    + (options_row["oiNetQty"] if options_row else 0)
                ),
            }
        )
    return summary


def parse_tx_reference(table: list[list[str]]) -> dict[str, Any]:
    for row in table[1:]:
        if len(row) < 17:
            continue
        if row[0] == "TX" and re.fullmatch(r"\d{6}", row[1]):
            return {
                "contract": row[1],
                "lastPrice": to_int(row[5]),
                "settlement": to_int(row[11]),
                "oi": to_int(row[12]),
            }
    raise ValueError("找不到近月 TX 行情")


def request_csv_rows(base: str, path: str, data: dict[str, str] | None = None, encoding: str = "cp950") -> list[list[str]]:
    raw = request_bytes(base, path, data)
    text = raw.decode(encoding, "ignore")
    return list(csv.reader(io.StringIO(text)))


def parse_large_trader_row(row: list[str]) -> dict[str, Any]:
    long_top5_qty, long_top5_specific_qty = parse_dual_number(row[2])
    long_top5_pct, long_top5_specific_pct = parse_dual_percent(row[3])
    long_top10_qty, long_top10_specific_qty = parse_dual_number(row[4])
    long_top10_pct, long_top10_specific_pct = parse_dual_percent(row[5])
    short_top5_qty, short_top5_specific_qty = parse_dual_number(row[6])
    short_top5_pct, short_top5_specific_pct = parse_dual_percent(row[7])
    short_top10_qty, short_top10_specific_qty = parse_dual_number(row[8])
    short_top10_pct, short_top10_specific_pct = parse_dual_percent(row[9])
    return {
        "contractName": row[0],
        "expiry": row[1],
        "longTop5Qty": long_top5_qty,
        "longTop5Pct": long_top5_pct,
        "longTop5SpecificQty": long_top5_specific_qty,
        "longTop5SpecificPct": long_top5_specific_pct,
        "longTop10Qty": long_top10_qty,
        "longTop10Pct": long_top10_pct,
        "longTop10SpecificQty": long_top10_specific_qty,
        "longTop10SpecificPct": long_top10_specific_pct,
        "shortTop5Qty": short_top5_qty,
        "shortTop5Pct": short_top5_pct,
        "shortTop5SpecificQty": short_top5_specific_qty,
        "shortTop5SpecificPct": short_top5_specific_pct,
        "shortTop10Qty": short_top10_qty,
        "shortTop10Pct": short_top10_pct,
        "shortTop10SpecificQty": short_top10_specific_qty,
        "shortTop10SpecificPct": short_top10_specific_pct,
        "marketOi": to_int(row[10]),
    }


def parse_large_trader_contracts(table: list[list[str]], monthly_code: str) -> list[dict[str, Any]]:
    expiry = f"{monthly_code[:4]} {monthly_code[4:]}"
    rows: list[dict[str, Any]] = []
    for row in table[3:]:
        if len(row) < 11:
            continue
        if row[0] == TARGET_LARGE_TRADER and row[1] in {"週契約", expiry}:
            item = parse_large_trader_row(row)
            item["contractType"] = "weekly" if row[1] == "週契約" else "monthly"
            item["contractLabel"] = "週契約" if row[1] == "週契約" else "月契約"
            rows.append(item)
    if not rows:
        raise ValueError("找不到大額交易人資料")
    rows.sort(key=lambda item: 0 if item["contractType"] == "weekly" else 1)
    return rows


def format_large_trader_expiry_label(expiry: str) -> str:
    expiry = expiry.strip()
    if expiry == "666666":
        return "週契約"
    if re.fullmatch(r"\d{6}", expiry):
        return f"{expiry[:4]} {expiry[4:]}"
    return expiry


def build_large_trader_item_from_csv(
    base_row: list[str],
    specific_row: list[str] | None,
) -> dict[str, Any]:
    market_oi = to_int(base_row[9])
    long_top5_qty = to_int(base_row[5])
    short_top5_qty = to_int(base_row[6])
    long_top10_qty = to_int(base_row[7])
    short_top10_qty = to_int(base_row[8])
    long_top5_specific_qty = to_int(specific_row[5]) if specific_row else None
    short_top5_specific_qty = to_int(specific_row[6]) if specific_row else None
    long_top10_specific_qty = to_int(specific_row[7]) if specific_row else None
    short_top10_specific_qty = to_int(specific_row[8]) if specific_row else None
    expiry_code = base_row[3].strip()
    return {
        "contractName": base_row[2].strip(),
        "expiry": format_large_trader_expiry_label(expiry_code),
        "longTop5Qty": long_top5_qty,
        "longTop5Pct": (long_top5_qty / market_oi * 100) if market_oi else 0.0,
        "longTop5SpecificQty": long_top5_specific_qty,
        "longTop5SpecificPct": (long_top5_specific_qty / market_oi * 100) if long_top5_specific_qty is not None and market_oi else None,
        "longTop10Qty": long_top10_qty,
        "longTop10Pct": (long_top10_qty / market_oi * 100) if market_oi else 0.0,
        "longTop10SpecificQty": long_top10_specific_qty,
        "longTop10SpecificPct": (long_top10_specific_qty / market_oi * 100) if long_top10_specific_qty is not None and market_oi else None,
        "shortTop5Qty": short_top5_qty,
        "shortTop5Pct": (short_top5_qty / market_oi * 100) if market_oi else 0.0,
        "shortTop5SpecificQty": short_top5_specific_qty,
        "shortTop5SpecificPct": (short_top5_specific_qty / market_oi * 100) if short_top5_specific_qty is not None and market_oi else None,
        "shortTop10Qty": short_top10_qty,
        "shortTop10Pct": (short_top10_qty / market_oi * 100) if market_oi else 0.0,
        "shortTop10SpecificQty": short_top10_specific_qty,
        "shortTop10SpecificPct": (short_top10_specific_qty / market_oi * 100) if short_top10_specific_qty is not None and market_oi else None,
        "marketOi": market_oi,
        "contractType": "weekly" if expiry_code == "666666" else "monthly",
        "contractLabel": "週契約" if expiry_code == "666666" else "月契約",
    }


def parse_large_trader_csv_rows(rows: list[list[str]], report_date: str, monthly_code: str) -> list[dict[str, Any]]:
    base_by_expiry: dict[str, list[str]] = {}
    specific_by_expiry: dict[str, list[str]] = {}
    target_expiries = {"666666", monthly_code.strip()}

    for row in rows[1:]:
        if len(row) < 10:
            continue
        if row[0].strip() != report_date or row[1].strip() != "TX":
            continue
        expiry = row[3].strip()
        if expiry not in target_expiries:
            continue
        if row[4].strip() == "0":
            base_by_expiry[expiry] = row
        elif row[4].strip() == "1":
            specific_by_expiry[expiry] = row

    result = []
    for expiry in ["666666", monthly_code.strip()]:
        base_row = base_by_expiry.get(expiry)
        if not base_row:
            continue
        result.append(build_large_trader_item_from_csv(base_row, specific_by_expiry.get(expiry)))

    if not result:
        raise ValueError("找不到大額交易人 CSV 資料")
    return result


def parse_large_trader(table: list[list[str]], monthly_code: str) -> dict[str, Any]:
    contracts = parse_large_trader_contracts(table, monthly_code)
    monthly = next((row for row in contracts if row["contractType"] == "monthly"), None)
    return monthly or contracts[0]


def fetch_large_trader_for_date(report_date: str, monthly_code: str) -> list[dict[str, Any]] | None:
    try:
        rows = request_csv_rows(
            TAIFEX,
            "/cht/3/dlLargeTraderFutDown",
            {"queryStartDate": report_date, "queryEndDate": report_date},
        )
        return parse_large_trader_csv_rows(rows, report_date, monthly_code)
    except Exception:
        return None


def fetch_previous_large_trader_business_day(report_date: str, monthly_code: str, limit: int = 10) -> tuple[str | None, list[dict[str, Any]] | None]:
    current = datetime.strptime(report_date, "%Y/%m/%d") - timedelta(days=1)
    checked = 0
    while checked < limit:
        date_text = current.strftime("%Y/%m/%d")
        rows = fetch_large_trader_for_date(date_text, monthly_code)
        if rows:
            return date_text, rows
        current -= timedelta(days=1)
        checked += 1
    return None, None


def parse_oi_change(table: list[list[str]], fallback_date: str | None = None) -> dict[str, Any]:
    row = table[1]
    if any(cell == "無資料" for cell in row[:5]):
        return {
            "date": fallback_date or "",
            "currentOi": None,
            "previousDate": None,
            "previousOi": None,
            "change": None,
            "missing": True,
        }
    return {
        "date": row[0],
        "currentOi": to_int(row[1]),
        "previousDate": row[2],
        "previousOi": to_int(row[3]),
        "change": to_int(row[4]),
        "missing": False,
    }


def fetch_previous_available_option_market(report_date: str, limit: int = 10) -> tuple[str | None, str | None]:
    current = datetime.strptime(report_date, "%Y/%m/%d") - timedelta(days=1)
    checked = 0
    while checked < limit:
        date_text = current.strftime("%Y/%m/%d")
        html = request_html(
            TAIFEX,
            "/cht/3/optDailyMarketExcel",
            {
                "queryType": "2",
                "marketCode": "0",
                "commodity_id": "TXO",
                "commodity_id2": "",
                "queryDate": date_text,
            },
        )
        try:
            find_table(parse_tables(html), "履約價")
            return date_text, html
        except Exception:
            current -= timedelta(days=1)
            checked += 1
    return None, None


def parse_option_market_rows(table: list[list[str]], series: str) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    for row in table[1:]:
        if len(row) < 20:
            continue
        if row[0] != "TXO" or row[1] != series:
            continue
        strike = to_int(row[3])
        entry = result.setdefault(strike, {"strike": strike, "callOi": None, "putOi": None})
        if row[4] == "Call":
            entry["callOi"] = to_int(row[15])
        elif row[4] == "Put":
            entry["putOi"] = to_int(row[15])
    return result


def classify_itm_bias(call_change: int, put_change: int, call_oi: int, put_oi: int) -> str:
    if call_change == put_change:
        return f"多空拉鋸；價內五檔增減相同，Call {format_signed(call_change)}、Put {format_signed(put_change)}。"
    change_gap = put_change - call_change
    oi_gap = put_oi - call_oi
    if change_gap >= 300:
        return f"Put 偏強；價內五檔 Put 增減 {format_signed(put_change)} 明顯高於 Call 的 {format_signed(call_change)}，差距 {change_gap:+,} 口。"
    if change_gap <= -300:
        return f"Call 偏強；價內五檔 Call 增減 {format_signed(call_change)} 明顯高於 Put 的 {format_signed(put_change)}，差距 {abs(change_gap):,} 口。"
    if abs(change_gap) < 300 and abs(oi_gap) < 1000:
        return f"多空拉鋸；價內五檔增減差距 {change_gap:+,} 口，OI 合計差距 {oi_gap:+,} 口。"
    if oi_gap > 0:
        return f"Put 偏強；雖然增減差距僅 {change_gap:+,} 口，但價內五檔 Put OI 合計 {format_number(put_oi)} 高於 Call 的 {format_number(call_oi)}。"
    return f"Call 偏強；雖然增減差距僅 {change_gap:+,} 口，但價內五檔 Call OI 合計 {format_number(call_oi)} 高於 Put 的 {format_number(put_oi)}。"


def parse_option_series_meta(table: list[list[str]]) -> list[dict[str, Any]]:
    series_meta: dict[str, dict[str, Any]] = {}
    for row in table[1:]:
        if len(row) < 20 or row[0] != "TXO":
            continue
        series = row[1]
        expiry_raw = row[2]
        if not re.fullmatch(r"\d{8}", expiry_raw):
            continue
        expiry = datetime.strptime(expiry_raw, "%Y%m%d")
        series_meta.setdefault(
            series,
            {
                "series": series,
                "expiryRaw": expiry_raw,
                "expiryDate": expiry.strftime("%Y/%m/%d"),
                "expiry": expiry,
            },
        )
    return sorted(series_meta.values(), key=lambda item: (item["expiry"], item["series"]))


def previous_calendar_day(date_text: str) -> str:
    date = datetime.strptime(date_text, "%Y/%m/%d")
    return (date - timedelta(days=1)).strftime("%Y/%m/%d")


def build_support_pressure_for_series(
    current_table: list[list[str]],
    previous_table: list[list[str]],
    series: str,
    settlement: int,
    label: str,
) -> dict[str, Any]:
    current_rows = parse_option_market_rows(current_table, series)
    previous_rows = parse_option_market_rows(previous_table, series)
    if not current_rows:
        raise ValueError(f"找不到 {series} 臺指選擇權履約價資料")

    strikes = sorted(current_rows.keys())
    atm_strike = min(strikes, key=lambda strike: abs(strike - settlement))

    rows = []
    for strike in strikes:
        current = current_rows[strike]
        previous = previous_rows.get(strike, {})
        rows.append(
            {
                "strike": strike,
                "callOi": current.get("callOi"),
                "putOi": current.get("putOi"),
                "callChange": (
                    None if current.get("callOi") is None else current.get("callOi", 0) - int(previous.get("callOi") or 0)
                ),
                "putChange": (
                    None if current.get("putOi") is None else current.get("putOi", 0) - int(previous.get("putOi") or 0)
                ),
            }
        )

    visible_rows = [
        row for row in rows
        if abs(row["strike"] - atm_strike) <= 600
        or (row["callOi"] or 0) >= 1000
        or (row["putOi"] or 0) >= 1000
    ]

    call_rows = [row for row in rows if row["callOi"] is not None]
    put_rows = [row for row in rows if row["putOi"] is not None]
    ceiling = max(call_rows, key=lambda row: row["callOi"] or 0)
    floor = max(put_rows, key=lambda row: row["putOi"] or 0)
    defense_candidates = [row for row in put_rows if row["strike"] <= atm_strike]
    defense = max(defense_candidates, key=lambda row: (row["putOi"] or 0, row["strike"])) if defense_candidates else floor

    call_atm = next((row for row in rows if row["strike"] == atm_strike), None)
    put_bias = (
        "Put 端較強"
        if (defense["putOi"] or 0) > ((call_atm or {}).get("callOi") or 0)
        else "Call 端較強"
    )

    call_itm_rows = [row for row in rows if row["strike"] < atm_strike][-5:]
    put_itm_rows = [row for row in rows if row["strike"] > atm_strike][:5]
    call_itm_oi = sum((row["callOi"] or 0) for row in call_itm_rows)
    call_itm_change = sum((row["callChange"] or 0) for row in call_itm_rows)
    put_itm_oi = sum((row["putOi"] or 0) for row in put_itm_rows)
    put_itm_change = sum((row["putChange"] or 0) for row in put_itm_rows)

    return {
        "label": label,
        "series": series,
        "txSettlement": settlement,
        "atmStrike": atm_strike,
        "ceiling": ceiling,
        "floor": floor,
        "defense": defense,
        "rows": visible_rows,
        "highlights": [
            f"{label} 序列 {series}：ATM 在 {atm_strike:,}，最大 Call OI 為 {ceiling['strike']:,} 的 {format_number(ceiling['callOi'])} 口。",
            f"{label} 最大 Put OI 為 {floor['strike']:,} 的 {format_number(floor['putOi'])} 口；近價主要防線在 {defense['strike']:,}，Put OI {format_number(defense['putOi'])} 口。",
            f"{label} 以 ATM 附近比較，{put_bias}；{defense['strike']:,} Put OI {format_number(defense['putOi'])} 口，相對 {defense['strike']:,} Call OI {format_number(next((row['callOi'] for row in rows if row['strike'] == defense['strike']), None))} 口。",
            f"{label} 價內五檔增減：Call 價內五檔 OI 合計 {format_number(call_itm_oi)} 口、增減 {format_signed(call_itm_change)}；Put 價內五檔 OI 合計 {format_number(put_itm_oi)} 口、增減 {format_signed(put_itm_change)}。",
            f"{label} 價內五檔判讀：{classify_itm_bias(call_itm_change, put_itm_change, call_itm_oi, put_itm_oi)}",
        ],
        "itmFiveAnalysis": {
            "callOi": call_itm_oi,
            "callChange": call_itm_change,
            "putOi": put_itm_oi,
            "putChange": put_itm_change,
            "bias": classify_itm_bias(call_itm_change, put_itm_change, call_itm_oi, put_itm_oi),
        },
    }


def select_support_pressure_series(
    current_table: list[list[str]],
    report_date: str,
    monthly_contract: str,
) -> list[dict[str, Any]]:
    metas = parse_option_series_meta(current_table)
    report_dt = datetime.strptime(report_date, "%Y/%m/%d")

    monthly = next((item for item in metas if item["series"] == monthly_contract), None)
    weekly = next((item for item in metas if "W" in item["series"] and item["expiry"] >= report_dt), None)
    flex = next(
        (
            item for item in metas
            if re.search(r"F\d+$", item["series"]) and item["series"] != monthly_contract and item["expiry"] >= report_dt
        ),
        None,
    )

    selected = []
    if monthly:
        selected.append({"label": "月選主契約", **monthly})
    if weekly:
        selected.append({"label": "最近一期 W", **weekly})
    if flex:
        selected.append({"label": "最近一期 F", **flex})
    return selected


def build_support_pressure_charts(
    current_html: str,
    previous_html: str,
    report_date: str,
    tx_reference: dict[str, Any],
) -> dict[str, Any]:
    current_table = find_table(parse_tables(current_html), "履約價")
    previous_table = find_table(parse_tables(previous_html), "履約價")
    selected = select_support_pressure_series(current_table, report_date, tx_reference["contract"])
    charts = [
        build_support_pressure_for_series(
            current_table,
            previous_table,
            item["series"],
            tx_reference["settlement"],
            item["label"],
        )
        for item in selected
    ]

    shared_supports = sorted(
        [
            {
                "label": chart["label"],
                "series": chart["series"],
                "strike": chart["defense"]["strike"],
                "oi": chart["defense"]["putOi"],
            }
            for chart in charts
        ],
        key=lambda item: item["strike"],
    )
    shared_resistances = sorted(
        [
            {
                "label": chart["label"],
                "series": chart["series"],
                "strike": chart["ceiling"]["strike"],
                "oi": chart["ceiling"]["callOi"],
            }
            for chart in charts
        ],
        key=lambda item: item["strike"],
    )

    support_min = min((item["strike"] for item in shared_supports), default=None)
    support_max = max((item["strike"] for item in shared_supports), default=None)
    resistance_min = min((item["strike"] for item in shared_resistances), default=None)
    resistance_max = max((item["strike"] for item in shared_resistances), default=None)

    support_cluster = (
        support_min is not None and support_max is not None and (support_max - support_min) <= 300
    )
    resistance_cluster = (
        resistance_min is not None and resistance_max is not None and (resistance_max - resistance_min) <= 500
    )

    combined_notes = []
    if charts:
        strongest_ceiling = max(charts, key=lambda item: item["ceiling"]["callOi"] or 0)
        strongest_floor = max(charts, key=lambda item: item["floor"]["putOi"] or 0)
        combined_notes.append(
            f"三張圖中最大 Call OI 出現在 {strongest_ceiling['label']} {strongest_ceiling['ceiling']['strike']:,}，為 {format_number(strongest_ceiling['ceiling']['callOi'])} 口。"
        )
        combined_notes.append(
            f"三張圖中最大 Put OI 出現在 {strongest_floor['label']} {strongest_floor['floor']['strike']:,}，為 {format_number(strongest_floor['floor']['putOi'])} 口。"
        )
        if support_min is not None and support_max is not None:
            combined_notes.append(
                f"共同支撐觀察區落在 {support_min:,} 至 {support_max:,}；"
                f"{'三張圖支撐相對集中。' if support_cluster else '三張圖支撐分散，需分開看。'}"
            )
        if resistance_min is not None and resistance_max is not None:
            combined_notes.append(
                f"共同壓力觀察區落在 {resistance_min:,} 至 {resistance_max:,}；"
                f"{'三張圖壓力相對集中。' if resistance_cluster else '三張圖壓力分散，需分開看。'}"
            )

    return {
        "txSettlement": tx_reference["settlement"],
        "monthlyContract": tx_reference["contract"],
        "charts": charts,
        "sharedSupport": {
            "rangeLow": support_min,
            "rangeHigh": support_max,
            "clustered": support_cluster,
            "items": shared_supports,
        },
        "sharedResistance": {
            "rangeLow": resistance_min,
            "rangeHigh": resistance_max,
            "clustered": resistance_cluster,
            "items": shared_resistances,
        },
        "combinedHighlights": combined_notes,
    }


def build_oi_change_detail(levels: dict[str, Any]) -> list[dict[str, Any]]:
    primary = levels["charts"][0]
    atm = primary["atmStrike"]
    focus = [row for row in primary["rows"] if abs(row["strike"] - atm) <= 200]
    if not focus:
        focus = primary["rows"][:5]
    return focus


def fetch_pc_ratio(end_date: str) -> list[dict[str, Any]]:
    start_date = (datetime.strptime(end_date, "%Y/%m/%d") - timedelta(days=30)).strftime("%Y/%m/%d")
    payload = {
        "queryStartDate": start_date,
        "queryEndDate": end_date,
        "down_type": "1",
    }
    raw = request_bytes(BQ888, "/cht/3/pcRatioDown", payload)
    text = raw.decode("cp950", "ignore")
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for row in reader:
        if not row.get("日期"):
            continue
        rows.append(
            {
                "date": row["日期"],
                "putVolume": to_int(row["賣權成交量"]),
                "callVolume": to_int(row["買權成交量"]),
                "volumeRatio": to_float(row["買賣權成交量比率%"]),
                "putOi": to_int(row["賣權未平倉量"]),
                "callOi": to_int(row["買權未平倉量"]),
                "oiRatio": to_float(row["買賣權未平倉量比率%"]),
            }
        )
    rows.sort(key=lambda item: item["date"], reverse=True)
    return rows[:5]


def extract_txo_pcr_from_market_html(html: str) -> dict[str, Any]:
    table = find_table(parse_tables(html), "履約價")
    stats = {"Call": {"vol": 0, "oi": 0}, "Put": {"vol": 0, "oi": 0}}
    for row in table[1:]:
        if len(row) < 20 or row[0] != "TXO":
            continue
        option_type = row[4]
        stats[option_type]["vol"] += to_int(row[14])
        stats[option_type]["oi"] += to_int(row[15])
    call_vol = stats["Call"]["vol"]
    call_oi = stats["Call"]["oi"]
    return {
        "putVolume": stats["Put"]["vol"],
        "callVolume": call_vol,
        "volumeRatio": (stats["Put"]["vol"] / call_vol * 100) if call_vol else 0.0,
        "putOi": stats["Put"]["oi"],
        "callOi": call_oi,
        "oiRatio": (stats["Put"]["oi"] / call_oi * 100) if call_oi else 0.0,
    }


def fetch_pc_ratio_fallback(end_date: str, count: int = 5) -> tuple[list[dict[str, Any]], str]:
    rows: list[dict[str, Any]] = []
    current = datetime.strptime(end_date, "%Y/%m/%d")
    checked = 0
    while len(rows) < count and checked < 14:
        date_text = current.strftime("%Y/%m/%d")
        html = request_html(
            TAIFEX,
            "/cht/3/optDailyMarketExcel",
            {
                "queryType": "2",
                "marketCode": "0",
                "commodity_id": "TXO",
                "commodity_id2": "",
                "queryDate": date_text,
            },
        )
        try:
            page_date = extract_page_date(html)
            values = extract_txo_pcr_from_market_html(html)
            if values["callVolume"] > 0 and values["callOi"] > 0:
                rows.append({"date": page_date, **values})
        except Exception:
            pass
        current -= timedelta(days=1)
        checked += 1
    return rows, "calculated"


def trend_arrow(delta: float) -> str:
    if delta > 0:
        return "↑"
    if delta < 0:
        return "↓"
    return "→"


def summarize_oi_focus(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "缺少 ATM 附近履約價資料，無法判讀近檔 OI 增減方向。"
    call_change = sum((row.get("callChange") or 0) for row in rows)
    put_change = sum((row.get("putChange") or 0) for row in rows)
    strongest_put = max(rows, key=lambda row: row.get("putChange") or 0)
    strongest_call = max(rows, key=lambda row: row.get("callChange") or 0)
    if put_change - call_change >= 200:
        return (
            f"ATM 附近賣權增倉較明顯，合計 Put 增減 {format_signed(put_change)}、"
            f"高於 Call 的 {format_signed(call_change)}；其中 {strongest_put['strike']:,} 履約價 Put 增倉 {format_signed(strongest_put['putChange'])} 最突出。"
        )
    if call_change - put_change >= 200:
        return (
            f"ATM 附近買權增倉較明顯，合計 Call 增減 {format_signed(call_change)}、"
            f"高於 Put 的 {format_signed(put_change)}；其中 {strongest_call['strike']:,} 履約價 Call 增倉 {format_signed(strongest_call['callChange'])} 最突出。"
        )
    return (
        f"ATM 附近 Call 與 Put 增倉差距不大，Call 合計 {format_signed(call_change)}、Put 合計 {format_signed(put_change)}，"
        "近檔籌碼偏向拉鋸。"
    )


def summarize_pc_ratio(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return [
            "缺資料。",
            "官方 PCR 端點未回傳資料列，且自行計算亦未取得有效資料。",
            "請以來源頁面再確認。"
        ]
    latest = rows[0]
    previous = rows[1] if len(rows) >= 2 else None
    highlights = [
        f"最新一筆 {latest['date']}：成交量 PCR {latest['volumeRatio']:.2f}%，未平倉量 PCR {latest['oiRatio']:.2f}%。",
        f"最新一筆賣權成交量 {format_number(latest['putVolume'])}，買權成交量 {format_number(latest['callVolume'])}；賣權未平倉量 {format_number(latest['putOi'])}，買權未平倉量 {format_number(latest['callOi'])}。",
    ]
    if previous:
        highlights.append(
            f"五日趨勢：成交量 PCR {trend_arrow(latest['volumeRatio'] - rows[-1]['volumeRatio'])} "
            f"{rows[-1]['volumeRatio']:.2f}% → {latest['volumeRatio']:.2f}%，"
            f"未平倉量 PCR {trend_arrow(latest['oiRatio'] - rows[-1]['oiRatio'])} "
            f"{rows[-1]['oiRatio']:.2f}% → {latest['oiRatio']:.2f}%。"
        )
        oi_delta = latest["oiRatio"] - previous["oiRatio"]
        if latest["oiRatio"] >= 110:
            highlights.append(
                f"未平倉量 PCR 高於 110%，代表賣權未平倉量明顯高於買權；較前一筆 {previous['date']} {format_signed(round(oi_delta, 2))} 個百分點，情緒偏向保護性或偏空避險。"
            )
        elif latest["oiRatio"] >= 100:
            highlights.append(
                f"未平倉量 PCR 站上 100%，表示賣權未平倉量略高於買權；較前一筆 {previous['date']} {format_signed(round(oi_delta, 2))} 個百分點，情緒偏中性偏保守。"
            )
        else:
            highlights.append(
                f"未平倉量 PCR 低於 100%，表示買權未平倉量高於賣權；較前一筆 {previous['date']} {format_signed(round(oi_delta, 2))} 個百分點，情緒偏向風險承擔。"
            )
    else:
        highlights.append("趨勢比較缺少前一筆資料，僅能先看當前水位。")
    return highlights


def specific_value_text(value: int | None) -> str:
    return format_number(value) if value is not None else "缺資料"


def specific_pct_text(value: float | None) -> str:
    return f"{value:.1f}%" if value is not None else "缺資料"


def build_analysis(report: dict[str, Any]) -> dict[str, Any]:
    summary_rows = report["tables"]["A"]["rows"]
    foreign = next(row for row in summary_rows if row["institution"] == "外資")
    dealer = next(row for row in summary_rows if row["institution"] == "自營商")
    investment = next(row for row in summary_rows if row["institution"] == "投信")
    tx_foreign = next(row for row in report["tables"]["B"]["rows"] if row["product"] == "臺股期貨" and row["institution"] == "外資")
    te_foreign = next((row for row in report["tables"]["B"]["rows"] if row["product"] == "電子期貨" and row["institution"] == "外資"), None)
    tf_foreign = next((row for row in report["tables"]["B"]["rows"] if row["product"] == "金融期貨" and row["institution"] == "外資"), None)
    mtx_foreign = next(row for row in report["tables"]["B"]["rows"] if row["product"] == "小型臺指期貨" and row["institution"] == "外資")
    tmf_foreign = next(row for row in report["tables"]["B"]["rows"] if row["product"] == "微型臺指期貨" and row["institution"] == "外資")
    large_rows = report["tables"]["C"]["rows"]
    large = next((row for row in large_rows if row.get("contractType") == "monthly"), large_rows[0])
    levels = report["tables"]["E"]
    primary_levels = levels["charts"][0]
    oi_focus = report["tables"]["F"]["rows"]
    pc_ratio = report["tables"]["G"]["rows"]
    latest_ratio = pc_ratio[0] if pc_ratio else None
    prev_ratio = pc_ratio[1] if len(pc_ratio) > 1 else None

    support_range = report["tables"]["E"]["sharedSupport"]
    resistance_range = report["tables"]["E"]["sharedResistance"]

    highlights = [
        f"外資台指相關商品未平倉淨額為 {foreign['combinedOiNetQty']:+,} 口；自營商為 {dealer['combinedOiNetQty']:+,} 口；投信為 {investment['combinedOiNetQty']:+,} 口。",
        f"近月 TX 結算價 {levels['txSettlement']:,}；月選主契約最大 Call OI 在 {primary_levels['ceiling']['strike']:,}，最大 Put OI 在 {primary_levels['floor']['strike']:,}。",
        f"大額交易人近月前五大買方占比 {large['longTop5Pct']:.1f}%，前五大賣方占比 {large['shortTop5Pct']:.1f}%。",
    ]

    contradictions = []
    if foreign["combinedOiNetQty"] < 0 and primary_levels["defense"]["strike"] <= primary_levels["atmStrike"]:
        contradictions.append("外資未平倉偏空，但下方 Put OI 仍有明確防線，籌碼呈現方向與支撐並存。")
    if latest_ratio and latest_ratio["oiRatio"] > 100 and foreign["combinedOiNetQty"] < 0:
        contradictions.append("Put/Call 未平倉比高於 100%，但法人主力淨部位仍偏空，情緒與部位並未完全同步。")

    if foreign["combinedOiNetQty"] < 0:
        direction = f"外資合計未平倉淨額 {foreign['combinedOiNetQty']:+,} 口，方向偏空。"
    elif foreign["combinedOiNetQty"] > 0:
        direction = f"外資合計未平倉淨額 {foreign['combinedOiNetQty']:+,} 口，方向偏多。"
    else:
        direction = "外資合計未平倉淨額接近零，方向偏中性。"

    pc_text = "缺資料"
    if latest_ratio:
        pc_text = f"{latest_ratio['date']} 買賣權未平倉量比率為 {latest_ratio['oiRatio']:.2f}%"
        if prev_ratio:
            delta = latest_ratio["oiRatio"] - prev_ratio["oiRatio"]
            pc_text += f"，較前一筆 {prev_ratio['date']} {delta:+.2f} 個百分點。"
        else:
            pc_text += "。"

    itm_bias_lines = "；".join(
        f"{chart['label']}：{chart['itmFiveAnalysis']['bias']}"
        for chart in levels["charts"]
    )

    atm_lines = []
    for row in oi_focus:
        atm_lines.append(
            f"{row['strike']:,} 履約價：Call OI {format_number(row['callOi'])}（增減 {format_signed(row['callChange'])}），"
            f"Put OI {format_number(row['putOi'])}（增減 {format_signed(row['putChange'])}）。"
        )

    return {
        "highlights": highlights,
        "sections": [
            {
                "title": "三大法人分析",
                "body": (
                    f"{direction} 投信合計為 {investment['combinedOiNetQty']:+,} 口，自營商合計為 {dealer['combinedOiNetQty']:+,} 口。"
                    "本段僅根據 A 表整理後數字描述，不延伸至未列示商品。"
                ),
            },
            {
                "title": "期貨分契約分析",
                "body": (
                    f"外資在臺股期貨未平倉淨額 {tx_foreign['oiNetQty']:+,} 口，"
                    f"較前一日 {tx_foreign['dayChangeOiNetQty']:+,} 口，自 {tx_foreign['cycleStartDate']} 起累積 {tx_foreign['cycleChangeOiNetQty']:+,} 口；"
                    f"電子期貨 {te_foreign['oiNetQty']:+,} 口、單日 {te_foreign['dayChangeOiNetQty']:+,} 口；"
                    f"金融期貨 {tf_foreign['oiNetQty']:+,} 口、單日 {tf_foreign['dayChangeOiNetQty']:+,} 口；"
                    f"小型臺指期貨 {mtx_foreign['oiNetQty']:+,} 口，微型臺指期貨 {tmf_foreign['oiNetQty']:+,} 口。"
                    "可看出外資主力空單仍集中在大台，電子與金融期部位相對較小。"
                ),
            },
            {
                "title": "大額交易人集中度分析",
                "body": (
                    f"近月前五大買方占比 {large['longTop5Pct']:.1f}%，前十大買方占比 {large['longTop10Pct']:.1f}%，"
                    f"代表買方第 6 至第 10 大合計再增加 {large['longTop10Qty'] - large['longTop5Qty']:,} 口、{large['longTop10Pct'] - large['longTop5Pct']:+.1f} 個百分點；"
                    f"前五大賣方占比 {large['shortTop5Pct']:.1f}%，前十大賣方占比 {large['shortTop10Pct']:.1f}%，"
                    f"代表賣方第 6 至第 10 大再增加 {large['shortTop10Qty'] - large['shortTop5Qty']:,} 口、{large['shortTop10Pct'] - large['shortTop5Pct']:+.1f} 個百分點。"
                    f" 特定法人部分，買方前五大為 {specific_value_text(large['longTop5SpecificQty'])} 口、前十大為 {specific_value_text(large['longTop10SpecificQty'])} 口；"
                    f"賣方前五大為 {specific_value_text(large['shortTop5SpecificQty'])} 口、前十大為 {specific_value_text(large['shortTop10SpecificQty'])} 口。"
                ),
            },
            {
                "title": "選擇權支撐壓力分析",
                "body": (
                    f"月選主契約 ATM 在 {primary_levels['atmStrike']:,}，"
                    f"最大 Call OI 落在 {primary_levels['ceiling']['strike']:,}（{format_number(primary_levels['ceiling']['callOi'])} 口），"
                    f"最大 Put OI 落在 {primary_levels['floor']['strike']:,}（{format_number(primary_levels['floor']['putOi'])} 口）。"
                    f"交叉比對三張圖後，共同支撐觀察區在 {format_number(support_range['rangeLow'])} 至 {format_number(support_range['rangeHigh'])}，"
                    f"{'支撐相對集中；' if support_range['clustered'] else '支撐偏分散；'}"
                    f"共同壓力觀察區在 {format_number(resistance_range['rangeLow'])} 至 {format_number(resistance_range['rangeHigh'])}，"
                    f"{'壓力相對集中。' if resistance_range['clustered'] else '壓力偏分散。'}"
                    f" 價內五檔判讀方面，{itm_bias_lines}"
                ),
            },
            {
                "title": "ATM OI 增減分析",
                "body": " ".join(atm_lines),
            },
            {
                "title": "Put/Call Ratio 情緒分析",
                "body": pc_text,
            },
            {
                "title": "綜合判讀",
                "body": (
                    "；".join(contradictions) if contradictions else "目前各表數據未見明顯互相衝突之處。"
                ),
            },
        ],
        "strategies": {
            "conservative": "若偏空但下方仍有支撐，可優先考慮 Bear Put Spread，最大損失可控。",
            "neutral": "若預期震盪，可考慮在主要支撐與壓力之外佈局 Bull Put Spread 或 Bear Call Spread，但仍應控制履約價間距與部位大小。",
            "aggressive": "若要提高方向性曝險，仍建議用 Bull Call Spread 或 Bear Put Spread 取代單邊裸部位，避免無上限風險。",
        },
        "conclusion": (
            f"目前數據顯示：法人主力部位偏空，但 {primary_levels['defense']['strike']:,} 至 {primary_levels['floor']['strike']:,} 一帶仍有 Put OI 支撐，"
            "較符合偏空震盪而非單邊失控的結構。"
        ),
    }


def build_telegram_text(report: dict[str, Any]) -> str:
    foreign = next(row for row in report["tables"]["A"]["rows"] if row["institution"] == "外資")
    levels = report["tables"]["E"]["charts"][0]
    ratio = report["tables"]["G"]["rows"][0] if report["tables"]["G"]["rows"] else None
    link = report["meta"].get("reportUrl", "")
    ratio_text = f"PCR OI {ratio['oiRatio']:.2f}%" if ratio else "PCR 缺資料"
    lines = [
        f"{report['meta']['date']} 台指籌碼完整快報",
        f"完整報告：{link}",
        "",
        "A. 三大法人總表",
    ]
    lines.extend(f"- {item}" for item in report["tables"]["A"]["highlights"])
    lines.extend(["", "B. 三大法人期貨分契約"])
    lines.extend(f"- {item}" for item in report["tables"]["B"]["highlights"])
    lines.extend(["", "C. 大額交易人未沖銷"])
    lines.extend(f"- {item}" for item in report["tables"]["C"]["highlights"])
    lines.extend(["", "D. 三大法人選擇權分契約"])
    lines.extend(f"- {item}" for item in report["tables"]["D"]["highlights"])
    lines.extend(["", "E. 選擇權支撐壓力"])
    lines.extend(f"- {item}" for item in report["tables"]["E"]["highlights"])
    for chart in report["tables"]["E"]["charts"]:
        lines.append(f"- {chart['label']} / {chart['series']}")
        lines.extend(f"  - {item}" for item in chart["highlights"])
    lines.extend(["", "F. OI 增減"])
    lines.extend(f"- {item}" for item in report["tables"]["F"]["highlights"])
    lines.extend(["", "G. Put/Call Ratio"])
    lines.extend(f"- {item}" for item in report["tables"]["G"]["highlights"])
    lines.extend(["", "H. 綜合分析"])
    lines.extend(f"- {item}" for item in report["analysis"]["highlights"])
    for section in report["analysis"]["sections"]:
        lines.append(f"- {section['title']}：{section['body']}")
    lines.extend(
        [
            "",
            "一句話結論",
            report["analysis"]["conclusion"],
            "",
            f"速讀：外資 {foreign['combinedOiNetQty']:+,} 口，TX 結算 {report['tables']['E']['txSettlement']:,}，ATM {levels['atmStrike']:,}，"
            f"壓力 {levels['ceiling']['strike']:,} Call，支撐 {levels['floor']['strike']:,} Put，{ratio_text}",
        ]
    )
    return "\n".join(lines)


def build_email_text(report: dict[str, Any]) -> str:
    analysis = report["analysis"]
    link = report["meta"].get("reportUrl", "")
    lines = [
        f"日期：{report['meta']['date']}",
        f"完整網頁：{link}",
        "",
        "今日重點摘要",
    ]
    lines.extend(f"- {item}" for item in analysis["highlights"])
    lines.extend(["", "表格摘要"])
    for key in ["A", "B", "C", "D", "E", "F", "G"]:
        section = report["tables"][key]
        lines.append(f"- {section['title']}")
        lines.extend(f"  - {item}" for item in section["highlights"])
        if key == "E":
            for chart in section["charts"]:
                lines.append(f"  - {chart['label']} / {chart['series']}")
                lines.extend(f"    - {item}" for item in chart["highlights"])
    lines.extend(["", "分項分析"])
    for section in analysis["sections"]:
        lines.append(f"- {section['title']}：{section['body']}")
    lines.extend(
        [
            "",
            "策略建議",
            f"- 保守版：{analysis['strategies']['conservative']}",
            f"- 中性版：{analysis['strategies']['neutral']}",
            f"- 積極版：{analysis['strategies']['aggressive']}",
            "",
            f"一句話結論：{analysis['conclusion']}",
        ]
    )
    return "\n".join(lines)


def format_number(value: Any) -> str:
    if value is None:
        return "缺資料"
    return f"{int(value):,}"


def format_signed(value: Any) -> str:
    if value is None:
        return "缺資料"
    return f"{int(value):+,}"


def pdf_escape(text: Any) -> str:
    value = str(text if text is not None else "缺資料")
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br/>")
    )


PDF_POS_COLOR = "#dc2626"
PDF_NEG_COLOR = "#16a34a"
PDF_NEUTRAL_COLOR = "#334155"
PDF_MUTED_COLOR = "#64748b"


def pdf_markup(text: Any) -> str:
    escaped = pdf_escape(text)
    escaped = re.sub(
        r"([+-]\d[\d,]*(?:\.\d+)?%?)",
        lambda m: f'<font color="{PDF_POS_COLOR if m.group(1).startswith("+") else PDF_NEG_COLOR}">{m.group(1)}</font>',
        escaped,
    )
    escaped = escaped.replace("缺資料", f'<font color="{PDF_MUTED_COLOR}">缺資料</font>')
    escaped = escaped.replace("尚未補齊", f'<font color="{PDF_MUTED_COLOR}">尚未補齊</font>')
    return escaped


def pdf_paragraph(text: Any, style: ParagraphStyle, whole_cell: bool = False) -> Paragraph:
    raw = str(text if text is not None else "缺資料").strip()
    if whole_cell:
        if raw.startswith("+"):
            return Paragraph(f'<font color="{PDF_POS_COLOR}">{pdf_escape(raw)}</font>', style)
        if raw.startswith("-"):
            return Paragraph(f'<font color="{PDF_NEG_COLOR}">{pdf_escape(raw)}</font>', style)
        if raw in {"缺資料", "尚未補齊"}:
            return Paragraph(f'<font color="{PDF_MUTED_COLOR}">{pdf_escape(raw)}</font>', style)
        return Paragraph(f'<font color="{PDF_NEUTRAL_COLOR}">{pdf_escape(raw)}</font>', style)
    return Paragraph(pdf_markup(raw), style)


def pdf_bullets(items: list[str], story: list[Any], body_style: ParagraphStyle) -> None:
    for item in items:
        story.append(Paragraph(f"• {pdf_markup(item)}", body_style))
        story.append(Spacer(1, 2 * mm))


def pdf_table(
    data: list[list[Any]],
    body_style: ParagraphStyle,
    header_style: ParagraphStyle,
    col_widths: list[float] | None = None,
) -> Table:
    styled_data: list[list[Any]] = []
    for row_index, row in enumerate(data):
        styled_row: list[Any] = []
        for col_index, cell in enumerate(row):
            if row_index == 0:
                styled_row.append(Paragraph(pdf_escape(cell), header_style))
            else:
                styled_row.append(pdf_paragraph(cell, body_style, whole_cell=(col_index > 0)))
        styled_data.append(styled_row)

    table = Table(styled_data, colWidths=col_widths, repeatRows=1)
    style_commands = [
        ("FONTNAME", (0, 0), (-1, -1), "STSong-Light"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("LEADING", (0, 0), (-1, -1), 10),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dbeafe")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#94a3b8")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    for row_index in range(1, len(data)):
        if row_index % 2 == 1:
            style_commands.append(("BACKGROUND", (0, row_index), (-1, row_index), colors.HexColor("#f8fafc")))
        else:
            style_commands.append(("BACKGROUND", (0, row_index), (-1, row_index), colors.white))
    table.setStyle(
        TableStyle(style_commands)
    )
    return table


def build_report_pdf(report: dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=12 * mm,
        bottomMargin=12 * mm,
        title=f"{report['meta']['date']} 台指期貨/選擇權籌碼完整報告",
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "PdfTitle",
        parent=styles["Title"],
        fontName="STSong-Light",
        fontSize=18,
        leading=24,
        alignment=TA_LEFT,
    )
    heading_style = ParagraphStyle(
        "PdfHeading",
        parent=styles["Heading2"],
        fontName="STSong-Light",
        fontSize=13,
        leading=18,
        spaceBefore=8,
        spaceAfter=6,
    )
    subheading_style = ParagraphStyle(
        "PdfSubHeading",
        parent=styles["Heading3"],
        fontName="STSong-Light",
        fontSize=10,
        leading=14,
        spaceBefore=6,
        spaceAfter=4,
    )
    body_style = ParagraphStyle(
        "PdfBody",
        parent=styles["BodyText"],
        fontName="STSong-Light",
        fontSize=9,
        leading=13,
        spaceAfter=3,
        textColor=colors.HexColor(PDF_NEUTRAL_COLOR),
    )
    table_header_style = ParagraphStyle(
        "PdfTableHeader",
        parent=body_style,
        fontName="STSong-Light",
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#0f172a"),
    )

    story: list[Any] = []
    story.append(Paragraph(f"{pdf_escape(report['meta']['date'])} 台指期貨 / 選擇權籌碼完整報告", title_style))
    story.append(Paragraph(f"完整網頁：{pdf_escape(report['meta'].get('reportUrl', ''))}", body_style))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("速覽彙總", heading_style))
    pdf_bullets(report["changeOverview"].get("highlights", []), story, body_style)
    if report["changeOverview"].get("largeTraderSummary"):
        story.append(Paragraph("大額交易人前五大 / 前十大", subheading_style))
        pdf_bullets(report["changeOverview"]["largeTraderSummary"], story, body_style)

    for key in ["A", "B", "C", "D", "E", "F", "G"]:
        section = report["tables"][key]
        story.append(Paragraph(pdf_escape(section["title"]), heading_style))
        story.append(Paragraph(f"日期：{pdf_escape(section['date'])}　單位：{pdf_escape(section['unit'])}", body_style))

        if key == "A":
            data = [["法人", "期貨交易淨額", "期貨未平倉淨額", "選擇權交易淨額", "選擇權未平倉淨額", "合計未平倉淨額"]]
            for row in section["rows"]:
                data.append([
                    row["institution"], format_signed(row["futuresTradeNetQty"]), format_signed(row["futuresOiNetQty"]),
                    format_signed(row["optionsTradeNetQty"]), format_signed(row["optionsOiNetQty"]), format_signed(row["combinedOiNetQty"])
                ])
            story.append(pdf_table(data, body_style, table_header_style, [28 * mm, 25 * mm, 25 * mm, 25 * mm, 25 * mm, 28 * mm]))
        elif key == "B":
            data = [["商品", "身份別", "交易淨額", "未平倉淨額", "前一日淨額", "單日變動", "基準淨額", "累積變動"]]
            for row in section["rows"]:
                data.append([
                    row["product"], row["institution"], format_signed(row["tradeNetQty"]), format_signed(row["oiNetQty"]),
                    format_signed(row["previousOiNetQty"]), format_signed(row["dayChangeOiNetQty"]),
                    format_signed(row["cycleStartOiNetQty"]), format_signed(row["cycleChangeOiNetQty"]),
                ])
            story.append(pdf_table(data, body_style, table_header_style, [24 * mm, 18 * mm, 18 * mm, 18 * mm, 18 * mm, 18 * mm, 18 * mm, 18 * mm]))
        elif key == "C":
            data = [["契約", "月份", "買前五特法", "買前十特法", "賣前五特法", "賣前十特法", "全市場OI"]]
            for row in section["rows"]:
                data.append([
                    row["contractLabel"], row["expiry"], format_number(row["longTop5SpecificQty"]), format_number(row["longTop10SpecificQty"]),
                    format_number(row["shortTop5SpecificQty"]), format_number(row["shortTop10SpecificQty"]), format_number(row["marketOi"])
                ])
            story.append(pdf_table(data, body_style, table_header_style, [18 * mm, 18 * mm, 24 * mm, 24 * mm, 24 * mm, 24 * mm, 20 * mm]))
        elif key == "D":
            data = [["身份別", "交易淨額", "未平倉淨額"]]
            for row in section["rows"]:
                data.append([row["institution"], format_signed(row["tradeNetQty"]), format_signed(row["oiNetQty"])])
            story.append(pdf_table(data, body_style, table_header_style, [30 * mm, 30 * mm, 30 * mm]))
        elif key == "E":
            data = [["圖表", "序列", "ATM", "主壓", "主撐", "近防線", "價內五檔判讀"]]
            for chart in section["charts"]:
                data.append([
                    chart["label"], chart["series"], format_number(chart["atmStrike"]),
                    f"{format_number(chart['ceiling']['strike'])} / {format_number(chart['ceiling']['callOi'])}",
                    f"{format_number(chart['floor']['strike'])} / {format_number(chart['floor']['putOi'])}",
                    f"{format_number(chart['defense']['strike'])} / {format_number(chart['defense']['putOi'])}",
                    chart["highlights"][-1] if chart["highlights"] else "缺資料",
                ])
            story.append(pdf_table(data, body_style, table_header_style, [20 * mm, 20 * mm, 15 * mm, 25 * mm, 25 * mm, 25 * mm, 45 * mm]))
        elif key == "F":
            data = [["日期", "當日未平倉量", "前一日", "前一日未平倉量", "整體增減"]]
            overall = section["overall"]
            data.append([
                overall.get("date") or "缺資料", format_number(overall.get("currentOi")), overall.get("previousDate") or "缺資料",
                format_number(overall.get("previousOi")), format_signed(overall.get("change"))
            ])
            story.append(pdf_table(data, body_style, table_header_style, [25 * mm, 30 * mm, 25 * mm, 30 * mm, 20 * mm]))
            detail = [["履約價", "Call OI", "Call 增減", "Put OI", "Put 增減"]]
            for row in section["rows"]:
                detail.append([format_number(row["strike"]), format_number(row["callOi"]), format_signed(row["callChange"]), format_number(row["putOi"]), format_signed(row["putChange"])])
            story.append(Spacer(1, 2 * mm))
            story.append(pdf_table(detail, body_style, table_header_style, [20 * mm, 20 * mm, 20 * mm, 20 * mm, 20 * mm]))
        elif key == "G":
            data = [["日期", "賣權成交量", "買權成交量", "成交量比率", "賣權未平倉量", "買權未平倉量", "未平倉量比率"]]
            for row in section["rows"]:
                data.append([
                    row["date"], format_number(row["putVolume"]), format_number(row["callVolume"]), f"{row['volumeRatio']:.2f}%",
                    format_number(row["putOi"]), format_number(row["callOi"]), f"{row['oiRatio']:.2f}%"
                ])
            story.append(pdf_table(data, body_style, table_header_style, [22 * mm, 23 * mm, 23 * mm, 20 * mm, 23 * mm, 23 * mm, 20 * mm]))

        story.append(Spacer(1, 2 * mm))
        story.append(Paragraph("表格解讀", subheading_style))
        story.append(Paragraph(pdf_markup(section["interpretation"]), body_style))
        story.append(Paragraph("重點摘要", subheading_style))
        pdf_bullets(section.get("highlights", []), story, body_style)
        story.append(Paragraph("資料來源", subheading_style))
        for source in section.get("sources", []):
            story.append(Paragraph(pdf_markup(source), body_style))
        story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("H. 綜合分析", heading_style))
    story.append(Paragraph("今日重點摘要", subheading_style))
    pdf_bullets(report["analysis"]["highlights"], story, body_style)
    for section in report["analysis"]["sections"]:
        story.append(Paragraph(pdf_escape(section["title"]), subheading_style))
        story.append(Paragraph(pdf_markup(section["body"]), body_style))
    story.append(Paragraph("策略建議", subheading_style))
    story.append(Paragraph(pdf_markup(f"保守版：{report['analysis']['strategies']['conservative']}"), body_style))
    story.append(Paragraph(pdf_markup(f"中性版：{report['analysis']['strategies']['neutral']}"), body_style))
    story.append(Paragraph(pdf_markup(f"積極版：{report['analysis']['strategies']['aggressive']}"), body_style))
    story.append(Paragraph(pdf_markup(f"一句話結論：{report['analysis']['conclusion']}"), body_style))

    story.append(Paragraph("I. Telegram 精簡版", heading_style))
    story.append(Paragraph(pdf_markup(report["telegram"]), body_style))
    story.append(Paragraph("J. Email 完整版", heading_style))
    story.append(Paragraph(pdf_markup(report["email"]), body_style))

    doc.build(story)
    return buffer.getvalue()


def build_report(report_date: str | None = None, report_url: str | None = None) -> dict[str, Any]:
    report_date = report_date or latest_business_day()
    futures_html = request_html(
        TAIFEX,
        "/cht/3/futContractsDateExcel",
        {"queryType": "1", "queryDate": report_date, "commodityId": ""} if report_date else None,
    )
    options_html = request_html(
        TAIFEX,
        "/cht/3/optContractsDateExcel",
        {"queryType": "1", "queryDate": report_date, "commodityId": ""} if report_date else None,
    )
    tx_daily_html = request_html(
        TAIFEX,
        "/cht/3/futDailyMarketExcel",
        {
            "queryType": "2",
            "marketCode": "0",
            "commodity_id": "TX",
            "commodity_id2": "",
            "queryDate": report_date,
        } if report_date else None,
    )
    current_option_market_html = request_html(
        TAIFEX,
        "/cht/3/optDailyMarketExcel",
        {
            "queryType": "2",
            "marketCode": "0",
            "commodity_id": "TXO",
            "commodity_id2": "",
            "queryDate": report_date,
        } if report_date else None,
    )
    oi_change_html = request_html(
        TAIFEX,
        "/cht/7/dailyIndOptChgData",
        {"queryDate": report_date} if report_date else None,
    )

    futures_table = find_table(parse_tables(futures_html), "臺股期貨")
    options_table = find_table(parse_tables(options_html), "商品 名稱")
    tx_daily_table = find_table(parse_tables(tx_daily_html), "契約")
    oi_change_table = find_table(parse_tables(oi_change_html), "前一日")
    effective_date = report_date or extract_page_date(futures_html)

    futures_contracts = parse_futures_contracts(futures_table)
    option_contracts = parse_option_contracts(options_table)
    total_summary = build_total_summary(futures_contracts, option_contracts)
    tx_reference = parse_tx_reference(tx_daily_table)
    large_trader_rows = fetch_large_trader_for_date(effective_date, tx_reference["contract"])
    if not large_trader_rows:
        raise ValueError("找不到大額交易人資料")
    large_trader = next((row for row in large_trader_rows if row["contractType"] == "monthly"), large_trader_rows[0])
    oi_change = parse_oi_change(oi_change_table, effective_date)
    base_date = oi_change["date"] or effective_date
    futures_history = fetch_futures_history_rows(base_date, 5)
    monthly_cycle_start_date = monthly_cycle_start(tx_reference["contract"])
    weekly_cycle_start_date = cycle_start_thursday(base_date)
    cycle_start_rows = futures_contracts if monthly_cycle_start_date == base_date else fetch_futures_rows_for_date(monthly_cycle_start_date)
    large_previous_date, large_previous_rows = fetch_previous_large_trader_business_day(base_date, tx_reference["contract"])
    large_cycle_rows = {
        "weekly": large_trader_rows if weekly_cycle_start_date == base_date else fetch_large_trader_for_date(weekly_cycle_start_date, tx_reference["contract"]),
        "monthly": large_trader_rows if monthly_cycle_start_date == base_date else fetch_large_trader_for_date(monthly_cycle_start_date, tx_reference["contract"]),
    }
    futures_contracts = enrich_futures_with_history(futures_contracts, futures_history, cycle_start_rows, monthly_cycle_start_date)
    futures_category_analysis = build_futures_category_analysis(futures_contracts)
    futures_delta_overview = build_futures_delta_overview(futures_contracts)
    previous_option_market_date = oi_change["previousDate"]
    previous_option_market_html = None
    if previous_option_market_date:
        previous_option_market_html = request_html(
            TAIFEX,
            "/cht/3/optDailyMarketExcel",
            {
                "queryType": "2",
                "marketCode": "0",
                "commodity_id": "TXO",
                "commodity_id2": "",
                "queryDate": previous_option_market_date,
            },
        )
    else:
        previous_option_market_date, previous_option_market_html = fetch_previous_available_option_market(base_date)
        oi_change["previousDate"] = previous_option_market_date

    support_pressure = build_support_pressure_charts(
        current_option_market_html,
        previous_option_market_html,
        base_date,
        tx_reference,
    )
    oi_change_detail = build_oi_change_detail(support_pressure)
    pc_ratio = fetch_pc_ratio(base_date)
    pc_ratio_method = "official"
    if not pc_ratio:
        pc_ratio, pc_ratio_method = fetch_pc_ratio_fallback(base_date)

    long_top10_add_qty = large_trader["longTop10Qty"] - large_trader["longTop5Qty"]
    short_top10_add_qty = large_trader["shortTop10Qty"] - large_trader["shortTop5Qty"]
    long_top10_add_pct = large_trader["longTop10Pct"] - large_trader["longTop5Pct"]
    short_top10_add_pct = large_trader["shortTop10Pct"] - large_trader["shortTop5Pct"]
    long_specific_share_5 = (
        large_trader["longTop5SpecificQty"] / large_trader["longTop5Qty"] * 100
        if large_trader["longTop5SpecificQty"] is not None and large_trader["longTop5Qty"]
        else None
    )
    long_specific_share_10 = (
        large_trader["longTop10SpecificQty"] / large_trader["longTop10Qty"] * 100
        if large_trader["longTop10SpecificQty"] is not None and large_trader["longTop10Qty"]
        else None
    )
    short_specific_share_5 = (
        large_trader["shortTop5SpecificQty"] / large_trader["shortTop5Qty"] * 100
        if large_trader["shortTop5SpecificQty"] is not None and large_trader["shortTop5Qty"]
        else None
    )
    short_specific_share_10 = (
        large_trader["shortTop10SpecificQty"] / large_trader["shortTop10Qty"] * 100
        if large_trader["shortTop10SpecificQty"] is not None and large_trader["shortTop10Qty"]
        else None
    )

    report = {
        "meta": {
            "date": base_date,
            "generatedAt": datetime.now().isoformat(timespec="seconds"),
            "reportUrl": report_url or PUBLIC_BASE_URL,
        },
        "changeOverview": futures_delta_overview,
        "tables": {
            "A": {
                "title": "A. 三大法人總表詳細版",
                "date": base_date,
                "unit": "口",
                "rows": total_summary,
                "interpretation": (
                    "本表為整理後資料，將 B 表的台指期貨分契約與 D 表的台指選擇權分契約，"
                    "依法人別彙總為同一張總表，用來先看法人整體未平倉方向。"
                ),
                "highlights": [
                    "本表屬於摘要整理，非單一官方原始表；數字均來自 B、D 兩張官方表加總。",
                    "若某法人在某來源缺資料，會直接標示缺資料，不自行補值。",
                ],
                "sources": [
                    f"{TAIFEX}/cht/3/futContractsDateExcel",
                    f"{TAIFEX}/cht/3/optContractsDateExcel",
                ],
            },
            "B": {
                "title": "B. 三大法人期貨分契約詳細版",
                "date": base_date,
                "unit": "口、千元",
                "cycleBasis": {
                    "monthly": monthly_cycle_start_date,
                },
                "rows": futures_contracts,
                "categoryAnalysis": futures_category_analysis,
                "interpretation": "本表直接整理近月期貨分契約的法人多空與未平倉淨額，並補上與前一營業日及前五個營業日相比的未平倉淨額變動，用來看部位是否持續擴張或收斂。",
                "highlights": [
                    "本表保留臺股期貨、電子期貨、金融期貨、小型臺指期貨、微型臺指期貨。",
                    "單日變動為當日未平倉淨額減前一營業日未平倉淨額；五日累積變動為當日未平倉淨額減前五個營業日前未平倉淨額。"
                ],
                "sources": [f"{TAIFEX}/cht/3/futContractsDateExcel"],
            },
            "C": {
                "title": "C. 大額交易人未沖銷詳細版",
                "date": base_date,
                "unit": "口、%",
                "cycleBasis": {
                    "weekly": weekly_cycle_start_date,
                    "monthly": monthly_cycle_start_date,
                },
                "rows": large_trader_rows,
                "interpretation": "本表用來看近月台股期貨大額交易人集中度，並將前五大與前十大之中特定法人合計一併拆開，觀察集中度是否由特定法人主導。",
                "highlights": [
                    "本表拆分為週契約與月契約兩列，不使用『所有契約』欄位，以避免和近月行情口徑混淆。",
                    "若欄位顯示括號內數字，代表前五大或前十大之中的特定法人合計部位。",
                    "本表改以官方下載 CSV 為準，不再依賴查詢頁 HTML，以避免歷史日期回傳口徑混淆。",
                ],
                "sources": [f"{TAIFEX}/cht/3/dlLargeTraderFutDown"],
            },
            "D": {
                "title": "D. 三大法人選擇權分契約詳細版",
                "date": base_date,
                "unit": "口、千元",
                "rows": option_contracts,
                "interpretation": "本表直接整理臺指選擇權法人分契約資料，主要看交易淨額與未平倉淨額。",
                "highlights": [
                    "此表為『區分各選擇權契約』，不是買權/賣權分計表。",
                ],
                "sources": [f"{TAIFEX}/cht/3/optContractsDateExcel"],
            },
            "E": {
                "title": "E. 選擇權支撐壓力詳細版",
                "date": base_date,
                "unit": "口",
                "txSettlement": support_pressure["txSettlement"],
                "monthlyContract": support_pressure["monthlyContract"],
                "charts": support_pressure["charts"],
                "sharedSupport": support_pressure["sharedSupport"],
                "sharedResistance": support_pressure["sharedResistance"],
                "interpretation": "本表同時整理三張圖：月選主契約、最近一期 W、最近一期 F。每張圖都用同一個近月 TX 結算價做 ATM 對照，方便直接比較不同序列的支撐與壓力是否集中。",
                "highlights": support_pressure["combinedHighlights"],
                "sources": [f"{TAIFEX}/cht/3/optDailyMarketExcel"],
            },
            "F": {
                "title": "F. OI 增減詳細版",
                "date": base_date,
                "unit": "口",
                "overall": oi_change,
                "rows": oi_change_detail,
                "interpretation": (
                    "本表分成兩層：上半部是官方整體未平倉量增減；下半部是同一近月序列各履約價 OI 與前一交易日相比的增減。"
                ),
                "highlights": [
                    (
                        f"整體未平倉量較前一交易日變動 {oi_change['change']:+,} 口。"
                        if oi_change["change"] is not None else
                        "官方整體未平倉量增減表本日回傳缺資料。"
                    ),
                    summarize_oi_focus(oi_change_detail),
                    "履約價增減為以當日近月序列 OI 減去前一交易日同序列 OI 計算。",
                ],
                "sources": [
                    f"{TAIFEX}/cht/7/dailyIndOptChgData",
                    f"{TAIFEX}/cht/3/optDailyMarketExcel",
                ],
            },
            "G": {
                "title": "G. Put/Call Ratio / 買賣權比",
                "date": base_date,
                "unit": "口、%",
                "rows": pc_ratio,
                "method": pc_ratio_method,
                "interpretation": (
                    "本表使用臺指選擇權買賣權比，觀察成交量與未平倉量在賣權與買權之間的相對強弱。"
                    if pc_ratio_method == "official"
                    else "本表因官方 PCR 端點未回傳資料列，改用期交所 optDailyMarketExcel 將 TXO 全部序列的 Put/Call 成交量與未平倉量加總計算。"
                ),
                "highlights": summarize_pc_ratio(pc_ratio) + [
                    "本表官方定義為週契約與各到期月份契約合併計算。" if pc_ratio_method == "official" else "本表為自行計算口徑：TXO 全部序列合計。"
                ],
                "sources": [f"{BQ888}/cht/3/pcRatioDown"] if pc_ratio_method == "official" else [f"{TAIFEX}/cht/3/optDailyMarketExcel", f"{BQ888}/cht/3/pcRatioDown"],
            },
        },
    }

    top_total_long = max(total_summary, key=lambda row: row["combinedOiNetQty"])
    top_total_short = min(total_summary, key=lambda row: row["combinedOiNetQty"])
    tx_rows = [row for row in futures_contracts if row["product"] == "臺股期貨"]
    te_rows = [row for row in futures_contracts if row["product"] == "電子期貨"]
    tf_rows = [row for row in futures_contracts if row["product"] == "金融期貨"]
    strongest_tx_long = max(tx_rows, key=lambda row: row["oiNetQty"])
    strongest_tx_short = min(tx_rows, key=lambda row: row["oiNetQty"])
    tx_foreign = next(row for row in tx_rows if row["institution"] == "外資")
    te_foreign = next((row for row in te_rows if row["institution"] == "外資"), None)
    tf_foreign = next((row for row in tf_rows if row["institution"] == "外資"), None)
    option_long = max(option_contracts, key=lambda row: row["oiNetQty"])
    option_short = min(option_contracts, key=lambda row: row["oiNetQty"])
    total_long_label = "淨多最高" if top_total_long["combinedOiNetQty"] > 0 else "相對最不偏空"
    option_long_label = "未平倉淨多最高" if option_long["oiNetQty"] > 0 else "未平倉相對最不偏空"

    report["tables"]["A"]["highlights"] = [
        f"外資合計未平倉淨額 {format_signed(next(row['combinedOiNetQty'] for row in total_summary if row['institution'] == '外資'))} 口；其中期貨 {format_signed(next(row['futuresOiNetQty'] for row in total_summary if row['institution'] == '外資'))}、選擇權 {format_signed(next(row['optionsOiNetQty'] for row in total_summary if row['institution'] == '外資'))}。",
        f"三大法人中{total_long_label}為 {top_total_long['institution']} {format_signed(top_total_long['combinedOiNetQty'])} 口；淨空最高為 {top_total_short['institution']} {format_signed(top_total_short['combinedOiNetQty'])} 口。",
        f"自營商合計未平倉淨額 {format_signed(next(row['combinedOiNetQty'] for row in total_summary if row['institution'] == '自營商'))} 口，投信為 {format_signed(next(row['combinedOiNetQty'] for row in total_summary if row['institution'] == '投信'))} 口。",
    ]
    report["tables"]["B"]["highlights"] = [
        f"B 表未平倉淨額比較基準：前一營業日為 {tx_foreign['previousDate'] or '缺資料'}，結算後累積基準日為 {tx_foreign['cycleStartDate'] or '缺資料'}。",
        f"臺股期貨未平倉淨多最高為 {strongest_tx_long['institution']} {format_signed(strongest_tx_long['oiNetQty'])} 口；未平倉淨空最高為 {strongest_tx_short['institution']} {format_signed(strongest_tx_short['oiNetQty'])} 口。",
        f"外資在臺股期貨未平倉淨額 {format_signed(tx_foreign['oiNetQty'])} 口，較前一日 {format_signed(tx_foreign['dayChangeOiNetQty'])} 口，自 {tx_foreign['cycleStartDate']} 起累積 {format_signed(tx_foreign['cycleChangeOiNetQty'])} 口。",
        f"外資在電子期貨未平倉淨額 {format_signed(te_foreign['oiNetQty']) if te_foreign else '缺資料'}，單日變動 {format_signed(te_foreign['dayChangeOiNetQty']) if te_foreign else '缺資料'}；金融期貨未平倉淨額 {format_signed(tf_foreign['oiNetQty']) if tf_foreign else '缺資料'}，單日變動 {format_signed(tf_foreign['dayChangeOiNetQty']) if tf_foreign else '缺資料'}。",
        f"外資在小型臺指期貨未平倉淨額 {format_signed(next(row['oiNetQty'] for row in futures_contracts if row['product'] == '小型臺指期貨' and row['institution'] == '外資'))} 口，微型臺指期貨 {format_signed(next(row['oiNetQty'] for row in futures_contracts if row['product'] == '微型臺指期貨' and row['institution'] == '外資'))} 口。",
    ]
    report["tables"]["C"]["highlights"] = [
        f"前五大買方 {format_number(large_trader['longTop5Qty'])} 口、占比 {large_trader['longTop5Pct']:.1f}%；其中特定法人 {specific_value_text(large_trader['longTop5SpecificQty'])} 口、占比 {specific_pct_text(large_trader['longTop5SpecificPct'])}。",
        f"前十大買方 {format_number(large_trader['longTop10Qty'])} 口、占比 {large_trader['longTop10Pct']:.1f}%；較前五大再增加 {format_number(long_top10_add_qty)} 口、占比增加 {long_top10_add_pct:+.1f} 個百分點；其中特定法人 {specific_value_text(large_trader['longTop10SpecificQty'])} 口。",
        f"前五大賣方 {format_number(large_trader['shortTop5Qty'])} 口、占比 {large_trader['shortTop5Pct']:.1f}%；前十大賣方 {format_number(large_trader['shortTop10Qty'])} 口、占比 {large_trader['shortTop10Pct']:.1f}%，較前五大再增加 {format_number(short_top10_add_qty)} 口、占比增加 {short_top10_add_pct:+.1f} 個百分點。",
        f"特定法人滲透率比對：買方前五大 {specific_pct_text(long_specific_share_5)}、前十大 {specific_pct_text(long_specific_share_10)}；賣方前五大 {specific_pct_text(short_specific_share_5)}、前十大 {specific_pct_text(short_specific_share_10)}。",
        f"全市場未沖銷部位數為 {format_number(large_trader['marketOi'])} 口，前五大買賣方占比差距 {large_trader['longTop5Pct'] - large_trader['shortTop5Pct']:+.1f} 個百分點，前十大差距 {large_trader['longTop10Pct'] - large_trader['shortTop10Pct']:+.1f} 個百分點。",
    ]
    report["tables"]["D"]["highlights"] = [
        f"臺指選擇權{option_long_label}為 {option_long['institution']} {format_signed(option_long['oiNetQty'])} 口；未平倉淨空最高為 {option_short['institution']} {format_signed(option_short['oiNetQty'])} 口。",
        f"外資臺指選擇權交易淨額 {format_signed(next(row['tradeNetQty'] for row in option_contracts if row['institution'] == '外資'))} 口，未平倉淨額 {format_signed(next(row['oiNetQty'] for row in option_contracts if row['institution'] == '外資'))} 口。",
        f"自營商臺指選擇權未平倉淨額 {format_signed(next(row['oiNetQty'] for row in option_contracts if row['institution'] == '自營商'))} 口；投信臺指選擇權未平倉淨額 {format_signed(next(row['oiNetQty'] for row in option_contracts if row['institution'] == '投信'))} 口。",
    ]
    previous_by_type = {row["contractType"]: row for row in (large_previous_rows or [])}
    cycle_by_type = {
        "weekly": {row["contractType"]: row for row in (large_cycle_rows.get("weekly") or [])}.get("weekly"),
        "monthly": {row["contractType"]: row for row in (large_cycle_rows.get("monthly") or [])}.get("monthly"),
    }
    large_highlights = [
        f"前五大買方 {format_number(large_trader['longTop5Qty'])} 口、占比 {large_trader['longTop5Pct']:.1f}%；其中特定法人 {specific_value_text(large_trader['longTop5SpecificQty'])} 口、占比 {specific_pct_text(large_trader['longTop5SpecificPct'])}。",
        f"前十大買方 {format_number(large_trader['longTop10Qty'])} 口、占比 {large_trader['longTop10Pct']:.1f}%；較前五大再增加 {format_number(long_top10_add_qty)} 口、占比增加 {long_top10_add_pct:+.1f} 個百分點；其中特定法人 {specific_value_text(large_trader['longTop10SpecificQty'])} 口。",
        f"前五大賣方 {format_number(large_trader['shortTop5Qty'])} 口、占比 {large_trader['shortTop5Pct']:.1f}%；前十大賣方 {format_number(large_trader['shortTop10Qty'])} 口、占比 {large_trader['shortTop10Pct']:.1f}%，較前五大再增加 {format_number(short_top10_add_qty)} 口、占比增加 {short_top10_add_pct:+.1f} 個百分點。",
        f"特定法人滲透率比對：買方前五大 {specific_pct_text(long_specific_share_5)}、前十大 {specific_pct_text(long_specific_share_10)}；賣方前五大 {specific_pct_text(short_specific_share_5)}、前十大 {specific_pct_text(short_specific_share_10)}。",
        f"全市場未沖銷部位數為 {format_number(large_trader['marketOi'])} 口，前五大買賣方占比差距 {large_trader['longTop5Pct'] - large_trader['shortTop5Pct']:+.1f} 個百分點，前十大差距 {large_trader['longTop10Pct'] - large_trader['shortTop10Pct']:+.1f} 個百分點。",
    ]
    report["changeOverview"]["largeTraderSummary"] = []
    for row in large_trader_rows:
        prev = previous_by_type.get(row["contractType"])
        cycle = cycle_by_type.get(row["contractType"])
        long5_day = None if not prev or prev["longTop5SpecificQty"] is None else row["longTop5SpecificQty"] - prev["longTop5SpecificQty"]
        long10_day = None if not prev or prev["longTop10SpecificQty"] is None else row["longTop10SpecificQty"] - prev["longTop10SpecificQty"]
        short5_day = None if not prev or prev["shortTop5SpecificQty"] is None else row["shortTop5SpecificQty"] - prev["shortTop5SpecificQty"]
        short10_day = None if not prev or prev["shortTop10SpecificQty"] is None else row["shortTop10SpecificQty"] - prev["shortTop10SpecificQty"]
        long5_cycle = None if not cycle or cycle["longTop5SpecificQty"] is None else row["longTop5SpecificQty"] - cycle["longTop5SpecificQty"]
        long10_cycle = None if not cycle or cycle["longTop10SpecificQty"] is None else row["longTop10SpecificQty"] - cycle["longTop10SpecificQty"]
        short5_cycle = None if not cycle or cycle["shortTop5SpecificQty"] is None else row["shortTop5SpecificQty"] - cycle["shortTop5SpecificQty"]
        short10_cycle = None if not cycle or cycle["shortTop10SpecificQty"] is None else row["shortTop10SpecificQty"] - cycle["shortTop10SpecificQty"]
        cycle_label_date = weekly_cycle_start_date if row["contractType"] == "weekly" else monthly_cycle_start_date
        report["changeOverview"]["largeTraderSummary"].append(
            (
                f"{row['contractLabel']}特定法人買方：前五大目前 {specific_value_text(row['longTop5SpecificQty'])} 口，較前一營業日 "
                f"{format_signed(long5_day)}，"
                f"自 {cycle_label_date} 起累積 "
                f"{format_signed(long5_cycle)}；"
                f"前十大目前 {specific_value_text(row['longTop10SpecificQty'])} 口，較前一營業日 "
                f"{format_signed(long10_day)}，"
                f"自 {cycle_label_date} 起累積 "
                f"{format_signed(long10_cycle)}。"
            )
        )
        report["changeOverview"]["largeTraderSummary"].append(
            (
                f"{row['contractLabel']}特定法人賣方：前五大目前 {specific_value_text(row['shortTop5SpecificQty'])} 口，較前一營業日 "
                f"{format_signed(short5_day)}，"
                f"自 {cycle_label_date} 起累積 "
                f"{format_signed(short5_cycle)}；"
                f"前十大目前 {specific_value_text(row['shortTop10SpecificQty'])} 口，較前一營業日 "
                f"{format_signed(short10_day)}，"
                f"自 {cycle_label_date} 起累積 "
                f"{format_signed(short10_cycle)}。"
            )
        )
        large_highlights.append(
            f"{row['contractLabel']}特定法人買方變動：前五大較前一營業日 {format_signed(long5_day)}、自 {cycle_label_date} 起累積 {format_signed(long5_cycle)}；前十大較前一營業日 {format_signed(long10_day)}、自 {cycle_label_date} 起累積 {format_signed(long10_cycle)}。"
        )
        large_highlights.append(
            f"{row['contractLabel']}特定法人賣方變動：前五大較前一營業日 {format_signed(short5_day)}、自 {cycle_label_date} 起累積 {format_signed(short5_cycle)}；前十大較前一營業日 {format_signed(short10_day)}、自 {cycle_label_date} 起累積 {format_signed(short10_cycle)}。"
        )

    report["tables"]["C"]["highlights"] = large_highlights
    report["analysis"] = build_analysis(report)
    report["telegram"] = build_telegram_text(report)
    report["email"] = build_email_text(report)
    return report


class Handler(SimpleHTTPRequestHandler):
    def do_HEAD(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/report":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "public, max-age=300")
            self.end_headers()
            return
        if parsed.path == "/api/report.pdf":
            self.send_response(200)
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Cache-Control", "public, max-age=300")
            self.end_headers()
            return
        if parsed.path == "/":
            self.path = "/index.html"
        return super().do_HEAD()

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/report":
            self.serve_report(parsed.query)
            return
        if parsed.path == "/api/report.pdf":
            self.serve_report_pdf(parsed.query)
            return
        if parsed.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def serve_report(self, query: str) -> None:
        try:
            params = urllib.parse.parse_qs(query)
            report_date = params.get("date", [None])[0]
            force_refresh = params.get("refresh", ["0"])[0] == "1"
            host = self.headers.get("Host", f"127.0.0.1:{PORT}")
            scheme = "http"
            report_query = f"?date={urllib.parse.quote(report_date)}" if report_date else ""
            if "onrender.com" in host:
                report_url = f"{PUBLIC_BASE_URL}/{report_query}" if report_query else PUBLIC_BASE_URL
            else:
                report_url = f"{scheme}://{host}/{report_query}"
            payload, _ = cached_report(report_date, report_url, force_refresh=force_refresh)
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "public, max-age=300")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as exc:  # noqa: BLE001
            self.send_response(502)
            error = json.dumps({"error": str(exc)}, ensure_ascii=False).encode("utf-8")
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(error)))
            self.end_headers()
            self.wfile.write(error)

    def serve_report_pdf(self, query: str) -> None:
        try:
            params = urllib.parse.parse_qs(query)
            report_date = params.get("date", [None])[0]
            force_refresh = params.get("refresh", ["0"])[0] == "1"
            host = self.headers.get("Host", f"127.0.0.1:{PORT}")
            report_query = f"?date={urllib.parse.quote(report_date)}" if report_date else ""
            if "onrender.com" in host:
                report_url = f"{PUBLIC_BASE_URL}/{report_query}" if report_query else PUBLIC_BASE_URL
            else:
                report_url = f"http://{host}/{report_query}"
            payload, key = cached_report(report_date, report_url, force_refresh=force_refresh)
            ttl = cache_ttl_for_date(payload["meta"]["date"])
            cached = load_cached_report(key, ttl)
            pdf_data = cached[1] if cached else None
            if pdf_data is None:
                pdf_data = build_report_pdf(payload)
                save_cached_report(key, payload, pdf_data)
                save_snapshot(payload["meta"]["date"], payload, pdf_data)
            filename_date = payload["meta"]["date"].replace("/", "-")
            self.send_response(200)
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Cache-Control", "public, max-age=300")
            self.send_header("Content-Disposition", f'attachment; filename="{filename_date}-taifex-report.pdf"')
            self.send_header("Content-Length", str(len(pdf_data)))
            self.end_headers()
            self.wfile.write(pdf_data)
        except Exception as exc:  # noqa: BLE001
            self.send_response(502)
            error = json.dumps({"error": str(exc)}, ensure_ascii=False).encode("utf-8")
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(error)))
            self.end_headers()
            self.wfile.write(error)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{self.log_date_time_string()}] {fmt % args}")


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Serving report at http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
