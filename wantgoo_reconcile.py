#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from datetime import datetime
from pathlib import Path

import server

ROOT = Path(__file__).resolve().parent
BASE = "https://www.wantgoo.com"
URLS = {
    "futures": f"{BASE}/futures/large-traders/bull-bear-open-interest",
    "options": f"{BASE}/option/large-traders-open-interest",
    "futureInstitutions": f"{BASE}/futures/institutional-investors/bull-bear-open-interest",
    "optionInstitutions": f"{BASE}/option/institutional-investors-call-put",
}
METRICS = ("longTop5", "shortTop5", "longTop10", "shortTop10",
           "longTop5Specific", "shortTop5Specific", "longTop10Specific", "shortTop10Specific")
INSTITUTIONS = ("外資", "投信", "自營商")


class SourceNotReady(ValueError):
    pass


def number(text: str) -> int:
    value = text.replace(",", "").replace("−", "-").strip()
    if not re.fullmatch(r"[+-]?\d+", value):
        raise SourceNotReady(f"玩股網未揭露有效整數：{text[:40]!r}")
    return int(value)


def quantity_change(text: str) -> tuple[int, int | None]:
    match = re.fullmatch(r"\s*([+\-−\d,]+)\s*(?:[（(]\s*([+\-−\d,]*)\s*[)）])?\s*", text)
    if not match:
        raise SourceNotReady(f"玩股網口數格式無法辨識：{text[:40]!r}")
    return number(match[1]), number(match[2]) if match[2] else None


def date_rows(html: str, expected_date: str, required_headers: tuple[str, ...]) -> list[list[str]]:
    candidates = []
    for table in server.parse_tables(html):
        headers = " ".join(" ".join(row) for row in table[:4])
        if not all(header in headers for header in required_headers):
            continue
        candidates.extend(row for row in table if row and row[0].replace("-", "/") == expected_date)
    # Sticky headers can duplicate a table, but conflicting duplicate values are never accepted.
    unique = list(dict.fromkeys(tuple(row) for row in candidates))
    if not unique:
        raise SourceNotReady(f"玩股網尚無 {expected_date} 的完整表格（{'/'.join(required_headers)}）")
    return [list(row) for row in unique]


def parse_futures(html: str, expected_date: str) -> dict:
    rows = date_rows(html, expected_date, ("前五大", "前十大", "前五特法"))
    if len(rows) != 1 or len(rows[0]) < 9:
        raise SourceNotReady("玩股網期貨大額表有重複或缺欄")
    result = {"date": expected_date, "contractType": "monthly"}
    for field, text in zip(METRICS, rows[0][1:9]):
        result[field + "Qty"], result[field + "Day"] = quantity_change(text)
    return result


def parse_options(html: str, expected_date: str, contract_type: str, monthly_contract: str) -> list[dict]:
    rows = date_rows(html, expected_date, ("契約", "權別", "前五特法"))
    results = []
    for row in rows:
        if len(row) < 12:
            raise SourceNotReady("玩股網選擇權大額表缺欄")
        contract = re.sub(r"\s+", "", row[1])
        if contract_type == "monthly" and contract != monthly_contract:
            raise SourceNotReady(f"玩股網月契約 {contract} 與報告 {monthly_contract} 不符")
        if contract_type == "weekly" and contract not in {"週契約", "周契約", "週", "周"}:
            raise SourceNotReady(f"玩股網週契約口徑非合計：{contract}")
        if row[2] not in {"買權", "賣權"}:
            raise SourceNotReady("玩股網權別不明")
        result = {"date": expected_date, "contractType": contract_type,
                  "contract": contract, "optionSide": "call" if row[2] == "買權" else "put"}
        for field, text in zip(METRICS, row[3:11]):
            result[field + "Qty"], result[field + "Day"] = quantity_change(text)
        result["marketOi"] = number(row[11])
        results.append(result)
    if len(results) != 2 or {row["optionSide"] for row in results} != {"call", "put"}:
        raise SourceNotReady("玩股網買權／賣權資料未齊或重複")
    return results


def parse_future_institutions(html: str, expected_date: str) -> list[dict]:
    rows = date_rows(html, expected_date, ("外資", "投信", "自營商"))
    if len(rows) != 1 or len(rows[0]) < 9:
        raise SourceNotReady("玩股網期貨法人表有重複或缺欄")
    results = []
    for institution, offset in (("外資", 1), ("投信", 5), ("自營商", 7)):
        long_qty, long_day = quantity_change(rows[0][offset])
        short_qty, short_day = quantity_change(rows[0][offset + 1])
        results.append({"date": expected_date, "institution": institution,
                        "oiLongQty": long_qty, "oiShortQty": short_qty, "oiNetQty": long_qty - short_qty,
                        "dayChangeOiLongQty": long_day, "dayChangeOiShortQty": short_day,
                        "dayChangeOiNetQty": long_day - short_day if long_day is not None and short_day is not None else None})
    return results


def parse_option_institutions(html: str, expected_date: str, institution: str) -> list[dict]:
    rows = date_rows(html, expected_date, ("權別", "買方", "賣方", "契約金額"))
    results = []
    for row in rows:
        if len(row) < 8 or row[1] not in {"買權", "賣權"}:
            raise SourceNotReady("玩股網選擇權法人表缺欄或權別不明")
        result = {"date": expected_date, "institution": institution,
                  "optionSide": "call" if row[1] == "買權" else "put"}
        for field, text in zip(("oiLongQty", "oiLongAmount", "oiShortQty", "oiShortAmount"), row[2:6]):
            result[field], result[field + "Day"] = quantity_change(text)
        result["oiNetQty"], result["oiNetAmount"] = number(row[6]), number(row[7])
        if result["oiNetQty"] != result["oiLongQty"] - result["oiShortQty"]:
            raise SourceNotReady("玩股網選擇權法人多空口數勾稽不符")
        results.append(result)
    if len(results) != 2 or {row["optionSide"] for row in results} != {"call", "put"}:
        raise SourceNotReady("玩股網選擇權法人買賣權未齊")
    return results


def collect_reference(report: dict, output: Path) -> dict:
    from playwright.sync_api import sync_playwright

    expected_date = report["meta"]["date"]
    monthly = report["tables"]["D"]["monthlyContract"]
    output.mkdir(parents=True, exist_ok=True)
    reference = {"date": expected_date, "monthlyContract": monthly, "sources": URLS,
                 "fetchedAt": datetime.now(server.TW_TZ).isoformat(timespec="seconds")}
    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(locale="zh-TW", timezone_id="Asia/Taipei")
        page = context.new_page()
        try:
            def loaded_html(key: str) -> str:
                deadline = time.monotonic() + 35
                while time.monotonic() < deadline:
                    html = page.content()
                    if any(row and row[0].replace("-", "/") == expected_date
                           for table in server.parse_tables(html) for row in table):
                        (output / f"{key}.html").write_text(html, encoding="utf-8")
                        return html
                    page.wait_for_timeout(1000)
                raise SourceNotReady(f"玩股網 {key} 尚未載入 {expected_date} 表格；不使用舊日或空白數字")

            for key, url in URLS.items():
                response = page.goto(url, wait_until="domcontentloaded", timeout=45000)
                if response is None or response.status >= 400:
                    raise SourceNotReady(f"玩股網 {key} 回應 {response.status if response else '無回應'}")
                if key == "futures":
                    reference["futures"] = parse_futures(loaded_html(key), expected_date)
                elif key == "options":
                    page.locator("#option").select_option("month")
                    reference["options"] = parse_options(loaded_html("monthlyOptions"), expected_date, "monthly", monthly)
                    old_html = page.content()
                    page.locator("#option").select_option("week")
                    page.wait_for_function("before => document.documentElement.outerHTML !== before", arg=old_html, timeout=15000)
                    reference["options"] += parse_options(loaded_html("weeklyOptions"), expected_date, "weekly", monthly)
                elif key == "futureInstitutions":
                    reference["futureInstitutions"] = parse_future_institutions(loaded_html(key), expected_date)
                else:
                    reference["optionInstitutions"] = []
                    selects = page.locator("select")
                    selector = None
                    labels = {}
                    for index in range(selects.count()):
                        options = selects.nth(index).locator("option").evaluate_all("nodes => nodes.map(n => ({text: n.textContent.trim(), value: n.value}))")
                        found = {institution: next((option["value"] for option in options if institution in option["text"]), None)
                                 for institution in INSTITUTIONS}
                        if all(value is not None for value in found.values()):
                            selector, labels = selects.nth(index), found
                            break
                    if selector is None:
                        raise SourceNotReady("玩股網法人選單格式變更，不猜測法人身份")
                    for institution, value in labels.items():
                        selector.select_option(value)
                        page.wait_for_timeout(1000)
                        reference["optionInstitutions"] += parse_option_institutions(loaded_html(f"options-{institution}"), expected_date, institution)
            return reference
        finally:
            browser.close()


def fingerprint(reference: dict) -> str:
    data = {key: value for key, value in reference.items() if key not in {"fetchedAt", "sources"}}
    return hashlib.sha256(json.dumps(data, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile the current TAIFEX snapshot against same-day public WantGoo data.")
    parser.add_argument("--date", required=True)
    parser.add_argument("--output", default="wantgoo-artifacts")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    datetime.strptime(args.date, "%Y/%m/%d")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    report = json.loads((ROOT / "snapshots" / f"{args.date.replace('/', '-')}.json").read_text(encoding="utf-8"))
    try:
        reference = collect_reference(report, output)
    except Exception as exc:
        result = {"date": args.date, "status": "pending", "reason": f"{type(exc).__name__}: {exc}", "telegramSent": False}
    else:
        (output / "reference.json").write_text(json.dumps(reference, ensure_ascii=False, indent=2), encoding="utf-8")
        result = {"date": args.date, "status": "source_ready", "fingerprint": fingerprint(reference), "telegramSent": False}
    (output / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
