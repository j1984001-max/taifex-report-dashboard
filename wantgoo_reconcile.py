#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
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
LARGE_FIELDS = tuple(field + suffix for field in METRICS for suffix in ("Qty", "Day"))
OPTION_FIELDS = ("oiLongQty", "oiLongQtyDay", "oiLongAmount", "oiLongAmountDay",
                 "oiShortQty", "oiShortQtyDay", "oiShortAmount", "oiShortAmountDay", "oiNetQty", "oiNetAmount")
FUTURE_FIELDS = ("oiLongQty", "oiShortQty", "oiNetQty", "dayChangeOiLongQty", "dayChangeOiShortQty", "dayChangeOiNetQty")


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
    from playwright.sync_api import Error as BrowserError, sync_playwright

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
                    try:
                        html = page.content()
                    except BrowserError:
                        # An ordinary challenge/redirect can still be navigating here.
                        page.wait_for_timeout(500)
                        continue
                    if any(row and row[0].replace("-", "/") == expected_date
                           for table in server.parse_tables(html) for row in table):
                        (output / f"{key}.html").write_text(html, encoding="utf-8")
                        return html
                    page.wait_for_timeout(1000)
                raise SourceNotReady(f"玩股網 {key} 尚未載入 {expected_date} 表格；不使用舊日或空白數字")

            for key, url in URLS.items():
                response = page.goto(url, wait_until="domcontentloaded", timeout=45000)
                if response is None or (response.status >= 400 and response.headers.get("cf-mitigated") != "challenge"):
                    raise SourceNotReady(f"玩股網 {key} 回應 {response.status if response else '無回應'}")
                if key == "futures":
                    reference["futures"] = parse_futures(loaded_html(key), expected_date)
                elif key == "options":
                    reference["options"] = parse_options(loaded_html("monthlyOptions"), expected_date, "monthly", monthly)
                    page.locator("#option").select_option("week")
                    page.wait_for_function("window.jQuery && window.jQuery.active === 0", timeout=25000)
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
                        page.wait_for_function("window.jQuery && window.jQuery.active === 0", timeout=25000)
                        reference["optionInstitutions"] += parse_option_institutions(loaded_html(f"options-{institution}"), expected_date, institution)
            return reference
        except SourceNotReady:
            raise
        except Exception as exc:
            raise SourceNotReady(f"公開表格未能完整載入：{type(exc).__name__}: {exc}") from exc
        finally:
            browser.close()


def fingerprint(reference: dict) -> str:
    data = {key: value for key, value in reference.items() if key not in {"fetchedAt", "sources"}}
    return hashlib.sha256(json.dumps(data, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def audit_path(report_date: str) -> Path:
    return ROOT / "snapshots" / f"{report_date.replace('/', '-')}.wantgoo.json"


def reference_bindings(report: dict, reference: dict):
    date = report["meta"]["date"]
    if reference.get("date") != date or reference.get("monthlyContract") != report["tables"]["D"]["monthlyContract"]:
        raise SourceNotReady("玩股網與報告的營業日或月契約不相符")
    futures_history = next(row for row in report["tables"]["C"]["historyRows"] if row["date"] == date)
    yield "futures", futures_history, reference["futures"], LARGE_FIELDS
    options = reference["options"]
    if len(options) != 4 or {(row["contractType"], row["optionSide"]) for row in options} != {
        (kind, side) for kind in ("weekly", "monthly") for side in ("call", "put")
    }:
        raise SourceNotReady("玩股網週／月買賣權參考資料不完整")
    for row in options:
        source = next(item for item in report["tables"]["D"]["largeTraderOptionHistoryRows"]
                      if item["date"] == date and item["contractType"] == row["contractType"] and item["optionSide"] == row["optionSide"])
        yield f"options.{row['contractType']}.{row['optionSide']}", source, row, (*LARGE_FIELDS, "marketOi")
    if {row["institution"] for row in reference["futureInstitutions"]} != set(INSTITUTIONS) or len(reference["futureInstitutions"]) != 3:
        raise SourceNotReady("玩股網期貨三大法人參考資料不完整")
    for row in reference["futureInstitutions"]:
        source = next(item for item in report["tables"]["B"]["rows"] if item["product"] == "臺股期貨" and item["institution"] == row["institution"])
        yield f"futureInstitutions.{row['institution']}", source, row, FUTURE_FIELDS
    if len(reference["optionInstitutions"]) != 6 or {(row["institution"], row["optionSide"]) for row in reference["optionInstitutions"]} != {
        (institution, side) for institution in INSTITUTIONS for side in ("call", "put")
    }:
        raise SourceNotReady("玩股網選擇權三大法人參考資料不完整")
    for row in reference["optionInstitutions"]:
        source = next(item for item in report["tables"]["D"]["institutionHistoryRows"]
                      if item["date"] == date and item["institution"] == row["institution"] and item["optionSide"] == row["optionSide"])
        yield f"optionInstitutions.{row['institution']}.{row['optionSide']}", source, row, OPTION_FIELDS


def compare_reference(report: dict, reference: dict) -> dict:
    differences, skipped, checked = [], [], 0
    for category, source, expected, fields in reference_bindings(report, reference):
        if expected.get("date") != report["meta"]["date"]:
            raise SourceNotReady(f"{category} 的來源日期不符")
        for field in fields:
            if field not in expected:
                raise SourceNotReady(f"{category}.{field} 缺少來源欄位")
            value = expected[field]
            if value is None and (field.endswith("Day") or field.startswith("dayChange")):
                skipped.append(f"{category}.{field}")
                continue
            if type(value) is not int:
                raise SourceNotReady(f"{category}.{field} 不是有效整數")
            if abs(value) > 2 ** 53 - 1:
                raise SourceNotReady(f"{category}.{field} 超出網頁可精確顯示範圍")
            if (field.endswith("Qty") or field == "marketOi") and "Net" not in field and not field.startswith("dayChange") and value < 0:
                raise SourceNotReady(f"{category}.{field} 未平倉口數為負")
            checked += 1
            if source.get(field) != value:
                differences.append({"field": f"{category}.{field}", "before": source.get(field), "after": value})
        if category == "futures" or category.startswith("options."):
            market = expected.get("marketOi")
            if category == "futures":
                market = next(row["marketOi"] for row in report["tables"]["C"]["rows"] if row["contractType"] == "monthly")
            for side in ("long", "short"):
                if not expected[side + "Top5Qty"] <= expected[side + "Top10Qty"] <= market:
                    raise SourceNotReady(f"{category} 前五大／前十大口數超出全市場或排序不符")
                for rank in (5, 10):
                    if expected[f"{side}Top{rank}SpecificQty"] > expected[f"{side}Top{rank}Qty"]:
                        raise SourceNotReady(f"{category} 特定法人超出前五／十大總口數")
            for field in METRICS:
                if expected.get(field + "Day") is not None and expected[field + "Qty"] - expected[field + "Day"] < 0:
                    raise SourceNotReady(f"{category} 單日增減反推前日為負")
        if category.startswith("optionInstitutions") or category.startswith("futureInstitutions"):
            if expected["oiLongQty"] - expected["oiShortQty"] != expected["oiNetQty"]:
                raise SourceNotReady(f"{category} 多空口數勾稽不符")
            if "oiNetAmount" in expected and expected["oiLongAmount"] - expected["oiShortAmount"] != expected["oiNetAmount"]:
                raise SourceNotReady(f"{category} 多空金額勾稽不符")
    return {"checkedCount": checked, "skippedFields": skipped, "differences": differences}


def update_values(target: dict, reference: dict, fields) -> None:
    for field in fields:
        if reference.get(field) is not None:
            target[field] = reference[field]


def shifted_cycle(before_qty, before_cycle, after_qty):
    if any(type(value) is not int for value in (before_qty, before_cycle, after_qty)):
        return before_cycle
    return before_cycle + after_qty - before_qty


def align_report(report: dict, reference: dict) -> tuple[dict, dict]:
    comparison = compare_reference(report, reference)
    result = copy.deepcopy(report)
    if not comparison["differences"]:
        return result, comparison
    date = result["meta"]["date"]
    c, d, b = (result["tables"][key] for key in ("C", "D", "B"))
    overview = result["changeOverview"]
    for _, target, source, fields in reference_bindings(result, reference):
        update_values(target, source, fields)
        if "oiLongQtyDay" in target and "oiShortQtyDay" in target:
            if target["oiLongQtyDay"] is not None and target["oiShortQtyDay"] is not None:
                target["oiNetQtyDay"] = target["oiLongQtyDay"] - target["oiShortQtyDay"]
    monthly = next(row for row in c["rows"] if row["contractType"] == "monthly")
    update_values(monthly, reference["futures"], [field + "Qty" for field in METRICS])
    for row in [monthly, *d["specificRows"]]:
        before = copy.deepcopy(row)
        if row is not monthly:
            source = next(item for item in reference["options"] if item["contractType"] == row["contractType"] and item["optionSide"] == row["optionSide"])
            update_values(row, source, [field for field in LARGE_FIELDS if "Specific" in field] + ["marketOi"])
            for field in METRICS:
                if "Specific" in field:
                    row[field + "Cycle"] = shifted_cycle(before.get(field + "Qty"), before.get(field + "Cycle"), row.get(field + "Qty"))
        for field in METRICS:
            if field + "Qty" in row and row.get("marketOi"):
                row[field + "Pct"] = row[field + "Qty"] / row["marketOi"] * 100

    for table_key, references in (("B", reference["futureInstitutions"]), ("D", reference["optionInstitutions"])):
        for source in references:
            row = next(item for item in result["tables"][table_key]["rows"] if item["institution"] == source["institution"]
                       and (table_key == "D" and item["optionSide"] == source["optionSide"] or table_key == "B" and item["product"] == "臺股期貨"))
            if table_key == "B":
                update_values(row, source, FUTURE_FIELDS)
            else:
                for field in OPTION_FIELDS:
                    target_field = "dayChange" + field[:-3][0].upper() + field[:-3][1:] if field.endswith("Day") else field
                    if source.get(field) is not None:
                        row[target_field] = source[field]
                if source.get("oiLongQtyDay") is not None and source.get("oiShortQtyDay") is not None:
                    row["dayChangeOiNetQty"] = source["oiLongQtyDay"] - source["oiShortQtyDay"]
            for side in ("Long", "Short", "Net"):
                qty_field = f"oi{side}Qty"
                if type(row.get(f"dayChangeOi{side}Qty")) is int:
                    row[f"previousOi{side}Qty"] = row[qty_field] - row[f"dayChangeOi{side}Qty"]
                for prefix in ("cycle", "fiveDay"):
                    baseline = row.get(f"{prefix}StartOi{side}Qty" if prefix == "cycle" else f"fiveDayOi{side}Qty")
                    if type(baseline) is int:
                        row[f"{prefix}ChangeOi{side}Qty"] = row[qty_field] - baseline

    foreign = next(row for row in b["rows"] if row["product"] == "臺股期貨" and row["institution"] == "外資")
    for history in overview.get("foreignFuturesHistoryRows", []):
        if history["date"] == date:
            for side, source_side in (("Buy", "Long"), ("Sell", "Short"), ("Net", "Net")):
                history[f"foreignFutures{side}Day"] = foreign[f"dayChangeOi{source_side}Qty"]
    refresh_alignment(result, report, reference)
    refresh_overview(result, report)
    remaining = compare_reference(result, reference)
    if remaining["differences"]:
        raise SourceNotReady("核對後仍有數值差異，不發布半套更正")
    from send_daily_push import report_is_ready, weekly_view_is_ready
    for ready, reason in (report_is_ready(result, mode="minimal", expected_date=date), weekly_view_is_ready(result)):
        if not ready:
            raise SourceNotReady(f"更正後勾稽失敗：{reason}")
    return result, comparison


def refresh_alignment(result: dict, before: dict, reference: dict) -> None:
    date = result["meta"]["date"]
    overview, old = result["changeOverview"], before["changeOverview"]
    foreign = next(row for row in result["tables"]["B"]["rows"] if row["product"] == "臺股期貨" and row["institution"] == "外資")
    for rows_key in ("highLowAlignmentRows", "highLowAlignmentSummaryRows"):
        for row in overview[rows_key]:
            if row["date"] != date:
                continue
            original = next(item for item in old[rows_key] if item["date"] == date)
            for side, source_side in (("Buy", "long"), ("Sell", "short")):
                for rank in (5, 10):
                    prefix = f"futures{side}Top{rank}Specific"
                    source_prefix = f"{source_side}Top{rank}Specific"
                    for suffix in ("Qty", "Day"):
                        if reference["futures"].get(source_prefix + suffix) is not None:
                            row[prefix + suffix] = reference["futures"][source_prefix + suffix]
                    row[prefix + "Cycle"] = shifted_cycle(original.get(prefix + "Qty"), original.get(prefix + "Cycle"), row[prefix + "Qty"])
                row[f"foreignFutures{side}Day"] = foreign[f"dayChangeOi{'Long' if side == 'Buy' else 'Short'}Qty"]
                old_foreign = next(item for item in before["tables"]["B"]["rows"] if item["product"] == "臺股期貨" and item["institution"] == "外資")
                qty_key = "oiLongQty" if side == "Buy" else "oiShortQty"
                row[f"foreignFutures{side}Cycle"] = shifted_cycle(old_foreign[qty_key], original.get(f"foreignFutures{side}Cycle"), foreign[qty_key])
            row["foreignFuturesNetDay"] = foreign["dayChangeOiNetQty"]
            for source in reference["options"]:
                if source["contractType"] != "monthly":
                    continue
                for side, source_side in (("Buy", "long"), ("Sell", "short")):
                    for rank in (5, 10):
                        for suffix in ("Qty", "Day"):
                            value = source.get(f"{source_side}Top{rank}Specific{suffix}")
                            if value is not None:
                                row[f"{source['optionSide']}{side}Top{rank}Specific{suffix}"] = value
            for source in reference["optionInstitutions"]:
                if source["institution"] == "外資":
                    prefix = "foreignCall" if source["optionSide"] == "call" else "foreignPut"
                    for side, source_side in (("Buy", "Long"), ("Sell", "Short")):
                        if source.get(f"oi{source_side}QtyDay") is not None:
                            row[prefix + side + "Day"] = source[f"oi{source_side}QtyDay"]
                    row[prefix + "NetDay"] = row[prefix + "BuyDay"] - row[prefix + "SellDay"]
            for prefix in ("futures", "call", "put"):
                row[prefix + "SpecificNetDay"] = row[prefix + "BuyTop10SpecificDay"] - row[prefix + "SellTop10SpecificDay"]
            for kind, side in (("highPointFuturesShort", "Sell"), ("lowPointFuturesLong", "Buy")):
                row[kind + "Total"] = row[f"futures{side}Top10SpecificDay"] + row[f"foreignFutures{side}Day"]
                row[kind + "Label"] = server.classify_pressure_strength(row[kind + "Total"], side="short" if side == "Sell" else "long")
    overview["highLowAlignmentHighlights"] = server.build_high_low_alignment_highlights(overview["highLowAlignmentRows"])
    weekly = overview.get("weeklyQuickView")
    if weekly:
        for group in weekly["groups"]:
            if group["key"] == "futures":
                continue
            source = next(row for row in result["tables"]["D"]["specificRows"] if row["contractType"] == "weekly" and row["optionSide"] == group["key"])
            group["marketOi"] = source["marketOi"]
            for metric in group["metrics"]:
                field = metric["key"] + "Specific"
                metric.update(qty=source[field + "Qty"], day=source[field + "Day"], cycle=source[field + "Cycle"])
                metric["previousQty"] = metric["qty"] - metric["day"]
                metric["baselineQty"] = metric["qty"] - metric["cycle"]


def refresh_overview(result: dict, before: dict) -> None:
    overview = result["changeOverview"]
    futures = server.build_futures_delta_overview(result["tables"]["B"]["rows"])
    options = server.build_option_delta_overview(result["tables"]["D"]["rows"])
    overview.update(items=futures["items"], highlights=futures["highlights"], optionItems=options["items"], optionHighlights=options["highlights"])
    overview["futuresOverviewHighlights"] = [item for item in futures["highlights"] if item.startswith("臺股期貨：")][:1]
    summary = next(row for row in overview["highLowAlignmentSummaryRows"] if row["date"] == result["meta"]["date"])
    for card in overview.get("largeTraderCards", []):
        if not card["label"].startswith("月契約"):
            continue
        side = "Buy" if card.get("sideKind") == "buy" else "Sell"
        for rank in (5, 10):
            prefix = f"futures{side}Top{rank}Specific"
            card[f"top{rank}Qty"] = server.format_number(summary[prefix + "Qty"])
            card[f"top{rank}Day"] = server.format_signed(summary[prefix + "Day"])
            card[f"top{rank}Cycle"] = server.format_signed(summary[prefix + "Cycle"])
    for card in overview.get("optionSpecificCards", []):
        kind = "weekly" if card["label"].startswith("週契約") else "monthly"
        side = "call" if "買權" in card["label"] else "put"
        source = next(row for row in result["tables"]["D"]["specificRows"] if row["contractType"] == kind and row["optionSide"] == side)
        for field in METRICS:
            if "Specific" not in field:
                continue
            for suffix in ("Qty", "Day", "Cycle"):
                key = field.replace("Specific", "") + suffix
                card[key] = server.format_number(source[field + suffix]) if suffix == "Qty" else server.format_signed(source[field + suffix])
    large_notes = []
    for card in overview.get("largeTraderCards", []):
        large_notes.append(f"{card['label']}：前五大 {server.format_current_day_cycle_summary(card['top5Qty'], card['top5Day'], card['top5Cycle'], card['cycleStartDate'])}；前十大 {server.format_current_day_cycle_summary(card['top10Qty'], card['top10Day'], card['top10Cycle'], card['cycleStartDate'])}。")
    option_notes = []
    for card in overview.get("optionSpecificCards", []):
        pieces = [f"{label} {server.format_current_day_cycle_summary(card[field + 'Qty'], card[field + 'Day'], card[field + 'Cycle'], card['cycleStartDate'])}"
                  for field, label in (("longTop5", "買方前五大"), ("longTop10", "買方前十大"), ("shortTop5", "賣方前五大"), ("shortTop10", "賣方前十大"))]
        option_notes.append(card["label"] + "：" + "；".join(pieces) + "。")
    overview.update(largeTraderSummary=large_notes, largeTraderOverviewHighlights=[item for item in large_notes if item.startswith("月契約")],
                    optionSpecificHighlights=option_notes, optionOverviewHighlights=options["highlights"] + option_notes)
    result["tables"]["C"]["highlights"] = large_notes
    result["tables"]["D"]["highlights"] = options["highlights"]
    result["tables"]["B"]["categoryAnalysis"] = server.build_futures_category_analysis(result["tables"]["B"]["rows"])
    result["analysis"] = server.build_analysis(result)
    overview["prediction"] = server.build_overview_prediction(result)
    result["telegram"] = server.build_telegram_text(result)
    result["email"] = server.build_email_text(result)


def apply_saved_reference(report: dict) -> dict:
    path = audit_path(report["meta"]["date"])
    if not path.exists():
        return report
    audit = json.loads(path.read_text(encoding="utf-8"))
    if audit.get("status") not in {"matched", "corrected"} or audit.get("date") != report["meta"]["date"]:
        return report
    reference = audit.get("reference") or {}
    if fingerprint(reference) != audit.get("fingerprint"):
        raise SourceNotReady("已儲存玩股網核對資料指紋不符")
    result, _ = align_report(report, reference)
    result["meta"]["wantgooReconciliation"] = audit["summary"]
    return result


def save_audit(audit: dict) -> None:
    path = audit_path(audit["date"])
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def persist_pending(date: str, reason: str, report: dict | None, *, final_attempt: bool = False) -> None:
    from send_daily_push import (DEFAULT_TELEGRAM_CHAT_ID, load_telegram_token, publish_snapshot,
                                 send_telegram_message)
    import os
    previous = load_audit(date)
    if previous.get("status") in {"matched", "corrected"}:
        return
    audit = {**previous, "date": date, "status": "pending", "reason": reason,
             "checkedAt": datetime.now(server.TW_TZ).isoformat(timespec="seconds")}
    if report is not None:
        report = copy.deepcopy(report)
        report["meta"]["wantgooReconciliation"] = {"status": "pending", "checkedAt": audit["checkedAt"]}
        server.save_snapshot(date, report)
    save_audit(audit)
    publish_snapshot(date)
    if final_attempt and not audit.get("pendingNotificationId"):
        response = send_telegram_message(load_telegram_token(), os.environ.get("TELEGRAM_CHAT_ID", DEFAULT_TELEGRAM_CHAT_ID),
                                         f"{date} 玩股網盤後核對尚未完成\n來源未能提供完整同日表格，保留官方數據，未標示核對成功。")
        message_id = response.get("result", {}).get("message_id")
        if type(message_id) is not int or message_id <= 0:
            raise RuntimeError("待核對提醒未取得送達編號")
        audit["pendingNotificationId"] = message_id
        save_audit(audit)
        publish_snapshot(date)


def load_audit(date: str) -> dict:
    path = audit_path(date)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def audit_delivery_complete(audit: dict, date: str) -> bool:
    required = ["text"] if audit.get("status") == "matched" else ["text", "high_low_cards", "high_low_summary", "weekly_quick_view"]
    notification = audit.get("notification") or {}
    items = notification.get("items") or {}
    return bool(audit.get("date") == date and audit.get("status") in {"matched", "corrected"}
                and notification.get("complete") is True
                and all(type(items.get(key)) is int and items[key] > 0 for key in required))


def difference_label(path: str) -> str:
    parts = path.split(".")
    field = parts[-1]
    category = parts[0]
    expected_parts = {"futures": 2, "options": 4, "futureInstitutions": 3, "optionInstitutions": 4}
    if category not in expected_parts or len(parts) != expected_parts[category]:
        return path
    if category == "futures":
        prefix = "月期貨"
    elif category == "options":
        prefix = ("週選" if parts[1] == "weekly" else "月選") + ("買權" if parts[2] == "call" else "賣權")
    elif category == "futureInstitutions":
        prefix = parts[1] + "期貨"
    else:
        prefix = parts[1] + ("買權" if parts[2] == "call" else "賣權")
    labels = {"marketOi": "全市場未平倉", "oiNetQty": "淨未平倉", "oiNetAmount": "淨金額（千元）"}
    for side, label in (("long", "買方"), ("short", "賣方")):
        for rank in (5, 10):
            for specific in ("", "Specific"):
                for suffix, meaning in (("Qty", "目前口數"), ("Day", "單日增減")):
                    labels[f"{side}Top{rank}{specific}{suffix}"] = f"{'特定法人' if specific else '大額交易人'}{label}前{'五' if rank == 5 else '十'}大{meaning}"
    for side, label in (("Long", "多方"), ("Short", "空方"), ("Net", "淨額")):
        labels[f"oi{side}Qty"] = label + "未平倉"
        labels[f"dayChangeOi{side}Qty"] = label + "單日增減"
        labels[f"oi{side}QtyDay"] = label + "單日增減"
        labels[f"oi{side}Amount"] = label + "金額（千元）"
        labels[f"oi{side}AmountDay"] = label + "金額增減（千元）"
    return prefix + " " + labels.get(field, field)


def notify_result(report: dict, audit: dict, previous: dict) -> None:
    from send_daily_push import (capture_report_screenshots, load_telegram_token, publish_snapshot,
                                 send_telegram_document, send_telegram_message)
    if previous.get("fingerprint") == audit["fingerprint"] and audit_delivery_complete(previous, audit["date"]):
        audit["notification"] = previous["notification"]
        save_audit(audit)
        publish_snapshot(audit["date"])
        return
    notification = previous.get("notification", {}) if previous.get("fingerprint") == audit["fingerprint"] else {}
    items = {key: value for key, value in notification.get("items", {}).items() if type(value) is int and value > 0}
    notification["items"] = items
    date, differences = audit["date"], audit["differences"]
    token = load_telegram_token()
    import os
    from send_daily_push import DEFAULT_TELEGRAM_CHAT_ID
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", DEFAULT_TELEGRAM_CHAT_ID)
    text = f"{date} 玩股網盤後核對\n已核對 {audit['summary']['checkedCount']} 項。"
    if differences:
        text += f"\n更正 {len(differences)} 項，網站與下方圖表已更新。\n"
        text += "\n".join(f"{difference_label(item['field'])}：{item['before']} → {item['after']}" for item in differences[:8])
    else:
        text += "數字一致，不重傳截圖。"
    if audit["summary"]["skippedCount"]:
        text += f"\n另有 {audit['summary']['skippedCount']} 項增減未揭露，未當成零核對。"

    def record(key: str, response: dict) -> None:
        message_id = response.get("result", {}).get("message_id")
        if type(message_id) is not int or message_id <= 0:
            raise RuntimeError("玩股網核對通知未取得有效送達編號")
        items[key] = message_id
        audit["notification"] = notification
        save_audit(audit)
        publish_snapshot(date)

    if "text" not in items:
        record("text", send_telegram_message(token, chat_id, text))
    required = ["text"]
    if differences:
        required += ["high_low_cards", "high_low_summary", "weekly_quick_view"]
        if any(key not in items for key in required[1:]):
            shots = capture_report_screenshots(date, report=report, include_detail_sections=False, include_weekly=True)
            for key in required[1:]:
                if key in items:
                    continue
                if not shots.get(key):
                    raise RuntimeError(f"玩股網更正圖未完成：{key}")
                record(key, send_telegram_document(token, chat_id, caption=f"{date} 玩股網核對更正版",
                                                   filename=f"wantgoo-{date.replace('/', '-')}-{key}.png", data=shots[key]))
    notification.update(complete=all(type(items.get(key)) is int and items[key] > 0 for key in required),
                        deliveredAt=datetime.now(server.TW_TZ).isoformat(timespec="seconds"))
    audit["notification"] = notification
    if not notification["complete"]:
        raise RuntimeError("玩股網核對結果通知尚未完整送達")
    save_audit(audit)
    publish_snapshot(date)


def reconcile_report(report: dict, reference: dict, *, dry_run: bool = True) -> dict:
    aligned, comparison = align_report(report, reference)
    date, digest = report["meta"]["date"], fingerprint(reference)
    checked_at = datetime.now(server.TW_TZ).isoformat(timespec="seconds")
    summary = {"status": "corrected" if comparison["differences"] else "matched", "sourceDate": date,
               "checkedAt": checked_at, "checkedCount": comparison["checkedCount"],
               "changedCount": len(comparison["differences"]), "skippedCount": len(comparison["skippedFields"]),
               "scope": "近月期貨、週／月選大額交易人及三大法人未平倉；累積沿用明列的官方基準日"}
    result = {"date": date, "status": summary["status"], "fingerprint": digest,
              **comparison, "telegramSent": False, "dryRun": dry_run}
    if dry_run:
        return result
    from send_daily_push import publish_snapshot
    previous = load_audit(date)
    audit = {"date": date, "status": summary["status"], "fingerprint": digest, "reference": reference,
             "summary": summary, "differences": comparison["differences"], "skippedFields": comparison["skippedFields"]}
    if previous.get("fingerprint") == digest:
        audit["differences"] = previous.get("differences", comparison["differences"])
        audit["summary"] = previous.get("summary", summary)
        audit["status"] = previous.get("status", summary["status"])
    if comparison["differences"] and "originalReport" not in previous:
        audit["originalReport"] = report
    elif "originalReport" in previous:
        audit["originalReport"] = previous["originalReport"]
    aligned["meta"]["wantgooReconciliation"] = audit["summary"]
    server.save_snapshot(date, aligned, server.build_report_pdf(aligned) if comparison["differences"] else None)
    save_audit(audit)
    publish_snapshot(date)
    notify_result(aligned, audit, previous)
    return {**result, "status": audit["status"], "telegramSent": True, "notification": audit["notification"]}


def resolve_gate(date: str | None, dry_run: bool, automatic: bool, now: datetime | None = None) -> dict:
    current = now or datetime.now(server.TW_TZ)
    current = current.replace(tzinfo=server.TW_TZ) if current.tzinfo is None else current.astimezone(server.TW_TZ)
    expected = date or current.strftime("%Y/%m/%d")
    datetime.strptime(expected, "%Y/%m/%d")
    if automatic and (not server.is_business_day(current) or not 17 <= current.hour < 19 or expected != current.strftime("%Y/%m/%d")):
        return {"date": expected, "run": False, "reason": "outside_17_00_19_00_business_day_window"}
    if not dry_run and current.date().strftime("%Y/%m/%d") == expected and current.hour < 17:
        return {"date": expected, "run": False, "reason": "before_wantgoo_17_00_check"}
    if automatic and not dry_run:
        previous = load_audit(expected)
        if audit_delivery_complete(previous, expected) and fingerprint(previous.get("reference") or {}) == previous.get("fingerprint"):
            return {"date": expected, "run": False, "reason": "already_reconciled"}
    return {"date": expected, "run": True, "reason": "ok"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile the current TAIFEX snapshot against same-day public WantGoo data.")
    parser.add_argument("--date")
    parser.add_argument("--output", default="wantgoo-artifacts")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--automatic", action="store_true")
    parser.add_argument("--gate", action="store_true")
    args = parser.parse_args()
    gate = resolve_gate(args.date, args.dry_run, args.automatic)
    if args.gate:
        import os
        if os.environ.get("GITHUB_OUTPUT"):
            with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as handle:
                handle.write(f"date={gate['date']}\nrun={'true' if gate['run'] else 'false'}\n")
        print(json.dumps(gate, ensure_ascii=False), flush=True)
        return
    if not gate["run"]:
        print(json.dumps(gate, ensure_ascii=False), flush=True)
        return
    args.date = gate["date"]
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    report = None
    try:
        report = json.loads((ROOT / "snapshots" / f"{args.date.replace('/', '-')}.json").read_text(encoding="utf-8"))
        reference = collect_reference(report, output)
    except (SourceNotReady, OSError, ValueError) as exc:
        result = {"date": args.date, "status": "pending", "reason": f"{type(exc).__name__}: {exc}", "telegramSent": False}
        if not args.dry_run:
            now = datetime.now(server.TW_TZ)
            persist_pending(args.date, result["reason"], report, final_attempt=now.hour == 18 and now.minute >= 45)
    else:
        (output / "reference.json").write_text(json.dumps(reference, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            result = reconcile_report(report, reference, dry_run=args.dry_run)
        except SourceNotReady as exc:
            result = {"date": args.date, "status": "pending", "reason": str(exc), "telegramSent": False}
            if not args.dry_run:
                now = datetime.now(server.TW_TZ)
                persist_pending(args.date, result["reason"], report, final_attempt=now.hour == 18 and now.minute >= 45)
    (output / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
