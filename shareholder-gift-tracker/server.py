#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import date
from html.parser import HTMLParser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8765"))
CURRENT_YEAR = date.today().year
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "1800"))

WESPAI_URL = f"https://stock.wespai.com/stock{CURRENT_YEAR - 1911}"
IDEAL_URL = "https://souvenir.ideal-labs.com/"
HONSEC_URL = "https://srd.honsec.com.tw/stock/souvenir.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Codex shareholder gift tracker)",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}

CACHE: dict[str, tuple[float, Any]] = {}


def normalize_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def json_response(handler: SimpleHTTPRequestHandler, payload: dict[str, Any], status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def fetch_text(url: str) -> str:
    request = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=30) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, "ignore")


def cached(name: str, loader) -> Any:
    now = time.time()
    item = CACHE.get(name)
    if item and now - item[0] < CACHE_TTL_SECONDS:
        return item[1]
    value = loader()
    CACHE[name] = (now, value)
    return value


def parse_mmdd_to_iso(value: str) -> str | None:
    cleaned = normalize_text(value)
    match = re.fullmatch(r"(\d{2})\.(\d{2})", cleaned)
    if not match:
        return None
    month, day = match.groups()
    return f"{CURRENT_YEAR}-{month}-{day}"


def parse_roc_date(value: str) -> str | None:
    cleaned = normalize_text(value)
    match = re.fullmatch(r"(\d{2,3})/(\d{2})/(\d{2})", cleaned)
    if not match:
        return None
    roc_year, month, day = match.groups()
    western_year = int(roc_year) + 1911
    return f"{western_year:04d}-{month}-{day}"


def parse_roc_range(value: str) -> tuple[str | None, str | None]:
    cleaned = normalize_text(value)
    if cleaned in {"", "-"}:
        return None, None
    match = re.fullmatch(r"(\d{2,3}/\d{2}/\d{2})\s*[-~]\s*(\d{2,3}/\d{2}/\d{2})", cleaned)
    if not match:
        single = parse_roc_date(cleaned)
        return single, single
    return parse_roc_date(match.group(1)), parse_roc_date(match.group(2))


class TableCell:
    def __init__(self, text: str) -> None:
        self.text = text


class SimpleTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[TableCell]]] = []
        self._stack: list[dict[str, Any]] = []
        self._in_cell = False
        self._buffer: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._stack.append({"rows": [], "row": None})
        elif tag == "tr" and self._stack:
            self._stack[-1]["row"] = []
        elif tag in {"td", "th"} and self._stack and self._stack[-1]["row"] is not None:
            self._in_cell = True
            self._buffer = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._in_cell:
            text = normalize_text(" ".join(self._buffer))
            self._stack[-1]["row"].append(TableCell(text))
            self._in_cell = False
        elif tag == "tr" and self._stack and self._stack[-1]["row"] is not None:
            self._stack[-1]["rows"].append(self._stack[-1]["row"])
            self._stack[-1]["row"] = None
        elif tag == "table" and self._stack:
            self.tables.append(self._stack.pop()["rows"])


class VisibleTextParser(HTMLParser):
    SKIP_TAGS = {"script", "style", "noscript"}

    def __init__(self) -> None:
        super().__init__()
        self.items: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self.SKIP_TAGS:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in self.SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        text = normalize_text(data)
        if text:
            self.items.append(text)


def load_wespai() -> dict[str, dict[str, Any]]:
    html = fetch_text(WESPAI_URL)
    parser = SimpleTableParser()
    parser.feed(html)
    target_table: list[list[TableCell]] | None = None
    for table in parser.tables:
        if not table:
            continue
        header_text = " ".join(cell.text for cell in table[0])
        if "代號" in header_text and "紀念品" in header_text and "最後買進日" in header_text:
            target_table = table
            break

    if not target_table or len(target_table) < 2:
        raise ValueError("找不到撿股讚的股東會紀念品表格")

    rows: dict[str, dict[str, Any]] = {}
    for row in target_table[1:]:
        values = [cell.text for cell in row]
        if len(values) < 15:
            continue
        code = values[1]
        if not re.fullmatch(r"\d{3,6}", code):
            continue
        rows[code] = {
            "code": code,
            "company_name": values[2],
            "price_text": values[3],
            "souvenir_name": values[4],
            "meeting_date_text": values[6],
            "meeting_date": parse_mmdd_to_iso(values[6]),
            "meeting_city": values[7],
            "last_buy_date_text": values[8],
            "last_buy_date": parse_mmdd_to_iso(values[8]),
            "transfer_agent_short": values[9],
            "transfer_agent_phone": values[10],
            "odd_lot_mail": values[12],
            "reelection": values[13],
            "source_url": WESPAI_URL,
            "official_doc_url": (
                f"https://doc.twse.com.tw/server-java/t57sb01?step=1&colorchg=1&co_id={code}"
                f"&year={CURRENT_YEAR - 1911}&mtype=F&"
            ),
        }
    return rows


def load_ideal() -> dict[str, dict[str, Any]]:
    html = fetch_text(IDEAL_URL)
    match = re.search(r'self\.__next_f\.push\(\[1,"(1c:.*?)"\]\)</script>', html)
    if not match:
        raise ValueError("找不到股東禮簿資料")
    decoded = json.loads(f"\"{match.group(1)}\"")
    payload = json.loads(decoded[3:])
    meetings = payload[3]["allMeetings"]
    return {item["stock_code"]: item for item in meetings}


def is_company_start(tokens: list[str], index: int) -> bool:
    return (
        index + 2 < len(tokens)
        and tokens[index + 1].endswith("年")
        and tokens[index + 2].startswith("代號 ")
    )


def load_honsec() -> dict[str, dict[str, Any]]:
    html = fetch_text(HONSEC_URL)
    parser = VisibleTextParser()
    parser.feed(html)
    tokens = parser.items

    schema = [
        ("distribution_rule", "發放原則"),
        ("last_buy_date_text", "最後買進日"),
        ("agent_distribution_period_text", "股代代發期間"),
        ("meeting_distribution", "股東會現場紀念品有無發放"),
        ("agent_only_distribution", "股代有無純代發紀念品"),
        ("souvenir_name", "紀念品"),
        ("meeting_distribution_rule", "開會現場發放條件"),
        ("has_evote", "是否有電子投票"),
        ("evote_period_text", "電子投票期間"),
        ("evote_pickup_place", "電投發放地點"),
        ("evote_pickup_period_text", "電投紀念品發放期間"),
        ("evote_pickup_rule", "電子投票發放條件"),
        ("proxy_agent", "受託代理人"),
        ("proxy_agent_locations", "徵求地點"),
        ("public_proxy", "個人公開徵求"),
        ("public_proxy_locations", "徵求地點"),
        ("agent_proxy", "受託或股代徵求"),
        ("agent_proxy_locations", "徵求地點"),
        ("proxy_distribution", "徵求場所紀念品有無發放"),
        ("proxy_period_text", "徵求期間"),
    ]
    known_labels = {label for _, label in schema}
    rows: dict[str, dict[str, Any]] = {}

    index = 0
    while index < len(tokens):
        if not is_company_start(tokens, index):
            index += 1
            continue

        company_name = tokens[index]
        code = tokens[index + 2].replace("代號", "").strip()
        index += 3

        record: dict[str, Any] = {
            "code": code,
            "company_name": company_name,
            "source_url": HONSEC_URL,
        }

        for field_name, label in schema:
            if index >= len(tokens) or tokens[index] != label:
                record[field_name] = ""
                continue

            index += 1
            if index >= len(tokens) or tokens[index] in known_labels or is_company_start(tokens, index):
                record[field_name] = ""
            else:
                record[field_name] = tokens[index]
                index += 1

        record["last_buy_date"] = parse_roc_date(record["last_buy_date_text"])
        record["evote_start_date"], record["evote_end_date"] = parse_roc_range(record["evote_period_text"])
        record["evote_pickup_start_date"], record["evote_pickup_end_date"] = parse_roc_range(
            record["evote_pickup_period_text"]
        )
        rows[code] = record

    return rows


def source_bundle() -> dict[str, Any]:
    wespai = cached("wespai", load_wespai)
    ideal = cached("ideal", load_ideal)
    honsec = cached("honsec", load_honsec)
    return {"wespai": wespai, "ideal": ideal, "honsec": honsec}


def clean_codes(raw: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for code in re.findall(r"\d{3,6}", raw):
        if code not in seen:
            seen.add(code)
            out.append(code)
    return out


def source_link(label: str, url: str) -> dict[str, str]:
    return {"label": label, "url": url}


def build_record(code: str, sources: dict[str, Any]) -> dict[str, Any]:
    wespai = sources["wespai"].get(code)
    ideal = sources["ideal"].get(code)
    honsec = sources["honsec"].get(code)

    company_name = (
        (wespai or {}).get("company_name")
        or (ideal or {}).get("stock", {}).get("stock_name")
        or (honsec or {}).get("company_name")
        or ""
    )
    meeting_date = (ideal or {}).get("meeting_date") or (wespai or {}).get("meeting_date")
    last_buy_date = (ideal or {}).get("last_buy_date") or (wespai or {}).get("last_buy_date") or (honsec or {}).get(
        "last_buy_date"
    )
    evote_start = (ideal or {}).get("evote_start_date") or (honsec or {}).get("evote_start_date")
    evote_end = (ideal or {}).get("evote_end_date") or (honsec or {}).get("evote_end_date")
    pickup_start = (honsec or {}).get("evote_pickup_start_date")
    pickup_end = (honsec or {}).get("evote_pickup_end_date")
    souvenir_name = (
        (wespai or {}).get("souvenir_name")
        or (ideal or {}).get("souvenir_name")
        or (honsec or {}).get("souvenir_name")
        or ""
    )
    transfer_agent_name = (
        (ideal or {}).get("transfer_agent_name")
        or (wespai or {}).get("transfer_agent_short")
        or ""
    )
    transfer_agent_phone = (
        (ideal or {}).get("transfer_agent_phone")
        or (wespai or {}).get("transfer_agent_phone")
        or ""
    )

    source_links: list[dict[str, str]] = []
    if wespai:
        source_links.append(source_link("撿股讚", wespai["source_url"]))
        source_links.append(source_link("官方開會資料", wespai["official_doc_url"]))
    if ideal:
        source_links.append(source_link("股東禮簿", IDEAL_URL))
    if honsec:
        source_links.append(source_link("宏遠股代", HONSEC_URL))

    is_published = bool(wespai or ideal or honsec and honsec.get("souvenir_name"))
    if not is_published and company_name:
        status = "partial"
    elif is_published:
        status = "published"
    else:
        status = "unpublished"

    return {
        "code": code,
        "companyName": company_name,
        "status": status,
        "isPublished": is_published,
        "souvenirName": souvenir_name,
        "meetingDate": meeting_date,
        "lastBuyDate": last_buy_date,
        "meetingCity": (wespai or {}).get("meeting_city", ""),
        "priceText": (wespai or {}).get("price_text", ""),
        "transferAgentName": transfer_agent_name,
        "transferAgentPhone": transfer_agent_phone,
        "transferAgentShort": (wespai or {}).get("transfer_agent_short", ""),
        "oddLotMail": (wespai or {}).get("odd_lot_mail", ""),
        "reelection": (wespai or {}).get("reelection", ""),
        "needVote": (ideal or {}).get("need_vote"),
        "fractionalOk": (ideal or {}).get("fractional_ok"),
        "evoteStartDate": evote_start,
        "evoteEndDate": evote_end,
        "evotePickupStartDate": pickup_start,
        "evotePickupEndDate": pickup_end,
        "evotePickupPlace": (honsec or {}).get("evote_pickup_place", ""),
        "evotePickupRule": (honsec or {}).get("evote_pickup_rule", ""),
        "meetingDistributionRule": (honsec or {}).get("meeting_distribution_rule", ""),
        "proxyPeriodText": (honsec or {}).get("proxy_period_text", ""),
        "agentDistributionPeriodText": (honsec or {}).get("agent_distribution_period_text", ""),
        "notes": (
            "目前在整合來源中尚未看到今年紀念品公告，建議保留 watchlist 持續追蹤。"
            if status == "unpublished"
            else ""
        ),
        "sources": source_links,
    }


class AppHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/lookup":
            query = urllib.parse.parse_qs(parsed.query)
            raw_codes = query.get("codes", [""])[0]
            self.handle_lookup(raw_codes)
            return
        if parsed.path == "/api/health":
            json_response(self, {"ok": True, "generatedAt": date.today().isoformat()})
            return
        if parsed.path == "/":
            self.path = "/index.html"
        super().do_GET()

    def handle_lookup(self, raw_codes: str) -> None:
        codes = clean_codes(raw_codes)
        if not codes:
            json_response(
                self,
                {
                    "ok": False,
                    "error": "請輸入至少一筆股票代號。",
                    "results": [],
                },
                status=400,
            )
            return

        try:
            sources = source_bundle()
            results = [build_record(code, sources) for code in codes]
            json_response(
                self,
                {
                    "ok": True,
                    "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "requestedCodes": codes,
                    "sourceStats": {
                        "wespai": len(sources["wespai"]),
                        "idealLabs": len(sources["ideal"]),
                        "honsec": len(sources["honsec"]),
                    },
                    "results": results,
                },
            )
        except Exception as error:  # pragma: no cover - we want a readable API error
            json_response(
                self,
                {
                    "ok": False,
                    "error": f"資料抓取失敗：{error}",
                    "results": [],
                },
                status=502,
            )


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), AppHandler)
    print(f"Shareholder gift tracker running at http://127.0.0.1:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
