"""Dev probe for PLG-023 TED notices (test branch only, never on main). Read-only: no R2 writes.

1. Documentation and terms pages (TED API search docs, TED legal notice, Publications Office legal notice,
   data.europa.eu TED dataset records): HTTP status, size, and the lines that name licence, reuse, re-use,
   attribution, limits, pagination and iteration. Full text goes to the run artifact (DEV_OUTPUT_DIR).
2. The Search API (POST /v3/notices/search): the supported field list (from the error of an unknown field), the
   number of Poland notices per period, one page of a Poland day with candidate fields, ITERATION paging.
One request at a time with a pause; identifying User-Agent.
"""

from __future__ import annotations

from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import time

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
API = "https://api.ted.europa.eu/v3/notices/search"
HEAD = {"User-Agent": bulk_file.USER_AGENT}
PAGES = {
    "ted_search_docs": "https://docs.ted.europa.eu/api/latest/search.html",
    "ted_api_index": "https://docs.ted.europa.eu/api/latest/index.html",
    "ted_legal_notice": "https://ted.europa.eu/en/legal-notice",
    "ted_legal_notice_simap": "https://ted.europa.eu/en/simap/legal-notice",
    "op_legal_notice": "https://op.europa.eu/en/web/about-us/legal-notices/publications-office-of-the-european-union-copyright",
    "op_legal_notice_eurlex": "https://eur-lex.europa.eu/content/legal-notice/legal-notice.html",
    "data_europa_ted_csv": "https://data.europa.eu/api/hub/search/datasets/ted-csv",
    "data_europa_ted_search": "https://data.europa.eu/api/hub/search/search?q=tenders%20electronic%20daily&filter=dataset&limit=5",
    "ted_reuse_docs": "https://docs.ted.europa.eu/home/index.html",
}
WORDS = re.compile(
    r"licen|reuse|re-use|re‑use|attribution|copyright|commercial|2011/833|cc[ -]by|creative commons|limit|"
    r"paginat|iteration|15[ ,.]?000|per page|rate|quota|too many",
    re.I,
)


class Text(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts: list[str] = []
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.skip += 1
        if tag in ("p", "li", "br", "div", "h1", "h2", "h3", "h4", "tr", "pre", "code"):
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self.skip:
            self.skip -= 1

    def handle_data(self, data):
        if not self.skip:
            self.parts.append(data)


def text_of(body: str) -> str:
    p = Text()
    p.feed(body)
    lines = [re.sub(r"\s+", " ", x).strip() for x in "".join(p.parts).splitlines()]
    return "\n".join(x for x in lines if x)


def pages() -> dict:
    out = {}
    for name, url in PAGES.items():
        time.sleep(2)
        try:
            r = requests.get(url, headers=HEAD, timeout=(20, 60), allow_redirects=True)
        except requests.RequestException as exc:
            out[name] = {"error": f"{type(exc).__name__}: {str(exc)[:150]}"}
            continue
        raw = r.text
        (OUT / f"{name}.raw").write_text(raw)
        txt = text_of(raw) if "html" in r.headers.get("Content-Type", "") else raw
        (OUT / f"{name}.txt").write_text(txt)
        hits = [ln[:300] for ln in txt.splitlines() if WORDS.search(ln)]
        out[name] = {
            "http": r.status_code,
            "final_url": r.url[:150],
            "type": r.headers.get("Content-Type", "")[:40],
            "bytes": len(r.content),
            "hits": len(hits),
        }
        annotate("notice", f"ted probe page {name}", {**out[name], "lines": hits[:40]})
    return out


def post(body: dict) -> tuple[int, dict | str, float]:
    time.sleep(1.5)
    t0 = time.monotonic()
    r = requests.post(API, json=body, headers={**HEAD, "Accept": "application/json"}, timeout=(20, 120))
    secs = round(time.monotonic() - t0, 2)
    try:
        return r.status_code, r.json(), secs
    except ValueError:
        return r.status_code, r.text[:2000], secs


def api() -> None:
    results: dict = {}
    # 1. supported fields: an unknown field name makes the API list the valid ones
    status, body, secs = post({"query": "buyer-country=POL", "fields": ["zz-unknown-field"], "limit": 1})
    (OUT / "api_unknown_field.json").write_text(json.dumps(body, indent=1))
    results["unknown_field"] = {"http": status, "secs": secs, "body": str(body)[:1500]}
    annotate("notice", "ted probe api fields", results["unknown_field"])
    # 2. counts
    counts = {}
    for label, query in {
        "pol_buyer_2026_11_10": "buyer-country=POL AND publication-date=20261110",
        "pol_buyer_2026": "buyer-country=POL AND publication-date>=20260101",
        "pol_buyer_2025": "buyer-country=POL AND publication-date>=20250101 AND publication-date<20260101",
        "pol_buyer_2024": "buyer-country=POL AND publication-date>=20240101 AND publication-date<20250101",
        "pol_place_2026": "place-of-performance=POL AND publication-date>=20260101",
        "pol_buyer_or_place_2026": "(buyer-country=POL OR place-of-performance=POL) AND publication-date>=20260101",
        "all_2026_11_10": "publication-date=20261110",
        "pol_buyer_sat_2026_11_07": "buyer-country=POL AND publication-date=20261107",
        "pol_buyer_sun_2026_11_08": "buyer-country=POL AND publication-date=20261108",
        "pol_buyer_mon_2026_11_09": "buyer-country=POL AND publication-date=20261109",
        "pol_buyer_2025_01_02": "buyer-country=POL AND publication-date=20250102",
    }.items():
        status, body, secs = post({"query": query, "fields": ["publication-number"], "limit": 1, "page": 1})
        counts[label] = {
            "http": status,
            "secs": secs,
            "total": body.get("totalNoticeCount") if isinstance(body, dict) else str(body)[:300],
            "keys": sorted(body) if isinstance(body, dict) else None,
        }
    annotate("notice", "ted probe api counts", counts)
    (OUT / "api_counts.json").write_text(json.dumps(counts, indent=1))


def sample(fields: list[str]) -> None:
    body = {
        "query": "buyer-country=POL AND publication-date=20261110",
        "fields": fields,
        "limit": 250,
        "page": 1,
        "scope": "ALL",
        "paginationMode": "PAGE_NUMBER",
    }
    status, res, secs = post(body)
    (OUT / "api_sample_day.json").write_text(json.dumps(res, indent=1, ensure_ascii=False))
    summary: dict = {"http": status, "secs": secs}
    if isinstance(res, dict) and res.get("notices"):
        n = res["notices"]
        summary["total"] = res.get("totalNoticeCount")
        summary["returned"] = len(n)
        summary["top_keys"] = sorted(res)
        keys = sorted({k for x in n for k in x})
        summary["field_types"] = {
            k: sorted({type(x.get(k)).__name__ for x in n}) for k in keys
        }
        summary["first"] = json.dumps(n[0], ensure_ascii=False)[:2500]
    else:
        summary["body"] = str(res)[:2500]
    annotate("notice", "ted probe api sample day", summary)


def limits_and_order() -> None:
    """Largest accepted page size, and whether two identical requests return identical bytes (stable order)."""
    out: dict = {}
    for limit in (250, 251, 500, 1000):
        status, res, _secs = post(
            {"query": "buyer-country=POL AND publication-date=20261110", "fields": ["publication-number"],
             "limit": limit, "page": 1}
        )  # fmt: skip
        out[f"limit_{limit}"] = {
            "http": status,
            "n": len(res.get("notices", [])) if isinstance(res, dict) else None,
            "body": None if isinstance(res, dict) and status == 200 else str(res)[:400],
        }
    hashes, orders = [], []
    for _ in range(2):
        time.sleep(1.5)
        r = requests.post(
            API,
            json={"query": "buyer-country=POL AND publication-date=20261110", "fields": ["publication-number",
                  "notice-type"], "limit": 100, "page": 2},
            headers={**HEAD, "Accept": "application/json"},
            timeout=(20, 120),
        )  # fmt: skip
        import hashlib

        hashes.append(hashlib.sha256(r.content).hexdigest()[:12])
        orders.append([x.get("publication-number") for x in r.json().get("notices", [])][:8])
    out["same_bytes_twice"] = hashes[0] == hashes[1]
    out["page2_first8"] = orders
    status, res, _ = post(
        {"query": "buyer-country=POL AND publication-date=20261110 SORT BY publication-number ASC",
         "fields": ["publication-number"], "limit": 5, "page": 1}
    )  # fmt: skip
    out["sort_by"] = {"http": status, "body": str(res)[:400]}
    status, res, _ = post({"query": "publication-date>=20000101", "fields": ["publication-number"], "limit": 250,
                           "page": 61})  # fmt: skip
    out["page_61_x_250"] = {"http": status, "body": str(res)[:400]}
    annotate("notice", "ted probe api limits and order", out)


def iteration() -> None:
    body = {
        "query": "buyer-country=POL AND publication-date=20261110",
        "fields": ["publication-number"],
        "limit": 100,
        "paginationMode": "ITERATION",
    }
    seen, pages_, token, out = [], 0, None, {}
    while pages_ < 6:
        if token:
            body["iterationNextToken"] = token
        status, res, secs = post(body)
        pages_ += 1
        if not isinstance(res, dict):
            out["error"] = {"http": status, "body": str(res)[:800]}
            break
        seen += [x.get("publication-number") for x in res.get("notices", [])]
        token = res.get("iterationNextToken")
        out[f"page{pages_}"] = {"http": status, "n": len(res.get("notices", [])), "token": bool(token), "secs": secs}
        if not token or not res.get("notices"):
            break
    out["distinct"] = len(set(seen))
    out["rows"] = len(seen)
    annotate("notice", "ted probe api iteration", out)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    pages()
    api()
    try:
        fields = json.loads((OUT / "fields_wanted.json").read_text())
    except FileNotFoundError:
        fields = [
            "publication-number", "notice-identifier", "notice-version", "notice-type", "form-type",
            "publication-date", "dispatch-date", "buyer-country", "buyer-name", "buyer-city", "place-of-performance",
            "classification-cpv", "contract-nature", "procedure-type", "notice-title", "total-value",
            "total-value-cur", "estimated-value-proc", "estimated-value-cur-proc", "links", "ojs-number",
        ]  # fmt: skip
    listed = set(re.findall(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)+", (OUT / "api_unknown_field.json").read_text()))
    extra = sorted(
        f for f in listed if re.search(r"nuts|value|cur|buyer|place-of-perf|notice-type|form-type", f, re.I)
    )
    annotate("notice", "ted probe fields listed", {"n": len(listed), "extra": extra[:150]})
    if len(listed) > 50:
        fields = [f for f in fields if f in listed] + [f for f in extra if f not in fields][:60]
    for attempt in range(3):
        status, res, _ = post({"query": "buyer-country=POL", "fields": fields, "limit": 1})
        if status == 200:
            break
        bad = re.findall(r"'([a-z0-9-]+)'", str(res))
        dropped = [f for f in fields if f in bad]
        annotate("warning", f"ted probe fields rejected {attempt}", {"http": status, "body": str(res)[:1200]})
        if not dropped:
            break
        fields = [f for f in fields if f not in dropped]
    sample(fields)
    limits_and_order()
    iteration()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
