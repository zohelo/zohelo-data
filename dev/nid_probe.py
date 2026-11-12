"""Read-only probe of the routes to the NID register of immovable monuments (register PLS-026, Task #2471).

GET/HEAD only, one request at a time, 2 s apart, bounded retries with backoff, robots.txt read first per host and
honoured. Prints to the job log: the dane.gov.pl dataset 2627 metadata (licence, resources, file URLs), the first
bytes of every resource file, and the links of NID's own open-data and map pages. Writes nothing.
"""

from __future__ import annotations

import json
import re
import time
import urllib.robotparser
from urllib.parse import urljoin, urlparse

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
ROBOTS: dict[str, urllib.robotparser.RobotFileParser | None] = {}
BUDGET = [90]


def robots_ok(url: str) -> bool:
    host = urlparse(url).scheme + "://" + urlparse(url).netloc
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            r = requests.get(host + "/robots.txt", headers={"User-Agent": UA}, timeout=(20, 40))
            print(f"--- robots {host}: {r.status_code} {r.text[:600]!r}", flush=True)
            if r.status_code in (401, 403):
                ROBOTS[host] = None
            else:
                rp.parse(r.text.splitlines() if r.status_code == 200 and "<html" not in r.text[:300].lower() else [])
                ROBOTS[host] = rp
        except requests.RequestException as exc:
            print(f"--- robots {host}: {type(exc).__name__} {str(exc)[:150]}", flush=True)
            rp.parse([])
            ROBOTS[host] = rp
    rp = ROBOTS[host]
    return rp is not None and rp.can_fetch(UA, url)


def get(url: str, *, head: bool = False, nbytes: int = 3_000_000, accept: str = "*/*", tries: int = 3):
    if BUDGET[0] <= 0:
        return None, "budget used", 0.0
    if not robots_ok(url):
        return None, "robots disallow or refused", 0.0
    for attempt in range(tries):
        BUDGET[0] -= 1
        time.sleep(2)
        t0 = time.monotonic()
        try:
            if head:
                r = requests.head(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"}, timeout=(30, 90),
                                  allow_redirects=True)
                body = b""
            else:
                with requests.get(url, headers={"User-Agent": UA, "Accept": accept, "Accept-Encoding": "identity"},
                                  timeout=(30, 120), allow_redirects=True, stream=True) as r:
                    body = b""
                    for block in r.iter_content(1 << 16):
                        body += block
                        if len(body) >= nbytes:
                            break
            hdr = {k: v[:120] for k, v in r.headers.items() if k.lower() in (
                "content-type", "last-modified", "etag", "content-length", "content-disposition", "retry-after")}
            st = f"{r.status_code} {r.url} {hdr}"
            if r.status_code >= 500 or r.status_code in (408, 429):
                raise RuntimeError(st)
            return body, st, time.monotonic() - t0
        except (requests.RequestException, RuntimeError) as exc:
            print(f"   try {attempt + 1}: {type(exc).__name__} {str(exc)[:200]} after {time.monotonic() - t0:.0f}s",
                  flush=True)
            if attempt < tries - 1:
                time.sleep(20 * (attempt + 1))
    return None, "failed", 0.0


def show(url: str, head_chars: int = 3000, **kw):
    body, st, sec = get(url, **kw)
    print(f"\n=== GET {url}\n--- {st} ({sec:.1f}s, {len(body or b'')} bytes)", flush=True)
    if body is None:
        return None
    text = body.decode("utf-8", "replace")
    try:
        doc = json.loads(text)
        print(json.dumps(doc, ensure_ascii=False)[:head_chars])
        return doc
    except ValueError:
        print(text[:head_chars])
        return text


def links(html: str, base: str, pattern: str) -> list[str]:
    found = []
    for href in re.findall(r'href="([^"]+)"', html):
        u = urljoin(base, href)
        if re.search(pattern, u, re.I) and u not in found:
            found.append(u)
    return found


def main() -> None:
    api = "https://api.dane.gov.pl/1.4/datasets/2627"
    ds = show(api, 6000, accept="application/json")
    if isinstance(ds, dict):
        attrs = (ds.get("data") or {}).get("attributes") or {}
        for k in ("title", "license_name", "license_condition_custom_description", "license_condition_source",
                  "update_frequency", "modified", "created", "verified", "institution"):
            print(f"DATASET {k}: {str(attrs.get(k))[:500]}")
    res = show(api + "/resources?per_page=50&page=1", 2000, accept="application/json")
    files = []
    if isinstance(res, dict):
        for item in res.get("data") or []:
            a = item.get("attributes") or {}
            print("RESOURCE", item.get("id"), json.dumps({k: a.get(k) for k in (
                "title", "format", "media_type", "file_url", "download_url", "link", "file_size", "data_date",
                "modified", "created", "openness_score", "description")}, ensure_ascii=False)[:1200])
            for k in ("download_url", "file_url", "link"):
                if a.get(k):
                    files.append(a[k])
    for u in files[:8]:
        show(u, head=True)
        show(u, 2500, nbytes=6000)
    show("https://dane.gov.pl/pl/dataset/2627", 400)
    # NID's own routes (producer)
    for page, pat in (
        ("https://nid.pl/", r"dane|zabytk|rejestr|ewidencj|otwart"),
        ("https://nid.pl/otwarte-dane/", r"csv|xls|zip|json|dane|rejestr|licenc"),
        ("https://zabytek.pl/pl", r"dane|rejestr|ewidencj|eksport|api"),
        ("https://mapy.zabytek.gov.pl/nid/", r"dane|wfs|wms|eksport|csv|zip|api|uslug"),
        ("https://dane.nid.pl/", r"csv|xls|zip|json|dane|rejestr|licenc"),
    ):
        text = show(page, 300)
        if isinstance(text, str):
            print("LINKS", page, json.dumps(links(text, page, pat)[:60], ensure_ascii=False))
    show("https://usluga.zabytek.gov.pl/INSPIRE_IMZ/service.svc/get?service=WFS&request=GetCapabilities", 3000)
    print("\nrequests left", BUDGET[0])


if __name__ == "__main__":
    main()
