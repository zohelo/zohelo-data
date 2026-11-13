"""Read-only probe of the routes to NID's register of immovable monuments (register PLS-026, Tasks #2471-#2473).

GET/HEAD only, one request at a time, 3 s apart, long timeouts, bounded retries with backoff, robots.txt read first
per host and honoured. Prints to the job log and to compact annotations: dane.gov.pl dataset 2627 metadata and
resources (licence, file URLs, first bytes), NID's own open-data and map pages (links), and the INSPIRE WFS of
usluga.zabytek.gov.pl (feature types, schema, hit counts, three sample features). Writes nothing anywhere.
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
BUDGET = [110]
NOTES: list[str] = []


def note(msg: str) -> None:
    NOTES.append(msg)
    print("NOTE " + msg, flush=True)


def robots_ok(url: str) -> bool:
    host = urlparse(url).scheme + "://" + urlparse(url).netloc
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            r = requests.get(host + "/robots.txt", headers={"User-Agent": UA}, timeout=(60, 120))
            print(f"--- robots {host}: {r.status_code} {r.text[:800]!r}", flush=True)
            if r.status_code in (401, 403):
                ROBOTS[host] = None
            else:
                ok = r.status_code == 200 and "<html" not in r.text[:300].lower()
                rp.parse(r.text.splitlines() if ok else [])
                ROBOTS[host] = rp
        except requests.RequestException as exc:
            print(f"--- robots {host}: {type(exc).__name__} {str(exc)[:200]}", flush=True)
            note(f"robots {host}: {type(exc).__name__}")
            rp.parse([])
            ROBOTS[host] = rp
    rp = ROBOTS[host]
    return rp is not None and rp.can_fetch(UA, url)


def get(url: str, *, head: bool = False, nbytes: int = 3_000_000, accept: str = "*/*", tries: int = 3,
        timeout=(60, 240)):
    if BUDGET[0] <= 0:
        return None, "budget used", 0.0
    if not robots_ok(url):
        return None, "robots disallow or refused", 0.0
    t0 = time.monotonic()
    for attempt in range(tries):
        BUDGET[0] -= 1
        time.sleep(3)
        t0 = time.monotonic()
        try:
            if head:
                r = requests.head(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"}, timeout=timeout,
                                  allow_redirects=True)
                body = b""
            else:
                with requests.get(url, headers={"User-Agent": UA, "Accept": accept, "Accept-Encoding": "identity"},
                                  timeout=timeout, allow_redirects=True, stream=True) as r:
                    body = b""
                    for block in r.iter_content(1 << 16):
                        body += block
                        if len(body) >= nbytes:
                            break
            hdr = {k: v[:160] for k, v in r.headers.items() if k.lower() in (
                "content-type", "last-modified", "etag", "content-length", "content-disposition", "retry-after")}
            st = f"{r.status_code} {r.url} {hdr}"
            if r.status_code >= 500 or r.status_code in (408, 429):
                raise RuntimeError(st)
            return body, st, time.monotonic() - t0
        except (requests.RequestException, RuntimeError) as exc:
            print(f"   try {attempt + 1}: {type(exc).__name__} {str(exc)[:240]} after {time.monotonic() - t0:.0f}s",
                  flush=True)
            if attempt < tries - 1:
                time.sleep(30 * (attempt + 1))
    return None, f"failed after {tries} tries", time.monotonic() - t0


def show(url: str, head_chars: int = 3000, **kw):
    body, st, sec = get(url, **kw)
    print(f"\n=== {'HEAD' if kw.get('head') else 'GET'} {url}\n--- {st} ({sec:.1f}s, {len(body or b'')} bytes)",
          flush=True)
    note(f"{url[:110]} -> {st[:150]} ({sec:.0f}s, {len(body or b'')} B)")
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
    for href in re.findall(r'href=["\']([^"\']+)["\']', html):
        u = urljoin(base, href)
        if re.search(pattern, u, re.I) and u not in found:
            found.append(u)
    return found


def dane_gov() -> None:
    api = "https://api.dane.gov.pl/1.4/datasets/2627"
    ds = show(api, 5000, accept="application/json", tries=2)
    if isinstance(ds, dict):
        attrs = (ds.get("data") or {}).get("attributes") or {}
        for k in ("title", "license_name", "license_condition_custom_description", "license_condition_source",
                  "update_frequency", "modified", "created", "verified", "institution"):
            print(f"DATASET {k}: {str(attrs.get(k))[:500]}")
        note(f"dane.gov.pl 2627 licence={attrs.get('license_name')} modified={attrs.get('modified')}")
    res = show(api + "/resources?per_page=50&page=1", 1500, accept="application/json", tries=2)
    files = []
    if isinstance(res, dict):
        for item in res.get("data") or []:
            a = item.get("attributes") or {}
            print("RESOURCE", item.get("id"), json.dumps({k: a.get(k) for k in (
                "title", "format", "media_type", "file_url", "download_url", "link", "file_size", "data_date",
                "modified", "created", "openness_score")}, ensure_ascii=False)[:1200])
            note(f"resource {item.get('id')} {a.get('format')} {a.get('file_size')} {a.get('data_date')} "
                 f"{(a.get('download_url') or a.get('file_url') or a.get('link') or '')[:90]}")
            for k in ("download_url", "file_url", "link"):
                if a.get(k):
                    files.append(a[k])
                    break
    for u in files[:6]:
        show(u, head=True, tries=2)
        body = show(u, 3000, nbytes=8000, tries=2)
        if isinstance(body, str) and ("," in body[:2000] or ";" in body[:2000]) and "<html" not in body[:500].lower():
            profile(u)


def profile(url: str) -> None:
    """Whole file to a temp dir, then aggregates only (no row is printed: addresses may identify private property)."""
    import tempfile

    import duckdb

    body, st, sec = get(url, nbytes=400_000_000, tries=2, timeout=(60, 600))
    note(f"full {url[:90]} -> {st[:80]} {len(body or b'')} B {sec:.0f}s")
    if not body:
        return
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/f.csv"
        open(path, "wb").write(body)
        con = duckdb.connect()
        try:
            con.sql(f"create table t as select * from read_csv('{path}', all_varchar=true, header=true)")
        except Exception as exc:  # noqa: BLE001
            note(f"read_csv failed {type(exc).__name__} {str(exc)[:300]}")
            return
        cols = [r[0] for r in con.sql("describe t").fetchall()]
        n = con.sql("select count(*) from t").fetchone()[0]
        note(f"PROFILE rows={n} cols={cols}")
        skip = re.compile(r"ulic|adres|numer_?dom|nr_?dom|wlasc|właśc|owner|osob|kod_?poczt|lat|lon|geom|wsp|x$|y$", re.I)
        for c in cols:
            q = f'"{c}"'
            nn, nd, mn, mx = con.sql(
                f"select count(*) filter (where {q} is null or trim({q}) = ''), count(distinct {q}), min({q}), max({q}) from t"
            ).fetchone()
            line = f"COL {c}: nulls={nn} distinct={nd}"
            if not skip.search(c):
                line += f" min={str(mn)[:40]!r} max={str(mx)[:40]!r}"
                if nd <= 60:
                    top = con.sql(f"select {q}, count(*) from t group by 1 order by 2 desc limit 25").fetchall()
                    line += " top=" + "; ".join(f"{str(v)[:40]}={k}" for v, k in top)
            print(line, flush=True)
            note(line[:600])


def nid_pages() -> None:
    for page, pat in (
        ("https://nid.pl/", r"dane|zabytk|rejestr|ewidencj|otwart|geoportal|mapy"),
        ("https://nid.pl/zasoby/", r"csv|xls|zip|json|dane|rejestr|ewidencj|licenc"),
        ("https://nid.pl/zasoby/rejestr-zabytkow/", r"csv|xls|zip|json|pdf|dane|rejestr|wojew"),
        ("https://zabytek.pl/pl", r"dane|rejestr|ewidencj|eksport|api|pobier"),
        ("https://mapy.zabytek.gov.pl/nid/", r"dane|wfs|wms|eksport|csv|zip|api|uslug|pobier"),
    ):
        text = show(page, 300, tries=2)
        if isinstance(text, str):
            found = links(text, page, pat)
            print("LINKS", page, json.dumps(found[:80], ensure_ascii=False), flush=True)
            note(f"links {page}: {' '.join(x for x in found if re.search(r'csv|xls|zip|json|rejestr', x, re.I))[:400]}")


def wfs() -> None:
    base = "https://usluga.zabytek.gov.pl/INSPIRE_IMZ/service.svc/get"
    caps = show(base + "?service=WFS&request=GetCapabilities", 4000, tries=2)
    if not isinstance(caps, str):
        return
    names = re.findall(r"<(?:wfs:)?Name>([^<]+)</(?:wfs:)?Name>", caps)
    versions = re.findall(r'version="([^"]+)"', caps[:2000])
    note(f"WFS feature types {names[:10]} versions {versions[:3]}")
    print("FEATURE TYPES", names, flush=True)
    for name in names[:4]:
        show(f"{base}?service=WFS&version=2.0.0&request=DescribeFeatureType&typeNames={name}", 6000, tries=2)
        hits = show(f"{base}?service=WFS&version=2.0.0&request=GetFeature&typeNames={name}&resultType=hits", 800,
                    tries=2)
        if isinstance(hits, str):
            m = re.search(r'numberMatched="([^"]+)"', hits)
            note(f"WFS {name} numberMatched={m.group(1) if m else '?'}")
        show(f"{base}?service=WFS&version=2.0.0&request=GetFeature&typeNames={name}&count=3", 9000, tries=2)


def main() -> None:
    try:
        dane_gov()
        nid_pages()
        wfs()
    finally:
        print("\nrequests left", BUDGET[0], flush=True)
        # Compact annotations: at most 10, each under about 4 KB.
        chunk, out = "", []
        for n in NOTES:
            if len(chunk) + len(n) > 3500:
                out.append(chunk)
                chunk = ""
            chunk += n.replace("\n", " ").replace("%", "%25") + " | "
        if chunk:
            out.append(chunk)
        for i, c in enumerate(out[:10]):
            print(f"::notice title=nid probe {i + 1}/{len(out)}::{c}", flush=True)


if __name__ == "__main__":
    main()
