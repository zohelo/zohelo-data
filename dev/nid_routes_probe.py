"""Read-only probe, round 2, of the routes to NID's register of immovable monuments (register PLS-026, #2471-#2473).

Round 1 (run 43343138018): api.dane.gov.pl timed out from Actions; nid.pl answered and links to
nid.pl/rejestr-zabytkow-zasoby, nid.pl/ewidencja-zabytkow-zasoby/ and files under mapy.zabytek.gov.pl/dane/;
zabytek.pl disallows robots. This round reads those NID pages (and their WordPress JSON), tries the
www host of dane.gov.pl, lists the file links and profiles the files: aggregates only (column names, row counts,
distinct counts, top values of short closed columns; no row and no address is printed).

GET/HEAD only, one request at a time, 3 s apart, bounded retries, robots.txt read first per host and honoured.
Writes nothing anywhere.
"""

from __future__ import annotations

import io
import json
import re
import struct
import tempfile
import time
import urllib.robotparser
import zipfile
from collections import Counter
from urllib.parse import urljoin, urlparse

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
ROBOTS: dict[str, urllib.robotparser.RobotFileParser | None] = {}
BUDGET = [120]
NOTES: list[str] = []
FILE_RE = re.compile(r"\.(csv|xlsx?|zip|json|ods|7z|gz)(\?|$)", re.I)
SKIP_COL = re.compile(r"ulic|adres|numer|nr_?dom|dom|lokal|wlasc|właśc|owner|osob|kod_?poczt|lat|lon|geom|wsp|^x$|^y$|"
                      r"miejsc|street|address|działk|dzialk|obręb|obreb", re.I)


def note(msg: str) -> None:
    NOTES.append(msg)
    print("NOTE " + msg, flush=True)


def robots_ok(url: str) -> bool:
    host = urlparse(url).scheme + "://" + urlparse(url).netloc
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            r = requests.get(host + "/robots.txt", headers={"User-Agent": UA}, timeout=(30, 60))
            print(f"--- robots {host}: {r.status_code} {r.text[:600]!r}", flush=True)
            if r.status_code in (401, 403):
                ROBOTS[host] = None
            else:
                ok = r.status_code == 200 and "<html" not in r.text[:300].lower()
                lines = r.text.splitlines() if ok else []
                # Non-standard "Deny: /" (zabytek.pl) is read as Disallow.
                lines = [re.sub(r"^\s*deny\s*:", "Disallow:", x, flags=re.I) for x in lines]
                rp.parse(lines)
                ROBOTS[host] = rp
        except requests.RequestException as exc:
            note(f"robots {host}: {type(exc).__name__}")
            ROBOTS[host] = None  # unreachable host: nothing else is tried there
    rp = ROBOTS[host]
    return rp is not None and rp.can_fetch(UA, url)


def get(url: str, *, head: bool = False, nbytes: int = 3_000_000, accept: str = "*/*", tries: int = 2,
        timeout=(30, 300)):
    if BUDGET[0] <= 0:
        return None, "budget used", 0.0
    if not robots_ok(url):
        return None, "robots disallow or host unreachable", 0.0
    t0 = time.monotonic()
    for attempt in range(tries):
        BUDGET[0] -= 1
        time.sleep(3)
        t0 = time.monotonic()
        try:
            if head:
                r = requests.head(url, headers={"User-Agent": UA}, timeout=timeout, allow_redirects=True)
                body = b""
            else:
                with requests.get(url, headers={"User-Agent": UA, "Accept": accept}, timeout=timeout,
                                  allow_redirects=True, stream=True) as r:
                    body = b""
                    for block in r.iter_content(1 << 16):
                        body += block
                        if len(body) >= nbytes:
                            break
            hdr = {k: v[:120] for k, v in r.headers.items() if k.lower() in (
                "content-type", "last-modified", "etag", "content-length", "content-disposition")}
            st = f"{r.status_code} {r.url} {hdr}"
            if r.status_code >= 500 or r.status_code in (408, 429):
                raise RuntimeError(st)
            return body, st, time.monotonic() - t0
        except (requests.RequestException, RuntimeError) as exc:
            print(f"   try {attempt + 1}: {type(exc).__name__} {str(exc)[:200]}", flush=True)
            if attempt < tries - 1:
                time.sleep(20 * (attempt + 1))
    return None, f"failed after {tries} tries", time.monotonic() - t0


def page_links(url: str) -> list[tuple[str, str]]:
    body, st, sec = get(url)
    note(f"{url[:100]} -> {st[:140]} ({sec:.0f}s, {len(body or b'')} B)")
    if not body:
        return []
    html = body.decode("utf-8", "replace")
    out = []
    for m in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html, re.I | re.S):
        u = urljoin(url, m.group(1).replace("&amp;", "&").replace("&#038;", "&"))
        text = re.sub(r"<[^>]+>|\s+", " ", m.group(2)).strip()
        out.append((u, text[:90]))
    # Text around files: dates such as "stan na ..." and licence words.
    for kw in ("stan na", "licenc", "CC BY", "aktualiz", "Creative"):
        for m in re.finditer(kw, html, re.I):
            frag = re.sub(r"<[^>]+>|\s+", " ", html[max(0, m.start() - 150): m.end() + 250])
            print(f"CONTEXT[{kw}] {frag[:400]}", flush=True)
            break
    return out


def dbf_profile(raw: bytes, name: str) -> None:
    if len(raw) < 32:
        return
    nrec, hlen = struct.unpack("<IH", raw[4:10])
    fields = []
    for i in range(32, hlen - 1, 32):
        fname = raw[i:i + 11].split(b"\0")[0].decode("latin-1")
        fields.append(f"{fname}:{chr(raw[i + 11])}{raw[i + 16]}")
    note(f"DBF {name}: records={nrec} fields={fields}")


def profile_table(con, name: str) -> None:
    cols = [r[0] for r in con.execute("describe t").fetchall()]
    n = con.execute("select count(*) from t").fetchone()[0]
    note(f"PROFILE {name}: rows={n} cols={cols}")
    for c in cols:
        q = f'"{c}"'
        nn, nd = con.execute(f"select count(*) filter (where {q} is null or trim({q}) = ''), count(distinct {q}) from t"
                         ).fetchone()
        line = f"COL {c}: nulls={nn} distinct={nd}"
        if not SKIP_COL.search(c):
            mn, mx, ml = con.execute(f"select min({q}), max({q}), max(length({q})) from t").fetchone()
            line += f" min={str(mn)[:40]!r} max={str(mx)[:40]!r} maxlen={ml}"
            if nd <= 80:
                top = con.execute(f"select {q}, count(*) from t group by 1 order by 2 desc limit 30").fetchall()
                line += " top=" + "; ".join(f"{str(v)[:40]}={k}" for v, k in top)
            else:
                shapes = con.execute(
                    f"select regexp_replace(regexp_replace({q}, '[0-9]', '9', 'g'), '[A-Za-zżźćńółęąśŻŹĆŃÓŁĘĄŚ]', 'a', 'g') s,"
                    f" count(*) from t group by 1 order by 2 desc limit 8").fetchall()
                line += " shapes=" + "; ".join(f"{str(v)[:30]}={k}" for v, k in shapes)
        print(line, flush=True)
        note(line[:500])


def profile_bytes(raw: bytes, name: str) -> None:
    import duckdb

    low = name.lower()
    if low.endswith(".zip"):
        z = zipfile.ZipFile(io.BytesIO(raw))
        infos = z.infolist()
        note(f"ZIP {name}: {len(infos)} members " + "; ".join(f"{i.filename}={i.file_size}" for i in infos[:40]))
        for i in infos:
            if i.filename.lower().endswith((".csv", ".xlsx", ".xls", ".dbf", ".json")) and i.file_size < 400_000_000:
                profile_bytes(z.read(i), f"{name}!{i.filename}")
        return
    if low.endswith(".dbf"):
        dbf_profile(raw, name)
        return
    con = duckdb.connect()
    with tempfile.TemporaryDirectory() as tmp:
        if low.endswith(".xlsx"):
            from ingestion.common.xlsx_cells import cells

            rows: dict[tuple[str, int], dict[int, str]] = {}
            sheets = Counter()
            for c in cells(raw):
                rows.setdefault((c["sheet_name"], c["row_number"]), {})[c["column_number"]] = c["value_text"]
                sheets[c["sheet_name"]] += 1
            note(f"XLSX {name}: cells by sheet {dict(sheets)}")
            for sheet in sheets:
                keys = sorted(r for s, r in rows if s == sheet)
                hdr_row = next((r for r in keys[:15] if len(rows[(sheet, r)]) >= 3), keys[0])
                hdr = rows[(sheet, hdr_row)]
                ncol = max(hdr)
                names = [str(hdr.get(i, f"c{i}")).strip().replace('"', "'") or f"c{i}" for i in range(1, ncol + 1)]
                seen = Counter()
                uniq = []
                for x in names:
                    seen[x] += 1
                    uniq.append(x if seen[x] == 1 else f"{x}_{seen[x]}")
                data = [[rows[(sheet, r)].get(i) for i in range(1, ncol + 1)] for r in keys if r > hdr_row]
                note(f"XLSX sheet {sheet}: header row {hdr_row}, {len(data)} rows below; first rows above header: "
                     + " | ".join(str(list(rows[(sheet, r)].values())[:3])[:120] for r in keys if r < hdr_row))
                import pyarrow as pa

                tbl = pa.table({u: [None if v is None else str(v) for v in col] for u, col in zip(uniq, zip(*data))})
                con.register("x", tbl)
                con.execute("create or replace table t as select * from x")
                profile_table(con, f"{name}#{sheet}")
            return
        if low.endswith(".xls"):
            import xlrd

            book = xlrd.open_workbook(file_contents=raw)
            for sh in book.sheets():
                note(f"XLS {name}#{sh.name}: {sh.nrows}x{sh.ncols} first rows "
                     + " | ".join(str(sh.row_values(r)[:6])[:150] for r in range(min(4, sh.nrows))))
            return
        path = f"{tmp}/f.csv"
        enc = "utf-8"
        try:
            raw.decode("utf-8")
        except UnicodeDecodeError:
            enc = "cp1250"
            raw = raw.decode("cp1250").encode("utf-8")
        open(path, "wb").write(raw)
        first = raw[:400].decode("utf-8", "replace").splitlines()[:1]
        note(f"CSV {name}: encoding {enc}, header line {first!r}"[:600])
        try:
            con.execute(f"create table t as select * from read_csv('{path}', all_varchar=true, header=true)")
        except Exception as exc:  # noqa: BLE001
            note(f"read_csv failed {type(exc).__name__} {str(exc)[:300]}")
            return
        profile_table(con, name)


def fetch_profile(url: str) -> None:
    _, st, _ = get(url, head=True)
    note(f"HEAD {url[:120]} -> {st[:200]}")
    body, st, sec = get(url, nbytes=500_000_000, timeout=(30, 900))
    note(f"GET {url[:100]} -> {st[:100]} {len(body or b'')} B {sec:.0f}s")
    if body:
        name = urlparse(url).path.rsplit("/", 1)[-1] or "file"
        if body[:2] == b"PK" and not name.lower().endswith((".zip", ".xlsx")):
            name += ".zip"
        try:
            profile_bytes(body, name)
        except Exception as exc:  # noqa: BLE001
            note(f"profile {name} failed {type(exc).__name__} {str(exc)[:300]}")


def main() -> None:
    try:
        files: list[str] = []
        for page in ("https://nid.pl/rejestr-zabytkow-zasoby", "https://nid.pl/ewidencja-zabytkow-zasoby/",
                     "https://nid.pl/zasoby/"):
            links = page_links(page)
            hits = [(u, t) for u, t in links if FILE_RE.search(u) or re.search(
                r"dane\.gov|zabytek\.gov|rejestr|ewidencj|pobierz|download", u + " " + t, re.I)]
            print("LINKS", page, json.dumps(hits[:120], ensure_ascii=False), flush=True)
            note(f"links {page}: " + " ".join(u for u, _ in hits if FILE_RE.search(u) or "dane.gov" in u)[:1500])
            files += [u for u, _ in hits if FILE_RE.search(u) and u not in files]
        for api in ("https://nid.pl/wp-json/wp/v2/pages?slug=rejestr-zabytkow-zasoby&_fields=link,modified,content",
                    "https://nid.pl/wp-json/wp/v2/pages?slug=ewidencja-zabytkow-zasoby&_fields=link,modified,content"):
            body, st, _ = get(api, accept="application/json")
            note(f"{api[:90]} -> {st[:80]}")
            if body:
                try:
                    for p in json.loads(body):
                        html = (p.get("content") or {}).get("rendered", "")
                        found = [urljoin(p.get("link", ""), h) for h in re.findall(r'href=["\']([^"\']+)', html)]
                        note(f"WP {p.get('link')} modified={p.get('modified')} links=" + " ".join(found)[:1500])
                        files += [u for u in found if FILE_RE.search(u) and u not in files]
                except ValueError:
                    pass
        # dane.gov.pl www host (api.dane.gov.pl timed out in round 1).
        for u in ("https://dane.gov.pl/pl/dataset/2627", "https://dane.gov.pl/pl/dataset/2627/resource"):
            body, st, sec = get(u, nbytes=400_000)
            note(f"{u} -> {st[:120]} ({sec:.0f}s)")
            if body:
                html = body.decode("utf-8", "replace")
                found = sorted(set(re.findall(r'https?://[^"\'\s<>]+?(?:\.csv|\.xlsx?|\.zip|/resource/\d+[^"\'\s<>]*)',
                                              html)))
                note(f"dane.gov.pl links: {' '.join(found)[:1500]}")
        for u in ("https://mapy.zabytek.gov.pl/dane/",):
            links = page_links(u)
            note(f"links {u}: " + " ".join(x for x, _ in links)[:1500])
            files += [x for x, _ in links if FILE_RE.search(x) and x not in files]
        note(f"FILES {len(files)}: " + " ".join(files)[:2500])
        wanted = [u for u in files if re.search(r"rejestr|ewidencj|zabytk|nieruch", u, re.I)
                  and not re.search(r"Raport|RAPORT|\.pdf", u)]
        for u in (wanted or files)[:8]:
            fetch_profile(u)
    finally:
        print("\nrequests left", BUDGET[0], flush=True)
        chunk, out = "", []
        for n in NOTES:
            if len(chunk) + len(n) > 3800:
                out.append(chunk)
                chunk = ""
            chunk += n.replace("\n", " ").replace("%", "%25") + " | "
        if chunk:
            out.append(chunk)
        for i, c in enumerate(out[:10]):
            print(f"::notice title=nid routes {i + 1}/{len(out)}::{c}", flush=True)


if __name__ == "__main__":
    main()
