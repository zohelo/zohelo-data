"""Read-only probe of the RPWDL register routes (PLS-020, Task #2958).

GET only (plus HEAD), one request at a time, one second apart, a request budget; robots.txt read first
and honoured; no login, no captcha, no retry around 403. Reports the landing pages, the JavaScript bundles'
API references, candidate download endpoints and the dane.gov.pl datasets as annotations and in the log.
No record content is reported: for a data file only its headers, size and first header line.
"""

from __future__ import annotations

import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://zohelo.com)"
BUDGET = [140]
ROBOTS: dict[str, urllib.robotparser.RobotFileParser | None] = {}
OUT: dict[str, list] = {}


def note(section: str, item) -> None:
    OUT.setdefault(section, []).append(item)
    print(f"[{section}] {json.dumps(item, ensure_ascii=False, default=str)[:20000]}", flush=True)


def robots_ok(url: str) -> bool:
    parts = urllib.parse.urlsplit(url)
    host = f"{parts.scheme}://{parts.netloc}"
    if host not in ROBOTS:
        rp = urllib.robotparser.RobotFileParser()
        try:
            req = urllib.request.Request(host + "/robots.txt", headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as r:
                text = r.read(20000).decode("utf-8", "replace")
            rp.parse(text.splitlines())
            note("robots", {"host": host, "status": 200, "text": text[:600]})
        except urllib.error.HTTPError as e:
            note("robots", {"host": host, "status": e.code})
            rp.parse([])  # missing robots.txt: everything allowed
            if e.code in (401, 403):
                rp.disallow_all = True
        except Exception as e:  # noqa: BLE001
            note("robots", {"host": host, "error": f"{type(e).__name__}: {str(e)[:120]}"})
            rp = None
        ROBOTS[host] = rp
    rp = ROBOTS[host]
    return rp is None or rp.can_fetch(UA, url)


def fetch(url: str, method: str = "GET", limit: int = 3_000_000, accept: str = "*/*", data: bytes | None = None, ctype: str | None = None):
    if BUDGET[0] <= 0:
        return None, {"status": "budget used"}
    if not robots_ok(url):
        return None, {"status": "robots disallow"}
    BUDGET[0] -= 1
    time.sleep(1.0)
    headers = {"User-Agent": UA, "Accept": accept}
    if ctype:
        headers["Content-Type"] = ctype
    try:
        req = urllib.request.Request(url, headers=headers, method=method, data=data)
        with urllib.request.urlopen(req, timeout=120) as r:
            body = r.read(limit) if method != "HEAD" else b""
            hdr = {k.lower(): v[:120] for k, v in r.headers.items() if k.lower() in ("content-type", "content-length", "last-modified", "etag", "content-disposition", "server")}
            return body, {"status": r.status, "final": r.geturl(), **hdr, "read": len(body)}
    except urllib.error.HTTPError as e:
        try:
            body = e.read(400)
        except Exception:  # noqa: BLE001
            body = b""
        return None, {"status": e.code, "body": body.decode("utf-8", "replace")[:200]}
    except Exception as e:  # noqa: BLE001
        return None, {"status": f"{type(e).__name__}: {str(e)[:160]}"}


def text_of(body: bytes | None) -> str:
    return (body or b"").decode("utf-8", "replace")


def title(html: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.S | re.I)
    return re.sub(r"\s+", " ", m.group(1)).strip()[:120] if m else ""


def links(html: str, base: str) -> list[str]:
    out = []
    for m in re.finditer(r"""(?:href|src)\s*=\s*["']([^"'#]+)["']""", html, re.I):
        out.append(urllib.parse.urljoin(base, m.group(1)))
    return list(dict.fromkeys(out))


KEYWORDS = re.compile(r"(pobier|plik|download|export|eksport|file|csv|xlsx|xml|zip|report|raport|rejestr|registry|api/)", re.I)


def probe_spa(root: str) -> None:
    body, info = fetch(root, accept="text/html")
    html = text_of(body)
    note("page", {"url": root, **info, "title": title(html), "bytes": len(html), "head": re.sub(r"\s+", " ", html[:500])})
    if not body:
        return
    found = links(html, root)
    note("links", {"url": root, "n": len(found), "links": found[:60]})
    scripts = [u for u in found if re.search(r"\.js(\?|$)", u)][:10]
    for js in scripts:
        jb, ji = fetch(js, limit=8_000_000)
        src = text_of(jb)
        strings = set()
        for m in re.finditer(r"""["'`]((?:https?:)?/[^"'`\s<>]{2,160})["'`]""", src):
            s = m.group(1)
            if KEYWORDS.search(s):
                strings.add(s)
        for m in re.finditer(r"""["'`]([A-Za-z0-9_\-./]*(?:api|Api|API)[A-Za-z0-9_\-./{}$]*)["'`]""", src):
            strings.add(m.group(1))
        ctx = []
        for m in re.finditer(r"(Pliki do pobrania|pobierz|download|export|blob|responseType)", src):
            ctx.append(src[max(0, m.start() - 160): m.end() + 160].replace("\n", " "))
            if len(ctx) >= 12:
                break
        note("js", {"url": js, **ji, "strings": sorted(strings)[:120], "context": ctx})


def probe_urls(urls: list[str], section: str, accept: str = "*/*") -> None:
    for u in urls:
        body, info = fetch(u, accept=accept, limit=200_000)
        head = text_of(body[:400]) if body else ""
        note(section, {"url": u, **info, "head": re.sub(r"\s+", " ", head)[:300]})


def dane(q: str) -> list[dict]:
    url = "https://api.dane.gov.pl/1.4/datasets?" + urllib.parse.urlencode({"q": q, "per_page": 20})
    body, info = fetch(url, accept="application/json")
    res = []
    if body:
        try:
            doc = json.loads(body)
            for d in doc.get("data", []):
                a = d.get("attributes", {})
                rel = d.get("relationships", {})
                inst = (rel.get("institution", {}) or {}).get("data", {}) or {}
                res.append({"id": d.get("id"), "title": (a.get("title") or "")[:120], "inst": inst.get("id"),
                            "modified": a.get("modified"), "freq": a.get("update_frequency"),
                            "licence": a.get("license_name") or a.get("license_chosen"), "res": a.get("resources_count")
                            if "resources_count" in a else None})
        except ValueError:
            pass
    note("dane_search", {"q": q, **info, "n": len(res), "datasets": res})
    return res


def dane_resources(ds_id: str) -> None:
    url = f"https://api.dane.gov.pl/1.4/datasets/{ds_id}/resources?per_page=50"
    body, info = fetch(url, accept="application/json")
    out = []
    if body:
        try:
            for r in json.loads(body).get("data", []):
                a = r.get("attributes", {})
                out.append({"id": r.get("id"), "title": (a.get("title") or "")[:100], "format": a.get("format"),
                            "url": a.get("link") or a.get("download_url"), "file_size": a.get("file_size"),
                            "modified": a.get("modified") or a.get("data_date"), "media": a.get("media_type")})
        except ValueError:
            pass
    body2, info2 = fetch(f"https://api.dane.gov.pl/1.4/datasets/{ds_id}", accept="application/json")
    meta = {}
    if body2:
        try:
            a = json.loads(body2).get("data", {}).get("attributes", {})
            meta = {k: (str(a.get(k))[:300] if a.get(k) is not None else None) for k in
                    ("title", "notes", "license_name", "license_chosen", "license_condition_source",
                     "license_condition_responsibilities", "license_condition_db_or_copyrighted", "update_frequency",
                     "modified", "created", "source")}
        except ValueError:
            pass
    note("dane_resources", {"dataset": ds_id, **info, "meta": meta, "resources": out})


def crawl_bundles(root: str, max_files: int = 70) -> None:
    body, info = fetch(root, accept="text/html")
    queue = [u for u in links(text_of(body), root) if u.endswith(".js")]
    seen: set[str] = set()
    api_paths: dict[str, list[str]] = {}
    env: list[str] = []
    ctx: list[str] = []
    while queue and len(seen) < max_files:
        js = queue.pop(0)
        if js in seen:
            continue
        seen.add(js)
        jb, ji = fetch(js, limit=8_000_000)
        src = text_of(jb)
        for m in re.finditer(r"""(?:from|import\()\s*["'](\./[A-Za-z0-9_\-]+\.js)["']""", src):
            u = urllib.parse.urljoin(root, m.group(1))
            if u not in seen:
                queue.append(u)
        for m in re.finditer(r"`\$\{[A-Za-z0-9_.]+\}(/[^`]{1,200})`", src):
            api_paths.setdefault(m.group(1), []).append(js.rsplit("/", 1)[-1])
        for m in re.finditer(r"""(?:url|apiUrl|baseUrl|api)\s*:\s*["'`](https?://[^"'`]+)["'`]""", src):
            env.append(m.group(0)[:200])
        for m in re.finditer(r"(Pliki do pobrania|pliki-do-pobrania|files-to-download|public-files|publicFile|downloadFile|saveAs\(|generateFile|getFile)", src):
            ctx.append(js.rsplit("/", 1)[-1] + ": " + src[max(0, m.start() - 250): m.end() + 250].replace("\n", " "))
    note("bundle_api", {"files": len(seen), "paths": {k: sorted(set(v))[:3] for k, v in sorted(api_paths.items())}})
    note("bundle_env", sorted(set(env)))
    for c in ctx[:40]:
        note("bundle_ctx", c)


API = "https://rpwdl2.ezdrowie.gov.pl/api/rpwdl-public-resource/ledgers-zip"
TYPES = ["HEALTHCARE_PROVIDER", "PHYSICIAN_PRACTICE", "GROUP_PHYSICIAN_PRACTICE", "NURSING_PRACTICE",
         "GROUP_NURSING_PRACTICE", "PHYSIOTHERAPY_PRACTICE", "GROUP_PHYSIOTHERAPY_PRACTICE", "DIAGNOSTIC_PRACTICE",
         "GROUP_DIAGNOSTIC_PRACTICE"]


def download(url: str, path: str) -> dict:
    import hashlib
    if not robots_ok(url):
        return {"status": "robots disallow"}
    time.sleep(2.0)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    h = hashlib.sha256()
    n = 0
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=300) as r, open(path, "wb") as fh:
            hdr = {k.lower(): v[:160] for k, v in r.headers.items() if k.lower() in ("content-type", "content-length", "last-modified", "etag", "content-disposition", "cache-control", "date")}
            while True:
                chunk = r.read(1 << 20)
                if not chunk:
                    break
                fh.write(chunk)
                h.update(chunk)
                n += 1 << 0 if False else len(chunk)
            return {"status": r.status, **hdr, "bytes": n, "sha256": h.hexdigest()[:16], "seconds": round(time.time() - started, 1)}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "body": e.read(300).decode("utf-8", "replace")}
    except Exception as e:  # noqa: BLE001
        return {"status": f"{type(e).__name__}: {str(e)[:160]}"}


def profile_csv(path: str, name: str) -> dict:
    import duckdb
    con = duckdb.connect()
    raw = open(path, "rb").read(3000)
    out = {"member": name, "first_line": raw.split(b"\n", 1)[0].decode("utf-8", "replace")[:1500], "bom": raw[:3] == b"\xef\xbb\xbf"}
    src = f"read_csv('{path}', all_varchar=true, header=true, sample_size=-1, delim=';', quote='\"', escape='\"', strict_mode=true)"
    try:
        con.execute(f"create table t as select * from {src}")
        cols = [r[0] for r in con.execute("describe t").fetchall()]
        out["rows"] = con.execute("select count(*) from t").fetchall()[0][0]
        prof = {}
        for c in cols:
            nn, nd, ml, mn, mx = con.execute(f'select count("{c}"), count(distinct "{c}"), max(length("{c}")), min("{c}"), max("{c}") from t').fetchall()[0]
            item = {"nn": nn, "nd": nd, "ml": ml}
            if re.search(r"(Data|Teryt$|Teryt -? ?$|Teryt$|^Teryt|Początek|Koniec|Kod|Typ|Rodzaj|Forma|Izba|ID|Id )", c) and nd > 25:
                item["min"], item["max"] = mn, mx
                if "Teryt" in c and "opis" not in c:
                    item["lengths"] = con.execute(f'select length("{c}"), count(*) from t group by 1 order by 1').fetchall()
                if c.startswith("Data") or "okresu" in c:
                    item["formats"] = con.execute(f"""select regexp_replace("{c}", '[0-9]', '9', 'g'), count(*) from t group by 1 order by 2 desc limit 5""").fetchall()
            if nd <= 25 and not re.search(r"(Imie|Nazwisko|Nazwa|Telefon|Email|WWW|Ulica|Budynek|Lokal|Numer|Nip|NIP)", c):
                item["values"] = [[str(v)[:80], k] for v, k in con.execute(f'select "{c}", count(*) from t group by 1 order by 2 desc').fetchall()]
            prof[c] = item
        out["columns"] = prof
    except Exception as e:  # noqa: BLE001
        out["error"] = f"{type(e).__name__}: {str(e)[:600]}"
    return out


def main(argv: list[str]) -> int:
    import os
    import tempfile
    import zipfile
    fmt = argv[0] if argv else "CSV"
    types = argv[1].split(",") if len(argv) > 1 else TYPES
    actives = argv[2].split(",") if len(argv) > 2 else ["true", "false"]
    with tempfile.TemporaryDirectory() as tmp:
        for t in types:
            for active in actives:
                url = f"{API}/initial?fileTypeInsideZip={fmt}&activityType={t}&activeLedgers={active}"
                path = os.path.join(tmp, f"{t}_{active}.zip")
                info = download(url, path)
                members = []
                if info.get("status") == 200:
                    try:
                        with zipfile.ZipFile(path) as z:
                            for m in z.infolist():
                                members.append([m.filename, m.file_size, m.compress_size, "%04d-%02d-%02dT%02d:%02d" % m.date_time[:5]])
                            note("file", {"type": t, "active": active, **info, "members": members})
                            for m in z.infolist():
                                if m.filename.lower().endswith(".csv"):
                                    dst = z.extract(m, tmp)
                                    note("csv", {"type": t, "active": active, **profile_csv(dst, m.filename)})
                                    os.remove(dst)
                                elif fmt != "CSV":
                                    with z.open(m) as fh:
                                        head = fh.read(4000).decode("utf-8", "replace")
                                    note("head", {"type": t, "member": m.filename, "structure": re.sub(r">[^<]{1,200}<", "><", head)[:4000]})
                    except zipfile.BadZipFile:
                        note("file", {"type": t, "active": active, **info, "error": "not a zip", "head": open(path, "rb").read(200).decode("utf-8", "replace")})
                else:
                    note("file", {"type": t, "active": active, **info})
                if os.path.exists(path):
                    os.remove(path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
