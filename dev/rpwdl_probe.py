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
    print(f"[{section}] {json.dumps(item, ensure_ascii=False, default=str)[:3000]}", flush=True)


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
        with urllib.request.urlopen(req, timeout=60) as r:
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


def main(argv: list[str]) -> int:
    probe_spa("https://rpwdl2.ezdrowie.gov.pl/")
    probe_urls([
        "https://rpwdl2.ezdrowie.gov.pl/swagger/v1/swagger.json",
        "https://rpwdl2.ezdrowie.gov.pl/api/swagger/v1/swagger.json",
        "https://rpwdl2.ezdrowie.gov.pl/v3/api-docs",
        "https://rpwdl2.ezdrowie.gov.pl/api",
        "https://rpwdl2.ezdrowie.gov.pl/Pliki",
        "https://rpwdl2.ezdrowie.gov.pl/pliki-do-pobrania",
    ], "rpwdl2_candidates")
    body, info = fetch("https://rpwdl.ezdrowie.gov.pl/", accept="text/html")
    html = text_of(body)
    note("page", {"url": "https://rpwdl.ezdrowie.gov.pl/", **info, "title": title(html), "bytes": len(html)})
    if body:
        found = links(html, info.get("final") or "https://rpwdl.ezdrowie.gov.pl/")
        note("links", {"url": "rpwdl", "n": len(found), "links": found[:80]})
        dl = [u for u in found if KEYWORDS.search(u) and "rpwdl" in u][:8]
        probe_urls(dl, "rpwdl_old_links", accept="text/html")
    probe_urls([
        "https://rpwdl.ezdrowie.gov.pl/Registry/Download",
        "https://rpwdl.ezdrowie.gov.pl/Home/Download",
        "https://rpwdl.ezdrowie.gov.pl/Registry/Files",
    ], "rpwdl_old_candidates", accept="text/html")
    seen = set()
    for q in ("RPWDL", "podmiotów wykonujących działalność leczniczą", "rejestr podmiotów leczniczych", "praktyki lekarskie", "Centrum e-Zdrowia rejestr"):
        for d in dane(q):
            t = d["title"].lower()
            if d["id"] not in seen and ("leczni" in t or "rpwdl" in t or "praktyk" in t or "zakład" in t):
                seen.add(d["id"])
    for ds_id in list(seen)[:8]:
        dane_resources(ds_id)
    probe_urls(["https://ezdrowie.gov.pl/portal/home/rejestry-medyczne/rejestr-podmiotow-wykonujacych-dzialalnosc-lecznicza",
                "https://www.gov.pl/web/zdrowie/rejestr-podmiotow-wykonujacych-dzialalnosc-lecznicza"], "ezdrowie", accept="text/html")

    import os
    out_dir = os.environ.get("DEV_OUTPUT_DIR")
    if out_dir:
        with open(os.path.join(out_dir, "rpwdl_probe.json"), "w", encoding="utf-8") as fh:
            json.dump(OUT, fh, ensure_ascii=False, indent=1, default=str)
    # Compact annotations: at most 10, each under the cap.
    OUT.setdefault("robots", []).append({"requests_left": BUDGET[0]})
    for section, items in list(OUT.items())[:10]:
        annotate("notice", f"rpwdl probe {section}", items)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
