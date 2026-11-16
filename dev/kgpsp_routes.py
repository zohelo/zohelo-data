"""Dev probe for #2970 (test branch only): which route reaches the KG PSP standard tables from a runner.

1. www.gov.pl page (two attempts, 60 s apart, plus a TCP connect test per address family).
2. dane.gov.pl API (the producer's page names dane.gov.pl as its open-data copy): datasets of the KG PSP, their
   resources (title, format, link, download_url, data date). Read-only, one request at a time, pauses.
3. With arg 'download': the newest and oldest standard-tables ZIP layout (dev/kgpsp_probe.walk). No R2 writes.
"""

from __future__ import annotations

import io
import json
import socket
import sys
import time
import zipfile

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

UA = {"User-Agent": bulk_file.USER_AGENT, "Accept": "application/json"}
API = "https://api.dane.gov.pl/1.4"
OUT: dict = {}


def tcp(host: str) -> dict:
    out = {}
    for fam, name in ((socket.AF_INET, "v4"), (socket.AF_INET6, "v6")):
        try:
            addr = socket.getaddrinfo(host, 443, fam)[0][4]
            t = time.monotonic()
            s = socket.create_connection(addr[:2], timeout=15)
            s.close()
            out[name] = f"ok {addr[0]} {time.monotonic() - t:.2f}s"
        except OSError as exc:
            out[name] = f"{type(exc).__name__}: {str(exc)[:80]}"
    return out


def get(url: str, **kw):
    time.sleep(2)
    try:
        r = requests.get(url, headers=UA, timeout=(20, 300), **kw)
        print(f"GET {url} -> {r.status_code} {len(r.content)} {r.headers.get('Content-Type')}", flush=True)
        return r
    except requests.RequestException as exc:
        print(f"GET {url} -> {type(exc).__name__} {str(exc)[:200]}", flush=True)
        return f"{type(exc).__name__}: {str(exc)[:150]}"


def main(argv: list[str]) -> int:
    OUT["tcp"] = {h: tcp(h) for h in ("www.gov.pl", "api.dane.gov.pl", "dane.gov.pl")}
    print(OUT["tcp"], flush=True)
    page = []
    for _ in range(2):
        r = get("https://www.gov.pl/web/kgpsp/interwencje-psp")
        page.append(r.status_code if isinstance(r, requests.Response) else r[:80])
        if isinstance(r, requests.Response):
            break
        time.sleep(60)
    OUT["gov_pl_page"] = page
    found = []
    for q in ("Państwowa Straż Pożarna", "interwencje PSP", "Komenda Główna Państwowej Straży Pożarnej"):
        r = get(f"{API}/datasets", params={"q": q, "per_page": 20})
        if not isinstance(r, requests.Response) or r.status_code != 200:
            found.append({"q": q, "error": r if isinstance(r, str) else r.status_code})
            continue
        for d in r.json().get("data", []):
            a = d.get("attributes", {})
            inst = a.get("institution")
            found.append({"id": d.get("id"), "title": (a.get("title") or "")[:90],
                          "inst": inst.get("title") if isinstance(inst, dict) else None,
                          "modified": a.get("modified"), "resources": a.get("resources_count")})
    OUT["datasets"] = found
    print(json.dumps(found, ensure_ascii=False), flush=True)
    ids = [f["id"] for f in found if f.get("id") and ("PSP" in f["title"] or "Straż" in f["title"] or "zdarze" in f["title"].lower())]
    res = []
    for did in list(dict.fromkeys(ids))[:6]:
        for pg in (1, 2, 3):
            r = get(f"{API}/datasets/{did}/resources", params={"per_page": 50, "page": pg})
            if not isinstance(r, requests.Response) or r.status_code != 200:
                res.append({"dataset": did, "error": r if isinstance(r, str) else r.status_code})
                break
            data = r.json().get("data", [])
            for x in data:
                a = x.get("attributes", {})
                res.append({"dataset": did, "id": x.get("id"), "title": (a.get("title") or "")[:80], "format": a.get("format"),
                            "link": (a.get("link") or "")[:110], "download": (a.get("download_url") or "")[:110],
                            "data_date": a.get("data_date"), "file_size": a.get("file_size")})
            if len(data) < 50:
                break
    OUT["resources"] = res
    for x in res:
        print("RES", json.dumps(x, ensure_ascii=False), flush=True)
    if "download" in argv:
        from dev.kgpsp_probe import walk

        tabs = [x for x in res if x.get("download") and "zdarze" not in x["title"].lower()
                and (x.get("format") or "").lower() == "zip"]
        for x in tabs[:1] + tabs[-1:]:
            r = get(x["download"])
            if isinstance(r, requests.Response) and r.status_code == 200 and r.content[:2] == b"PK":
                print(f"== tables {x['title']}", flush=True)
                OUT.setdefault("layout", {})[x["title"][:40]] = sorted(walk(zipfile.ZipFile(io.BytesIO(r.content)), "", 70, 80))[:60]
    s = json.dumps(OUT, ensure_ascii=False, default=str)
    for i in range(0, min(len(s), 30000), 3800):
        annotate("notice", f"kgpsp routes {i // 3800}", {"part": s[i:i + 3800]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
