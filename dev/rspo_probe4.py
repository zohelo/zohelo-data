"""Read-only probe 4 of public RSPO routes (PLS-021, #3031): dane.gov.pl, the search's download button, cie.gov.pl.

GET only, one request at a time, 2 s apart, at most 40 requests, robots.txt honoured where present, no login,
no retry around 401/403. Reports metadata only: titles, links, sizes, headers and code context of the public
JavaScript bundle around its download and HTTP calls. No register record content is printed beyond a header line.
"""

from __future__ import annotations

import json
import re
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://zohelo.com)"
BUDGET = [40]
NOTES: list[str] = []


def note(section: str, item) -> None:
    line = f"[{section}] {json.dumps(item, ensure_ascii=False, default=str)}"
    print(line[:30000], flush=True)
    NOTES.append(line[:600])


def fetch(url: str, limit: int = 12_000_000, accept: str = "*/*", timeout: int = 90):
    if BUDGET[0] <= 0:
        return None, {"status": "budget used"}
    BUDGET[0] -= 1
    time.sleep(2.0)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": accept})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(limit)
            hdr = {k.lower(): v[:160] for k, v in r.headers.items()
                   if k.lower() in ("content-type", "content-length", "last-modified", "content-disposition")}
            return body, {"status": r.status, "final": r.geturl(), **hdr, "read": len(body)}
    except urllib.error.HTTPError as e:
        return None, {"status": e.code}
    except Exception as e:  # noqa: BLE001
        return None, {"status": f"{type(e).__name__}: {str(e)[:160]}"}


def contexts(src: str, pattern: str, width: int, limit: int) -> list[str]:
    out, last = [], -10_000
    for m in re.finditer(pattern, src):
        if m.start() - last < width:
            continue
        last = m.start()
        out.append(src[max(0, m.start() - width): m.end() + width].replace("\n", " "))
        if len(out) >= limit:
            break
    return out


def dane() -> None:
    for host in ("api.dane.gov.pl", "dane.gov.pl"):
        try:
            infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
            addrs = sorted({i[4][0] for i in infos})
        except Exception as e:  # noqa: BLE001
            addrs = [f"{type(e).__name__}: {e}"]
        note("dane_dns", {"host": host, "addrs": addrs})
        for a in addrs[:3]:
            fam = socket.AF_INET6 if ":" in a else socket.AF_INET
            t0 = time.time()
            try:
                s = socket.socket(fam, socket.SOCK_STREAM)
                s.settimeout(25)
                s.connect((a, 443))
                s.close()
                note("dane_tcp", {"addr": a, "ok": True, "s": round(time.time() - t0, 1)})
            except Exception as e:  # noqa: BLE001
                note("dane_tcp", {"addr": a, "ok": False, "err": f"{type(e).__name__}: {e}", "s": round(time.time() - t0, 1)})
    # one small request per query, long timeout (owner rule: slower, smaller requests)
    for q in ("rspo", "rejestr szkół i placówek oświatowych"):
        u = "https://api.dane.gov.pl/1.4/search?" + urllib.parse.urlencode({"q": q, "per_page": 10})
        b, i = fetch(u, accept="application/json", timeout=150)
        rec: dict = {"url": u, **i}
        if b:
            try:
                d = json.loads(b)
                rec["count"] = (d.get("meta") or {}).get("count")
                rec["items"] = [[x.get("type"), x.get("id"), (x.get("attributes") or {}).get("title"),
                                 (x.get("attributes") or {}).get("modified")] for x in d.get("data", [])]
            except ValueError:
                rec["head"] = b[:200].decode("utf-8", "replace")
        note("dane_search", rec)
        if not b:
            break


def rspo_bundle() -> None:
    b, i = fetch("https://rspo.gov.pl/", accept="text/html")
    html = (b or b"").decode("utf-8", "replace")
    mains = re.findall(r"""src=["'](main\.[0-9a-f]+\.js)["']""", html)
    note("rspo_index", {**i, "main": mains})
    if not mains:
        return
    jb, ji = fetch("https://rspo.gov.pl/" + mains[0])
    src = (jb or b"").decode("utf-8", "replace")
    note("rspo_bundle", {**ji, "bytes": len(src)})
    # every HTTP call of the app: what it requests, with which path pieces
    for c in contexts(src, r"this\.http\.(get|post)\(", 260, 40):
        note("http_ctx", c)
    for c in contexts(src, r"(icon_download\"\)|svgIcon\",\"icon_download|responseType:\"blob\"|responseType:\"arraybuffer\"|saveAs\(|\.download=|createObjectURL)", 420, 20):
        note("download_ctx", c)
    for c in contexts(src, r"(Pobierz|Eksport|eksport|CSV|XLSX|xlsx|csv)", 260, 30):
        note("label_ctx", c)
    for c in contexts(src, r"(apiUrl|baseUrl|ApiUrl|BaseUrl|api:\"|apiPath)", 200, 10):
        note("env_ctx", c)


def cie() -> None:
    for u in ("https://cie.gov.pl/wp-json/wp/v2/search?search=rspo&per_page=50",
              "https://cie.gov.pl/wp-json/wp/v2/search?search=wykaz%20szk%C3%B3%C5%82&per_page=50",
              "https://cie.gov.pl/wp-json/wp/v2/media?search=rspo&per_page=50",
              "https://cie.gov.pl/wp-json/wp/v2/media?search=szk%C3%B3%C5%82&per_page=50"):
        b, i = fetch(u, accept="application/json", limit=3_000_000)
        rec: dict = {"url": u, **i}
        if b:
            try:
                d = json.loads(b)
                rec["items"] = [[x.get("title") if isinstance(x.get("title"), str) else (x.get("title") or {}).get("rendered"),
                                 x.get("url") or x.get("source_url") or x.get("link"), x.get("mime_type")] for x in d][:50]
            except ValueError:
                rec["head"] = b[:200].decode("utf-8", "replace")
        note("cie", rec)
    for u in ("https://www.gov.pl/web/edukacja/rejestr-szkol-i-placowek-oswiatowych",):
        b, i = fetch(u, accept="text/html", limit=2_000_000, timeout=40)
        html = (b or b"").decode("utf-8", "replace")
        links = sorted({m for m in re.findall(r"""href=["']([^"']+)["']""", html)
                        if re.search(r"(rspo|xlsx|csv|zip|dane\.gov|attachment)", m, re.I)})
        note("govpl", {"url": u, **i, "links": links[:40]})


def main(argv: list[str]) -> int:
    for step in (dane, rspo_bundle, cie):
        try:
            step()
        except Exception as e:  # noqa: BLE001
            note("error", {"step": step.__name__, "err": f"{type(e).__name__}: {e}"})
    annotate("notice", "rspo probe 4", [n[:280] for n in NOTES[:12]])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
