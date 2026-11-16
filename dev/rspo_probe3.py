"""Read-only probe 3 of the RSPO public search (PLS-021, Epic #445): the search's own backend calls and its download.

Downloads the public Angular bundle of rspo.gov.pl once and reports the code around every backend path ("api/")
and around the download and export code (icon_download, Blob, saveAs, xlsx, csv, Pobierz, Eksport). Then reads
only the public description of the backend, if the site publishes one (swagger), as status and path list.
GET only, one request at a time, one second apart, at most 12 requests. No register record is fetched.
"""

from __future__ import annotations

import json
import re
import sys
import time
import urllib.error
import urllib.request

UA = "zohelo-data/1.0 (+https://zohelo.com)"
BUNDLE = "https://rspo.gov.pl/main.3637f8defda927ef.js"


def note(section: str, item) -> None:
    print(f"[{section}] {json.dumps(item, ensure_ascii=False, default=str)[:30000]}", flush=True)


def fetch(url: str, limit: int = 12_000_000):
    time.sleep(1.0)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json, */*"})
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.read(limit), {"status": r.status, "type": r.headers.get("content-type")}
    except urllib.error.HTTPError as e:
        return None, {"status": e.code}
    except Exception as e:  # noqa: BLE001
        return None, {"status": f"{type(e).__name__}: {str(e)[:120]}"}


def contexts(src: str, pattern: str, width: int, limit: int) -> list[str]:
    out, last = [], -10_000
    for m in re.finditer(pattern, src):
        if m.start() - last < width:  # skip overlapping hits
            continue
        last = m.start()
        out.append(src[max(0, m.start() - width): m.end() + width].replace("\n", " "))
        if len(out) >= limit:
            break
    return out


def main(argv: list[str]) -> int:
    body, info = fetch(BUNDLE)
    src = (body or b"").decode("utf-8", "replace")
    note("bundle", {**info, "bytes": len(src)})
    api = sorted(set(re.findall(r"""[`"']([^`"'\s]{0,40}api/[^`"'\s]{0,120})[`"']""", src)))
    note("api_strings", api[:200])
    for c in contexts(src, r"api/(?!Dictionary)", 300, 30):
        note("api_ctx", c)
    for c in contexts(src, r"(icon_download|saveAs|new Blob|\.xlsx|\.csv|text/csv|spreadsheetml|Pobierz|Eksport|eksport|Content-Disposition)", 350, 30):
        note("download_ctx", c)
    for c in contexts(src, r"(environment|apiUrl|baseUrl|serverUrl|backendUrl)", 200, 10):
        note("env_ctx", c)
    for u in ("https://rspo.gov.pl/swagger/v1/swagger.json", "https://rspo.gov.pl/api/swagger/v1/swagger.json",
              "https://rspo.gov.pl/swagger/index.html", "https://rspo.gov.pl/api/"):
        b, i = fetch(u, limit=3_000_000)
        rec: dict = {"url": u, **i}
        if b:
            t = b.decode("utf-8", "replace")
            try:
                doc = json.loads(t)
                rec["paths"] = sorted((doc.get("paths") or {}).keys())[:120] if isinstance(doc, dict) else None
                rec["security"] = (doc.get("components") or {}).get("securitySchemes") if isinstance(doc, dict) else None
            except ValueError:
                rec["head"] = "(SPA index)" if t.lstrip().startswith("<!DOCTYPE") else re.sub(r"\s+", " ", t[:300])
        note("backend_doc", rec)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
