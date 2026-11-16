"""Read-only probe 4 for MRiRW ZSRIR (PLS-032): the portal's public endpoints (ZsrirData report files and
DynamicReportExternal). No R2 writes, no sign-in.

Prints the JavaScript around each public endpoint (how the page calls it), then calls the list endpoints the public
pages call. Chain verification as in probe 3 (the CA's own intermediate added to certifi's roots).
"""

from __future__ import annotations

import functools
import json
import re
import ssl
import sys
import tempfile
import time

import certifi
import requests

print = functools.partial(print, flush=True)  # noqa: A001
UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://zsrir.minrol.gov.pl"
AIA = "http://certumdvtlsg2r39ca.repository.certum.pl/certumdvtlsg2r39ca.cer"
PAUSE = 1.5
session = requests.Session()
session.headers.update({"User-Agent": UA, "Accept": "application/json, text/plain, */*"})
TARGETS = ["ZsrirData/", "DynamicReportExternal/", "API_BASE_URL", "Dictionary/GetProvinces", "Dictionary/GetUnits",
           "Dictionary/GetReportingPeriods", "regulamin", "licenc", "Regulamin"]


def bundle() -> str:
    pem = ssl.DER_cert_to_PEM_cert(session.get(AIA, timeout=(10, 30)).content)
    with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as f:
        f.write(open(certifi.where()).read() + "\n" + pem)
        return f.name


VERIFY = ""


def call(method: str, path: str, **kw):
    time.sleep(PAUSE)
    try:
        r = session.request(method, BASE + path, timeout=(10, 120), verify=VERIFY, **kw)
    except requests.RequestException as exc:
        print(f"{method} {path}: {type(exc).__name__} {str(exc)[:200]}")
        return None
    body = r.content
    print(f"{method} {path} {kw.get('json', kw.get('params', ''))}: {r.status_code} {len(body)} bytes "
          f"{r.headers.get('Content-Type')} {r.headers.get('Content-Disposition')}")
    if "json" in (r.headers.get("Content-Type") or "") or body[:1] in (b"[", b"{"):
        print("   " + r.text[:2500])
    return r


def main(argv: list[str]) -> int:
    global VERIFY
    VERIFY = bundle()
    index = session.get(BASE + "/", timeout=(10, 60), verify=VERIFY).text
    queue = sorted(set(re.findall(r'src="([^"]+\.js)"', index)))
    seen: set[str] = set()
    while queue and len(seen) < 45:
        js = queue.pop(0)
        if js in seen:
            continue
        seen.add(js)
        time.sleep(0.5)
        text = session.get(f"{BASE}/{js}", timeout=(10, 60), verify=VERIFY).text
        queue += [c.lstrip("./") for c in re.findall(r'["\'`](\./chunk-[A-Z0-9]+\.js|chunk-[A-Z0-9]+\.js)["\'`]', text)]
        for t in TARGETS:
            for m in re.finditer(re.escape(t), text):
                ctx = text[max(0, m.start() - 250): m.end() + 350].replace("\n", " ")
                print(f"\n-- {js} {t}: {ctx}")
    print("\n===== calls")
    reports = call("GET", "/api/ZsrirData/GetReportList")
    if reports is not None and reports.status_code == 405:
        reports = call("POST", "/api/ZsrirData/GetReportList", json={})
    call("GET", "/api/DynamicReportExternal/GetMarkets")
    call("GET", "/api/DynamicReportExternal/GetProvinces")
    call("GET", "/api/Dictionary/GetProvinces")
    call("GET", "/api/Dictionary/GetUnits")
    call("GET", "/api/Dictionary/GetReportingPeriods")
    for a in argv:
        method, path = a.split(":", 1)
        call(method, path)
    if reports is not None and reports.status_code == 200:
        try:
            items = reports.json()
            print("report list type", type(items).__name__, json.dumps(items, ensure_ascii=False)[:200])
        except ValueError:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
