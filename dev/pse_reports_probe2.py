"""Read-only Dev probe 2 for #2870 (test branch only): the web app's JS bundles (report catalogue, footer and terms
links), nextLink on a 100-period DST day, the his-wlk-cal row of 2024-06-13, the newest RCE day. One request at a
time, 1.5 s apart, no writes."""

from __future__ import annotations

import re
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://api.raporty.pse.pl/api"
S = requests.Session()
S.headers.update({"User-Agent": UA})
LAST = [0.0]


def get(url, params=None):
    wait = 1.5 - (time.monotonic() - LAST[0])
    if wait > 0:
        time.sleep(wait)
    LAST[0] = time.monotonic()
    return S.get(url, params=params, timeout=(20, 120))


def bundles():
    out = {}
    for root in ("https://raporty.pse.pl/", "https://api.raporty.pse.pl/"):
        page = get(root).text
        scripts = sorted(set(re.findall(r'src="(/assets/[^"]+\.js)"', page)) | set(re.findall(r'href="(/assets/[^"]+\.js)"', page)))
        found = {"scripts": scripts}
        for s in scripts[:4]:
            js = get(requests.compat.urljoin(root, s)).text
            found[s] = {
                "bytes": len(js),
                "entities": sorted(set(re.findall(r"""["'`/]((?:his|rce|pk5|pdgsz|crb|kmb|sk|ogr|zap|gen|pomb|poeb|cmbp|en|mbu|mbp|prog|plan|wyk|daily|pd|bpkd|bpp|pl|ceny|cen)[a-z0-9]*(?:-[a-z0-9]+)+)["'`?]""", js)))[:120],
                "terms_hits": [js[max(0, m.start() - 120) : m.start() + 220] for m in re.finditer(r"regulamin|licenc|warunki korzyst|terms of use|nota prawna|prawa autorskie|copyright|creative commons|CC BY|warunk", js, re.I)][:8],
                "api_hits": [js[max(0, m.start() - 80) : m.start() + 160] for m in re.finditer(r"api\.raporty|/api/", js)][:5],
                "urls": sorted(set(re.findall(r"https?://[a-zA-Z0-9./_-]+", js)))[:40],
            }
        out[root] = found
    return out


def api_checks():
    out = {}
    for ent in ("rce-pln", "his-wlk-cal"):
        r = get(f"{API}/{ent}", {"$filter": "business_date eq '2025-10-26'"})
        doc = r.json()
        nxt = doc.get("nextLink")
        out[f"{ent} 2025-10-26"] = {"rows": len(doc.get("value", [])), "keys": sorted(doc), "nextLink": nxt}
        if nxt:
            r2 = get(nxt)
            out[f"{ent} 2025-10-26 page 2"] = {"status": r2.status_code, "bytes": len(r2.content), "text": r2.text[:300]}
    r = get(f"{API}/his-wlk-cal", {"$filter": "business_date eq '2024-06-13'"})
    out["his-wlk-cal 2024-06-13"] = r.json()
    r = get(f"{API}/rce-pln", {"$filter": "business_date ge '2026-11-25'"})
    doc = r.json()
    vals = doc.get("value", [])
    out["rce-pln from 2026-11-25"] = {"days": sorted({v["business_date"] for v in vals}), "rows": len(vals),
                                      "nextLink": bool(doc.get("nextLink")), "pubs": sorted({v["publication_ts"] for v in vals})}
    r = get(f"{API}/his-wlk-cal", {"$filter": "business_date ge '2026-11-24'"})
    doc = r.json()
    vals = doc.get("value", [])
    out["his-wlk-cal from 2026-11-24"] = {"days": sorted({v["business_date"] for v in vals}), "rows": len(vals),
                                          "nextLink": bool(doc.get("nextLink")), "pubs": sorted({v["publication_ts"] for v in vals})}
    r = get(f"{API}/rce-pln", {"$filter": "business_date eq '2024-06-14'"})
    out["repeat 2024-06-14 identical bytes"] = {"bytes": len(r.content), "etag": r.headers.get("ETag")}
    r2 = get(f"{API}/rce-pln", {"$filter": "business_date eq '2024-06-14'"})
    out["repeat 2024-06-14 identical bytes"]["equal"] = r.content == r2.content
    r = get(f"{API}/rce-pln", {"$filter": "business_date eq '2025-06-15'"})
    vals = r.json().get("value", [])
    out["rce-pln 2025-06-15 min max negatives"] = [min(v["rce_pln"] for v in vals), max(v["rce_pln"] for v in vals), sum(v["rce_pln"] < 0 for v in vals)]
    return out


def main() -> int:
    for title, call in (("pse probe2 · web app bundles", bundles), ("pse probe2 · API checks", api_checks)):
        try:
            annotate("notice", title, call())
        except Exception as exc:  # each stage reports on its own
            annotate("error", title, f"{type(exc).__name__}: {str(exc)[:1500]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
