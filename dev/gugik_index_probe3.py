"""Read-only Dev probe 3 (PL-ENV-004 #3100, PL-ENV-005 #3103). (1) ORTO: is paging stable with SORTBY on a unique
property (url_do_pobrania)? The same page twice, and the sort order of the ids. (2) Elevation indexes (NMT, NMPT,
LIDAR measurement data; EVRF2007 and KRON86 services): hits of every feature type, DescribeFeatureType and a
2-feature sample of the newest and the oldest type. Writes nothing; one request at a time, 1 s apart."""

from __future__ import annotations

import re
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/"
ORTO = BASE + "ORTO/WFS/Skorowidze"
ELEVATION = [
    "NumerycznyModelTerenuEVRF2007", "NumerycznyModelTerenuKRON86",
    "NumerycznyModelPokryciaTerenuEVRF2007", "NumerycznyModelPokryciaTerenuKRON86",
    "DanePomiaroweLidarEVRF2007", "DanePomiaroweLidarKRON86",
]


def get(url, params=None, timeout=300):
    time.sleep(1)
    started = time.monotonic()
    try:
        r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=(30, timeout))
        return r, round(time.monotonic() - started, 1)
    except requests.RequestException as exc:
        return exc, round(time.monotonic() - started, 1)


def q(t, **extra):
    return {"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature", "TYPENAMES": t, **extra}


def main():
    t = "gugik:SkorowidzOrtofomapy2017"
    out = {}
    for label, sort in (("url", "gugik:url_do_pobrania ASC"), ("url", "gugik:url_do_pobrania ASC")):
        r, s = get(ORTO, q(t, COUNT=5000, STARTINDEX=50000, SORTBY=sort))
        text = getattr(r, "text", "")
        urls = re.findall(r"<gugik:url_do_pobrania>([^<]*)<", text)
        ids = re.findall(r'gml:id="SkorowidzOrtofomapy2017\.(\d+)"', text)
        out.setdefault(label, []).append({
            "http": getattr(r, "status_code", str(r)[:100]), "seconds": s, "features": len(ids),
            "first_ids": ids[:3], "last_id": ids[-1:], "sorted": urls == sorted(urls), "distinct_urls": len(set(urls)),
            "head": text[:200] if not ids else ""})
    print("[orto sort]", out, flush=True)
    annotate("notice", "orto sortby paging", out)

    for service in ELEVATION:
        url = BASE + service + "/WFS/Skorowidze"
        r, _ = get(url, {"SERVICE": "WFS", "REQUEST": "GetCapabilities"})
        names = re.findall(r"<(?:wfs:)?Name>([^<]+)</(?:wfs:)?Name>", getattr(r, "text", ""))
        hits = []
        for name in names:
            h, s = get(url, q(name, RESULTTYPE="hits"))
            m = re.search(r'numberMatched="([^"]+)"', getattr(h, "text", ""))
            hits.append([name.split(":")[-1], m.group(1) if m else str(getattr(h, "status_code", h))[:60], s])
        print(f"[hits] {service} {hits}", flush=True)
        annotate("notice", f"{service} hits", {"types": len(names), "hits": hits})
        for name in names[-1:] + names[:1]:
            d, _ = get(url, {"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "DescribeFeatureType", "TYPENAMES": name})
            fields = re.findall(r'<element name="([^"]+)"[^>]*type="([^"]+)"', getattr(d, "text", ""))
            print(f"===== {service} {name} fields {fields}", flush=True)
            sm, _ = get(url, q(name, COUNT=2))
            print(f"===== {service} {name} sample\n{getattr(sm, 'text', '')[:5000]}", flush=True)
            st, s2 = get(url, q(name, COUNT=3, SORTBY="gugik:url_do_pobrania ASC"))
            print(f"===== {service} {name} sortby url: {getattr(st, 'status_code', st)} {s2}s "
                  f"{re.findall(r'gml:id=.([A-Za-z0-9_.]+).', getattr(st, 'text', ''))[:6]} "
                  f"{getattr(st, 'text', '')[:200] if 'Exception' in getattr(st, 'text', '') else ''}", flush=True)


if __name__ == "__main__":
    main()
