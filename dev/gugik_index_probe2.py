"""Read-only Dev probe 2 (PL-ENV-004 #3100, PL-ENV-005 #3103). (1) ORTO index paging: time and bytes of one page of
5,000 and of 10,000 features, the same page twice (stable order?), SORTBY support. (2) NMT/NMPT/LIDAR index routes:
mapy.geoportal.gov.pl service links on the producer's pages, then GetCapabilities of each WFS found and of a few
candidates. Writes nothing; one request at a time, 1 s apart. No imagery, rasters or point clouds."""

from __future__ import annotations

import re
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
ORTO = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze"
PAGES = [
    "https://www.geoportal.gov.pl/pl/dane/numeryczny-model-terenu-nmt/",
    "https://www.geoportal.gov.pl/pl/dane/numeryczny-model-pokrycia-terenu-nmpt/",
    "https://www.geoportal.gov.pl/pl/dane/dane-pomiarowe-lidar-lidar/",
    "https://www.geoportal.gov.pl/pl/dane/ortofotomapa-orto/",
    "https://www.geoportal.gov.pl/pl/usludi-geoportal/",
    "https://www.geoportal.gov.pl/pl/uslugi/usluga-przegladania-wms-i-wmts/",
]
CANDIDATES = [
    "NMT/GRID1/WFS/Skorowidze", "NMT/GRID5/WFS/Skorowidze", "NMT/WFS/SkorowidzeNMT",
    "NMPT/GRID1/WFS/Skorowidze", "DanePomiaroweLIDAR/WFS/Skorowidze", "NumerycznyModelTerenu/WFS/Skorowidze",
    "NMT/WFS/SkorowidzeUkladKRON86", "NMT/WFS/SkorowidzeUkladEVRF2007",
]
OUT: list = []


def get(url, params=None, timeout=300):
    time.sleep(1)
    started = time.monotonic()
    try:
        r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=(30, timeout))
        return r, round(time.monotonic() - started, 1)
    except requests.RequestException as exc:
        return exc, round(time.monotonic() - started, 1)


def ids(text):
    return re.findall(r'gml:id="(SkorowidzOrtofomapy\d{4}\.\d+)"', text)


def main():
    t = "gugik:SkorowidzOrtofomapy2017"
    paging = {}
    for count, start in ((5000, 50000), (5000, 50000), (10000, 100000)):
        r, s = get(ORTO, {"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature", "TYPENAMES": t,
                          "COUNT": count, "STARTINDEX": start})
        found = ids(getattr(r, "text", ""))
        paging.setdefault(f"{count}@{start}", []).append(
            {"http": getattr(r, "status_code", str(r)[:100]), "seconds": s, "bytes": len(getattr(r, "content", b"")),
             "features": len(found), "first": found[:2], "last": found[-1:],
             "returned": re.findall(r'numberReturned="(\d+)"', getattr(r, "text", ""))[:1]})
    r, s = get(ORTO, {"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature", "TYPENAMES": t, "COUNT": 3,
                      "STARTINDEX": 0, "SORTBY": "gugik:godlo ASC"})
    paging["sortby"] = {"http": getattr(r, "status_code", ""), "seconds": s, "ids": ids(getattr(r, "text", "")),
                        "head": getattr(r, "text", "")[:300] if not ids(getattr(r, "text", "")) else ""}
    print("[paging]", paging, flush=True)
    annotate("notice", "orto paging", paging)

    links = set()
    for page in PAGES:
        r, s = get(page, timeout=60)
        text = getattr(r, "text", "")
        found = set(re.findall(r'https?://mapy\.geoportal\.gov\.pl/wss/[^\s"\'<>]+', text))
        found |= set(re.findall(r'https?://[a-z.]*geoportal\.gov\.pl/[^\s"\'<>]*(?:WFS|wfs|Skorowid)[^\s"\'<>]*', text))
        print(f"[page] {page} {getattr(r, 'status_code', r)} {len(text)} {sorted(found)[:60]}", flush=True)
        links |= found
    wfs = sorted({u.split("?")[0] for u in links if "WFS" in u or "wfs" in u})
    services = [u for u in wfs] + [f"https://mapy.geoportal.gov.pl/wss/service/PZGIK/{c}" for c in CANDIDATES]
    caps = []
    for url in services:
        r, s = get(url, {"SERVICE": "WFS", "REQUEST": "GetCapabilities"}, timeout=120)
        text = getattr(r, "text", "")
        names = re.findall(r"<(?:wfs:)?Name>([^<]+)</(?:wfs:)?Name>", text)
        caps.append([url.split("/PZGIK/")[-1], getattr(r, "status_code", str(r)[:80]), len(names), names[:6]])
        print(f"[caps] {url} {getattr(r, 'status_code', r)} {names}", flush=True)
        if names:
            print(text[:3000], flush=True)
    annotate("notice", "gugik elevation index routes", {"wfs_links": wfs[:30], "capabilities": caps})


if __name__ == "__main__":
    main()
