"""Read-only Dev probe (PL-ENV-004 #3100, PL-ENV-005 #3103): GUGiK WFS sheet indexes ("Skorowidze") of ORTO, NMT,
NMPT and the LIDAR measurement data. Per service: GetCapabilities (feature types, paging, output formats), per
feature type: resultType=hits, DescribeFeatureType and a 2-feature GetFeature sample. Writes nothing. One request
at a time, 1 s apart, bounded retries. No imagery, rasters or point clouds are requested."""

from __future__ import annotations

import re
import sys
import time
import xml.etree.ElementTree as ET

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/"
SERVICES = sys.argv[1:] or [
    "ORTO/WFS/Skorowidze",
    "NMT/WFS/Skorowidze",
    "NMPT/WFS/Skorowidze",
    "DanePomiaroweLIDAR/WFS/Skorowidze",
    "LIDAR/WFS/Skorowidze",
]
NOTES = {"left": 9}


def note(title, data, level="notice"):
    print(f"[{title}] {data}", flush=True)
    if NOTES["left"] > 0:
        NOTES["left"] -= 1
        annotate(level, title, data)


def get(url, params, timeout=120):
    for attempt in range(3):
        time.sleep(1)
        try:
            r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=(30, timeout))
            if r.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(int(r.headers.get("Retry-After", 10 * (attempt + 1))))
                continue
            return r
        except requests.RequestException as exc:
            if attempt == 2:
                return exc
            time.sleep(10 * (attempt + 1))
    return None


def local(tag):
    return tag.rsplit("}", 1)[-1]


def main():
    r = get("https://mapy.geoportal.gov.pl/robots.txt", {})
    print("[robots]", getattr(r, "status_code", r), (getattr(r, "text", "") or "")[:600], flush=True)
    for service in SERVICES:
        url = BASE + service
        cap = get(url, {"SERVICE": "WFS", "REQUEST": "GetCapabilities"})
        if not isinstance(cap, requests.Response) or cap.status_code != 200:
            note(f"{service} capabilities", {"http": str(getattr(cap, "status_code", cap))[:200]}, "warning")
            continue
        text = cap.text
        print(f"===== {service} capabilities ({len(text)} chars)\n{text[:6000]}", flush=True)
        try:
            root = ET.fromstring(cap.content)
        except ET.ParseError as exc:
            note(f"{service} capabilities", {"parse": str(exc), "head": text[:300]}, "warning")
            continue
        version = root.attrib.get("version")
        types = []
        for ft in root.iter():
            if local(ft.tag) == "FeatureType":
                name = next((c.text for c in ft if local(c.tag) == "Name"), None)
                title = next((c.text for c in ft if local(c.tag) == "Title"), None)
                crs = next((c.text for c in ft if local(c.tag) in ("DefaultCRS", "DefaultSRS")), None)
                types.append((name, title, crs))
        paging = re.findall(r'ImplementsResultPaging.{0,200}?DefaultValue>(\w+)<', text, re.S)
        count_default = re.findall(r'CountDefault.{0,200}?DefaultValue>(\d+)<', text, re.S)
        formats = sorted(set(re.findall(r"<(?:ows:)?Value>([^<]*(?:json|gml|GML|JSON|csv|CSV)[^<]*)</", text)))[:15]
        summary = {"version": version, "types": len(types), "paging": paging, "count_default": count_default,
                   "formats": formats}
        hits = []
        for name, title, crs in types:
            h = get(url, {"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature", "TYPENAMES": name,
                          "RESULTTYPE": "hits"})
            matched = None
            if isinstance(h, requests.Response):
                m = re.search(r'numberMatched="([^"]+)"', h.text) or re.search(r'numberOfFeatures="([^"]+)"', h.text)
                matched = m.group(1) if m else f"http {h.status_code}: {h.text[:150]}"
            hits.append([name, title, crs, matched])
            print(f"[hits] {service} {name} | {title} | {crs} | {matched}", flush=True)
        summary["hits"] = hits
        note(f"{service} index", summary)
        for name, _title, _crs, _m in ([hits[0], hits[-1]] if len(hits) > 1 else hits):
            d = get(url, {"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "DescribeFeatureType", "TYPENAMES": name})
            print(f"===== {service} {name} describe\n{getattr(d, 'text', d)[:4000]}", flush=True)
            s = get(url, {"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature", "TYPENAMES": name,
                          "COUNT": 2, "STARTINDEX": 0})
            body = getattr(s, "text", str(s))
            print(f"===== {service} {name} sample ({len(body)} chars, "
                  f"{getattr(s, 'headers', {}).get('Content-Type')})\n{body[:7000]}", flush=True)
            for fmt in ("application/json", "csv"):
                j = get(url, {"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature", "TYPENAMES": name,
                              "COUNT": 2, "OUTPUTFORMAT": fmt})
                print(f"===== {service} {name} sample {fmt}: {getattr(j, 'status_code', j)} "
                      f"{getattr(j, 'headers', {}).get('Content-Type')}\n{getattr(j, 'text', '')[:2500]}", flush=True)


if __name__ == "__main__":
    main()
