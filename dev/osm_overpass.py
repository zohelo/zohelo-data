"""Read-only Dev probe (#3262): Geofabrik reachability again, Overpass API terms and status, and one count query of
the planned feature list for the smallest and the largest voivodeship (timing, answer shape). No R2 writes.
Deleted before merge."""

from __future__ import annotations

import html
import json
import re
import sys
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://overpass-api.de/api"
FEATURES = (
    [("amenity", None), ("building", None), ("shop", None), ("tourism", None), ("leisure", None), ("office", None),
     ("craft", None), ("healthcare", None)]
    + [("amenity", v) for v in ("school", "kindergarten", "university", "college", "hospital", "clinic", "doctors",
                                "dentist", "pharmacy", "police", "fire_station", "post_office", "townhall", "library",
                                "place_of_worship", "restaurant", "cafe", "fast_food", "bar", "pub", "fuel",
                                "charging_station", "bank", "atm", "parking", "bicycle_parking", "marketplace",
                                "community_centre", "social_facility", "toilets")]
    + [("shop", v) for v in ("supermarket", "convenience", "bakery", "butcher", "clothes", "hairdresser", "car_repair",
                             "kiosk")]
    + [("tourism", v) for v in ("hotel", "guest_house", "museum")]
    + [("leisure", v) for v in ("playground", "park", "sports_centre", "pitch", "swimming_pool")]
    + [("highway", v) for v in ("bus_stop", "traffic_signals")]
    + [("railway", v) for v in ("station", "halt")]
    + [("emergency", "defibrillator"), ("public_transport", "station")]
)


def text(url: str) -> tuple[int, str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    except requests.RequestException as exc:
        return 0, repr(exc)[:300]
    page = re.sub(r"<script.*?</script>|<style.*?</style>", " ", r.text, flags=re.S | re.I)
    return r.status_code, re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", page))).strip()


def query(terc: str) -> str:
    parts = [f'[out:json][timeout:1800];area["boundary"="administrative"]["admin_level"="4"]["teryt:terc"="{terc}"]->.a;']
    for key, value in FEATURES:
        cond = f'["{key}"="{value}"]' if value else f'["{key}"]["{key}"!="no"]'
        parts.append(f"nwr{cond}(area.a);out count;")
    return "".join(parts)


def main(argv) -> int:
    pages = {}
    for url in ["https://download.geofabrik.de/robots.txt", "https://download.geofabrik.de/europe/poland.html",
                "https://overpass-api.de/", "https://overpass-api.de/robots.txt", f"{API}/status",
                "https://dev.overpass-api.de/overpass-doc/en/preface/commons.html",
                "https://wiki.openstreetmap.org/wiki/Overpass_API"]:
        code, body = text(url)
        keep = body[:1500]
        if "wiki" in url or "commons" in url:
            keep = " | ".join(body[max(0, m.start() - 300): m.start() + 600] for m in
                              re.finditer(r"10,000|10 000|1 GB|usage policy|heavy", body))[:2500]
        pages[url] = [code, keep]
        print(json.dumps({url: [code, keep]}, ensure_ascii=False), flush=True)
        time.sleep(2)
    for url, value in pages.items():
        annotate("notice", f"osm overpass page {url.split('//')[1][:50]}", value)
    q = '[out:json][timeout:120];area["boundary"="administrative"]["admin_level"="4"]["teryt:terc"];out tags;'
    r = requests.post(f"{API}/interpreter", data={"data": q}, headers={"User-Agent": UA}, timeout=300)
    areas = [(e["tags"].get("teryt:terc"), e["tags"].get("name"), e["tags"].get("ISO3166-2"), e["id"])
             for e in r.json().get("elements", [])] if r.status_code == 200 else r.text[:500]
    annotate("notice", "osm overpass areas", {"http": r.status_code, "areas": areas, "osm3s": r.json().get("osm3s") if r.status_code == 200 else None})
    print(json.dumps(areas, ensure_ascii=False), flush=True)
    for terc in argv or ["16", "14"]:
        time.sleep(10)
        code, status = text(f"{API}/status")
        t = time.monotonic()
        r = requests.post(f"{API}/interpreter", data={"data": query(terc)}, headers={"User-Agent": UA}, timeout=(30, 1900))
        seconds = round(time.monotonic() - t, 1)
        res: dict = {"http": r.status_code, "seconds": seconds, "bytes": len(r.content), "status_before": status[:300]}
        if r.status_code == 200:
            body = r.json()
            counts = [e.get("tags") for e in body.get("elements", [])]
            res.update({"osm3s": body.get("osm3s"), "remark": body.get("remark"), "counts": len(counts),
                        "values": [[f"{k}:{v or '_T'}", c.get("total"), c.get("nodes"), c.get("ways"), c.get("relations")]
                                   for (k, v), c in zip(FEATURES, counts, strict=False)]})
        else:
            res["body"] = r.text[:800]
        print(json.dumps({terc: res}, ensure_ascii=False), flush=True)
        annotate("notice", f"osm overpass counts {terc}", res)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
