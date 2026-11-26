"""Read-only Dev probe (#3262): Geofabrik regional taginfo for Poland's voivodeships (robots.txt, region list, one
key's value counts, data date). No R2 writes. Deleted before merge."""

from __future__ import annotations

import json
import re
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def get(url: str) -> tuple[int, str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
        return r.status_code, r.text
    except requests.RequestException as exc:
        return 0, repr(exc)[:300]


def main() -> int:
    out = {}
    for url in [
        "https://taginfo.geofabrik.de/robots.txt",
        "https://taginfo.geofabrik.de/",
        "https://taginfo.openstreetmap.org/robots.txt",
        "https://taginfo.geofabrik.de/europe:poland:mazowieckie/api/4/site/info",
        "https://taginfo.geofabrik.de/europe:poland:mazowieckie/api/4/site/sources",
        "https://taginfo.geofabrik.de/europe:poland:mazowieckie/api/4/key/values?key=amenity&page=1&rp=8&sortname=count&sortorder=desc",
        "https://taginfo.geofabrik.de/europe:poland:mazowieckie/api/4/tag/stats?key=amenity&value=pharmacy",
        "https://taginfo.geofabrik.de/europe:poland/api/4/tag/stats?key=amenity&value=pharmacy",
        "https://download.geofabrik.de/robots.txt",
    ]:
        code, body = get(url)
        if "taginfo.geofabrik.de/" == url.split("//")[1]:
            body = " ".join(sorted(set(re.findall(r'href="/?(europe:poland[^"/]*)', body))))[:1500] or body[:600]
        out[url] = [code, body[:1500]]
        print(json.dumps({url: [code, body[:4000]]}, ensure_ascii=False), flush=True)
        time.sleep(2)
    for url, value in out.items():
        annotate("notice", f"osm taginfo {url.split('//')[1][:70]}", value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
