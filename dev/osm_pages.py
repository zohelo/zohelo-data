"""Read-only Dev probe (#3262): the text of the OpenStreetMap France download pages (terms, usage) and of the
Geofabrik answers seen from a runner. No downloads of extracts, no R2 writes. Deleted before merge."""

from __future__ import annotations

import html
import json
import re
import time

import requests

from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def text(url: str) -> tuple[int, str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    except requests.RequestException as exc:
        return 0, repr(exc)[:300]
    page = re.sub(r"<script.*?</script>|<style.*?</style>", " ", r.text, flags=re.S | re.I)
    return r.status_code, re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", page))).strip()


def main() -> int:
    out = {}
    for url in [
        "https://download.openstreetmap.fr/",
        "https://download.openstreetmap.fr/extracts/",
        "https://download.openstreetmap.fr/extracts/europe/poland/",
        "https://download.openstreetmap.fr/robots.txt",
        "https://osm-internal.download.geofabrik.de/",
        "https://www.geofabrik.de/data/download.html",
        "https://www.openstreetmap.fr/telechargement/",
    ]:
        code, body = text(url)
        out[url] = [code, body[:1500]]
        print(json.dumps({url: [code, body[:6000]]}, ensure_ascii=False), flush=True)
        time.sleep(2)
    for url, (code, body) in out.items():
        annotate("notice", f"osm pages {url.split('//')[1][:60]}", {"http": code, "text": body[:1400]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
