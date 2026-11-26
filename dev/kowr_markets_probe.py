"""Read-only probe for KOWR weekly market reports (register PLS-033, Epic #457), 26 Nov 2026.

Lists the attachments of the three gov.pl pages and prints the text layer of a few weekly PDFs (pypdf) so the Bronze
parser can be designed: newest, a 2025 and the oldest grain report, the newest meat report, the newest combined report.
GET only, one request at a time, 5 s between attachment requests, the platform User-Agent. Writes nothing to R2.
"""

from __future__ import annotations

import io
import json
import re
import time

import requests

from ingestion.common.gus_publications import _links
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

PAGES = {
    "zboza": "https://www.gov.pl/web/kowr/Rynek-zboz",
    "mieso": "https://www.gov.pl/web/kowr/Rynek-miesa",
    "tygodniowe": "https://www.gov.pl/web/kowr/raporty-tygodniowe",
}


def notice(title: str, data: object) -> None:
    msg = json.dumps(data, ensure_ascii=False, default=str)[:3800]
    print(f"::notice title={title}::{msg.replace('%', '%25').replace(chr(10), '%0A')}", flush=True)


def fetch(url: str) -> requests.Response:
    time.sleep(5)
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    r.raise_for_status()
    return r


def dump(label: str, url: str) -> None:
    from pypdf import PdfReader

    r = fetch(url)
    reader = PdfReader(io.BytesIO(r.content))
    print(f"##### PDF {label} {url} bytes={len(r.content)} cd={r.headers.get('content-disposition')!r} "
          f"lm={r.headers.get('last-modified')!r} etag={r.headers.get('etag')!r} pages={len(reader.pages)}", flush=True)
    for i, page in enumerate(reader.pages):
        print(f"----- page {i + 1} (plain)", flush=True)
        print(page.extract_text(), flush=True)
    for i, page in enumerate(reader.pages):
        print(f"----- page {i + 1} (layout)", flush=True)
        try:
            print(page.extract_text(extraction_mode="layout"), flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"layout failed: {exc}", flush=True)
    notice(f"pdf {label}", {"url": url, "bytes": len(r.content), "pages": len(reader.pages),
                            "cd": r.headers.get("content-disposition")})


def main() -> None:
    lists = {}
    for key, page in PAGES.items():
        time.sleep(2)
        atts = [(h, t.replace("​", "")) for h, t in _links(page) if "/attachment/" in h]
        lists[key] = atts
        print(f"##### LIST {key} {len(atts)}", flush=True)
        for h, t in atts:
            print(f"  {h} | {t}", flush=True)
        names = [t for _, t in atts]
        notice(f"list {key}", {"n": len(atts), "first": names[:3], "last": names[-3:],
                               "years": sorted({m for t in names for m in re.findall(r"20\d\d", t)})})
    grain = lists["zboza"]
    picks = [("grain_newest", grain[0][0]), ("grain_last", grain[-1][0])]
    mid = [h for h, t in grain if ".2025" in t]
    if mid:
        picks.insert(1, ("grain_2025", mid[len(mid) // 2]))
    picks.append(("meat_newest", lists["mieso"][0][0]))
    weekly = [(h, t) for h, t in lists["tygodniowe"]]
    for h, t in weekly:
        if "zbó" in t.lower() or "zbo" in t.lower():
            picks.append(("weekly_grain", h))
            break
    for label, url in picks:
        try:
            dump(label, url)
        except Exception as exc:  # noqa: BLE001
            notice(f"pdf {label} failed", {"url": url, "error": str(exc)[:300]})


if __name__ == "__main__":
    main()
