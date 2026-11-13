"""Read-only probe of the ARiMR direct-payment reports (register PLS-034): the gov.pl pages list only monthly PDF
reports "Dane wg stanu na <date>" (probe run 43343134150). Downloads a few of them (newest, year ends, the PS WPR
2023-2027 twin) one at a time, and prints the PDF metadata and the text layer (pypdf, layout mode) so the table
layout can be designed. GET only, PAUSE seconds apart; nothing is written to storage."""

from __future__ import annotations

import io
import re
import subprocess
import sys
import time

import requests

from ingestion.common.gus_publications import _links
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

PAGES = {
    "pb": "https://www.gov.pl/web/arimr/platnosci-bezposrednie2",
    "pswpr": "https://www.gov.pl/web/arimr/ps-wpr-platnosci-bezposrednie",
}
PAUSE = 2.0
WANT = {
    "pb": ["30.09.2026", "31.12.2025", "31.12.2024", "31.12.2023", "31.12.2022", "31.12.2019", "31.12.2017"],
    "pswpr": ["30.09.2026", "31.12.2025", "31.05.2026"],
}


def main() -> int:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pypdf==5.4.0"], check=True)
    from pypdf import PdfReader

    for key, page in PAGES.items():
        time.sleep(PAUSE)
        links = [(h, t.replace("​", "")) for h, t in _links(page) if "/attachment/" in h]
        print(f"\n##### {key} {page}: {len(links)} attachments; texts not matching 'Dane wg stanu':")
        for h, t in links:
            if not re.search(r"wg\.? ?stanu na", t):
                print("   ", t, h)
        for date in WANT[key]:
            hit = [(h, t) for h, t in links if date in t]
            if not hit:
                print("no link for", date)
                continue
            h, t = hit[0]
            time.sleep(PAUSE)
            r = requests.get(h, headers={"User-Agent": USER_AGENT}, timeout=120)
            hdr = {k: v for k, v in r.headers.items() if k.lower() in ("content-type", "last-modified", "content-disposition", "etag", "content-length")}
            print(f"\n=== {key} {date} {t!r}\n    {h} {r.status_code} {len(r.content)} bytes {hdr}")
            try:
                reader = PdfReader(io.BytesIO(r.content))
                print("    meta:", dict(reader.metadata or {}), "pages:", len(reader.pages))
                limit = 400 if date in ("30.09.2026", "31.12.2025") else 140
                n = 0
                for i, pg in enumerate(reader.pages, 1):
                    text = pg.extract_text(extraction_mode="layout") or ""
                    print(f"    --- page {i} ({len(text)} chars, mediabox {pg.mediabox})")
                    for line in text.splitlines():
                        if line.strip():
                            n += 1
                            if n <= limit:
                                print("    |" + line.rstrip()[:240])
                    if n > limit:
                        print(f"    ... (cut at {limit} lines)")
                        break
            except Exception as exc:  # noqa: BLE001
                print(f"    pdf ERR {type(exc).__name__}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
