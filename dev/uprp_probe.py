"""Read-only probe (#3265): the UPRP annual-report page and the text layer of the Polish reports (pypdf).

No R2 access, no writes. Prints the PDF links of the page and the plain text layer of the selected reports to the
job log (artifacts are not readable from the session). One request at a time, 3 s apart, the platform's User-Agent.
"""

from __future__ import annotations

import io
import re
import sys
import time

import requests

from ingestion.common.gus_publications import _links
from ingestion.to_landing.patterns import bulk_file

PAGE = "https://uprp.gov.pl/pl/publikacje/raport-roczny-uprp"


def main(argv: list[str]) -> int:
    from pypdf import PdfReader

    full = {int(a) for a in argv if a.isdigit()} or set(range(2016, 2026))
    peek = {2005, 2010, 2013}
    links = _links(PAGE)
    pdfs = [(h, t) for h, t in links if ".pdf" in h.lower()]
    print(f"== {len(links)} links, {len(pdfs)} PDF links", flush=True)
    for h, t in pdfs:
        print(f"LINK | {t} | {h}", flush=True)
    for href, text in pdfs:
        years = [int(y) for y in re.findall(r"(20[0-2]\d)", href + " " + text)]
        english = bool(re.search(r"annual|report|_en|\ben\b|english", (href + " " + text).lower()))
        if not years or english:
            continue
        year = years[0]
        if year not in full and year not in peek:
            continue
        time.sleep(3)
        r = requests.get(href, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(30, 300))
        print(f"== FILE {year} {r.status_code} {len(r.content)} bytes LM={r.headers.get('Last-Modified')} {href}")
        if r.status_code != 200 or not r.content.startswith(b"%PDF"):
            continue
        reader = PdfReader(io.BytesIO(r.content))
        pages = reader.pages if year in full else reader.pages[:25]
        print(f"== PAGES {year} {len(reader.pages)}", flush=True)
        for n, page in enumerate(pages, start=1):
            try:
                txt = page.extract_text() or ""
            except Exception as exc:  # noqa: BLE001 - probe output only
                txt = f"<extract error {exc!r}>"
            for line in txt.splitlines():
                print(f"T{year}|{n}|{line}")
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
