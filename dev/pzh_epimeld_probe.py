"""Read-only probe for PLS-019 pzh_epimeld (Epic #443), 26 Nov 2026.

GET only, one request at a time, 5 s apart, the platform User-Agent. Reads robots.txt, the EPIMELD index and the annual
bulletins Ch_<year>.pdf / Ch_<year>a.pdf from 2006; writes the PDFs to DEV_OUTPUT_DIR (workflow artifact, 7 days) and
prints, per file, size, Last-Modified, page count and the text of the pages that carry the national summary table.
Nothing is written to R2.
"""

from __future__ import annotations

import base64
import io
import json
import os
from pathlib import Path
import re
import time
import zlib

import requests

from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

INDEX = "https://wwwold.pzh.gov.pl/oldpage/epimeld/index_p.html"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
PAUSE = 5.0


def get(url: str) -> requests.Response:
    time.sleep(PAUSE)
    return requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(30, 120))


def main() -> None:
    from pypdf import PdfReader

    OUT.mkdir(parents=True, exist_ok=True)
    robots = get("https://wwwold.pzh.gov.pl/robots.txt")
    print("ROBOTS", robots.status_code, robots.text[:300].replace("\n", " | "), flush=True)
    if robots.status_code == 200 and "disallow" in robots.text.lower():
        print("ROBOTS TEXT", robots.text[:2000], flush=True)
    index = get(INDEX)
    print("INDEX", index.status_code, len(index.content), index.headers.get("Last-Modified"), flush=True)
    links = bulk_file.links_matching(index.text, index.url, r"\.pdf$")
    ch = [u for u in links if re.search(r"/Ch_\d{4}a?\.pdf$", u, re.IGNORECASE)]
    print("PDF_LINKS", len(links), "CH_LINKS", json.dumps(ch), flush=True)
    print("OTHER_SAMPLE", json.dumps([u for u in links if u not in ch][:15]), flush=True)
    summary = []
    for url in ch:
        year = int(re.search(r"Ch_(\d{4})", url).group(1))
        if year < 2006:
            continue
        name = url.rsplit("/", 1)[-1]
        r = get(url)
        row = {"file": name, "http": r.status_code, "bytes": len(r.content), "lm": r.headers.get("Last-Modified")}
        if r.status_code != 200 or not r.content.startswith(b"%PDF-"):
            summary.append(row)
            continue
        (OUT / name).write_bytes(r.content)
        pdf = PdfReader(io.BytesIO(r.content))
        row["pages"] = len(pdf.pages)
        hits = []
        for i, page in enumerate(pdf.pages[:60]):
            text = page.extract_text() or ""
            if re.search(r"zapadalno", text, re.I) and re.search(r"A0[0-9]|Dur brzuszny", text):
                hits.append(i + 1)
        row["table_pages"] = hits[:12]
        blob = {}
        for i in hits[:8]:
            blob[i] = {
                "plain": pdf.pages[i - 1].extract_text() or "",
                "layout": pdf.pages[i - 1].extract_text(extraction_mode="layout") or "",
            }
        enc = base64.b64encode(zlib.compress(json.dumps(blob).encode(), 9)).decode()
        print(f"TEXT {name} {enc}", flush=True)
        summary.append(row)
        print("FILE", json.dumps(row), flush=True)
    annotate("notice", "pzh_epimeld probe", {"robots": robots.status_code, "index": index.status_code,
                                              "ch_links": len(ch), "files": summary[:30]})


if __name__ == "__main__":
    main()
