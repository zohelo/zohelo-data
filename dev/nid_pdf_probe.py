"""Read-only probe, round 6: the text layer of NID's register lists (register PLS-026, #2471-#2473).

Rounds 1-5 (runs 43343138018, 43797381655, 43797698239, 43798051945, 43798254885): the register of immovable
monuments is published by NID as one PDF list per voivodeship on nid.pl/zasoby/rejestr-zabytkow-zasoby/
(<WOJ>-rej_13.08.2026.pdf, 17 files, Warsaw separate); dane.gov.pl times out from Actions; the INSPIRE register
service INSPIRE_IMD is a WMS only (the WFS services cover Monuments of History and UNESCO). This round downloads two
lists (one at a time, 3 s apart), extracts their text with pypdf (installed for this probe only) and prints the
page count, the layout of the first pages and pattern counts (register numbers, dates, headings).
Writes nothing anywhere.
"""

from __future__ import annotations

import io
import re
import subprocess
import sys
import time
from collections import Counter

import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
NOTES: list[str] = []
FILES = ["https://nid.pl/wp-content/uploads/2026/08/OPO-rej_13.08.2026.pdf",
         "https://nid.pl/wp-content/uploads/2026/08/MAZ-War-rej_13.08.2026.pdf"]


def note(msg: str) -> None:
    NOTES.append(msg)
    print("NOTE " + msg, flush=True)


def main() -> None:
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pypdf==6.21.4"], check=True)
        from pypdf import PdfReader

        for url in FILES:
            time.sleep(3)
            r = requests.get(url, headers={"User-Agent": UA}, timeout=(30, 300))
            note(f"{url.rsplit('/', 1)[-1]}: {r.status_code} {len(r.content)} B lm={r.headers.get('Last-Modified')}")
            reader = PdfReader(io.BytesIO(r.content))
            meta = reader.metadata or {}
            note(f"pages={len(reader.pages)} producer={meta.get('/Producer')} creator={meta.get('/Creator')} "
                 f"title={meta.get('/Title')} created={meta.get('/CreationDate')}")
            texts = [p.extract_text() or "" for p in reader.pages]
            full = "\n".join(texts)
            lines = [x for x in full.splitlines() if x.strip()]
            note(f"chars={len(full)} lines={len(lines)} nr_rej={len(re.findall(r'nr rej', full, re.I))} "
                 f"dates={len(re.findall(r'\\b\\d{1,2}\\.\\d{1,2}\\.\\d{4}\\b', full))} "
                 f"powiat_heads={len(re.findall(r'(?m)^\\s*powiat ', full, re.I))} "
                 f"gmina_heads={len(re.findall(r'(?m)^\\s*gm\\.? ', full, re.I))}")
            print("=" * 30, url, flush=True)
            for i in (0, 1, len(texts) // 2):
                print(f"--- page {i + 1} ---\n{texts[i][:3500]}", flush=True)
            shapes = Counter(re.sub(r"\d", "9", re.sub(r"[^\W\d_]", "a", x.strip()))[:24] for x in lines)
            note("line starts: " + "; ".join(f"{k!r}={v}" for k, v in shapes.most_common(25)))
            note("page 2 head: " + texts[1][:1200].replace("\n", " / ") if len(texts) > 1 else "one page")
            # The layout extractor too, for column positions.
            try:
                lay = reader.pages[1].extract_text(extraction_mode="layout")
                print(f"--- page 2 layout ---\n{lay[:4000]}", flush=True)
                note("layout p2: " + lay[:1500].replace("\n", " / "))
            except Exception as exc:  # noqa: BLE001
                note(f"layout failed {type(exc).__name__}")
    finally:
        chunk, out = "", []
        for n in NOTES:
            if len(chunk) + len(n) > 3800:
                out.append(chunk)
                chunk = ""
            chunk += n.replace("\n", " ").replace("%", "%25") + " | "
        if chunk:
            out.append(chunk)
        for i, c in enumerate(out[:10]):
            print(f"::notice title=nid pdf {i + 1}/{len(out)}::{c}", flush=True)


if __name__ == "__main__":
    main()
