"""Read-only probe of the ZUS yearbook PDFs (PLS-010). Downloads each edition once (GET, one at a time, pauses), records
size, Last-Modified, page count and text-layer characters, and keeps the PDFs and their text (pypdf layout mode) in
$DEV_OUTPUT_DIR for the run artifact. No R2 writes. Deleted before merge."""

from __future__ import annotations

import io
import os
from pathlib import Path
import sys
import time

import pypdf
import requests

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
BASE = "https://www.zus.pl/documents/10182/167642/"
FILES = {
    "rocznik_2021_2023": "0600_25+Rocznik+statystyczny+2021_2023.pdf/dbae306d-f987-0359-ed4d-80a4bca948bf",
    "rocznik_2018_2020": "Rocznik+statystyczny+ubezpiecze%C5%84+spo%C5%82ecznych+2018-2020.pdf/166ed10a-1986-4d4f-1531-3dde9a6bd8fe",
    "rocznik_2015_2017": "Rocznik+statystyczny+2015_2017.pdf/b1c94916-3012-c38e-fff8-abf0dfec9242",
    "rocznik_2012_2014": "Rocznik+statystyczny+2012_2014.pdf/04c53d63-f65d-4ef9-9588-62e0e1c1b1ec",
    "rocznik_2009_2011": "Rocznik+Statystyczny+Ubezpiecze%C5%84+Spo%C5%82ecznych+2009-2011.pdf/7e6fcc34-8333-4af0-b3e6-9e64ea734600",
    "rocznik_2006_2008": "Rocznik_statystyczny_2006-2008.pdf/888b05ab-fa1f-44a3-8b17-ef57c681f886",
    "rocznik_2003_2005": "Rocznik+Statystyczny2003-2005.pdf/e462ab97-ac82-4a56-a51c-eafef686d72b",
    "rocznik_1999_2002": "Rocznik+stat+system+pozarolniczy+1999+2002.pdf/d36c56e4-94e3-4bd2-a643-7c29b831de1f",
}


def dane() -> None:
    """dane.gov.pl: ZUS datasets that may hold the yearbook tables as XLSX/CSV (one request at a time)."""
    for url in ("https://api.dane.gov.pl/1.4/search?q=rocznik%20statystyczny%20ubezpiecze%C5%84%20spo%C5%82ecznych&per_page=20",
                "https://api.dane.gov.pl/1.4/datasets?q=ZUS%20rocznik&per_page=20",
                "https://api.dane.gov.pl/1.4/datasets?q=Zak%C5%82ad%20Ubezpiecze%C5%84%20Spo%C5%82ecznych&per_page=50"):
        try:
            r = requests.get(url, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=(90, 120))
            data = r.json()
            print("dane", r.status_code, url, "count", data.get("meta", {}).get("count"), flush=True)
            for item in data.get("data", []):
                a = item.get("attributes", {})
                print("  ", item.get("type"), item.get("id"), (a.get("title") or "")[:110], "|",
                      (a.get("formats") or a.get("format") or ""), flush=True)
        except Exception as exc:  # noqa: BLE001 - probe continues
            print("dane error", url, type(exc).__name__, str(exc)[:160], flush=True)
        time.sleep(3)


def main(argv) -> int:
    dane()
    out = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
    out.mkdir(parents=True, exist_ok=True)
    for n, (entity, path) in enumerate(FILES.items()):
        if n:
            time.sleep(5)
        r = None
        for attempt in range(3):
            try:
                r = requests.get(BASE + path, headers={"User-Agent": UA}, timeout=(30, 120))
                break
            except requests.RequestException as exc:
                print(entity, "error", type(exc).__name__, str(exc)[:150], flush=True)
                time.sleep(20 * (attempt + 1))
        if r is None:
            continue
        body = r.content
        print(f"{entity} status={r.status_code} bytes={len(body)} type={r.headers.get('Content-Type')} "
              f"lm={r.headers.get('Last-Modified')} etag={r.headers.get('ETag')} magic={body[:5]!r}", flush=True)
        if r.status_code != 200 or not body.startswith(b"%PDF"):
            continue
        (out / f"{entity}.pdf").write_bytes(body)
        try:
            reader = pypdf.PdfReader(io.BytesIO(body))
            pages = len(reader.pages)
            chars, empty = 0, 0
            with open(out / f"{entity}.txt", "w", encoding="utf-8") as f:
                for i, page in enumerate(reader.pages, start=1):
                    t0 = time.monotonic()
                    try:
                        text = page.extract_text(extraction_mode="layout")
                    except Exception as exc:  # noqa: BLE001 - probe continues
                        text = f"<<error {type(exc).__name__}: {exc}>>"
                    chars += len(text.strip())
                    empty += not text.strip()
                    f.write(f"\n=================== PAGE {i} ({time.monotonic() - t0:.2f}s)\n{text}\n")
            print(f"  pages={pages} text_chars={chars} empty_pages={empty} meta={reader.metadata}", flush=True)
        except Exception as exc:  # noqa: BLE001 - probe continues
            print("  pypdf error", type(exc).__name__, exc, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
