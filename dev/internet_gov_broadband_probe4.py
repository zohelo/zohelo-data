"""Read-only Dev probe 4 (PL-ENV-034, #2963): positioned pieces and cell lines (pdf_text.py of this branch) of pages
4-9 of every national PDF, for the table reader. One request at a time, 3 s pauses. Writes nothing to R2."""

from __future__ import annotations

import io
import json
import time

import requests
from pypdf import PdfReader

from ingestion.to_bronze.internet_gov_broadband import pdf_text
from ingestion.to_landing.patterns import bulk_file

UA = bulk_file.USER_AGENT


def main():
    for spec in bulk_file.declaration("internet_gov_broadband"):
        time.sleep(3)
        r = requests.get(spec["url"], headers={"User-Agent": UA}, timeout=(30, 120))
        reader = PdfReader(io.BytesIO(r.content))
        for i, page in enumerate(reader.pages, start=1):
            if 4 <= i <= 9:
                found = pdf_text.pieces(page)
                print(f"PIECES {spec['entity']} {i} " + json.dumps(found, ensure_ascii=False), flush=True)
                print(f"LINES {spec['entity']} {i} " + json.dumps(pdf_text.lines(found), ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
