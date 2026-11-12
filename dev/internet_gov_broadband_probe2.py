"""Read-only Dev probe 2 of internet.gov.pl broadband national PDFs (PL-ENV-034, #2963): text fragments with
positions (pypdf visitor) and the content-stream operators of the table pages, plus plain and layout text of the
editions probe 1 did not read. One request at a time, 3 s pauses. Writes nothing to R2. Deleted before merge."""

from __future__ import annotations

import io
import json
import time

import requests
from pypdf import PdfReader
from pypdf.generic import ContentStream

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
A = "https://www.gov.pl/attachment/"
EDITIONS = {  # quarter: national attachment (release pages read by probe 1, run 43323654663)
    "2024Q2": "defdeee4-dd1b-4344-8711-5574959d047b",
    "2024Q3": "b54d4888-10f7-4b1d-a02f-6693941fca97",
    "2024Q4": "0db69ba7-f72c-4bfb-b328-0dd1123de6e9",
    "2025Q1": "3b6faa68-9d91-4eec-ac7a-10ff8c28f868",
    "2025Q2": "0bfe442d-a9a3-4e44-bb71-fdbe4b0e467a",
    "2025Q3": "b8a7745e-08a4-4488-9f13-21946d3ad6ee",
    "2025Q4": "3e992fab-b68c-4fd1-9aba-88e677d118cb",
    "2026Q1": "8a50664d-fd73-4139-9797-d3e391dfa165",
    "2026Q2": "9448f5fa-2f9d-43bc-ae1a-03c5ac1487fb",
    "2026Q3": "b5fbb888-d29f-4c4d-9b0c-0b4b20e66f5d",
}
NEW = {"2024Q4", "2025Q1", "2025Q3"}
OPS = {"2024Q2", "2026Q3"}


def main():
    for quarter, uuid in EDITIONS.items():
        time.sleep(3)
        r = requests.get(A + uuid, headers={"User-Agent": UA}, timeout=(30, 120))
        print(f"EDITION {quarter} {r.status_code} {len(r.content)} {r.headers.get('Last-Modified')} {r.headers.get('Content-Type')}", flush=True)
        reader = PdfReader(io.BytesIO(r.content))
        print(f"META {quarter} pages={len(reader.pages)} {reader.metadata}", flush=True)
        for i, page in enumerate(reader.pages, start=1):
            if quarter in NEW:
                print(f"=== {quarter} PAGE {i} PLAIN ===\n{page.extract_text()}", flush=True)
                print(f"=== {quarter} PAGE {i} LAYOUT ===\n{page.extract_text(extraction_mode='layout')}", flush=True)
            if not 5 <= i <= 9:
                continue
            frags = []

            def visit(text, cm, tm, font, size, frags=frags):
                if text.strip():
                    x = tm[4] * cm[0] + tm[5] * cm[2] + cm[4]
                    y = tm[4] * cm[1] + tm[5] * cm[3] + cm[5]
                    frags.append([round(x, 2), round(y, 2), text, round(size, 2), round(tm[0] * cm[0], 3)])

            page.extract_text(visitor_text=visit)
            print(f"FRAGS {quarter} {i} " + json.dumps(frags, ensure_ascii=False), flush=True)
            if quarter in OPS and i in (5, 6):
                ops = ContentStream(page.get_contents(), reader).operations
                out = []
                for operands, op in ops[:1500]:
                    if op in (b"Tj", b"TJ", b"Td", b"TD", b"Tm", b"Tf", b"cm", b"BT", b"ET", b"'", b'"', b"T*", b"Tc", b"Tw", b"Tz"):
                        out.append([op.decode(), repr(operands)[:160]])
                print(f"OPS {quarter} {i} " + json.dumps(out, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
