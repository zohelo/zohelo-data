"""Read-only Dev probe 3 of internet.gov.pl broadband national PDFs (PL-ENV-034, #2963): pypdf layout-mode debug
output (text groups with x/y positions, bts.json; text show operations, tjs.json) of the table pages 5-8, to fix
the column reading of the 2024 (cairo) editions. One request at a time, 3 s pauses. Writes nothing to R2."""

from __future__ import annotations

import io
import json
from pathlib import Path
import tempfile
import time

import requests
from pypdf import PdfReader

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
A = "https://www.gov.pl/attachment/"
EDITIONS = {"2024Q2": "defdeee4-dd1b-4344-8711-5574959d047b", "2024Q3": "b54d4888-10f7-4b1d-a02f-6693941fca97",
            "2024Q4": "0db69ba7-f72c-4bfb-b328-0dd1123de6e9", "2026Q3": "b5fbb888-d29f-4c4d-9b0c-0b4b20e66f5d"}


def main():
    for quarter, uuid in EDITIONS.items():
        time.sleep(3)
        r = requests.get(A + uuid, headers={"User-Agent": UA}, timeout=(30, 120))
        reader = PdfReader(io.BytesIO(r.content))
        for i in range(5, 9):
            with tempfile.TemporaryDirectory() as d:
                reader.pages[i - 1].extract_text(extraction_mode="layout", layout_mode_debug_path=Path(d))
                bts = json.loads((Path(d) / "bts.json").read_text())
                out = [[round(b["tx"], 2), round(b["ty"], 2), round(b["displaced_tx"], 2), b["text"]] for b in bts]
                print(f"BTS {quarter} {i} " + json.dumps(out, ensure_ascii=False), flush=True)
                if quarter in ("2024Q2", "2026Q3") and i == 5:
                    tjs = json.loads((Path(d) / "tjs.json").read_text())
                    keys = sorted(tjs[0]) if tjs else []
                    print(f"TJKEYS {keys}", flush=True)
                    tj = [[round(t.get("tx", 0), 2), round(t.get("ty", 0), 2), round(t.get("displaced_tx", 0), 2),
                           t.get("txt", t.get("text"))] for t in tjs]
                    print(f"TJS {quarter} {i} " + json.dumps(tj, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()
