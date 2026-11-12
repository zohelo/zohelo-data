"""Read-only Dev probe 3 of internet.gov.pl broadband national PDFs (PL-ENV-034, #2963): pypdf layout-mode debug
output (text groups with x/y positions, bts.json; text show operations, tjs.json) of the table pages 5-8, to fix
the column reading of the 2024 (cairo) editions. One request at a time, 3 s pauses. Writes nothing to R2."""

from __future__ import annotations

import io
import json
import time

import requests
from pypdf import PdfReader
from pypdf._text_extraction import _layout_mode
from pypdf.generic import ContentStream

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
            page = reader.pages[i - 1]
            fonts = page._layout_mode_fonts()
            ops = iter(ContentStream(page["/Contents"].get_object(), reader, "bytes").operations)
            bts = _layout_mode.text_show_operations(ops, fonts, True, None)
            out = [[round(b["tx"], 2), round(b["ty"], 2), round(b["displaced_tx"], 2), round(b["font_height"], 2),
                    b["text"]] for b in bts]
            print(f"BTS {quarter} {i} " + json.dumps(out, ensure_ascii=False), flush=True)

if __name__ == "__main__":
    main()
