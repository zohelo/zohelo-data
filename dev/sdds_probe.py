"""Read-only layout probe (TP-012/TP-013): every cell of the SDDS advance release calendar (Polish copy). Nothing
written to R2. Deleted before merge."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import time

import requests

from ingestion.common import xlsx_cells
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

PAGE = "https://stat.gov.pl/banki-i-bazy-danych/sdds/kalendarz-upowszechniania-danych"


def main(argv):
    spec = {"url_from": {"page": PAGE, "pattern": r"sdds_kalendarz_upowszechniania_danych.*\.xlsx$"}}
    links = bulk_file.page_links(spec)
    time.sleep(2)
    url = links[0]
    r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 120))
    annotate("notice", "sdds probe file", {"links": links, "status": r.status_code, "bytes": len(r.content),
                                            "headers": {k: r.headers.get(k) for k in ("Last-Modified", "ETag", "Content-Type")}})
    cells = list(xlsx_cells.cells(r.content))
    out = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp")) / "cells.json"
    out.write_text(json.dumps(cells, ensure_ascii=False, indent=0))
    by_sheet = {}
    for c in cells:
        print(c["sheet_index"], c["sheet_name"], c["cell_ref"], c["cell_type"], c["number_format"], repr(c["value_text"]), flush=True)
        by_sheet.setdefault(c["sheet_name"], []).append(f'{c["cell_ref"]}={c["value_text"]}'.replace("\n", "/"))
    n = 0
    for name, items in by_sheet.items():
        text = " ; ".join(items)
        for i in range(0, len(text), 3700):
            if n < 9:
                annotate("notice", f"sdds {name[:20]} {i // 3700}", text[i:i + 3700])
            n += 1
    annotate("notice", "sdds formats", sorted({(c["sheet_name"], c["number_format"], c["cell_type"]) for c in cells}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
