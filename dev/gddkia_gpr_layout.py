"""Read-only layout probe of the GDDKiA traffic census (GPR, register PL-ENV-022) spreadsheets on gov.pl.

Prints every attachment link of the two edition pages, then downloads only the spreadsheet attachments (XLSX / XLS),
one request at a time 2 s apart, and prints each workbook's layout to the job log: sheets and sizes, the first rows
in full, a few rows from the middle and the end, and per column the count of numeric cells and the most frequent
non-numeric strings below the header. Writes nothing to R2. Deleted before merge.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import re
import time

import requests

from ingestion.common import xlsx_cells
from ingestion.common.gus_publications import _links
from ingestion.to_landing.patterns import bulk_file

PAGES = {
    "gpr_2025": "https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-2025",
    "gpr_2020": "https://www.gov.pl/web/gddkia/generalny-pomiar-ruchu-20202021",
}
SHEET = re.compile(r"\bxlsx?\b", re.IGNORECASE)
NUM = re.compile(r"^-?\d+([.,]\d+)?([eE][-+]?\d+)?$")


def grid(body: bytes) -> dict[str, dict[tuple[int, int], str]]:
    out: dict[str, dict[tuple[int, int], str]] = defaultdict(dict)
    if body[:2] == b"PK":
        for c in xlsx_cells.cells(body):
            out[c["sheet_name"]][(c["row_number"], c["column_number"])] = c["value_text"]
        return out
    import xlrd

    book = xlrd.open_workbook(file_contents=body)
    for sheet in book.sheets():
        for r in range(sheet.nrows):
            for c in range(sheet.ncols):
                v = sheet.cell_value(r, c)
                if v in ("", None):
                    continue
                if isinstance(v, float):
                    v = str(int(v)) if v.is_integer() else repr(v)
                out[sheet.name][(r + 1, c + 1)] = str(v)
    return out


def row_text(cells: dict[tuple[int, int], str], r: int, ncol: int) -> str:
    parts = [f"{c}:{cells[(r, c)][:40]!r}" for c in range(1, ncol + 1) if (r, c) in cells]
    return " | ".join(parts)


def dump(name: str, body: bytes) -> None:
    for sheet, cells in grid(body).items():
        if not cells:
            continue
        nrow = max(r for r, _ in cells)
        ncol = max(c for _, c in cells)
        print(f"--- {name} sheet={sheet!r} rows={nrow} cols={ncol} cells={len(cells)}", flush=True)
        rows = sorted({r for r, _ in cells})
        show = rows[:14] + rows[len(rows) // 2 : len(rows) // 2 + 2] + rows[-3:]
        for r in dict.fromkeys(show):
            print(f"  r{r}: {row_text(cells, r, ncol)}")
        body_rows = set(rows[8:])
        for c in range(1, ncol + 1):
            vals = [cells[(r, c)] for r in body_rows if (r, c) in cells]
            nums = sum(1 for v in vals if NUM.match(v.strip()))
            text = Counter(v.strip()[:30] for v in vals if not NUM.match(v.strip()))
            print(f"  col{c}: filled={len(vals)} numeric={nums} distinct_text={len(text)} top={text.most_common(6)}")


def main() -> int:
    for edition, page in PAGES.items():
        time.sleep(2)
        links = [(h, t) for h, t in _links(page) if "/attachment/" in h]
        print(f"===== {edition}: {len(links)} attachments", flush=True)
        for href, text in links:
            print(f"  {href} :: {text[:150]}")
        for n, (href, text) in enumerate(links):
            if not SHEET.search(text) or "pdf" in text.lower().split()[-3:]:
                continue
            time.sleep(2)
            resp = requests.get(href, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=120)
            body = resp.content
            name = f"{edition}_{n:02d}"
            print(
                f"\n##### {name} status={resp.status_code} bytes={len(body)} lm={resp.headers.get('Last-Modified')}"
                f" ct={resp.headers.get('Content-Type')} cd={resp.headers.get('Content-Disposition')} text={text!r}",
                flush=True,
            )
            try:
                dump(name, body)
            except Exception as exc:  # a probe: report and go on
                print(f"  cannot read: {type(exc).__name__}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
