"""Read-only probe of the CEPiK statistics pages on gov.pl (#2966): the topic and year pages, their attachments,
and a cell dump of every XLSX workbook (no R2 writes). Deleted before merge."""

from __future__ import annotations

import re
import sys
import time

import requests

from ingestion.common import xlsx_cells
from ingestion.common.gus_publications import _links
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

BASE = "https://www.gov.pl/web/cepik/"
PAUSE = 2.0
MAX_ROWS = 80  # rows printed per sheet


def main(argv) -> int:
    summary = {}
    links = _links(BASE + "statystyki")
    print("STATYSTYKI LINKS")
    for h, t in links:
        print("  ", h, "|", t[:120])
    pages = {
        h
        for h, _t in links
        if "/web/cepik/" in h and re.search(r"(pojazdy|prawa-jazdy|stacje-kontroli)", h)
    }
    for year in range(2015, 2027):
        pages.add(f"{BASE}pojazdy-zarejestrowane-w-{year}-roku")
        pages.add(f"{BASE}prawa-jazdy-wydane-w-{year}-roku")
    summary["pages"] = len(pages)
    workbooks = []
    for page in sorted(pages):
        time.sleep(PAUSE)
        try:
            found = _links(page)
        except Exception as exc:  # noqa: BLE001
            print("PAGE", page, "ERR", str(exc)[:120])
            continue
        att = [(h, t) for h, t in found if "/attachment/" in h]
        print("PAGE", page, "attachments", len(att))
        for h, t in att:
            print("   ATT", h, "|", t[:160])
            if "xls" in t.lower():
                workbooks.append((page, h, t))
    summary["workbooks"] = len(workbooks)
    for page, href, text in workbooks:
        time.sleep(PAUSE)
        r = requests.get(href, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=60)
        print(
            f"\n##### WORKBOOK {page.rsplit('/', 1)[-1]} | {text[:100]} | {r.status_code} {len(r.content)} bytes "
            f"| LM {r.headers.get('Last-Modified')} | CD {r.headers.get('Content-Disposition', '')[:120]}"
        )
        if r.status_code != 200 or r.content[:2] != b"PK":
            print("   not xlsx", r.content[:8])
            continue
        rows: dict = {}
        for c in xlsx_cells.cells(r.content):
            fmt = c["number_format"]
            extra = f"[{c['cell_type']}/{fmt}]" if c["cell_type"] not in ("s", "n") or fmt not in (None, "General") else ""
            rows.setdefault((c["sheet_index"], c["sheet_name"], c["row_number"]), []).append(
                f"{c['cell_ref']}={c['value_text'][:40].replace(chr(10), ' ')}{extra}"
            )
        shown: dict = {}
        for (si, sn, rn), cs in sorted(rows.items()):
            shown[(si, sn)] = shown.get((si, sn), 0) + 1
            if shown[(si, sn)] <= MAX_ROWS:
                print(f"  {si}:{sn}:{rn}: " + " | ".join(cs)[:1500])
        for (si, sn), k in sorted(shown.items()):
            print(f"  SHEET {si}:{sn} rows {k}")
    annotate("notice", "cepik probe", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
