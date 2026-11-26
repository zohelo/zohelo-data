"""Read-only layout probe of the KG PSP standard tables (#2970): no writes anywhere.

Downloads the standard-tables ZIPs the page lists (one request at a time, pauses), lists every workbook member with
its sheets and size, and prints the cells (rows 1-70, columns 1-30) of every sheet of the years named in the
arguments (default: the newest and the oldest year) to the job log. Annotations: page and member summary.
"""

from __future__ import annotations

import collections
import re
import sys
import time

import requests

from ingestion.common import xlsx_cells
from ingestion.common.report import annotate
from ingestion.to_bronze import kgpsp_interventions as kb
from ingestion.to_landing import kgpsp_interventions as kl

UA = {"User-Agent": "zohelo-data dev probe (open-data research)"}


def sheet_cells(data: bytes):
    if data[:2] == b"PK":
        for c in xlsx_cells.cells(data):
            yield c["sheet_name"], c["row_number"], c["column_number"], c["value_text"]
    else:
        for c in kb._xls_cells(data):
            yield c["sheet_name"], c["row_number"], c["column_number"], c["value_text"]


def main(argv: list[str]) -> int:
    found = kl.discover()
    years = sorted(found)
    dump = set(argv) or {years[0], years[-1]}
    annotate("notice", "kgpsp layout page", {"years": years, "urls": {k: v["url"][-36:] for k, v in found.items()}})
    summary = {}
    for entity in years:
        time.sleep(2)
        t0 = time.monotonic()
        resp = requests.get(found[entity]["url"], headers=UA, timeout=(20, 180))
        resp.raise_for_status()
        members = kb.workbooks(resp.content)
        print(f"\n##### {entity}: {len(resp.content)} bytes, {len(members)} workbooks, {time.monotonic() - t0:.0f}s,"
              f" last-modified {resp.headers.get('Last-Modified')}", flush=True)
        names = []
        for member, data in members:
            sheets = collections.OrderedDict()
            cells = list(sheet_cells(data))
            for s, r, c, _v in cells:
                n = sheets.setdefault(s, [0, 0, 0])
                n[0], n[1], n[2] = max(n[0], r), max(n[1], c), n[2] + 1
            print(f"MEMBER {entity} | {member} | {len(data)} B | " + " ; ".join(f"{s}:{v[0]}x{v[1]}/{v[2]}" for s, v in sheets.items()), flush=True)
            names.append(member.rsplit("/", 1)[-1])
            base = member.rsplit("/", 1)[-1]
            if re.match(r"^(1|10)(\.|\s|_(?![12][_.]))", base):
                for s in sheets:
                    last = sheets[s][0]
                    print(f"=== {entity} :: {member} :: {s}", flush=True)
                    rows = collections.defaultdict(list)
                    for s2, r, c, v in cells:
                        if s2 == s and (r <= 9 or r > last - 14):
                            rows[r].append(f"c{c}={v.replace(chr(10), ' ')[:45]}")
                    for r in sorted(rows):
                        print(f"  r{r}: " + " | ".join(rows[r]), flush=True)
        summary[entity] = {"bytes": len(resp.content), "workbooks": len(members), "first": names[:6]}
    annotate("notice", "kgpsp layout members", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
