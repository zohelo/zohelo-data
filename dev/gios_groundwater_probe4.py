"""Read-only probe 4 (PL-ENV-012, #2870): full headers and value profile of the 12 result files of dane.gov.pl
dataset 1405 (GIOŚ groundwater monitoring results 2019-2024). No R2, no writes; requests 3 s apart."""

from __future__ import annotations

from collections import Counter, defaultdict
import csv
import io
import json
import re
import time

import requests

from ingestion.common import xlsx_cells

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
API = "https://api.dane.gov.pl/1.4/datasets/1405/resources?per_page=100&page=1"
ATTRS = ("Województwo", "Powiat", "Typ ośrodka", "Zwierciadło", "Rodzaj punktu", "Nazwa dorzecza", "Rok badań",
         "Użytkowanie", "RZGW", "Stratygrafia")


def get(url):
    time.sleep(3)
    return requests.get(url, headers={"User-Agent": UA}, timeout=(30, 300))


def table(body: bytes, fmt: str) -> list[list[str]]:
    if fmt == "csv":
        text = body.decode("utf-8-sig")
        dialect = csv.Sniffer().sniff(text[:5000], delimiters=",;\t")
        print("  csv dialect:", repr(dialect.delimiter), repr(dialect.quotechar), "first lines:",
              json.dumps(text.splitlines()[1:3], ensure_ascii=False)[:1500], flush=True)
        return [row for row in csv.reader(io.StringIO(text), dialect)]
    grid: dict[int, dict[int, str]] = defaultdict(dict)
    kinds = Counter()
    sheets = set()
    for c in xlsx_cells.cells(body):
        sheets.add(c["sheet_name"])
        grid[c["row_number"]][c["column_number"]] = c["value_text"]
        if c["column_number"] in (27, 30, 31):
            kinds[(c["column_number"], c["cell_type"], c["number_format"])] += 1
    print("  sheets:", sorted(sheets), "date-ish column cell kinds:", kinds.most_common(8), flush=True)
    width = max(max(r) for r in grid.values())
    return [[grid[r].get(c, "") for c in range(1, width + 1)] for r in sorted(grid)]


def main() -> int:
    items = get(API).json()["data"]
    for it in sorted(items, key=lambda i: i["attributes"]["title"]):
        a = it["attributes"]
        fmt = (a.get("format") or "").lower()
        if fmt not in ("csv", "xlsx"):
            continue
        r = get(a["file_url"])
        print("=" * 100)
        print(json.dumps({"resource": it["id"], "title": a["title"], "bytes": len(r.content)}, ensure_ascii=False))
        rows = table(r.content, fmt)
        head = rows[0]
        print("  header:", json.dumps(list(enumerate(head, 1)), ensure_ascii=False), flush=True)
        body = [row for row in rows[1:] if any(v.strip() for v in row)]
        print("  data rows:", len(body), "blank rows:", len(rows) - 1 - len(body), "ragged:",
              Counter(len(x) for x in rows).most_common(3), flush=True)
        idx = {h: i for i, h in enumerate(head)}
        date_col = next(i for i, h in enumerate(head) if h.startswith("Data poboru"))
        monbada = next(i for i, h in enumerate(head) if "MONBADA" in h)
        first_ind = date_col + 1
        keys = Counter((row[monbada].strip(), row[date_col].strip()) for row in body)
        coords = defaultdict(set)
        for row in body:
            coords[row[monbada].strip()].add((row[idx[[h for h in head if h.startswith('PUWG 1992 X')][0]]],
                                              row[idx[[h for h in head if 'Województwo' in h][0]]]))
        print("  points:", len(coords), "empty MONBADA rows:", sum(1 for row in body if not row[monbada].strip()),
              "dup (point,date):", sum(1 for v in keys.values() if v > 1), [k for k, v in keys.items() if v > 1][:5],
              "points with >1 coord/voiv:", sum(1 for v in coords.values() if len(v) > 1), flush=True)
        print("  dates sample:", [row[date_col] for row in body[:3]], "years:",
              Counter(row[idx[[h for h in head if h.startswith('Rok')][0]]] for row in body).most_common(4), flush=True)
        for h in head:
            if h.startswith(ATTRS):
                vals = Counter(row[idx[h]].strip() for row in body)
                print(f"  attr {h[:40]!r}: {len(vals)} distinct", json.dumps(vals.most_common(20), ensure_ascii=False)[:900],
                      flush=True)
        shapes, texts, empties = Counter(), Counter(), 0
        for row in body:
            for v in row[first_ind:]:
                s = v.strip()
                if not s:
                    empties += 1
                    continue
                if re.fullmatch(r"-?[0-9]+([.,][0-9]+)?([eE][-+]?[0-9]+)?", s):
                    shapes["number"] += 1
                    continue
                shape = re.sub(r"[0-9]+", "N", s)
                shapes[shape] += 1
                if not s.startswith("<"):
                    texts[s] += 1
        print("  indicator cells: empty", empties, "shapes", json.dumps(shapes.most_common(15), ensure_ascii=False),
              flush=True)
        print("  indicator texts (not '<'):", json.dumps(texts.most_common(30), ensure_ascii=False), flush=True)
        nitr = [i for i, h in enumerate(head) if h.lower().startswith("azotany")]
        if nitr:
            vals = [row[nitr[0]].strip() for row in body]
            nums = [float(v.replace(",", ".")) for v in vals if re.fullmatch(r"[0-9]+([.,][0-9]+)?([eE][-+]?[0-9]+)?", v)]
            print("  nitrate:", head[nitr[0]], "cells", len(vals), "numeric", len(nums), "max", max(nums) if nums else None,
                  "above 50:", sum(1 for x in nums if x > 50), "texts", Counter(v for v in vals if v and v not in
                  [str(x) for x in []] and not re.fullmatch(r"[0-9.,eE+-]+", v)).most_common(8), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
