"""Dev probe 2 (read-only): full labels of the MF and KRUS workbooks; older budget year pages."""

from __future__ import annotations

from collections import defaultdict
import re

import requests

from ingestion.common import xlsx_cells
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

H = {"User-Agent": USER_AGENT}
LINK = re.compile(r'<a[^>]+href="([^"]*attachment/[0-9a-f-]{36})"[^>]*>(.*?)</a>', re.S | re.I)
FILES = {
    "treasury": "https://www.gov.pl/attachment/eef6f5f6-9a6f-4124-9afe-b2ebe76f89b9",
    "public": "https://www.gov.pl/attachment/2b996897-1345-4885-a639-f4558dfda347",
    "budget": "https://www.gov.pl/attachment/1b66a224-18ba-4959-94f6-cf0e2b6964fa",
    "krus": "https://www.gov.pl/attachment/c27176c2-0f4f-4329-8f09-afb707d1fc09",
}


def grid(body):
    g = defaultdict(lambda: defaultdict(dict))
    f = {}
    for c in xlsx_cells.cells(body):
        g[c["sheet_name"]][c["row_number"]][c["column_number"]] = c["value_text"]
        f[(c["sheet_name"], c["row_number"], c["column_number"])] = c["number_format"]
    return g, f


def show(name, g, f, sheets=None, rows=None, cols=(1, 2, 3, 4, 5), tail=3):
    for s, rs in g.items():
        if sheets and not any(s.startswith(x) for x in sheets):
            continue
        maxc = max(max(r) for r in rs.values())
        print(f"--- {name} sheet {s!r} maxc={maxc}")
        for r in sorted(rs)[: rows or 9999]:
            keep = sorted(set(c for c in rs[r] if c in cols or c > maxc - tail))
            print(f"  r{r}: " + " | ".join(f"{c}={rs[r][c]}" + ("[d]" if "yy" in (f.get((s, r, c)) or "") else "") for c in keep))


def main():
    bodies = {k: requests.get(u, headers=H, timeout=90).content for k, u in FILES.items()}
    g, f = grid(bodies["public"])
    show("public", g, f)
    g, f = grid(bodies["treasury"])
    show("treasury", g, f, cols=(1, 2, 3, 4, 5, 6))
    g, f = grid(bodies["krus"])
    show("krus", g, f, cols=tuple(range(1, 20)), rows=8)
    g, f = grid(bodies["budget"])
    show("budget", g, f, sheets=["TYTUŁ", "SPIS", "UWAGA", "TABLICA 1", "TABLICA 2", "TABLICA 3"], cols=tuple(range(1, 13)), tail=0)
    for year in range(2026, 2009, -1):
        url = f"https://www.gov.pl/web/finanse/sprawozdania-miesieczne-{year}"
        r = requests.get(url, headers=H, timeout=40)
        links = LINK.findall(r.text)
        names = [re.sub(r"\s+", " ", re.sub("<[^>]+>", " ", t)).replace("&#8203;", "").strip()[-90:] for _, t in links]
        print(f"year {year} {r.status_code} links={len(links)} xlsx={sum('xls' in n for n in names)}")
        for n in names[:2] + names[-2:]:
            print("   ", n)


if __name__ == "__main__":
    main()
