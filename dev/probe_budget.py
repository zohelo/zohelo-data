"""Dev probe (read-only): table 1 header rows of the December budget reports 2018–2023."""

from collections import defaultdict

from ingestion.common import xlsx_cells
from ingestion.to_landing.patterns import bulk_file

for year in (2018, 2019, 2020, 2021, 2022, 2023):
    spec = {"url_from": {"page": f"https://www.gov.pl/web/finanse/sprawozdania-miesieczne-{year}", "pattern": "/attachment/", "pick": "first"}}
    url = bulk_file.resolve_url(spec)
    body = bulk_file.requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=90).content
    g = defaultdict(dict)
    for c in xlsx_cells.cells(body):
        if c["sheet_name"].strip().upper().startswith("TABLICA 1") and c["sheet_name"].strip().upper() in ("TABLICA 1", "TABLICA 1."):
            g[(c["sheet_name"], c["row_number"])][c["column_number"]] = c["value_text"]
    sheets = sorted({s for s, _ in g})
    print(f"\n#### {year} {url} sheets={sheets}")
    for (s, r), cols in sorted(g.items()):
        if r <= 40 or any(isinstance(v, str) and "Wyszczeg" in v for v in cols.values()):
            print(f"  r{r}: " + " | ".join(f"{k}={str(v)[:40]!r}" for k, v in sorted(cols.items())))
