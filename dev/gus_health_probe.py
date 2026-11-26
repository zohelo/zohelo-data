"""Read-only probe of the GUS health publication "Zdrowie i ochrona zdrowia w <year> r." (PLS-004, #2451): lists the
file links of every edition page, downloads each edition's tables file(s) into memory (one request at a time, PAUSE
apart; nothing is written to R2) and prints the titles and grids of the medical-personnel and hospital sheets, and
the symbol legend. Deleted before merge."""

from __future__ import annotations

import io
import re
import sys
import time
import zipfile

import duckdb
import requests

from ingestion.common import gus_publications as gp
from ingestion.common import xlsx_cells
from ingestion.to_landing.patterns import bulk_file

SERIES_PAGE = "https://stat.gov.pl/obszary-tematyczne/zdrowie/zdrowie/"
EDITION = r"^Zdrowie i ochrona zdrowia w (\d{4})(?: r\.| roku)?$"
WANTED = r"(?i)(pracown|lekarz|piel[eę]gn|szpital|łóżk|lozk|znaki umowne|objaśnienia znaków)"
GRID = r"(?i)((pracownicy medyczni|lekarze|piel[eę]gniarki).*(wojew)|szpitale ogólne.*wojew|znaki umowne)"


def get(url: str) -> bytes:
    for attempt in range(4):
        try:
            r = requests.get(url, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 120))
            r.raise_for_status()
            return r.content
        except requests.RequestException as exc:
            print("retry", url, exc, flush=True)
            time.sleep(20 * (attempt + 1))
    raise RuntimeError(url)


def xls_rows(body: bytes, member: str):
    import xlrd

    book = xlrd.open_workbook(file_contents=body, on_demand=True)
    for index, name in enumerate(book.sheet_names(), 1):
        sheet = book.sheet_by_name(name)
        for r in range(sheet.nrows):
            for c in range(sheet.ncols):
                v = sheet.cell(r, c).value
                if v in ("", None):
                    continue
                text = str(int(v)) if isinstance(v, float) and v.is_integer() else str(v)
                yield (member, name, r + 1, c + 1, text)


def cells_of(body: bytes, name: str):
    out = []
    if body[:2] != b"PK":
        return list(xls_rows(body, name))
    z = zipfile.ZipFile(io.BytesIO(body))
    if "xl/workbook.xml" in z.namelist():
        books = [(name, body)]
    else:
        books = [(n, z.read(n)) for n in sorted(z.namelist()) if n.lower().endswith((".xlsx", ".xls"))]
        print("   members:", len(books), [n for n, _ in books][:80], flush=True)
    for member, data in books:
        try:
            if data[:2] == b"PK":
                out += [(member, c["sheet_name"], c["row_number"], c["column_number"], c["value_text"]) for c in xlsx_cells.cells(data)]
            else:
                out += list(xls_rows(data, member))
        except Exception as exc:  # noqa: BLE001 - probe reports
            print("   cannot read", member, type(exc).__name__, exc, flush=True)
    return out


def main(argv) -> int:
    series = gp.Series("gus_health", SERIES_PAGE, EDITION, "x", "zdrowie", 2000)
    found = gp.editions(series, gp._links(SERIES_PAGE))
    newest = max(found)
    time.sleep(gp.PAUSE)
    found = {**gp.editions(series, gp._links(found[newest])), **found}
    print("EDITIONS", {y: found[y] for y in sorted(found)}, flush=True)
    con = duckdb.connect()
    con.sql("create table c (edition int, member varchar, sheet varchar, r int, k int, v varchar)")
    for year in sorted(found):
        time.sleep(gp.PAUSE)
        links = gp._links(found[year])
        files = list(dict.fromkeys((h, t) for h, t in links if re.search(r"\.(zip|xlsx?|rar|7z|pdf)$", h, re.I)))
        print(f"\n== {year} {found[year]}", flush=True)
        for h, t in files:
            print(f"   {h} | {t}", flush=True)
        tables = [(h, t) for h, t in files if re.search(r"\.(zip|xlsx?)$", h, re.I)]
        for h, t in tables:
            time.sleep(gp.PAUSE)
            body = get(h)
            print(f"   got {len(body)} bytes {h}", flush=True)
            rows = cells_of(body, h.rsplit("/", 1)[-1])
            print(f"   cells {len(rows)}", flush=True)
            if rows:
                con.executemany("insert into c values (?, ?, ?, ?, ?, ?)", [(year, *r) for r in rows])
    con.sql("""create table t as select edition, member, sheet,
        string_agg(trim(regexp_replace(v, '\\s+', ' ', 'g')), ' ' order by r, k) filter (where r <= 4) as title
        from c group by all""")
    print("\n##### sheets per edition", con.sql("select edition, count(*) from t group by 1 order by 1").fetchall(), flush=True)
    print("\n##### wanted titles", flush=True)
    for row in con.sql(f"select edition, member, sheet, left(title, 260) from t where regexp_matches(title, '{WANTED}') order by all").fetchall():
        print(" | ".join(str(x) for x in row), flush=True)
    print("\n##### grids", flush=True)
    grids = con.sql(f"select edition, member, sheet from t where regexp_matches(title, '{GRID}') order by all").fetchall()
    for edition, member, sheet in grids:
        print(f"\n--- {edition} {member} {sheet}", flush=True)
        for r, line in con.execute(
            """select r, string_agg(k || ':' || replace(left(v, 120), chr(10), ' '), ' | ' order by k)
            from c where edition = ? and member = ? and sheet = ? and r <= 40 group by r order by r""",
            [edition, member, sheet],
        ).fetchall():
            print(f"{r}: {line}", flush=True)
    print("\n##### symbol texts (cells of 1-2 chars, not numbers) in grid sheets", flush=True)
    print(con.sql(f"""select edition, string_agg(distinct trim(v), ' ') from c join (select edition, member, sheet from t
        where regexp_matches(title, '{GRID}')) using (edition, member, sheet)
        where length(trim(v)) <= 2 and try_cast(v as double) is null group by 1 order by 1""").fetchall(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
