"""Read-only probe 7 (PLS-021, #3031): layout of the MEN school lists 2018-2025 on dane.gov.pl (dataset 839).

Downloads each workbook once (2 s apart), reads every sheet with DuckDB's excel extension (all cells as text, no
header) and prints: sheet names, the rows above the header, the header, the row count, and per column the filled
count, distinct count and maximum length; the most frequent values only for code, type, status and date columns
(never names, addresses, phone numbers, e-mails, web addresses or persons).
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import sys
import tempfile
import time
import zipfile

import duckdb
import requests

from ingestion.common import xlsx_cells
from ingestion.common.report import annotate

UA = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
FILES = {
    "2025": "https://api.dane.gov.pl/media/resources/20260216/Wykaz_szk%C3%B3%C5%82_i_plac%C3%B3wek_o%C5%9Bwiatowych_30.09.2025_.xlsx",
    "2024": "https://api.dane.gov.pl/media/resources/20250317/Wykaz_szk%C3%B3%C5%82_i_plac%C3%B3wek_o%C5%9Bwiatowych_30.09.2024_.xlsx",
    "2023": "https://api.dane.gov.pl/media/resources/20240719/Wykaz_szk%C3%B3%C5%82_i_plac%C3%B3wek_o%C5%9Bwiatowych_30.09.2023_.xlsx",
    "2022": "https://api.dane.gov.pl/media/resources/20240719/Wykaz_szk%C3%B3%C5%82_i_plac%C3%B3wek_o%C5%9Bwiatowych_30.09.2022_.xlsx",
    "2021": "https://api.dane.gov.pl/media/resources/20240719/Wykaz_szk%C3%B3%C5%82_i_plac%C3%B3wek_o%C5%9Bwiatowych_30.09.2021_.xlsx",
    "2020": "https://api.dane.gov.pl/media/resources/20240527/Wykaz_szk%C3%B3%C5%82_i_plac%C3%B3wek_o%C5%9Bwiatowych_30.09.2020_.xlsx",
    "2018": "https://api.dane.gov.pl/media/resources/20230918/Wykaz_szk%C3%B3%C5%82_i_plac%C3%B3wek_wg_stanu_na_30._IX._2018.xlsx",
}
UNSAFE = re.compile(r"(nazwa|imi|nazwisk|dyrektor|telefon|faks|fax|mail|www|strona|ulica|nr domu|numer domu|lokal|adres|"
                    r"kod poczt|poczta|miejscowo|patron|regon|nip|budynk|osob)", re.I)
NOTES: list[str] = []
HEADERS: dict[str, list[str]] = {}


def note(section: str, item) -> None:
    line = f"[{section}] {json.dumps(item, ensure_ascii=False, default=str)}"
    print(line[:40000], flush=True)
    NOTES.append(line[:500])


def profile(year: str, path: Path, con) -> None:
    with zipfile.ZipFile(path) as z:
        sheets = [name for name, _ in xlsx_cells.sheets(z)]
    note("sheets", {"year": year, "sheets": sheets})
    for sheet in sheets:
        rel = None
        for opts in ("header = false, all_varchar = true, stop_at_empty = false", "header = false, all_varchar = true"):
            try:
                rel = f"read_xlsx('{path}', sheet = '{sheet}', {opts})"
                cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()]
                break
            except duckdb.Error as exc:
                note("read_error", {"year": year, "sheet": sheet, "opts": opts, "err": str(exc)[:300]})
                rel = None
        if rel is None:
            continue
        con.execute(f"CREATE OR REPLACE TABLE t AS SELECT row_number() OVER () AS rn, * FROM {rel}")
        total = con.execute("SELECT count(*) FROM t").fetchone()[0]
        top = con.execute("SELECT * FROM t WHERE rn <= 8 ORDER BY rn").fetchall()
        # header row: first row with at least 5 non-empty cells
        h = next((r[0] for r in top if sum(1 for c in r[1:] if c and str(c).strip()) >= 5), 1)
        header_row = next(r for r in top if r[0] == h)[1:]
        pre = [[str(c)[:80] for c in r[1:] if c and str(c).strip()] for r in top if r[0] < h]
        header = [str(c).strip() if c is not None else "" for c in header_row]
        HEADERS[f"{year}#{sheet}"] = header
        note("layout", {"year": year, "sheet": sheet, "rows_total": total, "header_row": h, "pre_header": pre,
                        "columns": len(header), "data_rows": total - h})
        for j, (col, name) in enumerate(zip(cols, header)):
            q = f'"{col}"'
            filled, distinct, maxlen = con.execute(
                f"SELECT count({q}) FILTER (WHERE trim({q}) <> ''), count(DISTINCT {q}), max(length({q})) FROM t WHERE rn > {h}"
            ).fetchone()
            rec = {"year": year, "sheet": sheet, "col": j, "name": name, "filled": filled, "distinct": distinct, "maxlen": maxlen}
            if not UNSAFE.search(name):
                rows = con.execute(
                    f"SELECT {q}, count(*) c FROM t WHERE rn > {h} GROUP BY 1 ORDER BY c DESC LIMIT {60 if distinct <= 150 else 6}"
                ).fetchall()
                rec["top"] = rows
            else:
                rec["top"] = "(withheld)"
            note("column", rec)


def main(argv: list[str]) -> int:
    con = duckdb.connect()
    con.execute("SET memory_limit = '6GB'; INSTALL excel; LOAD excel;")
    with tempfile.TemporaryDirectory() as tmp:
        for year, url in FILES.items():
            time.sleep(2.0)
            path = Path(tmp) / f"{year}.xlsx"
            started = time.monotonic()
            r = requests.get(url, headers={"User-Agent": UA}, timeout=(30, 300))
            path.write_bytes(r.content)
            note("file", {"year": year, "status": r.status_code, "bytes": len(r.content), "seconds": round(time.monotonic() - started, 1),
                          "last_modified": r.headers.get("Last-Modified")})
            if r.status_code == 200:
                try:
                    profile(year, path, con)
                except Exception as exc:  # noqa: BLE001
                    note("error", {"year": year, "err": f"{type(exc).__name__}: {str(exc)[:300]}"})
            path.unlink()
    keys = list(HEADERS)
    same = {k: HEADERS[k] == HEADERS[keys[0]] for k in keys}
    note("header_equal_to_first", {"first": keys[0] if keys else None, "equal": same})
    for k in keys[1:]:
        a, b = HEADERS[keys[0]], HEADERS[k]
        if a != b:
            note("header_diff", {"vs": k, "only_first": [x for x in a if x not in b], "only_this": [x for x in b if x not in a]})
    annotate("notice", "rspo probe 7", [n[:300] for n in NOTES if n.startswith(("[layout]", "[header_", "[error]"))][:14])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
