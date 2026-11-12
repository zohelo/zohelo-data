"""Read-only probe (#2756 follow-up): IDS series codes in observations but not in the series member; ICP API timing.

Reads production bronze.ids_observations and bronze.ids_metadata (no writes). Times one ICP series page at
per_page 50000 and 10000. Prints to the job log.
"""

from __future__ import annotations

import time

import duckdb
import requests

from ingestion.common.r2 import catalog

UA = {"User-Agent": "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"}


def ids() -> None:
    cat = catalog()
    con = duckdb.connect()
    con.register("obs", cat.load_table(("bronze", "ids_observations")).scan().to_arrow())
    con.register("meta", cat.load_table(("bronze", "ids_metadata")).scan().to_arrow())
    q = {
        "versions": "select source_version, raw_sha256, count(*) from obs group by all",
        "members": "select member, count(*), count(distinct source_row_number) from meta group by 1",
        "series fields": "select distinct field from meta where member ilike '%series%' order by 1",
        "orphans": """with o as (select trim(series_code) c, any_value(series_name) n, count(*) k from obs group by 1),
                      s as (select distinct trim(value_text) c from meta where member ilike '%seriesmetadata.csv' and field = 'Code')
                      select c, n, k, length(c), hex(c) from o anti join s using (c) order by k desc""",
        "series not in obs": """with o as (select distinct trim(series_code) c from obs),
                      s as (select distinct trim(value_text) c from meta where member ilike '%seriesmetadata.csv' and field = 'Code')
                      select c, length(c), hex(c) from s anti join o using (c)""",
        "codes with odd chars": "select distinct series_code, hex(series_code) from obs where regexp_matches(series_code, '[^A-Za-z0-9._ ]') limit 20",
    }
    for title, sql in q.items():
        rows = con.execute(sql).fetchall()
        print(f"IDS | {title} | {len(rows)} | {rows[:60]}")


def icp() -> None:
    s = requests.Session()
    s.headers.update(UA)
    for per_page in (10000, 50000):
        url = f"https://api.worldbank.org/v2/sources/90/country/all/series/1101120/time/all?format=json&per_page={per_page}&page=1"
        start = time.monotonic()
        try:
            r = s.get(url, timeout=600)
            print(f"ICP | per_page {per_page} | {r.status_code} | {len(r.content)} bytes | {time.monotonic() - start:.1f}s | {r.text[:150]!r}")
        except Exception as exc:  # noqa: BLE001
            print(f"ICP | per_page {per_page} | {type(exc).__name__} {exc} | {time.monotonic() - start:.1f}s")
        time.sleep(2)


if __name__ == "__main__":
    ids()
    icp()
