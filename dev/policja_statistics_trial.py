"""Dev trial of policja_statistics (PLG-033): Landing and Bronze into zz_dev_policja/ (both buckets) and namespace
zz_dev_policja, each twice, then (when the branch has them) the Silver and Gold SQL applied in memory with DuckDB
(Jinja-rendered with warehouse/macros), checks, profile and Poland spot values printed and annotated. Without a
Silver model (args: dump) it prints every sheet's first rows so the layout can be read. Reads silver.teryt_terc
(read-only) for the voivodeship names. Everything written is removed at the end (finally). Deleted before merge."""

from __future__ import annotations

from pathlib import Path
import re
import sys
import time

import duckdb
import jinja2

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file
from ingestion.to_bronze.policja_statistics import load as bronze

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "policja_statistics"
SCRATCH = "zz_dev_policja"
SILVER = ROOT / "warehouse/models/silver/policja_statistics/policja_statistics_offences.sql"
GOLD = ROOT / "warehouse/models/gold/poc_build_policja_statistics.sql"


def retried(what, fn):
    """Bounded retries with backoff for R2 under load (429 / SlowDown / ServiceUnavailable)."""
    for attempt in range(5):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - re-raised unless a throttling answer
            text = repr(exc)
            if attempt == 4 or not re.search(r"429|SlowDown|ServiceUnavailable|Too Many", text):
                raise
            print(f"RETRY {what} {attempt + 1}: {text[:200]}", flush=True)
            time.sleep(15 * (attempt + 1))
    return None


def render(path: Path, tables: dict[str, str]) -> str:
    macros = "".join(p.read_text() for p in sorted((ROOT / "warehouse/macros").glob("*.sql")) if p.stem in ("xlsx_cells",))
    macros += (ROOT / "warehouse/macros/sources/policja_statistics.sql").read_text()
    text = macros + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def show(con, title, sql, limit=200):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:140] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")
    return rows


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    land = f"{SCRATCH}/01_landing"
    lb = req("STORAGE_LANDING_BUCKET_NAME")
    try:
        landing = []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            retried("landing", lambda: bulk_file.main(SOURCE, "load", landing_root=land, pause_seconds=2))
            files = []
            for spec in bulk_file.declaration(SOURCE):
                held = bulk_file.landed(client, lb, bulk_file.prefix(SOURCE, spec["entity"], land))
                files.append((spec["entity"], len(held), held[-1]["size_bytes"] if held else 0,
                              held[-1]["version_token"] if held else None))
            landing.append(files)
        print("LANDING", landing, flush=True)
        annotate("notice", "policja landing", {
            "files": len(landing[1]), "bytes": sum(f[2] for f in landing[1]),
            "versions_after_run1": sum(f[1] for f in landing[0]), "versions_after_run2": sum(f[1] for f in landing[1]),
            "entities": {f[0]: f[3] for f in landing[1]}})
        counts = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            retried("bronze", lambda: bronze.load(10, landing_root=land, namespace=SCRATCH))
            cells = retried("scan", lambda: cat.load_table((SCRATCH, f"{SOURCE}_cells")).scan().to_arrow())
            counts.append(cells.num_rows)
        try:
            rejected = cat.load_table((SCRATCH, f"{SOURCE}_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001 - no rejections table when nothing was rejected
            rejected = []
        con.register("cells", cells)
        annotate("notice", "policja bronze", {
            "rows_after_run1": counts[0], "rows_after_run2": counts[1],
            "rejected": [(r.get("entity"), (r.get("error") or "")[:120]) for r in rejected],
            "per_entity": con.sql("select string_agg(entity || ':' || n, ' ' order by entity) from (select entity, count(*) n from cells group by 1)").fetchone()[0]})
        if "dump" in argv or not SILVER.exists():
            show(con, "sheets", "select entity, sheet_name, count(*) cells, max(row_number) max_row, max(column_number) max_col from cells group by all order by all")
            show(con, "number formats", "select cell_type, number_format, count(*) from cells group by all order by 3 desc")
            show(con, "non-numeric texts in data area", """select value_text, count(*) from cells
                where row_number > 3 and column_number > 1 and try_cast(value_text as double) is null group by 1 order by 2 desc""", 80)
            show(con, "layout", """select entity, sheet_name, row_number,
                string_agg(column_number || '=' || replace(left(value_text, 50), chr(10), ' '), ' | ' order by column_number)
                from cells where row_number <= 45 or row_number > (select max(row_number) - 6 from cells c2 where c2.entity = cells.entity and c2.sheet_name = cells.sheet_name)
                group by all order by entity, sheet_name, row_number""", 4000)
            return 0

        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        con.sql("create table silver as " + render(SILVER, {"policja_statistics_cells": "cells"}))
        n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
        key = "entity, category_code, area_name, measure_code, period_year"
        s = {
            "rows": n("select count(*) from silver"),
            "dup_keys": n(f"select count(*) from (select 1 from silver group by {key} having count(*) > 1)"),
            "null_keys": n("select count(*) from silver where category_code is null or area_name is null or measure_code is null or period_year is null or raw_sha256 is null"),
            "categories": n("select string_agg(x, '; ' order by x) from (select category_code || ' ' || min(period_year) || '-' || max(period_year) || ' n=' || count(*) x from silver group by category_code)"),
            "areas": n("select count(distinct area_name) from silver"),
            "status": n("select string_agg(cell_status || '=' || c, ' ') from (select cell_status, count(*) c from silver group by 1)"),
            "null_values": n("select count(*) from silver where value is null"),
            "rate_out_of_0_100": n("select count(*) from silver where measure_code = 'detection_rate' and (value < 0 or value > 100)"),
            "count_negative": n("select count(*) from silver where measure_code <> 'detection_rate' and value < 0"),
        }
        print("SILVER", s, flush=True)
        annotate("notice", "policja silver", s)
        show(con, "silver areas", "select area_name, count(*) from silver group by 1 order by 1")
        show(con, "silver non-ok", "select * exclude (raw_sha256) from silver where cell_status <> 'ok' limit 60")
        # completeness check as in warehouse/tests (rendered by hand: the test refs only the Silver model)
        test = ROOT / "warehouse/tests/policja_statistics_offences_complete.sql"
        if test.exists():
            fails = con.sql(render(test, {"policja_statistics_offences": "silver"})).fetchall()
            annotate("notice" if not fails else "warning", "policja silver completeness test", {"failing_rows": len(fails), "sample": [str(f)[:150] for f in fails[:8]]})

        con.sql("""create table geo as
            select 'policja_statistics' as source_system, source_code, geography_key from (
            """ + render_geo() + ")")
        con.sql("create table gold as " + render(GOLD, {"policja_statistics_offences": "silver", "poc_map_geography": "geo"}))
        g = {
            "rows": n("select count(*) from gold"),
            "dup_series_date": n("select count(*) from (select 1 from gold group by measure_key, geography_key, breakdown_json, date_key having count(*) > 1)"),
            "unmatched_geo": n("select count(*) from gold where geography_key = '_X'"),
            "unmatched_codes": n("select string_agg(distinct geography_source_code, '; ') from gold where geography_key = '_X'"),
            "measures": n("select count(distinct measure_key) from gold"),
            "geographies": n("select count(distinct geography_key) from gold"),
            "dates": n("select min(date_key) || '..' || max(date_key) from gold"),
            "status": n("select string_agg(observation_status_key || '=' || c, ' ') from (select observation_status_key, count(*) c from gold group by 1)"),
        }
        print("GOLD", g, flush=True)
        annotate("notice", "policja gold", g)
        spot = n("""select string_agg(measure_codes || ' ' || period_label || '=' || value, '; ' order by measure_codes, period_label)
            from gold where geography_key = 'PL' and period_label in ('1999', '2015', '2020', '2021', '2022', '2023')""")
        annotate("notice", "policja gold Poland", {"values": (spot or "")[:3500]})
        spot2 = n("""select string_agg(geography_key || ' ' || measure_codes || ' ' || period_label || '=' || value, '; ' order by all)
            from gold where period_label = '2021' and measure_codes like 'ogolem:%'""")
        annotate("notice", "policja gold 2021 all offences by area", {"values": (spot2 or "")[:3500]})
        show(con, "gold sample", "select measure_key, geography_source_code, geography_key, period_label, value, observation_status_key from gold order by all limit 80")
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    retried("delete", lambda k=obj["Key"], b=bucket: client.delete_object(Bucket=b, Key=k))
                    removed += 1
        dropped = []
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
            cat.drop_namespace(SCRATCH)
        except Exception as exc:  # noqa: BLE001
            print("drop failed", exc)
        left = sum(len(p.get("Contents", [])) for b in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
                   for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"))
        annotate("notice", "policja trial cleanup", {"objects_removed": removed, "objects_left": left, "tables_dropped": dropped})
    return 0


def render_geo() -> str:
    """The geography CTE of macros/sources/policja_statistics.sql, run against silver.teryt_terc as `geo`."""
    macro = (ROOT / "warehouse/macros/sources/policja_statistics.sql").read_text()
    body = jinja2.Environment().from_string(macro + "{{ poc_geography__policja_statistics() }}").render()
    body = body.strip()
    cte = re.match(r"policja_statistics as \((.*)\)\s*$", body, re.S).group(1)
    return f"""with geo as (select 'TERC:' || terc_code as geography_key, name_pl, 'wojewodztwo' as level_name
        from terc where unit_level = 'wojewodztwo')
        select source_code, geography_key from ({cte})"""


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
