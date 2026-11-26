"""Dev trial of kgpsp_interventions (register EX-003, #2970) on the branch's final code: Landing and Bronze into
zz_dev_kgpsp_interventions/ (both buckets) and namespace zz_dev_kgpsp_interventions, each twice; then the branch's
Silver model, its schema tests and data tests, and the Gold build stage applied in local DuckDB (Jinja-rendered with
warehouse/macros), each built twice, with profiles, grain checks and Poland spot values. Reads silver.teryt_terc
(read-only) for the voivodeship codes. Everything written is removed at the end (finally). Deleted before merge."""

from __future__ import annotations

import json
from pathlib import Path
import re
import sys

import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze, report
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze import kgpsp_interventions as bronze
from ingestion.to_landing import kgpsp_interventions as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "kgpsp_interventions"
SCRATCH = "zz_dev_kgpsp_interventions"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/xlsx_cells.sql").read_text() + (ROOT / f"warehouse/macros/sources/{SOURCE}.sql").read_text()
ANNOTATE = report.annotate
CAPTURED: list = []


def capture(level, title, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:2000]}", flush=True)


report.annotate = capture
bulk_bronze.annotate = capture


def note(title: str, data, level: str = "notice") -> None:
    print(f"### {title}: {json.dumps(data, default=str, ensure_ascii=False)}", flush=True)
    ANNOTATE(level, title, data)


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def show(con, title, sql, limit=60):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:100] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def schema_tests(con, model: str) -> dict:
    spec = yaml.safe_load((ROOT / f"warehouse/models/silver/{SOURCE}/_{SOURCE}.yml").read_text())
    m = next(x for x in spec["models"] if x["name"] == model)
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    out = {}
    for t in m.get("tests", []):
        if isinstance(t, dict) and "unique" in t:
            out["unique"] = n(f"select count(*) from (select {t['unique']['column_name']} k from {model} group by 1 having count(*) > 1)")
    for col in m.get("columns", []):
        for t in col.get("tests", []) or []:
            if t == "not_null":
                out[f"not_null {col['name']}"] = n(f"select count(*) from {model} where {col['name']} is null")
            elif isinstance(t, dict) and "accepted_values" in t:
                vals = ", ".join("'" + str(v).replace("'", "''") + "'" for v in t["accepted_values"]["arguments"]["values"])
                out[f"accepted {col['name']}"] = n(f"select count(*) from {model} where {col['name']} is not null and cast({col['name']} as varchar) not in ({vals})")
    return out


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    try:
        # Landing, twice: the second run must land nothing
        runs = []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            CAPTURED.clear()
            code = landing.extract(60, landing_root=LAND)
            d = next((d for t, d in CAPTURED if t.endswith("landing extract")), {})
            runs.append({"exit": code, **{k: d.get(k) for k in ("planned", "landed", "unchanged", "refused", "deferred_to_next_run")}})
        held = landing.held(LAND)
        receipts = bulk_file.landed(client, req("STORAGE_LANDING_BUCKET_NAME"), f"{LAND}/{SOURCE}/snapshot/data/")
        verify = bulk_file.main(SOURCE, "verify", landing_root=LAND, specs=[{"entity": e} for e in sorted(held)])
        stats = {"landing": {
            "runs": runs, "verify_exit": verify, "files": len(receipts), "entities": len(held),
            "years": f"{min(held)}..{max(held)}", "bytes": sum(r["size_bytes"] for r in receipts),
            "copies_per_entity_max": max(sum(1 for r in receipts if r["entity"] == e) for e in held),
        }}
        # Bronze, twice: the second run must add nothing
        loads = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            CAPTURED.clear()
            code = bronze.bronze_load(sorted(held), landing_root=LAND, namespace=SCRATCH)
            d = next((d for t, d in CAPTURED if "Bronze from Landing" in t), {})
            rows = cat.load_table((SCRATCH, f"{SOURCE}_cells")).scan().to_arrow().num_rows
            loads.append({"exit": code, "candidates": d.get("candidates"), "files_added": d.get("appended_files"), "rows_added": d.get("rows_added"), "rejected": d.get("rejected"), "rows_after": rows})
        cells = cat.load_table((SCRATCH, f"{SOURCE}_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, f"{SOURCE}_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:
            rejected = []
        con.register("cells_raw", cells)
        con.sql("create table cells as select * from cells_raw")
        stats["bronze"] = {
            "loads": loads,
            "files": n("select count(distinct raw_sha256) from cells"),
            "members": n("select count(distinct entity || member) from cells"),
            "dup_cell_keys": n("select count(*) from (select 1 from cells group by entity, member, sheet_name, row_number, column_number having count(*) > 1)"),
            "null_keys": n("select count(*) from cells where entity is null or row_number is null or column_number is null or raw_sha256 is null"),
            "rejected": [(r.get("entity"), (r.get("error") or "")[:200]) for r in rejected],
        }
        note(f"{SOURCE} trial landing+bronze", stats)
        show(con, "members", "select entity, member, sheet_name, max(row_number), max(column_number), count(*) from cells group by all order by all")

        # Silver, twice, with its tests
        s = f"{SOURCE}_counts"
        sql = render(ROOT / f"warehouse/models/silver/{SOURCE}/{s}.sql", {f"{SOURCE}_cells": "cells"})
        counts = []
        for _ in (1, 2):
            con.sql(f"create or replace table {s} as {sql}")
            counts.append(n(f"select count(*) from {s}"))
        tests = schema_tests(con, s)
        complete = con.sql(render(ROOT / f"warehouse/tests/{s}_complete.sql", {s: s})).fetchall()
        entities = con.sql(render(ROOT / f"warehouse/tests/{SOURCE}_entities_in_silver.sql", {f"{SOURCE}_cells": "cells", s: s})).fetchall()
        failing = {k: v for k, v in tests.items() if v}
        stats["silver"] = {
            "rows_two_builds": counts,
            "schema_tests": len(tests), "schema_tests_failing": failing,
            "complete_rows": len(complete), "complete_first": [list(map(str, r)) for r in complete[:8]],
            "entities_without_silver": [list(r) for r in entities],
            "years": n(f"select min(report_year) || '..' || max(report_year) from {s}"),
            "per_layout": n(f"select string_agg(k, '; ' order by k) from (select layout || ' ' || table_code || ': ' || count(*) || ' rows, ' || count(distinct area_code) || ' areas, ' || count(distinct report_year) || ' years' k from {s} group by layout, table_code)"),
            "not_numeric": n(f"select coalesce(string_agg(value_text || '=' || c, ' '), 'none') from (select value_text, count(*) c from {s} where value_status <> 'ok' group by 1)"),
            "non_integer_values": n(f"select count(*) from {s} where value <> floor(value)"),
            "negative_values": n(f"select count(*) from {s} where value < 0"),
        }
        note(f"{SOURCE} trial silver", stats["silver"], "notice" if not failing and not complete and not entities else "warning")
        show(con, "complete test rows", render(ROOT / f"warehouse/tests/{s}_complete.sql", {s: s}), limit=40)
        show(con, "labels per year", f"select report_year, table_code, measure_code, string_agg(distinct measure_label, ' || ') from {s} group by all order by all", limit=400)
        show(con, "gmina TERYT codes on several rows", f"""select report_year, table_code, area_code, count(*) / count(distinct measure_code) as rows_, string_agg(distinct area_name, ' / ')
            from {s} where layout = 'gmina' group by all having count(*) > count(distinct measure_code) order by all""", limit=80)
        show(con, "gmina rows per year", f"select report_year, table_code, count(distinct area_code), count(*) from {s} where layout = 'gmina' group by all order by all")

        # Gold: geography map from teryt_terc (read-only), build stage twice, grain and spot values
        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        con.sql("create table geo as select terc_code, 'TERC:' || terc_code as geography_key, 'wojewodztwo' as level_name from terc where unit_level = 'wojewodztwo'")
        geo_cte = jinja2.Environment().from_string(MACROS + "{{ poc_geography__" + SOURCE + "() }}").render(ref=lambda m: m)
        con.sql(f"create table map_geo as with geo as (select * from geo), {geo_cte} select * from {SOURCE}")
        gold_sql = render(ROOT / f"warehouse/models/gold/poc_build_{SOURCE}.sql", {s: s, "poc_map_geography": "map_geo"})
        gold_counts = []
        for _ in (1, 2):
            con.sql(f"create or replace table gold as {gold_sql}")
            gold_counts.append(n("select count(*) from gold"))
        bounds = {"P": (500, 4e5), "MZ": (1000, 1.5e6), "AF": (50, 2e5)}
        stats["gold"] = {
            "rows_two_builds": gold_counts,
            "blocks": n("select string_agg(k, '; ') from (select block_id || ' ' || count(*) || ' rows, ' || count(distinct measure_key) || ' measures' k from gold group by 1)"),
            "areas": n("select count(distinct geography_key) from gold"),
            "years": n("select min(period_label) || '..' || max(period_label) from gold"),
            "dup_series_date": n("select count(*) from (select 1 from gold group by measure_key || '|' || geography_key || '|' || breakdown_json, date_key having count(*) > 1)"),
            "freq_per_measure_gt1": n("select count(*) from (select measure_key from gold group by 1 having count(distinct frequency) > 1)"),
            "unmatched": n("select count(*) from gold where geography_key = '_X'"),
            "null_values": n("select count(*) from gold where value is null"),
            "rows_per_year_min_max": n("select min(c) || '..' || max(c) from (select count(*) c from gold group by period_label)"),
            "kpi_outside_bounds": {k: n(f"select count(*) from gold where measure_codes = '{k}' and block_id = 'incidents' and (value < {lo} or value > {hi})") for k, (lo, hi) in bounds.items()},
            "kpi_range": {k: n(f"select min(value) || '..' || max(value) from gold where measure_codes = '{k}' and block_id = 'incidents'") for k in bounds},
            "voiv_sum_vs_PL_max_abs_diff": n("select max(abs(r - p)) from (select measure_key, date_key, sum(value) filter (where geography_key like 'TERC:%') r, max(value) filter (where geography_key = 'PL') p from gold group by all)"),
        }
        note(f"{SOURCE} trial gold", stats["gold"])
        spot = n("""select string_agg(period_label || ' ' || k, '; ' order by period_label) from (select period_label, string_agg(replace(measure_codes, 'OGÓŁEM:ogolem:', 'all_') || '=' || cast(value as bigint), ' ' order by measure_codes) k
            from gold where geography_key = 'PL' and measure_codes in ('P', 'MZ', 'AF', 'OGÓŁEM', 'OGÓŁEM:ogolem:smiertelne', 'OGÓŁEM:ogolem:ranni') group by 1)""")
        note(f"{SOURCE} trial Poland", {"values": spot})
        show(con, "gold voivodeships, fires", "select geography_key, period_label, value from gold where measure_codes = 'P' order by 1, 2", limit=200)
        from warehouse.seed_rows import rows as seed_rows

        row = next(r for r in seed_rows("source") if r["source_key"] == SOURCE)
        lic = next((r for r in seed_rows("licence") if r["licence_key"] == row["licence_key"]), None)
        note(f"{SOURCE} trial licence", {"source_licence_key": row["licence_key"], "licence_row": lic})
        print("STATS", json.dumps(stats, default=str, ensure_ascii=False), flush=True)
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    removed += 1
        dropped = []
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
            cat.drop_namespace(SCRATCH)
        except Exception as exc:
            print("drop failed", exc)
        left = sum(len(p.get("Contents", [])) for b in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
                   for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"))
        note(f"{SOURCE} trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
