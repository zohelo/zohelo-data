"""Dev trial of internet_gov_broadband (PL-ENV-034, #2963): Landing and Bronze into zz_dev_igb/ (both buckets) and
namespace zz_dev_igb, each twice (the second pass must write nothing), then the branch's Silver model, its tests
(unique grain, not_null, accepted_values, the completeness test) and the Gold build applied in memory
(Jinja-rendered with the source's macros), grain check by series_key x date_key, Poland spot values. Everything
written is removed at the end (finally). Deleted before merge.

Local mode (args: local=<dir of <YYYYQn>.json with the layout text of every page>): Bronze cells from the probe's
layout text, same Silver / tests / Gold; no network, no R2."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sys

import duckdb
import jinja2
import pyarrow as pa
import yaml

ROOT = Path(os.environ.get("TRIAL_ROOT") or Path(__file__).resolve().parents[1])
SOURCE = "internet_gov_broadband"
SCRATCH = "zz_dev_igb"
MACROS = (ROOT / f"warehouse/macros/sources/{SOURCE}.sql").read_text()


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def macro(name: str) -> str:
    return jinja2.Environment().from_string(MACROS + "{{ " + name + "() }}").render()


def report(stats_title: str, stats: dict) -> None:
    print("STATS", stats_title, json.dumps(stats, ensure_ascii=False, default=str), flush=True)
    try:
        from ingestion.common.report import annotate

        annotate("notice", stats_title, stats)
    except Exception as exc:  # local mode without the runner's environment
        print("annotate skipped", exc)


def analyse(con: duckdb.DuckDBPyConnection, cells: pa.Table, extra: dict) -> int:
    con.register("cells", cells)
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    bronze = {
        "bronze_rows": cells.num_rows,
        "editions": n("select count(distinct entity) from cells"),
        "per_table": n("select string_agg(table_code || '=' || c, ' ' order by table_code) from (select table_code, count(*) c from cells group by 1)"),
        "per_edition_min_max": n("select min(c) || '..' || max(c) from (select entity, count(*) c from cells group by 1)"),
        "dup_cells": n("select count(*) from (select 1 from cells group by entity, table_code, row_label, column_code having count(*) > 1)"),
        "null_values": n("select count(*) from cells where value_text is null or value_text = ''"),
        "non_numeric_values": n("select count(*) from cells where not regexp_matches(value_text, '^[0-9][0-9 \u00a0\u202f]*$|^[0-9]{1,3},[0-9]{2}%$|^100%$')"),
        "zero_cells": n("select string_agg(value_text || '=' || c, ' ') from (select value_text, count(*) c from cells where value_text in ('0', '0,00%') group by 1)"),
        "reference_dates": n("select string_agg(distinct reference_date_text, '; ' order by reference_date_text) from cells"),
        **extra,
    }
    report(f"{SOURCE} trial Bronze", bronze)

    silver_sql = render(ROOT / f"warehouse/models/silver/{SOURCE}/{SOURCE}_coverage.sql", {f"{SOURCE}_cells": "cells"})
    con.sql("create or replace table coverage as " + silver_sql)
    spec = yaml.safe_load((ROOT / f"warehouse/models/silver/{SOURCE}/_{SOURCE}.yml").read_text())["models"][0]
    failures = {}
    for test in spec.get("tests", []):
        expr = test["unique"]["column_name"]
        failures[f"unique({expr[:40]})"] = n(f"select count(*) from (select {expr} k from coverage group by 1 having count(*) > 1)")
    for col in spec["columns"]:
        for test in col.get("tests", []):
            if test == "not_null":
                failures[f"not_null({col['name']})"] = n(f"select count(*) from coverage where {col['name']} is null")
            elif isinstance(test, dict) and "accepted_values" in test:
                values = test["accepted_values"]["arguments"]["values"]
                listed = ", ".join("'" + str(v) + "'" for v in values)
                failures[f"accepted_values({col['name']})"] = n(f"select count(*) from coverage where {col['name']} is not null and cast({col['name']} as varchar) not in ({listed})")
    complete_sql = render(ROOT / f"warehouse/tests/{SOURCE}_coverage_complete.sql", {f"{SOURCE}_coverage": "coverage"})
    failures["complete"] = n(f"select count(*) from ({complete_sql})")
    silver = {
        "silver_rows": n("select count(*) from coverage"),
        "per_table": n("select string_agg(table_code || '=' || c, ' ' order by table_code) from (select table_code, count(*) c from coverage group by 1)"),
        "reference_dates": n("select count(distinct reference_date) || ' from ' || min(reference_date) || ' to ' || max(reference_date) from coverage"),
        "test_failures": failures,
        "bandwidth_sum_minus_suma": n("""select string_agg(period_label || ':' || d, ' ' order by period_label) from (
            select period_label, sum(address_points) filter (where class_code <> '_T') - max(address_points) filter (where class_code = '_T') d
            from coverage where table_code = 'bandwidth' group by 1) where d <> 0"""),
    }
    report(f"{SOURCE} trial Silver", silver)

    geo = "with " + macro(f"poc_geography__{SOURCE}") + f" select * from {SOURCE}"
    con.sql("create or replace table geo as " + geo)
    gold_sql = render(ROOT / f"warehouse/models/gold/poc_build_{SOURCE}.sql",
                      {f"{SOURCE}_coverage": "coverage", "poc_map_geography": "geo"})
    con.sql("create or replace table gold as " + gold_sql)
    gold = {
        "gold_rows": n("select count(*) from gold"),
        "per_measure": n("select string_agg(measure_codes || '=' || c, ' ' order by measure_codes) from (select measure_codes, count(*) c from gold group by 1)"),
        "dup_series_date": n("select count(*) from (select 1 from gold group by measure_key, geography_key, breakdown_json, date_key having count(*) > 1)"),
        "geography_unmatched": n("select count(*) from gold where geography_key = '_X'"),
        "geographies": n("select count(distinct geography_key) from gold"),
        "null_values": n("select count(*) from gold where value is null"),
        "dates": n("select min(date_key) || '..' || max(date_key) || ' (' || count(distinct date_key) || ')' from gold"),
    }
    report(f"{SOURCE} trial Gold", gold)
    spots = n("""select string_agg(period_label || ' ' || measure_codes || '=' || value, '; ' order by period_label, measure_codes)
        from gold where geography_key = 'PL' and period_label in ('2024-Q2', '2024-Q4', '2026-Q3')
        and measure_codes in ('covered', 'total', 'covered_100plus', 'covered_1000plus', 'no_coverage', 'no_coverage_share')""")
    voiv = n("""select string_agg(period_label || ' ' || geography_key || ' ' || measure_codes || '=' || value, '; ' order by all)
        from gold where geography_key in ('TERC:28', 'TERC:14') and period_label in ('2024-Q2', '2026-Q3')""")
    report(f"{SOURCE} trial Poland spot values", {"poland": spots, "voivodeships": voiv})
    bad = sum(failures.values()) + gold["dup_series_date"] + gold["geography_unmatched"] + gold["null_values"]
    return 1 if bad else 0


def local(folder: str) -> int:
    sys.path.insert(0, str(ROOT))
    from ingestion.to_bronze.internet_gov_broadband import load, tables

    rows = []
    for path in sorted(Path(folder).glob("*.json")):
        q = path.stem
        entity = f"polska_{q[:4]}q{q[-1]}"
        printed, cells = tables.read_report(json.loads(path.read_text())["layout"], load.quarter_end(entity))
        for c in cells:
            rows.append({"entity": entity, "reference_date_text": printed, **c, "raw_sha256": "local-" + q,
                         "source_version": "local", "ingested_at": "2026-11-13T00:00:00Z"})
    return analyse(duckdb.connect(), pa.Table.from_pylist(rows), {"mode": "local"})


def main(argv: list[str]) -> int:
    folder = next((a[6:] for a in argv if a.startswith("local=")), None)
    if folder:
        return local(folder)
    from ingestion.common import bronze_files as bf
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.to_bronze.internet_gov_broadband import load
    from ingestion.to_landing import internet_gov_broadband as landing
    from ingestion.to_landing.patterns import bulk_file

    client = s3()
    cat = bf.retry(catalog, "catalog")
    land = f"{SCRATCH}/01_landing"
    bucket = req("STORAGE_LANDING_BUCKET_NAME")
    try:
        passes = []
        for attempt in (1, 2):
            bulk_file.main(SOURCE, "load", landing_root=land, pause_seconds=3.0)
            objects = [o for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{land}/")
                       for o in page.get("Contents", [])]
            files = [o for o in objects if not o["Key"].endswith(".receipt.json")]
            passes.append({"pass": attempt, "files": len(files), "bytes": sum(o["Size"] for o in files),
                           "receipts": sum(o["Key"].endswith(".receipt.json") for o in objects)})
        missing = landing.check_editions()
        bronze_passes = []
        for attempt in (1, 2):
            load.load(10, landing_root=land, namespace=SCRATCH)
            table = cat.load_table((SCRATCH, f"{SOURCE}_cells"))
            snapshots = len(table.snapshots())
            bronze_passes.append({"pass": attempt, "rows": table.scan().to_arrow().num_rows, "snapshots": snapshots})
        cells = cat.load_table((SCRATCH, f"{SOURCE}_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, f"{SOURCE}_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:
            rejected = []
        extra = {
            "landing_passes": passes,
            "editions_missing_from_seed": missing,
            "bronze_passes": bronze_passes,
            "rejected": [(r.get("entity"), (r.get("error") or "")[:160]) for r in rejected],
        }
        return analyse(duckdb.connect(), cells, extra)
    finally:
        removed = 0
        for b in (bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=b, Key=obj["Key"])
                    removed += 1
        dropped = []
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
            cat.drop_namespace(SCRATCH)
        except Exception as exc:
            print("drop failed", exc)
        report(f"{SOURCE} trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
