"""Dev trial of the GUS publication sources (args: source systems; default gus_culture gus_culture_participation
gus_tourism): Landing and Bronze into zz_dev_w3gus/ (both buckets) and namespace zz_dev_w3gus, each twice, then the
Silver and Gold SQL of the branch applied in memory (Jinja-rendered with warehouse/macros), checks and samples
printed and annotated. Reads silver.teryt_terc (read-only) for the voivodeship names. Everything written is removed
at the end (finally). Deleted before merge."""

from __future__ import annotations

import importlib
from pathlib import Path
import re
import sys

import duckdb
import jinja2

from ingestion.common import bronze_files as bf
from ingestion.common import gus_publications as gp
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_w3gus"
MACROS = "".join((ROOT / f"warehouse/macros/{m}.sql").read_text() for m in ("xlsx_cells", "gus_tables"))


def render(path: Path, tables: dict[str, str]) -> str:
    env = jinja2.Environment()
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    tpl = env.from_string(text)
    return tpl.render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def show(con, title, sql, limit=120):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:90] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def main(argv) -> int:
    sources = argv or ["gus_culture", "gus_culture_participation", "gus_tourism"]
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    try:
        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        for source in sources:
            series = importlib.import_module(f"ingestion.to_landing.{source}").SERIES
            land = f"{SCRATCH}/01_landing"
            for attempt in (1, 2):
                print(f"\n##### {source} landing extract {attempt}", flush=True)
                gp.extract(series, 60, landing_root=land)
            entities = sorted(gp.held(series, land))
            for attempt in (1, 2):
                print(f"\n##### {source} bronze load {attempt}", flush=True)
                gp.bronze_load(source, entities, landing_root=land, namespace=SCRATCH)
            cells = cat.load_table((SCRATCH, f"{source}_cells")).scan().to_arrow()
            try:
                rejected = cat.load_table((SCRATCH, f"{source}_rejected_files")).scan().to_arrow().to_pylist()
            except Exception:
                rejected = []
            con.register(f"{source}_cells", cells)
            silver_path = next((ROOT / f"warehouse/models/silver/{source}").glob("*_tables.sql"))
            con.sql(f"create or replace table {source}_tables as " + render(silver_path, {f"{source}_cells": f"{source}_cells"}))
            con.sql(f"""create or replace table geo_{source} as
                select '{source}' as source_system, lower(name_pl) as source_code, 'TERC:' || terc_code as geography_key
                from terc where unit_level = 'wojewodztwo'
                union all select '{source}', 'polska', 'PL'""")
            gsql = render(ROOT / f"warehouse/models/gold/poc_build_{source}.sql",
                          {f"{source}_tables": f"{source}_tables", "poc_map_geography": f"geo_{source}"})
            con.sql(f"create or replace table gold_{source} as " + gsql)
            n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
            s, g = f"{source}_tables", f"gold_{source}"
            stats = {
                "editions": entities,
                "bronze_rows": cells.num_rows,
                "rejected": [(r.get("entity"), (r.get("error") or "")[:80]) for r in rejected],
                "silver_rows": n(f"select count(*) from {s}"),
                "silver_dup": n(f"select count(*) from (select 1 from {s} group by table_code, measure_code, area_name, group_code, period_year having count(*) > 1)"),
                "silver_null_keys": n(f"select count(*) from {s} where table_code is null or area_name is null or period_year is null or raw_sha256 is null"),
                "status": n(f"select string_agg(cell_status || '=' || c, ' ') from (select cell_status, count(*) c from {s} group by 1)"),
                "per_measure_years": n(f"select string_agg(x, '; ' order by x) from (select measure_code || ' ' || string_agg(period_year || ':' || c, ',' order by period_year) x from (select measure_code, period_year, count(*) c from {s} group by all) group by measure_code)"),
                "gold_rows": n(f"select count(*) from {g}"),
                "gold_dup_series_date": n(f"select count(*) from (select 1 from {g} group by measure_key, geography_key, breakdown_json, date_key having count(*) > 1)"),
                "gold_unmatched": n(f"select count(*) from {g} where geography_key = '_X'"),
            }
            print("STATS", source, stats, flush=True)
            annotate("notice", f"{source} trial", stats)
            spot = n(f"select string_agg(measure_codes || ' ' || period_label || ' ' || coalesce(sex_code, '') || '=' || value, '; ' order by measure_codes, period_label) from {g} where geography_key = 'PL' and is_total")
            annotate("notice", f"{source} trial Poland", {"values": spot[:3500] if spot else None})
            show(con, f"{source} matched sheets", f"select entity, table_code, member, sheet_name, any_value(header_path) hp, count(*) from {s} group by all order by all")
            show(con, f"{source} gold sample", f"select measure_key, geography_key, period_label, sex_code, value, observation_status_key from {g} order by all limit 60")
            show(con, f"{source} non-ok cells", f"select entity, table_code, area_name, group_code, value_text, cell_status from {s} where cell_status <> 'ok' limit 40")
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
        annotate("notice", "w3gus trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
