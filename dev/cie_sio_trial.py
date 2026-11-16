"""Dev trial of cie_sio (PLS-022) on the branch's final code: Landing and Bronze into zz_dev_sio/ (both buckets) and
namespace zz_dev_sio, each run twice; then the Silver models, their YAML tests and completeness tests, the Gold build
stage and the KPI value bound applied in memory (Jinja-rendered with the branch's macros). Reads silver.teryt_terc
(read-only) for the voivodeship geography keys. Everything written is removed at the end (finally). Deleted before
merge."""

from __future__ import annotations

from pathlib import Path
import re
import sys

import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze import cie_sio as bronze
from ingestion.to_landing import cie_sio as landing

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_sio"
MACROS = "".join((ROOT / f"warehouse/macros/{m}.sql").read_text() for m in ("xlsx_cells", "sources/cie_sio"))
MODELS = ("cie_sio_teachers", "cie_sio_schools", "cie_sio_graduates")


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def show(con, title, sql, limit=80):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:100] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def yaml_tests(con) -> dict[str, int]:
    """The schema tests of _cie_sio.yml (unique, not_null, accepted_values): failing rows per test."""
    spec = yaml.safe_load((ROOT / "warehouse/models/silver/cie_sio/_cie_sio.yml").read_text())
    out = {}
    for model in spec["models"]:
        name = model["name"]
        for t in model.get("tests", []):
            expr = t["unique"]["column_name"]
            out[f"{name}.unique"] = con.execute(f"select count(*) from (select {expr} k from {name} group by 1 having count(*) > 1)").fetchall()[0][0]
        for col in model.get("columns", []):
            for t in col.get("tests", []):
                if t == "not_null":
                    out[f"{name}.{col['name']}.not_null"] = con.execute(f"select count(*) from {name} where {col['name']} is null").fetchall()[0][0]
                elif isinstance(t, dict) and "accepted_values" in t:
                    vals = ", ".join("'" + str(v) + "'" for v in t["accepted_values"]["arguments"]["values"])
                    out[f"{name}.{col['name']}.accepted"] = con.execute(
                        f"select count(*) from {name} where {col['name']} is not null and {col['name']} not in ({vals})").fetchall()[0][0]
    return out


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    land = f"{SCRATCH}/01_landing"
    try:
        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        landed = {}
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            landing.extract(60, landing_root=land)
            landed[attempt] = dict(landing.held(land))
        entities = sorted(landed[2])
        files = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=req("STORAGE_LANDING_BUCKET_NAME"), Prefix=f"{land}/"):
            files += [o for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")]
        added = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            bronze.bronze_load(entities, landing_root=land, namespace=SCRATCH)
            added.append(len(cat.load_table((SCRATCH, "cie_sio_cells")).scan().to_arrow()))
        cells = cat.load_table((SCRATCH, "cie_sio_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "cie_sio_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:
            rejected = []
        con.register("cie_sio_cells", cells)
        annotate("notice", "cie_sio trial landing and bronze", {
            "entities": entities, "files": len(files), "bytes": sum(o["Size"] for o in files),
            "second_landing_changed": landed[1] != landed[2], "bronze_rows_after_load_1_and_2": added,
            "bronze_entities": con.execute("select count(distinct entity) from cie_sio_cells").fetchall()[0][0],
            "rejected": [(r.get("entity"), (r.get("error") or "")[:80]) for r in rejected],
        })
        tables = {"cie_sio_cells": "cie_sio_cells"}
        for m in MODELS:
            con.sql(f"create table {m} as " + render(ROOT / f"warehouse/models/silver/cie_sio/{m}.sql", tables))
            tables[m] = m
        tests = yaml_tests(con)
        for m in MODELS:
            tests[f"{m}_complete"] = len(con.sql(render(ROOT / f"warehouse/tests/{m}_complete.sql", tables)).fetchall())
            show(con, f"{m} completeness failures", render(ROOT / f"warehouse/tests/{m}_complete.sql", tables))
        failing = {k: v for k, v in tests.items() if v}
        n = lambda q: con.execute(q).fetchall()[0][0]  # noqa: E731
        silver = {m: n(f"select count(*) from {m}") for m in MODELS}
        silver["teachers_dates"] = n("select min(reference_date) || '..' || max(reference_date) from cie_sio_teachers")
        silver["graduates_years"] = n("select string_agg(distinct period_year::varchar, ',' order by period_year::varchar) from cie_sio_graduates")
        silver["non_numeric_values"] = {m: n(f"select count(*) from {m} where value is null") for m in MODELS}
        silver["non_numeric_texts"] = n("select string_agg(distinct value_text, ' ~ ') from (select value_text from cie_sio_teachers where value is null union all select value_text from cie_sio_schools where value is null union all select value_text from cie_sio_graduates where value is null)")
        silver["zeros"] = {m: n(f"select count(*) from {m} where value = 0") for m in MODELS}
        annotate("warning" if failing else "notice", "cie_sio trial silver", {**silver, "tests_run": len(tests), "failing": failing})
        con.sql("""create table geo as select 'cie_sio' as source_system, terc_code as source_code, 'TERC:' || terc_code as geography_key
                   from terc where unit_level = 'wojewodztwo' union all select 'cie_sio', 'PL', 'PL'""")
        tables["poc_map_geography"] = "geo"
        con.sql("create table gold as " + render(ROOT / "warehouse/models/gold/poc_build_cie_sio.sql", tables))
        gold = {
            "rows": n("select count(*) from gold"),
            "per_block": n("select string_agg(block_id || '=' || c, ' ') from (select block_id, count(*) c from gold group by 1)"),
            "dup_series_date": n("select count(*) from (select 1 from gold group by measure_key, geography_key, breakdown_json, date_key having count(*) > 1)"),
            "unmatched": n("select count(*) from gold where geography_key = '_X'"),
            "measures": n("select count(distinct measure_key) from gold"),
            "series": n("select count(distinct measure_key || geography_key || breakdown_json) from gold"),
            "kpi_rows_teachers": n("select count(*) from gold where measure_key = 'cie_sio:teachers:persons:persons' and is_total"),
            "kpi_out_of_bounds": n("select count(*) from gold where measure_key = 'cie_sio:teachers:persons:persons' and is_total and (value < 0 or value > 2e6)"),
            "max_rows_per_area_period_kpi": n("select max(c) from (select count(*) c from gold where measure_key = 'cie_sio:teachers:persons:persons' and is_total group by geography_key, date_key)"),
        }
        annotate("notice", "cie_sio trial gold", gold)
        spot = {
            "teachers_persons_PL": n("select string_agg(period_label || '=' || value, '; ' order by date_key) from gold where measure_key = 'cie_sio:teachers:persons:persons' and is_total and geography_key = 'PL'"),
            "teachers_posts_PL_2018": n("select max(value) from gold where measure_key = 'cie_sio:teachers:fte:posts' and is_total and geography_key = 'PL' and year(date_key) = 2018"),
            "teachers_persons_sum_voiv_2018": n("select sum(value) from gold where measure_key = 'cie_sio:teachers:persons:persons' and is_total and geography_key <> 'PL' and year(date_key) = 2018"),
            "graduates_PL_total_T": n("select string_agg(period_label || ' ' || cat || '=' || v, '; ' order by period_label, cat) from (select period_label, json_extract_string(breakdown_json, '$.pupil_category') cat, value v from gold where block_id = 'graduates' and geography_key = 'PL' and json_extract_string(breakdown_json, '$.school_type') = 'Razem' and sex_code = 'T')"),
            "graduates_PL_gimnazjum_001": n("select string_agg(period_label || '=' || value, '; ' order by period_label) from gold where block_id = 'graduates' and geography_key = 'PL' and json_extract_string(breakdown_json, '$.school_type') = 'Gimnazjum' and json_extract_string(breakdown_json, '$.pupil_category') = '001' and sex_code = 'T'"),
            "schools_sum_voiv_2018": n("select sum(value) from gold where measure_key = 'cie_sio:schools:schools:schools'"),
            "pupils_sum_voiv_2018": n("select sum(value) from gold where measure_key = 'cie_sio:schools:persons:pupils'"),
        }
        annotate("notice", "cie_sio trial Poland", {k: (v[:1500] if isinstance(v, str) else v) for k, v in spot.items()})
        show(con, "bronze per entity", "select entity, count(*) cells, count(distinct sheet_index) sheets, min(source_version) from cie_sio_cells group by all order by 1")
        show(con, "graduates per year x category x sex", "select period_year, pupil_category_code, sex_code, count(*), count(distinct area_code), count(distinct school_type_name) from cie_sio_graduates group by all order by all")
        show(con, "graduates school types", "select school_type_code, school_type_name, string_agg(distinct period_year::varchar, ',') from cie_sio_graduates group by all order by all", 200)
        show(con, "schools types and labels", "select school_type_code, school_type_name, pupil_category, publicness, governing_body, count(*) from cie_sio_schools group by all order by all", 200)
        show(con, "teachers sample", "select reference_date, area_code, area_name, measure_code, grade_code, value from cie_sio_teachers where grade_code = '_T' order by all", 200)
        for k, v in failing.items():
            print("FAILING", k, v)
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
        annotate("notice", "cie_sio trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
