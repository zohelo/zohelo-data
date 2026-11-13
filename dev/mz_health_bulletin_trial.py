"""Dev trial of mz_health_bulletin (#2957): Landing and Bronze into zz_dev_mzhb/ (both buckets) and namespace
zz_dev_mzhb, each twice; Landing verify; then the branch's Silver, Gold and test SQL applied in memory (Jinja-rendered
with warehouse/macros), with profiles, checks and spot values annotated. Reads silver.teryt_terc (read-only) for the
voivodeship names. Everything written is removed at the end (finally). Deleted before merge."""

from __future__ import annotations

from pathlib import Path
import re
import sys

import duckdb
import jinja2

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_mzhb"
SOURCE = "mz_health_bulletin"
MACROS = "".join((ROOT / f"warehouse/macros/{m}.sql").read_text()
                 for m in ("xlsx_cells", "gus_tables", "sources/mz_health_bulletin"))
# the Landing and Bronze modules write 5 notices first (extract x2, verify, Bronze x2); GitHub keeps 10 per level
COUNTS = {"notice": 5, "warning": 0, "error": 0}


def note(title, data, level="notice"):
    order = [level] + [lv for lv in ("notice", "warning", "error") if lv != level]
    for lv in order:
        if COUNTS[lv] < 10:
            COUNTS[lv] += 1
            annotate(lv, title, data)
            return
    print(title, data, flush=True)


def render(path: Path, tables: dict[str, str]) -> str:
    env = jinja2.Environment(extensions=["jinja2.ext.do"])
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return env.from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def main(argv) -> int:
    from ingestion.to_bronze import mz_health_bulletin as tb
    from ingestion.to_landing import mz_health_bulletin as tl
    from ingestion.to_landing.patterns import bulk_file

    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    land = f"{SCRATCH}/01_landing"
    out: dict = {}
    try:
        found, summary = tl.discover()
        out["page"] = summary
        for attempt in (1, 2):
            rc = tl.extract(60, landing_root=land)
            out[f"landing_extract_{attempt}_rc"] = rc
        held = tl.held(land)
        lb = req("STORAGE_LANDING_BUCKET_NAME")
        receipts = bulk_file.landed(client, lb, f"{land}/{SOURCE}/snapshot/data/")
        out["landing_files"] = len(receipts)
        out["landing_bytes"] = sum(r["size_bytes"] for r in receipts)
        out["landing_entities"] = sorted(held)
        out["landing_verify_rc"] = bulk_file.main(SOURCE, "verify", landing_root=land,
                                                  specs=[{"entity": e} for e in sorted(held)])
        for attempt in (1, 2):
            tb.bronze_load(sorted(held), landing_root=land, namespace=SCRATCH)
        cells_t = cat.load_table((SCRATCH, f"{SOURCE}_cells"))
        cells = cells_t.scan().to_arrow()
        snaps = len(list(cells_t.metadata.snapshots))
        try:
            rejected = cat.load_table((SCRATCH, f"{SOURCE}_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:
            rejected = []
        con.register("cells_raw", cells)
        con.sql("create table cells as select * from cells_raw")
        n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
        out["bronze_rows"] = cells.num_rows
        out["bronze_snapshots"] = snaps
        out["bronze_files"] = n("select count(distinct raw_sha256) from cells")
        out["bronze_rows_per_entity"] = n("select string_agg(entity || ':' || c || '/' || s, ' ' order by entity) from (select entity, count(*) c, count(distinct sheet_name) s from cells group by 1)")
        out["landing_sha_not_in_bronze"] = [r["entity"] for r in receipts if r["sha256"] not in set(con.sql("select distinct raw_sha256 from cells").fetchnumpy()["raw_sha256"].tolist())]
        out["rejected"] = [(r.get("entity"), (r.get("error") or "")[:100]) for r in rejected]

        out["bronze_profile"] = n("select string_agg(x, ' ') from (select cell_type || '=' || count(*) x from cells group by cell_type)")
        out["bronze_null_value_text"] = n("select count(*) from cells where value_text is null")
        out["bronze_sheets"] = n("select count(distinct entity || '|' || sheet_name) from cells")
        note("mzhb landing+bronze", out)

        d = ROOT / "warehouse/models/silver/mz_health_bulletin"
        sheets_sql = render(d / "mz_health_bulletin_sheets.sql", {f"{SOURCE}_cells": "cells"})
        con.sql("create table sheets as " + sheets_sql)
        silver_sql = render(d / "mz_health_bulletin_tables.sql",
                            {f"{SOURCE}_cells": "cells", "mz_health_bulletin_sheets": "sheets"})
        con.sql("create table silver as " + silver_sql)
        con.sql("create table silver2 as " + silver_sql)
        sh = {
            "sheet_rows": n("select count(*) from sheets"),
            "status": n("select string_agg(status || '=' || c, ' ') from (select status, count(*) c from sheets group by 1)"),
            "dup_entity_sheet": n("select count(*) from (select 1 from sheets group by entity, sheet_name having count(*) > 1)"),
            "null_keys": n("select count(*) from sheets where entity is null or edition_year is null or sheet_name is null or status is null or raw_sha256 is null"),
            "rejected": n("select string_agg(edition_year || ' ' || sheet_name || ': ' || reason, '; ') from sheets where status = 'rejected'"),
            "not_modelled_reasons": n("select string_agg(reason || ' (' || c || ')', '; ') from (select reason, count(*) c from sheets where status = 'not_modelled' group by 1)"),
            "several_years": n("select string_agg(edition_year || ' ' || sheet_name, ', ') from sheets where reason like 'licensed pharmacists%'"),
            "modelled_per_edition": n("select string_agg(x, ' ' order by x) from (select edition_year || ':' || count(*) || '>' || max(period_year) x from sheets where status = 'modelled' group by edition_year)"),
            "title_year_differs": n("select string_agg(edition_year || ' ' || sheet_name || ' title ' || coalesce(cast(title_year as varchar), 'none') || '->' || coalesce(cast(period_year as varchar), 'none'), '; ') from sheets where title_year_differs and status = 'modelled'"),
            "accepted_table_kind": n("select count(*) from sheets where table_kind not in ('licensed', 'employed', 'hospitals', 'hospital_activity')"),
        }
        complete_s = render(ROOT / "warehouse/tests/mz_health_bulletin_sheets_complete.sql", {"mz_health_bulletin_sheets": "sheets"})
        sh["complete_failures"] = n(f"select count(*) from ({complete_s})")
        note("mzhb silver sheets", sh)

        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        con.sql(f"""create table geo as select '{SOURCE}' as source_system, lower(name_pl) as source_code,
                    'TERC:' || terc_code as geography_key from terc where unit_level = 'wojewodztwo'
                    union all select '{SOURCE}', 'polska', 'PL'""")
        gold_sql = render(ROOT / "warehouse/models/gold/poc_build_mz_health_bulletin.sql",
                          {"mz_health_bulletin_tables": "silver", "poc_map_geography": "geo"})
        con.sql("create table gold as " + gold_sql)
        s = {
            "silver_rows": n("select count(*) from silver"),
            "silver_rebuild_same": n("select count(*) from (select * from silver except all select * from silver2)") == 0,
            "silver_dup_grain": n("select count(*) from (select 1 from silver group by edition_year, table_kind, profession_code, measure_code, area_name having count(*) > 1)"),
            "silver_null_keys": n("select count(*) from silver where edition_year is null or table_kind is null or profession_code is null or measure_code is null or unit_code is null or area_name is null or period_year is null or cell_status is null or is_latest_edition is null or raw_sha256 is null"),
            "cell_status": n("select string_agg(cell_status || '=' || c, ' ') from (select cell_status, count(*) c from silver group by 1)"),
            "non_numeric_value_text": n("select string_agg(distinct value_text, ' | ') from silver where cell_status not in ('ok', 'repeated')"),
            "value_null_where_ok": n("select count(*) from silver where cell_status = 'ok' and value is null"),
            "value_not_null_where_not_ok": n("select count(*) from silver where cell_status <> 'ok' and value is not null"),
            "repeated": n("select string_agg(distinct edition_year || ' ' || sheet_name || ' (' || period_year || ') = ' || repeated_from, '; ') from silver where cell_status = 'repeated'"),
            "editions_years": n("select string_agg(x, ' ' order by x) from (select edition_year || '>' || table_kind || ':' || period_year || '(' || count(*) || ')' x from silver group by edition_year, table_kind, period_year)"),
            "keys_in_two_editions": n("select count(*) from (select 1 from silver group by table_kind, profession_code, measure_code, area_name, period_year having count(*) > 1)"),
            "measures": n("select count(distinct table_kind || profession_code || measure_code) from silver"),
            "areas": n("select count(distinct area_name) from silver"),
            "period_range": n("select min(period_year) || '-' || max(period_year) from silver"),
        }
        accepted = {
            "table_kind": ["licensed", "employed", "hospitals", "hospital_activity"],
            "measure_code": ["count", "per_10k", "per_bed", "mean", "days", "pct"],
            "cell_status": ["ok", "dash", "missing", "not_applicable", "repeated"],
            "unit_code": ["persons", "per_10k_population", "hospitals", "beds", "cases", "cases_per_bed",
                          "thousand_patient_days", "days", "pct"],
            "profession_code": ["doctors", "dentists", "pharmacists", "lab_diagnosticians", "nurses", "midwives",
                                "physiotherapists", "paramedics", "dietitians", "hospitals", "beds", "inpatients",
                                "patient_days", "length_of_stay", "bed_occupancy"],
        }
        for c, vals in accepted.items():
            s[f"accepted_{c}"] = n(f"select count(*) from silver where {c} not in ({', '.join(repr(v) for v in vals)})")
        complete = render(ROOT / "warehouse/tests/mz_health_bulletin_tables_complete.sql", {"mz_health_bulletin_tables": "silver"})
        s["complete_failures"] = n(f"select count(*) from ({complete})")
        s["complete_rows"] = n(f"select string_agg(measure || ' ' || period_year || ' ' || found, '; ') from ({complete})")
        note("mzhb silver tables", s)
        vr = render(ROOT / "warehouse/tests/mz_health_bulletin_value_range.sql", {"poc_build_mz_health_bulletin": "gold"})
        g = {
            "gold_rows": n("select count(*) from gold"),
            "gold_measures": n("select count(distinct measure_key) from gold"),
            "gold_dup_series_date": n("select count(*) from (select 1 from gold group by measure_key, geography_key, breakdown_json, date_key having count(*) > 1)"),
            "gold_not_null": n("select count(*) from gold where measure_key is null or date_key is null or geography_key is null or value is null or raw_sha256 is null"),
            "gold_unmatched_geo": n("select count(*) from gold where geography_key = '_X'"),
            "gold_geos": n("select count(distinct geography_key) from gold"),
            "date_range": n("select min(date_key) || ' ' || max(date_key) from gold"),
            "value_range_failures": n(f"select count(*) from ({vr})"),
            "value_range_rows": n(f"select string_agg(measure_key || ' ' || geography_key || ' ' || date_key || '=' || value, '; ') from (select * from ({vr}) limit 10)"),
            "repeated_2020_in_gold": n("select count(*) from gold where period_label = '2020' and measure_codes in ('employed.physiotherapists.count', 'employed.paramedics.count', 'employed.dietitians.count')"),
            "spot_PL": n("select string_agg(measure_codes || ' ' || period_label || '=' || value, '; ' order by measure_codes, period_label) from gold where geography_key = 'PL' and measure_codes in ('employed.doctors.count', 'hospitals.beds.count', 'employed.physiotherapists.count') and period_label in ('2018', '2019', '2020', '2023', '2024')"),
            "spot_voiv_2024": n("select string_agg(measure_codes || ' ' || geography_key || '=' || value, '; ' order by measure_codes, geography_key) from gold where period_label = '2024' and geography_key in ('TERC:1400000', 'TERC:3000000') and measure_codes in ('employed.doctors.count', 'hospitals.beds.count', 'licensed.nurses.count')"),
            "spot_PL_2024_drops": n("select string_agg(measure_codes || ' ' || period_label || '=' || value, '; ' order by measure_codes, period_label) from gold where geography_key = 'PL' and measure_codes in ('employed.dentists.count', 'employed.lab_diagnosticians.count') and period_label in ('2023', '2024')"),
        }
        note("mzhb gold", g)
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
        note("mzhb trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
