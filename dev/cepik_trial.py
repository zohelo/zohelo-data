"""Dev trial of cepik (#2966) on the final code: Landing and Bronze into zz_dev_cepik/ (both buckets) and namespace
zz_dev_cepik, each twice; then the branch's Silver SQL, its tests (unique, not_null, accepted_values from _cepik.yml,
the completeness test), the Gold build stage and the metrics bound applied in memory (Jinja-rendered with
warehouse/macros/xlsx_cells.sql). Reads silver.teryt_terc (read-only) for the voivodeship names. Everything written
is removed at the end (finally). Deleted before merge."""

from __future__ import annotations

from pathlib import Path
import re
import sys

import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common import gus_publications as gp
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing import cepik as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_cepik"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/xlsx_cells.sql").read_text()
SILVER = ROOT / "warehouse/models/silver/cepik/cepik_vehicle_registrations.sql"


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def landed_state(client, bucket) -> list[tuple[str, int]]:
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{LAND}/"):
        out += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return sorted(out)


def main(argv) -> int:
    client = s3()
    land_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    try:
        # Landing twice: the second run must write nothing
        states = []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            rc = landing.extract(60, landing_root=LAND)
            states.append(landed_state(client, land_bucket))
            print("exit", rc, "objects", len(states[-1]), flush=True)
        entities = sorted(landing.held(LAND))
        verify_rc = bulk_file.main("cepik", "verify", landing_root=LAND, specs=[{"entity": e} for e in entities])
        landing_stats = {
            "files": len([k for k, _ in states[0] if not k.endswith(".receipt.json")]),
            "bytes": sum(s for k, s in states[0] if not k.endswith(".receipt.json")),
            "entities": len(entities),
            "first_last": [entities[0], entities[-1]] if entities else None,
            "second_run_unchanged": states[0] == states[1],
            "verify_exit": verify_rc,
        }
        annotate("notice", "cepik trial landing", landing_stats)

        # Bronze twice: the second run must add nothing
        snapshots = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            gp.bronze_load("cepik", entities, landing_root=LAND, namespace=SCRATCH)
            table = cat.load_table((SCRATCH, "cepik_cells"))
            snap = table.current_snapshot()
            snapshots.append((snap.snapshot_id if snap else None, table.scan().to_arrow().num_rows))
        cells = cat.load_table((SCRATCH, "cepik_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "cepik_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # the table exists only after a rejection
            rejected = []
        con.register("cepik_cells", cells)
        bronze_stats = {
            "rows": cells.num_rows,
            "entities": n("select count(distinct entity) from cepik_cells"),
            "sheets": n("select string_agg(distinct sheet_name, ',') from cepik_cells"),
            "rejected": [(r.get("entity"), (r.get("error") or "")[:80]) for r in rejected],
            "second_run_unchanged": snapshots[0] == snapshots[1],
            "snapshots": snapshots,
        }
        annotate("notice", "cepik trial bronze", bronze_stats)

        # Silver and its tests
        con.sql("create table s as " + render(SILVER, {"cepik_cells": "cepik_cells"}))
        spec = yaml.safe_load((ROOT / "warehouse/models/silver/cepik/_cepik.yml").read_text())["models"][0]
        failures = {}
        failures["unique"] = n(
            "select count(*) from (select 1 from s group by table_code, group_code, period_year, period_month having count(*) > 1)"
        )
        for column in spec["columns"]:
            for test in column["tests"]:
                if test == "not_null":
                    failures[f"not_null {column['name']}"] = n(f"select count(*) from s where {column['name']} is null")
                elif isinstance(test, dict) and "accepted_values" in test:
                    values = test["accepted_values"]["arguments"]["values"]
                    con.execute("create or replace temp table allowed as select unnest(?) as v", [values])
                    failures[f"accepted_values {column['name']}"] = n(
                        f"select count(*) from s where {column['name']} not in (select v from allowed)"
                    )
        complete = render(ROOT / "warehouse/tests/cepik_vehicle_registrations_complete.sql", {"cepik_vehicle_registrations": "s"})
        failures["complete"] = n(f"select count(*) from ({complete})")
        silver_stats = {
            "rows": n("select count(*) from s"),
            "per_table": n("select string_agg(table_code || '=' || c || ' ' || f || '..' || l, '; ') from (select table_code, count(*) c, min(period_date) f, max(period_date) l from s group by 1)"),
            "test_failures": {k: v for k, v in failures.items() if v} or "none",
            "tests_run": len(failures),
            "check_status": n("select string_agg(check_status || '=' || c, ' ') from (select check_status, count(*) c from s group by 1)"),
            "order_suspect": n("select string_agg(distinct entity || ' month ' || period_month, ', ') from s where check_status <> 'ok'"),
            "non_numeric_value_cells": n("select count(*) from s where value is null"),
        }
        annotate("notice" if not any(failures.values()) else "error", "cepik trial silver", silver_stats)

        # markers: placeholder zeros of the national row left out, text cells in value positions, totals
        markers = {
            "national_zero_cells_dropped": n("""with r as (select entity, sheet_name, row_number from cepik_cells where column_number = 1 and trim(value_text) = 'I')
                select count(*) from cepik_cells c join r on c.entity = r.entity and c.sheet_name = r.sheet_name and c.row_number = r.row_number + 1
                where try_cast(c.value_text as double) = 0"""),
            "text_cells_in_columns_2_plus": n("select string_agg(distinct trim(value_text), ' | ') from cepik_cells where column_number > 1 and try_cast(value_text as double) is null and length(trim(value_text)) <= 3"),
            "zero_values_kept": n("select count(*) from s where value = 0"),
            "pl_vs_voiv_sum": n("""select string_agg(p.period_date || ' ' || p.value || '/' || v.t, '; ') from (select period_date, value from s where group_code = 'polska') p
                join (select period_date, sum(value) t from s where table_code = 'voivodeship' and group_code <> 'polska' group by 1) v using (period_date) where p.value <> v.t"""),
            "pl_vs_type_sum": n("""select string_agg(p.period_date || ' ' || p.value || '/' || v.t, '; ') from (select period_date, value from s where group_code = 'polska') p
                join (select period_date, sum(value) t from s where table_code = 'vehicle_type' group by 1) v using (period_date) where p.value <> v.t"""),
            "intro_texts": n("select string_agg(distinct entity || ': ' || value_text, ' || ') from cepik_cells where length(value_text) > 40"),
        }
        annotate("notice", "cepik trial markers", markers)

        # Gold build stage, with the TERYT names as poc_map_geography (geo of the source's geography macro)
        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        con.sql("""create table geo as select 'cepik' as source_system, lower(name_pl) as source_code, 'TERC:' || terc_code as geography_key
            from terc where unit_level = 'wojewodztwo' union all select 'cepik', 'polska', 'PL'""")
        con.sql("create table g as " + render(ROOT / "warehouse/models/gold/poc_build_cepik.sql",
                                              {"cepik_vehicle_registrations": "s", "poc_map_geography": "geo"}))
        series = "measure_key || '|' || geography_key || '|' || breakdown_json"
        gold_stats = {
            "rows": n("select count(*) from g"),
            "totals": n("select count(*) from g where is_total"),
            "by_type": n("select count(*) from g where not is_total"),
            "dup_series_date": n(f"select count(*) from (select 1 from g group by {series}, date_key having count(*) > 1)"),
            "unmatched_geo": n("select count(*) from g where geography_key = '_X'"),
            "geographies": n("select count(distinct geography_key) from g"),
            "metrics_rows": n("select count(*) from g where is_total and geography_key <> '_X'"),
            "metrics_max_per_area_period": n("select max(c) from (select count(*) c from g where is_total group by geography_key, date_key)"),
            "metrics_out_of_bounds_0_1e6": n("select count(*) from g where is_total and (value < 0 or value > 1e6)"),
            "max_value": n("select max(value) from g where is_total"),
        }
        annotate("notice", "cepik trial gold", gold_stats)
        spot = n("""select string_agg(geography_source_code || ' ' || period_label || '=' || cast(value as bigint), '; ' order by geography_source_code, period_label)
            from g where is_total and geography_source_code in ('polska', 'mazowieckie', 'łódzkie', 'opolskie')
            and period_label in ('2016-01', '2020-12', '2025-12', '2026-08', '2026-09')""")
        spot_type = n("""select string_agg(json_extract_string(breakdown_json, '$.vehicle_type') || ' ' || period_label || '=' || cast(value as bigint), '; ')
            from g where not is_total and period_label in ('2026-09') and json_extract_string(breakdown_json, '$.vehicle_type') in ('samochód osobowy', 'motocykl', 'autobus')""")
        annotate("notice", "cepik trial spot values", {"totals": spot, "poland_by_type_2026_09": spot_type})
        print("STATS", landing_stats, bronze_stats, silver_stats, markers, gold_stats, flush=True)
    finally:
        removed = 0
        for bucket in (land_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
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
        except Exception as exc:  # report, do not hide
            print("drop failed", exc)
        left = sum(len(p.get("Contents", [])) for b in (land_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
                   for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"))
        annotate("notice", "cepik trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
