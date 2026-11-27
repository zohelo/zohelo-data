"""Dev trial of gios_surface_water (#3312) on the branch's code: Landing and Bronze into zz_dev_3312/ (both buckets) and
namespace zz_dev_3312, each twice; Bronze profiled before Silver; then the branch's Silver SQL and its tests (unique,
not_null, accepted_values from _gios_surface_water.yml, the completeness test), the Gold build stage with its tests,
the grain check of tests/fact_grain.sql, the KPI rows with the value-range bound, and the licence gate of
warehouse/publish.py on the branch's code lists, applied in memory (Jinja-rendered with
warehouse/macros/sources/gios_surface_water.sql). Everything written is removed at the end (finally). Never merged."""

from __future__ import annotations

import csv
from pathlib import Path
import re
import sys
import time

import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gios_surface_water import load as bronze
from ingestion.to_landing import gios_surface_water as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_3312"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/sources/gios_surface_water.sql").read_text()
SILVER = ROOT / "warehouse/models/silver/gios_surface_water"
SERIES = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
          "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def landed_state(client, bucket) -> list[tuple[str, int]]:
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{LAND}/"):
        out += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return sorted(out)


def yml_tests(con, n, path: Path) -> dict[str, int]:
    """unique, not_null and accepted_values of a schema file, run on in-memory tables."""
    failures = {}
    for model in yaml.safe_load(path.read_text())["models"]:
        name = model["name"]
        if not n(f"select count(*) from information_schema.tables where table_name = '{name}'"):
            continue
        for test in model.get("tests", []):
            if "unique" in test:
                expr = test["unique"]["column_name"]
                failures[f"{name} unique"] = n(f"select count(*) from (select 1 from {name} group by {expr} having count(*) > 1)")
        for column in model.get("columns", []):
            for test in column.get("tests", []):
                c = column["name"]
                if test == "not_null":
                    failures[f"{name} not_null {c}"] = n(f"select count(*) from {name} where {c} is null")
                elif isinstance(test, dict) and "accepted_values" in test:
                    values = [str(v) for v in test["accepted_values"]["arguments"]["values"]]
                    con.execute("create or replace temp table allowed as select unnest(?) as v", [values])
                    failures[f"{name} accepted_values {c}"] = n(
                        f"select count(*) from {name} where {c} is not null and cast({c} as varchar) not in (select v from allowed)")
    return failures


def main(argv) -> int:
    client = s3()
    land_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    try:
        states, codes, seconds = [], [], []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            started = time.monotonic()
            codes.append(landing.extract(60, landing_root=LAND))
            seconds.append(round(time.monotonic() - started))
            states.append(landed_state(client, land_bucket))
        entities = landing.held(LAND)
        verify_rc = bulk_file.main("gios_surface_water", "verify", landing_root=LAND, specs=[{"entity": e} for e in entities])
        files = [(k, s) for k, s in states[0] if not k.endswith(".receipt.json")]
        landing_stats = {
            "files": len(files), "bytes": sum(s for _, s in files), "largest": max(s for _, s in files),
            "pages_per_layer": {lay: sum(1 for e in entities if e.rsplit("_p", 1)[0] == lay) for lay in landing.LAYERS},
            "exit_codes": codes, "seconds": seconds, "second_run_unchanged": states[0] == states[1],
            "verify_exit": verify_rc,
        }
        annotate("notice" if states[0] == states[1] and not any(codes) else "error", "gios_surface_water trial landing", landing_stats)

        snapshots = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            bronze.load(60, landing_root=LAND, namespace=SCRATCH)
            snap = cat.load_table((SCRATCH, "gios_surface_water_features")).current_snapshot()
            snapshots.append(snap.snapshot_id if snap else None)
        rows = cat.load_table((SCRATCH, "gios_surface_water_features")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "gios_surface_water_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001  the table exists only after a rejection
            rejected = []
        con.register("b", rows)
        con.sql("create table bl as select *, regexp_extract(entity, '^(.*)_p[0-9]{2}$', 1) as layer from b")
        bronze_stats = {
            "rows": rows.num_rows,
            "files": n("select count(distinct raw_sha256) from b"),
            "rejected": [(x.get("entity"), (x.get("error") or "")[:300]) for x in rejected],
            "second_run_unchanged": snapshots[0] == snapshots[1],
            "per_layer": n("select string_agg(layer || '=' || c, ' ' order by layer) from (select layer, count(*) c from bl group by 1)"),
        }
        profile = {
            "distinct_codes": n("select count(distinct kod_jcwp) from b"),
            "null_codes": n("select count(*) from b where kod_jcwp is null or kod_jcwp = ''"),
            "code_prefixes": n("select string_agg(p || '=' || c, ' ') from (select left(kod_jcwp, 4) p, count(*) c from b group by 1)"),
            "s_p_eko": n("select string_agg(coalesce(v, 'NULL') || '=' || c, '; ' order by c desc) from (select s_p_eko v, count(*) c from bl where layer like '%ecological%' group by 1)"),
            "stan_chem": n("select string_agg(coalesce(v, 'NULL') || '=' || c, '; ' order by c desc) from (select stan_chem v, count(*) c from bl where layer like '%chemical%' group by 1)"),
            "ocena": n("select string_agg(coalesce(v, 'NULL') || '=' || c, '; ' order by c desc) from (select ocena v, count(*) c from bl where layer like '%overall%' group by 1)"),
            "status": n("select string_agg(coalesce(v, 'NULL') || '=' || c, '; ' order by c desc) from (select status v, count(*) c from b group by 1)"),
            "empty_or_blank_class": n("""select count(*) from bl where trim(coalesce(case when layer like '%ecological%' then s_p_eko
                when layer like '%chemical%' then stan_chem else ocena end, '')) = ''"""),
            "null_names": n("select count(*) from b where nazwa_jcwp is null"),
        }
        bronze_stats["profile"] = profile
        annotate("notice" if not rejected else "error", "gios_surface_water trial bronze", bronze_stats)

        tables = {"gios_surface_water_features": "b"}
        s_sql = render(SILVER / "gios_surface_water_status.sql", tables)
        con.sql("create table gios_surface_water_status as " + s_sql)
        s = "gios_surface_water_status"
        failures = yml_tests(con, n, SILVER / "_gios_surface_water.yml")
        test = render(ROOT / "warehouse/tests/gios_surface_water_status_complete.sql", {s: s})
        failures[f"{s} complete"] = n(f"select count(*) from ({test})")
        silver_stats = {
            "rows": n(f"select count(*) from {s}"),
            "rows_rebuild": n(f"select count(*) from ({s_sql})"),
            "dup_grain": n(f"select count(*) from (select 1 from {s} group by water_category, assessment, kod_jcwp having count(*) > 1)"),
            "null_keys": n(f"select count(*) from {s} where kod_jcwp is null or status_class is null or raw_sha256 is null"),
            "per_layer": n(f"""select string_agg(water_category || '.' || assessment || '=' || c, ' ' order by 1) from (select water_category,
                assessment, count(*) c from {s} group by 1, 2)"""),
            "classes": n(f"""select string_agg(assessment || ':' || status_class || '=' || c, '; ' order by assessment, c desc) from
                (select assessment, status_class, count(*) c from {s} group by 1, 2)"""),
            "codes_in_two_categories": n(f"""select count(*) from (select assessment, kod_jcwp from {s} group by 1, 2
                having count(distinct water_category) > 1)"""),
            "tests_run": len(failures),
            "test_failures": {k: v for k, v in failures.items() if v} or "none",
        }
        annotate("notice" if not any(failures.values()) else "error", "gios_surface_water trial silver", silver_stats)

        macro = jinja2.Environment().from_string(MACROS + "{{ poc_geography__gios_surface_water() }}").render()
        con.sql(f"create table map as with {macro} select * from gios_surface_water")
        g = "poc_build_gios_surface_water"
        g_sql = render(ROOT / f"warehouse/models/gold/{g}.sql", {s: s, "poc_map_geography": "map"})
        con.sql(f"create table {g} as " + g_sql)
        gold_failures = yml_tests(con, n, ROOT / "warehouse/models/gold/_gios_surface_water.yml")
        gold_stats = {
            "rows": n(f"select count(*) from {g}"),
            "rows_rebuild": n(f"select count(*) from ({g_sql})"),
            "measures": n(f"select count(distinct measure_key) from {g}"),
            "series": n(f"select count(distinct {SERIES}) from {g}"),
            "dup_series_date": n(f"select count(*) from (select 1 from {g} group by {SERIES}, date_key having count(*) > 1)"),
            "frequencies_per_measure": n(f"select max(c) from (select count(distinct frequency) c from {g} group by measure_key)"),
            "unmatched_rows": n(f"select count(*) from {g} where geography_key = '_X'"),
            "dates": n(f"select string_agg(distinct date_key || ' ' || period_label || ' ' || frequency, ',') from {g}"),
            "zero_values": n(f"select count(*) from {g} where value = 0"),
            "share_range": n(f"select min(value) || '..' || max(value) from {g} where unit_code = '%'"),
            "class_sum_eq_total": n(f"""select count(*) from (select measure_key, sum(value) v from {g} where unit_code = 'water_bodies'
                and not is_total group by 1) x join {g} t using (measure_key) where t.is_total and x.v <> t.value"""),
            "all_eq_sum": n(f"""select count(*) from (select split_part(measure_codes, '.', 2) a, sum(value) v from {g}
                where unit_code = 'water_bodies' and is_total and measure_codes not like 'all.%' group by 1) x join {g} t
                on t.measure_codes = 'all.' || x.a and t.unit_code = 'water_bodies' and t.is_total where x.v <> t.value"""),
            "test_failures": {k: v for k, v in gold_failures.items() if v} or "none",
        }
        spot = n(f"""select string_agg(measure_codes || ' ' || unit_code || '=' || round(value, 2), '; ' order by measure_codes, unit_code)
            from {g} where is_total""")
        km = list(csv.DictReader((ROOT / "warehouse/seed_rows/kpi_measure/gios_surface_water.csv").open()))
        pattern = km[0]["measure_key_pattern"]
        kpi = {
            "pattern": pattern,
            "rows": n(f"select count(*) from {g} where is_total and measure_key like '{pattern}'"),
            "max_per_area_period": n(f"""select max(c) from (select count(*) c from {g} where is_total and measure_key like '{pattern}'
                group by source_system, geography_key, date_key)"""),
            "value": n(f"select string_agg(geography_key || ' ' || period_label || '=' || round(value, 3), ' ') from {g} where is_total and measure_key like '{pattern}'"),
            "outside_bound_0_100": n(f"select count(*) from {g} where is_total and measure_key like '{pattern}' and value not between 0 and 100"),
        }
        from warehouse import publish

        sources = publish.seed("source")
        licences = publish.seed("licence")
        src = [r for r in sources if r["source_key"] == "gios_surface_water"]
        lic = [r for r in licences if src and r["licence_key"] == src[0]["licence_key"]]
        kpi["catalogue"] = {"source_row": {k: src[0][k] for k in ("source_key", "register_id", "licence_key")} if src else "MISSING",
                            "licence_row": {k: lic[0][k] for k in ("licence_key", "name", "commercial_ok")} if lic else "MISSING",
                            "licensed_by_gate": "gios_surface_water" in publish.licensed_sources(sources, licences)}
        gold_stats.update({"spot_pl_totals": spot[:1500], "kpi": kpi})
        bad = any(gold_failures.values()) or gold_stats["dup_series_date"] or gold_stats["unmatched_rows"]
        annotate("error" if bad else "notice", "gios_surface_water trial gold", gold_stats)
        print("STATS", landing_stats, bronze_stats, silver_stats, gold_stats, flush=True)
        print("SPOT", spot, flush=True)
        print("CLASSES", n(f"select string_agg(measure_codes || ' ' || unit_code || ' ' || breakdown_json || '=' || round(value, 2), chr(10) order by 1) from {g}"), flush=True)
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
        except Exception as exc:  # noqa: BLE001  report, do not hide
            print("drop failed", exc)
        left = sum(len(p.get("Contents", [])) for b in (land_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
                   for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"))
        annotate("notice", "gios_surface_water trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
