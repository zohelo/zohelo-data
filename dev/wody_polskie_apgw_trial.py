"""Dev trial of wody_polskie_apgw (#3313) on the branch's code: Landing and Bronze into zz_dev_3313/ (both buckets) and
namespace zz_dev_3313, each twice; Bronze profiled before Silver; then the branch's Silver SQL and its tests (unique,
not_null, accepted_values from _wody_polskie_apgw.yml, the three completeness tests), the Gold build stage with its
tests, the grain check by series_key x date_key, spot values and the source/licence rows, applied in memory
(Jinja-rendered with warehouse/macros/sources/wody_polskie_apgw.sql). Reads silver.teryt_terc (read-only) for the
voivodeships of the geography map. Inner activity annotations go to the log only (GitHub keeps 10 per step); the
trial's own annotations are compact. Everything written is removed at the end (finally). Never merged."""

from __future__ import annotations

import csv
import os
from pathlib import Path
import re
import sys
import time

import duckdb
import jinja2
import requests
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.wody_polskie_apgw import load as bronze
from ingestion.to_landing import wody_polskie_apgw as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_3313"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/sources/wody_polskie_apgw.sql").read_text()
SILVER = ROOT / "warehouse/models/silver/wody_polskie_apgw"
MODELS = ["wody_polskie_apgw_surface_water_bodies", "wody_polskie_apgw_groundwater_bodies", "wody_polskie_apgw_planning_units"]


def quiet(fn, *args, **kwargs):
    """Run an activity with its annotations printed to the log instead (GitHub keeps 10 notices per step)."""
    os.environ["GITHUB_ACTIONS"] = "false"
    try:
        return fn(*args, **kwargs)
    finally:
        os.environ["GITHUB_ACTIONS"] = "true"


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
                        f"select count(*) from {name} where {c} is not null and cast({c} as varchar) not in (select v from allowed)"
                    )
    return failures


def main(argv) -> int:
    client = s3()
    land_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    os.environ["GITHUB_ACTIONS"] = "true"
    started = time.monotonic()
    try:
        r = requests.get("https://api.dane.gov.pl/1.4/datasets/599", headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 90))
        a = r.json()["data"]["attributes"]
        reach = {"status": r.status_code, "title": a.get("title"), "licence": a.get("license_name"), "frequency": a.get("update_frequency")}
    except Exception as exc:  # noqa: BLE001
        reach = {"error": f"{type(exc).__name__}: {str(exc)[:160]}"}
    if reach.get("status") != 200:
        annotate("error", "wody_polskie_apgw trial reach dane.gov.pl", reach)
        return 1
    try:
        states, codes, seconds = [], [], []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            t0 = time.monotonic()
            codes.append(quiet(landing.extract, 120, landing_root=LAND))
            seconds.append(round(time.monotonic() - t0))
            states.append(landed_state(client, land_bucket))
        held = landing.held(LAND)
        entities = sorted(held)
        verify_rc = quiet(bulk_file.main, "wody_polskie_apgw", "verify", landing_root=LAND, specs=[{"entity": e} for e in entities])
        receipt = held[entities[0]][-1] if entities else {}
        landing_stats = {
            "dataset": reach,
            "files": len([k for k, _ in states[0] if not k.endswith(".receipt.json")]),
            "bytes": sum(s for k, s in states[0] if not k.endswith(".receipt.json")),
            "entities": entities,
            "version": receipt.get("version_token"),
            "etag": (receipt.get("detail") or {}).get("http", {}).get("etag"),
            "sha256": receipt.get("sha256"),
            "exit_codes": codes,
            "seconds": seconds,
            "second_run_unchanged": states[0] == states[1],
            "verify_exit": verify_rc,
        }
        annotate("notice", "wody_polskie_apgw trial landing", landing_stats)

        snapshots = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            quiet(bronze.load, 60, landing_root=LAND, namespace=SCRATCH)
            snap = cat.load_table((SCRATCH, "wody_polskie_apgw_attributes")).current_snapshot()
            snapshots.append(snap.snapshot_id if snap else None)
        rows = cat.load_table((SCRATCH, "wody_polskie_apgw_attributes")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "wody_polskie_apgw_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001  the table exists only after a rejection
            rejected = []
        con.register("b", rows)
        bronze_stats = {
            "rows": rows.num_rows,
            "per_layer": n("""select string_agg(layer || '=' || f || 'x' || c, ' ' order by layer) from (select layer,
                count(distinct feature_number) f, count(distinct field) c from b group by 1)"""),
            "dup_feature_field": n("select count(*) from (select 1 from b group by entity, layer, feature_number, field having count(*) > 1)"),
            "gdb_name": n("select string_agg(distinct gdb_name, ',') from b"),
            "rejected": [(x.get("entity"), (x.get("error") or "")[:300]) for x in rejected],
            "second_run_unchanged": snapshots[0] == snapshots[1],
            "layers_in_gdb": len(bronze.LAST_LAYERS),
            "listed_not_loaded": {k: v for k, v in sorted(bronze.LAST_LAYERS.items()) if k not in bronze.LOADED},
        }
        annotate("notice" if not rejected else "error", "wody_polskie_apgw trial bronze", bronze_stats)
        v = lambda layer, field: f"from b where layer = '{layer}' and lower(field) = '{field}'"  # noqa: E731
        profile = {
            "null_values": n("select count(*) from b where value is null"),
            "null_values_by_field": n("""select string_agg(layer || '.' || field || '=' || c, ' ') from (select layer, field, count(*) c
                from b where value is null group by 1, 2 order by 3 desc limit 12)"""),
            "markers_909_907": n("""select string_agg(field || ':' || value || '=' || c, ' ') from (select field, value, count(*) c from b
                where value in ('-909', '-907', '-9', '-999') group by 1, 2 order by 3 desc limit 20)"""),
            "non_numeric_area_length": n("""select coalesce(string_agg(distinct layer || '.' || field || ':' || value, ' '), 'none') from b
                where lower(field) in ('pow_jcwp', 'powierzchn', 'area', 'dlug_pow', 'pow_zl', 'nr_jcwpd') and try_cast(value as double) is null"""),
            "zero_or_negative_area": n("""select count(*) from b where lower(field) in ('pow_jcwp', 'powierzchn', 'area')
                and try_cast(value as double) <= 0"""),
            "jcwp_codes": n(f"select count(*) || ' rows, ' || count(distinct value) || ' distinct' {v('Charakterystyka_JCWP', 'kod_jcwp')}"),
            "code_prefixes": n(f"""select string_agg(p || '=' || c, ' ') from (select left(value, 2) p, count(*) c
                {v('Charakterystyka_JCWP', 'kod_jcwp')} group by 1)"""),
            "voivodeship_lists_multi": n(f"""select count(*) {v('Charakterystyka_JCWP', 'wojewodz')} and value like '%;%'"""),
            "voivodeship_examples": n(f"""select string_agg(value, ' | ') from (select distinct value {v('Charakterystyka_JCWP', 'wojewodz')}
                and value like '%(%(%' limit 4)"""),
            "voivodeship_shapes": n(f"""select string_agg(s || '=' || c, ' ') from (select regexp_replace(regexp_replace(value, '[^;()]', '', 'g'), '\\s', '', 'g') s,
                count(*) c {v('Charakterystyka_JCWP', 'wojewodz')} group by 1 order by 2 desc limit 6)"""),
            "jcwpd_char_rows_per_body": n(f"""select string_agg(k || '=' || c, ' ') from (select k, count(*) c from (select value, count(*) k
                {v('Charakterystyka_JCWPd', 'kod_jcwpd')} group by 1) group by 1 order by 1)"""),
            "jcwpd_codes_area_layer": n(f"select count(*) || '/' || count(distinct value) {v('Jednolite_części_wód_podziemnych', 'kod_jcwpd')}"),
        }
        annotate("notice", "wody_polskie_apgw trial bronze profile", profile)

        tables = {"wody_polskie_apgw_attributes": "b"}
        sqls = {}
        for model in MODELS:
            sqls[model] = render(SILVER / f"{model}.sql", tables)
            con.sql(f"create table {model} as " + sqls[model])
        failures = yml_tests(con, n, SILVER / "_wody_polskie_apgw.yml")
        tests_run = len(failures)
        for model in MODELS:
            test = render(ROOT / f"warehouse/tests/{model}_complete.sql", {model: model})
            found = con.sql(f"select * from ({test}) limit 5").fetchall()
            failures[f"{model} complete"] = len(found)
            if found:
                print("COMPLETE-FAIL", model, found, flush=True)
            tests_run += 1
        s_, g_, p_ = MODELS
        silver_stats = {
            "rows": {m: n(f"select count(*) from {m}") for m in MODELS},
            "rebuild_same": all(n(f"select count(*) from ({sqls[m]})") == n(f"select count(*) from {m}") for m in MODELS),
            "surface_by_category": n(f"""select string_agg(category || '=' || c || ' (res ' || r || ', area ' || a || ')', ' ' order by category)
                from (select category, count(*) c, count(*) filter (where is_reservoir) r, count(area_km2) a from {s_} group by 1)"""),
            "surface_by_district": n(f"""select string_agg(river_basin_district_code || '=' || c, ' ' order by 1) from (select
                river_basin_district_code, count(*) c from {s_} group by 1)"""),
            "voivodeship_parts": n(f"select string_agg(p || '=' || c, ' ') from (select voivodeship_parts p, count(*) c from {s_} group by 1 order by 1)"),
            "designation": n(f"select string_agg(designation || '=' || c, ' ') from (select designation, count(*) c from {s_} group by 1)"),
            "edition_dates": n(f"select string_agg(distinct cast(edition_date as varchar), ',') from {s_}"),
            "groundwater_by_district": n(f"""select string_agg(river_basin_district_code || '=' || c, ' ' order by 1) from (select
                river_basin_district_code, count(*) c from {g_} group by 1)"""),
            "groundwater_multi_district": n(f"select count(*) from {g_} where river_basin_district_names > 1"),
            "groundwater_area_sum": n(f"select round(sum(area_km2), 2) from {g_}"),
            "units": n(f"select string_agg(unit_kind || '=' || c, ' ') from (select unit_kind, count(*) c from {p_} group by 1)"),
            "districts": n(f"select string_agg(unit_code || ':' || name_pl || ':' || round(area_km2), ' | ' order by unit_code) from {p_} where unit_kind = 'river_basin_district'"),
            "tests_run": tests_run,
            "test_failures": {k: v for k, v in failures.items() if v} or "none",
        }
        annotate("notice" if not any(failures.values()) else "error", "wody_polskie_apgw trial silver", silver_stats)

        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        con.sql("""create table geo as select 'TERC:' || terc_code as geography_key, name_pl, 'wojewodztwo' as level_name,
            terc_code from terc where unit_level = 'wojewodztwo'""")
        macro = jinja2.Environment().from_string(MACROS + "{{ poc_geography__wody_polskie_apgw() }}").render()
        con.sql(f"create table map as with {macro} select * from wody_polskie_apgw")
        gm = "poc_build_wody_polskie_apgw"
        con.sql(f"create table {gm} as " + render(ROOT / f"warehouse/models/gold/{gm}.sql", {**{m: m for m in MODELS}, "poc_map_geography": "map"}))
        gold_failures = yml_tests(con, n, ROOT / "warehouse/models/gold/_wody_polskie_apgw.yml")
        series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
                  "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
        gold_stats = {
            "rows": n(f"select count(*) from {gm}"),
            "map_rows": n("select count(*) from map"),
            "measures": n(f"select string_agg(distinct replace(measure_key, 'wody_polskie_apgw:water_bodies:', ''), ' ') from {gm}"),
            "rows_by_measure": n(f"""select string_agg(m || '=' || c, ' ' order by m) from (select replace(measure_key, 'wody_polskie_apgw:water_bodies:', '') m,
                count(*) c from {gm} group by 1)"""),
            "dup_series_date": n(f"select count(*) from (select 1 from {gm} group by {series}, date_key having count(*) > 1)"),
            "frequencies_per_measure_max": n(f"select max(c) from (select measure_key, count(distinct frequency) c from {gm} group by 1)"),
            "date_keys": n(f"select string_agg(distinct cast(date_key as varchar), ',') from {gm}"),
            "unmatched_rows": n(f"select count(*) from {gm} where geography_key = '_X'"),
            "unmatched_codes": n(f"select coalesce(string_agg(distinct geography_source_code, ' '), 'none') from {gm} where geography_key = '_X'"),
            "geographies": n(f"select count(distinct geography_key) from {gm}"),
            "zero_values": n(f"select count(*) from {gm} where value = 0"),
            "district_sum_ne_total": n(f"""select count(*) from (select measure_key, date_key, sum(value) v from {gm} where geography_key = 'PL'
                and not is_total group by 1, 2) x join {gm} t using (measure_key, date_key) where t.geography_key = 'PL' and t.is_total
                and abs(x.v - t.value) > 1e-6"""),
            "category_sum_ne_all": n(f"""select abs(sum(value) filter (where measure_codes in ('surface.river', 'surface.lake', 'surface.transitional',
                'surface.coastal')) - sum(value) filter (where measure_codes = 'surface.all')) from {gm} where geography_key = 'PL' and is_total"""),
            "test_failures": {k: v for k, v in gold_failures.items() if v} or "none",
        }
        annotate("notice" if not any(gold_failures.values()) and not gold_stats["dup_series_date"] else "error",
                 "wody_polskie_apgw trial gold", gold_stats)
        spot = {
            "pl_totals": n(f"""select string_agg(measure_codes || '=' || round(value, 2), '; ' order by measure_codes) from {gm}
                where geography_key = 'PL' and is_total"""),
            "pl_surface_all_by_district": n(f"""select string_agg(breakdown_label_json || '=' || value, '; ' order by breakdown_json) from {gm}
                where geography_key = 'PL' and measure_codes = 'surface.all' and not is_total"""),
            "pl_groundwater_area_by_district": n(f"""select string_agg(breakdown_json || '=' || round(value, 1), '; ' order by breakdown_json) from {gm}
                where geography_key = 'PL' and measure_codes = 'groundwater_area' and not is_total"""),
            "voivodeships_surface_all": n(f"""select string_agg(geography_source_code || '=' || value, ' ' order by geography_source_code) from {gm}
                where geography_key <> 'PL' and measure_codes = 'surface.all'"""),
            "mazowieckie_14": n(f"""select string_agg(measure_codes || '=' || value, ' ' order by measure_codes) from {gm}
                where geography_source_code = '14'"""),
            "largest_lake": n(f"select jcwp_code || ' ' || jcwp_name || ' ' || area_km2 from {s_} where category = 'lake' order by area_km2 desc limit 1"),
        }
        annotate("notice", "wody_polskie_apgw trial spot values", spot)
        src = list(csv.DictReader((ROOT / "warehouse/seed_rows/source/wody_polskie_apgw.csv").open()))
        lic = {r["licence_key"]: r for f in (ROOT / "warehouse/seed_rows/licence").glob("*.csv") for r in csv.DictReader(f.open())}
        units = {r["unit_key"]: r for f in (ROOT / "warehouse/seed_rows/unit").glob("*.csv") for r in csv.DictReader(f.open())}
        umap = list(csv.DictReader((ROOT / "warehouse/seed_rows/unit_map/wody_polskie_apgw.csv").open()))
        catalogue = {
            "source_row": src[0],
            "licence_row": lic.get(src[0]["licence_key"], "MISSING"),
            "unit_codes_in_gold": n(f"select string_agg(distinct unit_code, ',') from {gm}"),
            "unit_map": [(u["unit_code"], u["unit_key"], u["unit_key"] in units) for u in umap],
            "seconds_total": round(time.monotonic() - started),
        }
        annotate("notice", "wody_polskie_apgw trial catalogue and licence", catalogue)
        print("STATS", landing_stats, bronze_stats, profile, silver_stats, gold_stats, spot, catalogue, flush=True)
        print("GOLD-ROWS", con.sql(f"select measure_codes, geography_source_code, breakdown_json, value from {gm} order by 1, 2, 3").fetchall(), flush=True)
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
        annotate("notice", "wody_polskie_apgw trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
