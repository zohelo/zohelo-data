"""Dev trial of gios_groundwater (#2870) on the branch's code: Landing and Bronze into zz_dev_gios_groundwater/ (both
buckets) and namespace zz_dev_gios_groundwater, each twice; Bronze profiled before Silver; then the branch's Silver
SQL and its tests (unique, not_null, accepted_values from _gios_groundwater.yml, the two completeness tests), the Gold
build stage with its tests and the KPI rows, applied in memory (Jinja-rendered with
warehouse/macros/sources/gios_groundwater.sql). Reads silver.teryt_terc (read-only) for the voivodeship names of the
geography map. Everything written is removed at the end (finally). Never merged."""

from __future__ import annotations

import csv
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
from ingestion.to_bronze.gios_groundwater import load as bronze
from ingestion.to_landing import gios_groundwater as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_gios_groundwater"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/sources/gios_groundwater.sql").read_text()
SILVER = ROOT / "warehouse/models/silver/gios_groundwater"


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
    started = time.monotonic()
    try:
        r = requests.get("https://api.dane.gov.pl/1.4/datasets/1405", headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 90))
        a = r.json()["data"]["attributes"]
        inst = r.json()["data"]["relationships"]["institution"]["data"]["id"]
        time.sleep(2)
        i = requests.get(f"https://api.dane.gov.pl/1.4/institutions/{inst}", headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 90))
        reach = {"status": r.status_code, "title": a.get("title"), "licence": a.get("license_name"),
                 "frequency": a.get("update_frequency"), "institution": inst,
                 "institution_title": i.json()["data"]["attributes"].get("title")}
    except Exception as exc:  # noqa: BLE001
        reach = {"error": f"{type(exc).__name__}: {str(exc)[:160]}"}
    reach["seconds"] = round(time.monotonic() - started, 1)
    if reach.get("status") != 200:
        annotate("error", "gios_groundwater trial reach dane.gov.pl", reach)
        return 1
    try:
        states, codes = [], []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            codes.append(landing.extract(60, landing_root=LAND))
            states.append(landed_state(client, land_bucket))
        entities = sorted(landing.held(LAND))
        verify_rc = bulk_file.main("gios_groundwater", "verify", landing_root=LAND, specs=[{"entity": e} for e in entities])
        landing_stats = {
            "files": len([k for k, _ in states[0] if not k.endswith(".receipt.json")]),
            "bytes": sum(s for k, s in states[0] if not k.endswith(".receipt.json")),
            "entities": entities,
            "exit_codes": codes,
            "second_run_unchanged": states[0] == states[1],
            "verify_exit": verify_rc,
            "reach": reach,
        }
        annotate("notice", "gios_groundwater trial landing", landing_stats)

        snapshots = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            bronze.load(60, landing_root=LAND, namespace=SCRATCH)
            snap = cat.load_table((SCRATCH, "gios_groundwater_records")).current_snapshot()
            snapshots.append(snap.snapshot_id if snap else None)
        rows = cat.load_table((SCRATCH, "gios_groundwater_records")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "gios_groundwater_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001  the table exists only after a rejection
            rejected = []
        con.register("b", rows)
        bronze_stats = {
            "rows": rows.num_rows,
            "per_entity": n("""select string_agg(entity || '=' || s || '/' || v, ' ' order by entity) from (select entity,
                count(*) filter (where record_kind = 'sample') s, count(*) filter (where record_kind = 'value') v from b group by 1)"""),
            "rejected": [(x.get("entity"), (x.get("error") or "")[:300]) for x in rejected],
            "second_run_unchanged": snapshots[0] == snapshots[1],
        }
        profile = {
            "samples_no_monbada": n("select count(*) from b where record_kind = 'sample' and point_monbada is null"),
            "samples_no_monbada_no_idm": n("select count(*) from b where record_kind = 'sample' and point_monbada is null and point_id_monitoring is null"),
            "idm_to_several_monbada": n("""select count(*) from (select entity, point_id_monitoring from b where record_kind = 'sample'
                and point_id_monitoring is not null group by 1, 2 having count(distinct coalesce(point_monbada, '-')) > 1)"""),
            "monbada_to_several_idm": n("""select count(*) from (select entity, point_monbada from b where record_kind = 'sample'
                and point_monbada is not null group by 1, 2 having count(distinct coalesce(point_id_monitoring, '-')) > 1)"""),
            "years": n("select string_agg(distinct survey_year, ',' order by survey_year) from b where record_kind = 'sample'"),
            "date_shapes": n("""select string_agg(s || '=' || c, ' ') from (select regexp_replace(sample_date, '[0-9]', 'N', 'g') s, count(*) c
                from b where record_kind = 'sample' group by 1)"""),
            "value_shapes": n("""select string_agg(s || '=' || c, ' ' order by c desc) from (select case
                when regexp_full_match(trim(value), '-?[0-9]+([.,][0-9]+)?([eE][-+]?[0-9]+)?') then 'number'
                else regexp_replace(trim(value), '[0-9]+', 'N', 'g') end s, count(*) c from b where record_kind = 'value' group by 1)"""),
            "voivodeships": n("select count(distinct voivodeship) || ': ' || string_agg(distinct voivodeship, ',') from b where record_kind = 'sample'"),
            "null_voivodeship": n("select count(*) from b where record_kind = 'sample' and voivodeship is null"),
            "null_coords": n("select count(*) from b where record_kind = 'sample' and (puwg1992_x is null or puwg1992_y is null)"),
            "indicators": n("select count(distinct lower(regexp_replace(indicator, '\\s+', '', 'g'))) from b where record_kind = 'value'"),
            "nitrate_headers": n("select string_agg(distinct indicator, ' | ') from b where record_kind = 'value' and lower(indicator) like 'azotany%'"),
        }
        bronze_stats["profile"] = profile
        annotate("notice" if not rejected else "error", "gios_groundwater trial bronze", bronze_stats)

        tables = {"gios_groundwater_records": "b"}
        res_sql = render(SILVER / "gios_groundwater_results.sql", tables)
        con.sql("create table gios_groundwater_results as " + res_sql)
        pts_sql = render(SILVER / "gios_groundwater_points.sql", tables)
        con.sql("create table gios_groundwater_points as " + pts_sql)
        failures = yml_tests(con, n, SILVER / "_gios_groundwater.yml")
        for model in ("gios_groundwater_results", "gios_groundwater_points"):
            test = render(ROOT / f"warehouse/tests/{model}_complete.sql", {model: model})
            failures[f"{model} complete"] = n(f"select count(*) from ({test})")
        r_ = "gios_groundwater_results"
        silver_stats = {
            "results": n(f"select count(*) from {r_}"),
            "results_rebuild": n(f"select count(*) from ({res_sql})"),
            "points_rows": n("select count(*) from gios_groundwater_points"),
            "per_year": n(f"""select string_agg(survey_year || ':' || monitoring_kind || '=' || c || '/' || p, ' ' order by survey_year)
                from (select survey_year, monitoring_kind, count(*) c, count(distinct point_key) p from {r_} group by 1, 2)"""),
            "status": n(f"select string_agg(value_status || '=' || c, ' ') from (select value_status, count(*) c from {r_} group by 1)"),
            "not_determined_dropped": n("select count(*) from b where record_kind = 'value' and trim(value) = 'n.o.'"),
            "zero_values": n(f"select count(*) from {r_} where value = 0"),
            "zero_by_indicator": n(f"""select string_agg(indicator || ' ' || value_text || '=' || c, '; ') from (select indicator, value_text, count(*) c
                from {r_} where value = 0 group by 1, 2 order by c desc)"""),
            "dup_grain": n(f"select count(*) from (select 1 from {r_} group by point_key, indicator_key, sample_date having count(*) > 1)"),
            "dup_examples": n(f"""select string_agg(k, ' ') from (select point_key || '/' || indicator_key || '/' || sample_date k from {r_}
                group by point_key, indicator_key, sample_date having count(*) > 1 limit 5)"""),
            "conflicts": n("""select coalesce(string_agg(a || '=' || c, ' '), 'none') from (select unnest(string_split(conflicting_attributes, ', ')) a,
                count(*) c from gios_groundwater_points where conflicting_attributes is not null group by 1)"""),
            "date_range": n(f"select min(sample_date) || '..' || max(sample_date) from {r_}"),
            "date_outside_year": n(f"select count(*) from {r_} where year(sample_date) <> survey_year"),
            "tests_run": len(failures),
            "test_failures": {k: v for k, v in failures.items() if v} or "none",
        }
        annotate("notice" if not any(failures.values()) else "error", "gios_groundwater trial silver", silver_stats)

        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        con.sql("""create table geo as select 'TERC:' || terc_code as geography_key, name_pl, 'wojewodztwo' as level_name,
            terc_code from terc where unit_level = 'wojewodztwo'""")
        macro = jinja2.Environment().from_string(MACROS + "{{ poc_geography__gios_groundwater() }}").render()
        con.sql(f"create table map as with {macro} select * from gios_groundwater")
        g = "poc_build_gios_groundwater"
        con.sql(f"create table {g} as " + render(ROOT / f"warehouse/models/gold/{g}.sql", {
            "gios_groundwater_results": r_, "gios_groundwater_points": "gios_groundwater_points", "poc_map_geography": "map"}))
        gold_failures = yml_tests(con, n, ROOT / "warehouse/models/gold/_gios_groundwater.yml")
        series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
                  "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
        gold_stats = {
            "rows": n(f"select count(*) from {g}"),
            "map_rows": n("select count(*) from map"),
            "measures": n(f"select string_agg(distinct measure_key, ' ') from {g}"),
            "dup_series_date": n(f"select count(*) from (select 1 from {g} group by {series}, date_key having count(*) > 1)"),
            "unmatched_rows": n(f"select count(*) from {g} where geography_key = '_X'"),
            "unmatched_codes": n(f"select coalesce(string_agg(distinct geography_source_code, ' '), 'none') from {g} where geography_key = '_X'"),
            "geographies_per_year": n(f"""select string_agg(period_label || '=' || c, ' ' order by period_label) from (select period_label,
                count(distinct geography_key) c from {g} group by 1)"""),
            "share_range": n(f"select min(value) || '..' || max(value) from {g} where unit_code = '%'"),
            "voiv_sum_eq_pl": n(f"""select count(*) from (select measure_key, date_key, sum(value) v from {g} where geography_key <> 'PL'
                and unit_code <> '%' group by 1, 2) x join {g} p using (measure_key, date_key) where p.geography_key = 'PL' and x.v <> p.value"""),
            "test_failures": {k: v for k, v in gold_failures.items() if v} or "none",
        }
        spot = {
            "pl": n(f"""select string_agg(period_label || ' ' || measure_codes || '=' || round(value, 2), '; ' order by period_label, measure_codes)
                from {g} where geography_key = 'PL'"""),
            "mazowieckie_2022": n(f"""select string_agg(measure_codes || '=' || round(value, 2), '; ') from {g}
                where geography_source_code = 'mazowieckie' and period_label = '2022'"""),
            "max_nitrate": n(f"select max(value) from {r_} where indicator_key = 'azotany[mgno3/l]'"),
        }
        # KPI rows: the metrics rows the concept would take, per area x period (at most 1)
        km = list(csv.DictReader((ROOT / "warehouse/seed_rows/kpi_measure/gios_groundwater.csv").open()))
        pattern = km[0]["measure_key_pattern"]
        kpi_stats = {
            "pattern": pattern,
            "rows": n(f"select count(*) from {g} where measure_key like '{pattern}'"),
            "max_per_area_period": n(f"select max(c) from (select count(*) c from {g} where measure_key like '{pattern}' group by geography_key, date_key)"),
        }
        src = list(csv.DictReader((ROOT / "warehouse/seed_rows/source/gios_groundwater.csv").open()))
        lic = {r["licence_key"]: r for f in (ROOT / "warehouse/seed_rows/licence").glob("*.csv") for r in csv.DictReader(f.open())}
        row = lic.get(src[0]["licence_key"], {})
        kpi_stats["licence_row"] = {k: row.get(k) for k in ("licence_key", "name", "commercial_ok")} if row else "MISSING"
        gold_stats.update({"spot": spot, "kpi": kpi_stats})
        annotate("notice" if not any(gold_failures.values()) else "error", "gios_groundwater trial gold", gold_stats)
        print("STATS", landing_stats, bronze_stats, profile, silver_stats, gold_stats, spot, kpi_stats, flush=True)
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
        annotate("notice", "gios_groundwater trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
