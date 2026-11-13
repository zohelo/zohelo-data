"""Dev trial of IMGW archive (PL-ENV-009) and API (PL-ENV-008), #2959: Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_imgw/, Iceberg namespace zz_dev_imgw; everything written is removed in
`finally`. Landing and Bronze each run twice (the second run must write nothing). Silver dbt models and the Gold build
stages are rendered with Jinja from warehouse/ and run in local DuckDB over the scratch tables, twice, with the schema
tests of the YAML files and the singular tests. Read-only reads of production: snapshot row counts of gold.fact_daily
and gold.fact_monthly. Output: the job log (full) and at most 10 notices. Deleted before merge.
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import time
import traceback

import duckdb
from jinja2 import Environment
import yaml

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.imgw_api import load as api_load
from ingestion.to_bronze.imgw_archive import entities as arch
import ingestion.to_landing.imgw_api as api_landing
import ingestion.to_landing.imgw_archive as arch_landing
from ingestion.to_landing.patterns import bulk_file

SCRATCH = "zz_dev_imgw"
CAPTURED: list = []
NOTES = 0
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]


def _capture(level, title, data):
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:6000]}", flush=True)


import ingestion.common.report as report  # noqa: E402

for module in (report, arch, bulk_bronze, bulk_file):
    module.annotate = _capture  # the trial's own `annotate` (imported above) stays the real one

# scratch locations
arch_landing.LANDING_ROOT = f"{SCRATCH}/01_landing"
arch.LANDING_ROOT = f"{SCRATCH}/01_landing"
arch.BRONZE_ROOT = f"{SCRATCH}/02_bronze"
arch.SILVER_ROOT = f"{SCRATCH}/03_silver"
arch.BRONZE = (SCRATCH, "imgw_archive_synop_daily")
arch.SILVER = (SCRATCH, "silver_imgw_archive_synop_daily")
arch.REJECTED = (SCRATCH, "imgw_archive_rejected_files")
api_landing.LANDING_ROOT = f"{SCRATCH}/01_landing"


def note(title, data):
    global NOTES
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    NOTES += 1
    if NOTES <= 9:  # with the scratch notice: at most 10 annotations
        annotate("notice", title, json.loads(text) if len(text) <= 3800 else {"truncated": text[:3700]})


def one(con, q):
    return con.execute(q).fetchone()[0]


def rows(con, q, limit=80):
    return [[str(v) for v in r] for r in con.execute(q).fetchall()][:limit]


class P:
    budget_minutes = 200.0


def landing_objects(prefix):
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    files = size = receipts = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".receipt.json"):
                receipts += 1
            else:
                files += 1
                size += o["Size"]
    return {"files": files, "bytes": size, "receipts": receipts}


def archive_landing():
    out = {"runs": []}
    prefix = f"{SCRATCH}/01_landing/imgw_archive/"
    for n in (1, 2):
        CAPTURED.clear()
        start = time.monotonic()
        code = arch_landing._run("load", P)
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        out["runs"].append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                            **{k: summary.get(k) for k in ("listed", "folders", "missing", "stale", "current",
                                                           "outcomes", "failed", "failed_sample", "stopped_at_budget")},
                            "after": landing_objects(prefix)})
    CAPTURED.clear()
    out["inspect_exit"] = arch_landing._run("inspect", P)
    out["inspect"] = next((d for t, d in CAPTURED if "inspect" in t), {})
    CAPTURED.clear()
    out["verify_exit"] = arch_landing._run("verify", P)
    v = next((d for t, d in CAPTURED if "verify" in t), {})
    out["verify_missing_data"] = sum(f.get("missing_data", 0) for f in v.get("files", []))
    # staleness evidence: index time (Warsaw → UTC, minute) against the version token (Last-Modified)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    specs = {s["entity"]: s for s in arch_landing.catalogue()}
    held = arch_landing.landing_state(client, bucket)
    receipts = arch.landing_newest(client, bucket)
    basis = {}
    for r in receipts.values():
        basis[r.get("version_basis")] = basis.get(r.get("version_basis"), 0) + 1
    out["version_basis"] = basis
    out["listed_vs_version_sample"] = [[e, specs[e]["listed"], held.get(e)] for e in list(specs)[:3] + list(specs)[-3:]]
    out["bytes_by_period"] = {}
    for r in receipts.values():
        period = "current_year_months" if arch.MONTHLY.match(r["entity"]) else r["entity"].split("_")[2][:3] + "0s"
        out["bytes_by_period"][period] = out["bytes_by_period"].get(period, 0) + r["size_bytes"]
    return out


def archive_bronze_silver():
    out = {"loads": []}
    for n in (1, 2):
        res = arch.run(200)
        out["loads"].append({"run": n, **res})
    out["completeness"] = arch.completeness()
    return out


def scan(table_id, fields=None):
    t = catalog().load_table(table_id)
    return t.scan(selected_fields=tuple(fields) if fields else ("*",)).to_arrow()


def api_landing_bronze():
    out = {"landing": [], "bronze": []}
    prefix = f"{SCRATCH}/01_landing/imgw_api/"
    for n in (1, 2):
        CAPTURED.clear()
        code = api_landing._extract(P)
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        out["landing"].append({"run": n, "exit": code, "fetched_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
                               "files": [{k: f.get(k) for k in ("entity", "outcome", "size_mb")} for f in summary.get("files", [])],
                               "after": landing_objects(prefix)})
    for n in (1, 2):
        CAPTURED.clear()
        code = api_load.load(0, landing_root=f"{SCRATCH}/01_landing", namespace=SCRATCH)
        out["bronze"].append({"run": n, "exit": code, "tables": [
            {k: d.get(k) for k in ("table", "entities_landed", "candidates", "appended_files", "rows_added", "rejected")}
            for t, d in CAPTURED]})
    # membership (the source's bronze.verify rule): every landed snapshot in Bronze or rejected
    from ingestion.common import bronze_files as bf

    cat = catalog()
    loaded = set()
    for e in api_load.FIELDS:
        loaded |= bf.loaded_hashes(cat.load_table((SCRATCH, f"imgw_api_{e}")))
    landed = set()
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    for e in api_load.FIELDS:
        landed |= {r["sha256"] for r in bulk_file.landed(client, bucket, bulk_file.prefix("imgw_api", e, f"{SCRATCH}/01_landing"))}
    out["verify_unexplained"] = len(landed - loaded)
    return out


def env():
    return Environment(extensions=["jinja2.ext.do"])


def render(path, refs, sources):
    def optional_ref(model, _cols):
        return refs[model]

    return env().from_string(Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda name: refs.get(name, name), source=lambda _s, t: sources[t],
        poc_optional_ref=optional_ref)


def schema_tests(con, yml, table, model=None, source_table=None):
    spec = yaml.safe_load(Path(yml).read_text())
    if model:
        m = next(x for x in spec["models"] if x["name"] == model)
    else:
        m = next(t for s in spec["sources"] for t in s.get("tables", []) if t["name"] == source_table and t.get("tests"))
    results = {}
    for t in m.get("tests", []):
        if "unique" in t:
            expr = t["unique"]["column_name"]
            results[f"unique {expr[:50]}"] = one(con, f"select count(*) from (select {expr} k from {table} group by 1 having count(*) > 1)")
    for col in m.get("columns", []):
        for t in col.get("tests", []):
            if t == "not_null":
                results[f"not_null {col['name']}"] = one(con, f"select count(*) from {table} where {col['name']} is null")
            elif isinstance(t, dict) and "accepted_values" in t:
                vals = t["accepted_values"]["arguments"]["values"]
                lst = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)
                results[f"accepted_values {col['name']}"] = one(
                    con, f"select count(*) from {table} where {col['name']} is not null and cast({col['name']} as varchar) not in ({lst})")
    return results


def singular(con, path, refs, sources):
    sql = render(path, refs, sources)
    return one(con, f"select count(*) from ({sql})")


def models(con):
    s = {}
    src = {"imgw_archive_synop_daily": "arch_silver", "imgw_api_synop": "api_bronze_synop", "imgw_api_meteo": "api_bronze_meteo"}
    # archive Bronze profile (selected columns)
    s["bronze_profile"] = {
        "rows": one(con, "select count(*) from arch_bronze"),
        "entities": one(con, "select count(distinct entity) from arch_bronze"),
        "stations": one(con, "select count(distinct nsp) from arch_bronze"),
        "years": rows(con, "select min(rok), max(rok) from arch_bronze"),
        "dup_station_day": one(con, "select count(*) from (select nsp, rok, mc, dz from arch_bronze group by all having count(*) > 1)"),
        "null_keys": one(con, "select count(*) from arch_bronze where nsp is null or rok is null or mc is null or dz is null or raw_sha256 is null"),
        "status_values": {c: rows(con, f"select coalesce({c}, '<null>'), count(*) from arch_bronze group by 1 order by 2 desc")
                          for c in ("wtmax", "wtmin", "wstd", "wtmng", "wsmdb", "wpksn", "wusl")},
        "non_numeric_values_blank_status": rows(con, " union all ".join(
            f"select '{v}', {v}, count(*) from arch_bronze where coalesce(trim(w{v}), '') = '' and {v} is not null "
            f"and try_cast(trim({v}) as double) is null group by all" for v in ("tmax", "tmin", "std", "tmng", "smdb", "pksn", "usl"))),
        "empty_value_blank_status": rows(con, " union all ".join(
            f"select '{v}', count(*) from arch_bronze where coalesce(trim(w{v}), '') = '' and coalesce(trim({v}), '') = ''"
            for v in ("tmax", "tmin", "std", "tmng", "smdb", "pksn", "usl"))),
        "zero_value_with_status_8_9": rows(con, " union all ".join(
            f"select '{v}', w{v}, count(*) from arch_bronze where w{v} in ('8', '9') and try_cast({v} as double) = 0 group by all"
            for v in ("tmax", "tmin", "std", "tmng", "smdb", "pksn", "usl"))),
        "stations_per_year": rows(con, "select rok, count(distinct nsp) from arch_bronze group by 1 order by 1", 70),
    }
    # archive Silver (entity table)
    s["archive_silver"] = {
        "rows": one(con, "select count(*) from arch_silver"),
        "by_parameter_status": rows(con, "select parameter, observation_status, count(*) from arch_silver group by all order by all"),
        "dates": rows(con, "select min(observation_date), max(observation_date) from arch_silver"),
        "source_tests": schema_tests(con, "warehouse/models/bronze/imgw_archive.yml", "arch_silver", source_table="imgw_archive_synop_daily"),
        "test imgw_archive_synop_daily_complete": singular(con, "warehouse/tests/imgw_archive_synop_daily_complete.sql", {}, src),
        "value_ranges": rows(con, "select parameter, min(value), max(value), avg(value)::decimal(8,2) from arch_silver group by 1 order by 1"),
    }
    # API Silver
    api = {}
    for model in ("imgw_api_synop", "imgw_api_stations"):
        sql = render(f"warehouse/models/silver/imgw_api/{model}.sql", {}, src)
        counts = []
        for _ in (1, 2):
            con.execute(f"create or replace table {model} as {sql}")
            counts.append(one(con, f"select count(*) from {model}"))
        api[model] = {"rows_two_builds": counts,
                      "tests": schema_tests(con, "warehouse/models/silver/imgw_api/_imgw_api.yml", model, model=model)}
    api["synop_hours"] = rows(con, "select observed_at, count(*), count(temperature_c), min(ingested_at), max(ingested_at) from imgw_api_synop group by 1 order by 1")
    api["synop_sample"] = rows(con, "select * exclude (raw_sha256, source_version) from imgw_api_synop order by station_id limit 3")
    api["stations_sample"] = rows(con, "select * exclude (raw_sha256, source_version) from imgw_api_stations order by station_code limit 3")
    api["test imgw_api_synop_complete (warn)"] = singular(con, "warehouse/tests/imgw_api_synop_complete.sql", {}, src)
    api["test imgw_api_stations_complete (warn)"] = singular(con, "warehouse/tests/imgw_api_stations_complete.sql", {}, src)
    s["api_silver"] = api
    # station dimension
    sql = render("warehouse/models/silver/imgw_archive/imgw_archive_synop_stations.sql", {"imgw_api_stations": "imgw_api_stations"}, src)
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table imgw_archive_synop_stations as {sql}")
        counts.append(one(con, "select count(*) from imgw_archive_synop_stations"))
    s["stations"] = {
        "rows_two_builds": counts,
        "tests": schema_tests(con, "warehouse/models/silver/imgw_archive/_imgw_archive.yml", "imgw_archive_synop_stations", model="imgw_archive_synop_stations"),
        "test imgw_archive_synop_stations_complete": singular(con, "warehouse/tests/imgw_archive_synop_stations_complete.sql", {}, src),
        "with_coordinates": one(con, "select count(*) from imgw_archive_synop_stations where latitude is not null"),
        "active_last_month": one(con, "select count(*) from imgw_archive_synop_stations where last_date >= (select date_trunc('month', max(last_date)) from imgw_archive_synop_stations)"),
        "sample": rows(con, "select * exclude (raw_sha256) from imgw_archive_synop_stations order by last_date desc, station_code limit 5"),
        "renamed_examples": rows(con, "select station_code, count(distinct station_name), string_agg(distinct station_name, ' / ') from arch_silver group by 1 having count(distinct station_name) > 1 limit 8"),
    }
    return s


def gold(con):
    src = {"imgw_archive_synop_daily": "arch_silver"}
    g = {}
    arch_refs = {"prg_units": "prg_units", "imgw_archive_synop_stations": "imgw_archive_synop_stations"}
    for model, refs in (("poc_build_imgw_archive", arch_refs), ("poc_build_imgw_api", {"imgw_api_synop": "imgw_api_synop"})):
        sql = render(f"warehouse/models/gold/{model}.sql", refs, src)
        counts = []
        for _ in (1, 2):
            con.execute(f"create or replace table {model} as {sql}")
            counts.append(one(con, f"select count(*) from {model}"))
        t = model
        g[model] = {
            "rows_two_builds": counts,
            "contract_columns": [d[0] for d in con.execute(f"select * from {t} limit 0").description] == CONTRACT,
            "dup_series_date": one(con, f"select count(*) from (select measure_key || '|' || geography_key || '|' || breakdown_json, date_key from {t} group by all having count(*) > 1)"),
            "rows_by_geography_level": rows(con, f"select case when geography_key = 'PL' then 'PL' else 'voivodeship' end, frequency, count(*), count(distinct geography_key) from {t} group by all order by all"),
            "measures_with_two_frequencies": one(con, f"select count(*) from (select measure_key from {t} group by 1 having count(distinct frequency) > 1)"),
            "by_measure": rows(con, f"select measure_key, frequency, count(*), min(date_key), max(date_key), min(value), max(value), avg(value)::decimal(8,2) from {t} group by all order by 1"),
            "null_values": one(con, f"select count(*) from {t} where value is null"),
            "statuses": rows(con, f"select observation_status_key, count(*) from {t} group by 1"),
            "tests": schema_tests(con, f"warehouse/models/gold/_{model.removeprefix('poc_build_')}.yml", t, model=model),
        }
    a = "poc_build_imgw_archive"
    g["spot_monthly_2025_pl"] = rows(con, f"select period_label, measure_codes, value from {a} where frequency = 'M' and geography_key = 'PL' and period_label like '2025-%' order by 2, 1")
    g["spot_jan_jul_pl"] = rows(con, f"select year(date_key), round(avg(value) filter (where month(date_key) = 1), 2), round(avg(value) filter (where month(date_key) = 7), 2) from {a} where measure_key like '%synop_monthly:degC%' and geography_key = 'PL' and year(date_key) in (1970, 1990, 2010, 2024, 2025) group by 1 order by 1")
    g["spot_voiv_2025_07_temp_and_precip"] = rows(con, f"select geography_key, measure_codes, value from {a} where frequency = 'M' and date_key = date '2025-07-01' and geography_key <> 'PL' order by 2, 1", 40)
    g["stations_per_voivodeship"] = rows(con, """select 'TERC:' || v.teryt_code, count(*), string_agg(s.station_name, '; ' order by s.station_name)
        from imgw_archive_synop_stations s join prg_units v on st_contains(st_geomfromwkb(v.geometry_wkb), st_point(s.longitude, s.latitude))
        where s.longitude is not null group by 1 order by 1""", 20)
    g["stations_with_coordinates_not_placed"] = rows(con, """select s.station_code, s.station_name, s.longitude, s.latitude from imgw_archive_synop_stations s
        where s.longitude is not null and not exists (select 1 from prg_units v where st_contains(st_geomfromwkb(v.geometry_wkb), st_point(s.longitude, s.latitude)))""")
    g["stations_without_coordinates"] = rows(con, "select count(*), min(last_date), max(last_date) from imgw_archive_synop_stations where longitude is null")
    g["active_stations_without_coordinates"] = rows(con, "select station_code, station_name, last_date from imgw_archive_synop_stations where longitude is null and last_date >= date '2025-01-01'")
    # station link archive <-> API synop (id rule '12' + last three digits), checked against the names
    g["api_synop_link"] = {
        "api_stations": one(con, "select count(distinct station_id) from imgw_api_synop"),
        "matched_by_code_rule": one(con, "select count(distinct a.station_id) from imgw_api_synop a join imgw_archive_synop_stations s on s.api_synop_station_id = a.station_id"),
        "matched_names_agree": one(con, """select count(distinct a.station_id) from imgw_api_synop a join imgw_archive_synop_stations s on s.api_synop_station_id = a.station_id
            where replace(replace(upper(strip_accents(a.station_name)), '-', ' '), '  ', ' ') = replace(replace(upper(strip_accents(s.station_name)), '-', ' '), '  ', ' ')"""),
        "name_differences": rows(con, """select a.station_id, a.station_name, s.station_name from imgw_api_synop a join imgw_archive_synop_stations s on s.api_synop_station_id = a.station_id
            where replace(replace(upper(strip_accents(a.station_name)), '-', ' '), '  ', ' ') <> replace(replace(upper(strip_accents(s.station_name)), '-', ' '), '  ', ' ') group by all""", 30),
        "unmatched_api": rows(con, "select distinct a.station_id, a.station_name from imgw_api_synop a anti join imgw_archive_synop_stations s on s.api_synop_station_id = a.station_id"),
    }
    g["spot_annual_mean_temp"] = rows(con, f"select year(date_key), round(avg(value), 2), count(*) from {a} where measure_key like '%synop_monthly:degC%' and year(date_key) in (1971, 1991, 2001, 2020, 2023, 2024, 2025) group by 1 order by 1")
    g["spot_annual_precip"] = rows(con, f"select year(date_key), round(sum(value), 1), count(*) from {a} where measure_key like '%synop_monthly:mm%' and year(date_key) in (1991, 2001, 2010, 2020, 2023, 2024, 2025) group by 1 order by 1")
    g["spot_daily"] = rows(con, f"select date_key, value from {a} where frequency = 'D' and date_key in ('2025-01-01', '2025-07-01', '2026-10-31', '1966-01-01') order by 1")
    g["months_missing"] = rows(con, f"""with m as (select unnest(generate_series(date '1966-01-01', (select max(date_key) from {a} where frequency = 'M'), interval 1 month))::date d)
        select measure_key, count(*) from (select distinct measure_key from {a} where frequency = 'M') k cross join m anti join {a} b on b.measure_key = k.measure_key and b.date_key = m.d group by 1""")
    g["stations_per_month_sample"] = rows(con, """select parameter, cast(date_trunc('month', observation_date) as date) m, count(distinct station_code) from arch_silver
        where observation_date >= date '2025-01-01' group by all order by all""", 40)
    # KPI patterns and bounds (metrics_value_range rows)
    g["kpi_patterns"] = rows(con, f"select k.kpi_key, count(b.measure_key), min(b.value), max(b.value) from read_csv('warehouse/seeds/kpi_measure.csv', all_varchar=true) k "
                                  f"left join {a} b on b.measure_key like k.measure_key_pattern where k.source_system = 'imgw_archive' group by 1 order by 1")
    g["kpi_rows_per_area_period_max"] = one(con, f"select max(c) from (select k.kpi_key, b.geography_key, b.date_key, count(*) c from read_csv('warehouse/seeds/kpi_measure.csv', all_varchar=true) k "
                                                 f"join {a} b on b.measure_key like k.measure_key_pattern where k.source_system = 'imgw_archive' group by all)")
    g["outside_bounds"] = one(con, f"select count(*) from {a} where (measure_key like '%synop_monthly:degC%' and (value < -30 or value > 35)) or (measure_key like '%synop_monthly:mm%' and (value < 0 or value > 1000))")
    g["metrics_rows_per_kpi"] = rows(con, f"select k.kpi_key, count(*) from read_csv('warehouse/seeds/kpi_measure.csv', all_varchar=true) k join {a} b on b.measure_key like k.measure_key_pattern where k.source_system = 'imgw_archive' group by 1")
    g["max_monthly_precip_by_level"] = rows(con, f"select geography_key = 'PL', max(value), arg_max(geography_key || ' ' || period_label, value) from {a} where measure_key like '%synop_monthly:mm%' group by 1")
    return g


def production_reads():
    out = {}
    cat = catalog()
    for t in ("fact_daily", "fact_monthly"):
        try:
            snap = cat.load_table(("gold", t)).current_snapshot()
            out[f"{t}_rows"] = int(snap.summary["total-records"]) if snap else None
        except Exception as exc:  # noqa: BLE001
            out[f"{t}_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    return out


def licence():
    from warehouse.publish import licensed_sources

    def seed(name):
        with open(f"warehouse/seeds/{name}.csv", newline="", encoding="utf-8") as h:
            return list(csv.DictReader(h))

    out = {}
    for s in ("imgw_archive", "imgw_api"):
        src = [r for r in seed("source") if r["source_key"] == s]
        out[s] = {"source_row_licence_key": src[0]["licence_key"] if src else None,
                  "licence_row": [r for r in seed("licence") if src and r["licence_key"] == src[0]["licence_key"]],
                  "licensed_by_gate": s in licensed_sources(seed("source"), seed("licence"))}
    return out


def cleanup():
    removed = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:  # noqa: BLE001
                cat.drop_table(table)
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:  # noqa: BLE001
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    for var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"), ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
        bucket = req(var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/"))
    try:
        removed["namespace_left"] = SCRATCH in [n[0] for n in cat.list_namespaces()]
    except Exception:  # noqa: BLE001
        pass
    print(f"### scratch removed: {json.dumps(removed)}", flush=True)
    annotate("notice", "imgw scratch removed", removed)


def main(argv):
    status = 0
    results = {}
    try:
        results["prod"] = production_reads()
        print("### production reads", results["prod"], flush=True)
        results["api"] = api_landing_bronze()
        note("imgw_api landing (twice), bronze (twice), verify", results["api"])
        results["arch_landing"] = archive_landing()
        note("imgw_archive landing (twice), inspect, verify", results["arch_landing"])
        results["arch_bs"] = archive_bronze_silver()
        note("imgw_archive bronze+silver (twice), completeness", results["arch_bs"])
        con = duckdb.connect()
        con.execute("install spatial; load spatial")
        from pyiceberg.expressions import EqualTo

        prg = catalog().load_table(("silver", "prg_units")).scan(
            row_filter=EqualTo("unit_level", "wojewodztwo"), selected_fields=("unit_level", "teryt_code", "name_pl", "geometry_wkb")).to_arrow()
        con.register("prg_arrow", prg)
        con.execute("create table prg_units as select * from prg_arrow")
        results["prg_voivodeships"] = len(prg)
        con.register("arch_bronze", scan(arch.BRONZE, ["entity", "nsp", "rok", "mc", "dz", "tmax", "wtmax", "tmin", "wtmin", "std", "wstd",
                                                        "tmng", "wtmng", "smdb", "wsmdb", "pksn", "wpksn", "usl", "wusl", "raw_sha256"]))
        con.register("arch_silver", scan(arch.SILVER))
        con.register("api_bronze_synop", scan((SCRATCH, "imgw_api_synop")))
        con.register("api_bronze_meteo", scan((SCRATCH, "imgw_api_meteo")))
        m = models(con)
        results["models"] = m
        bp = m["bronze_profile"]
        note("imgw bronze profile", {k: bp[k] for k in ("rows", "entities", "stations", "years", "dup_station_day", "null_keys", "status_values")})
        note("imgw bronze markers", {k: bp[k] for k in ("non_numeric_values_blank_status", "empty_value_blank_status", "zero_value_with_status_8_9")})
        a_s, api, st = m["archive_silver"], m["api_silver"], m["stations"]
        note("imgw silver", {"archive": {k: a_s[k] for k in a_s if k != "by_parameter_status"}, "by_parameter_status": a_s["by_parameter_status"],
                             "api": {k: api[k] for k in ("imgw_api_synop", "imgw_api_stations", "test imgw_api_synop_complete (warn)", "test imgw_api_stations_complete (warn)")},
                             "stations": {k: st[k] for k in ("rows_two_builds", "tests", "test imgw_archive_synop_stations_complete", "with_coordinates", "active_last_month")}})
        g = gold(con)
        results["gold"] = g
        results["licence"] = licence()
        note("imgw gold build stages, licence gate, production fact rows", {**{k: g[k] for k in ("poc_build_imgw_archive", "poc_build_imgw_api")},
                                                                            "licence": results["licence"], "prod": results["prod"]})
        note("imgw gold spot values", {k: g[k] for k in ("spot_jan_jul_pl", "spot_monthly_2025_pl", "spot_annual_precip", "spot_daily", "months_missing", "outside_bounds", "metrics_rows_per_kpi", "max_monthly_precip_by_level", "kpi_rows_per_area_period_max")})
        note("imgw voivodeships and station link", {"prg_voivodeships": results["prg_voivodeships"], **{k: g[k] for k in ("stations_per_voivodeship", "stations_with_coordinates_not_placed", "stations_without_coordinates", "active_stations_without_coordinates", "api_synop_link", "spot_voiv_2025_07_temp_and_precip")}})

    except Exception as exc:  # noqa: BLE001
        status = 1
        traceback.print_exc()
        annotate("error", "imgw trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        import os

        out_dir = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "imgw_trial.json").write_text(json.dumps(results, default=str, ensure_ascii=False, indent=1))
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
