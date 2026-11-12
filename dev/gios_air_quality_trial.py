"""Dev trial of gios_air_quality (register PL-ENV-010, #2960): Landing → Bronze → Silver → Gold into scratch.

Landing extract twice into zz_dev_gios_air/01_landing (Landing bucket) plus inspect and verify, Bronze load twice into
namespace zz_dev_gios_air (lake prefix zz_dev_gios_air/; the load also writes the Silver entity table gios_air_hourly
there), a Bronze profile, the dbt Silver models and their schema and completeness tests and the Gold build stage
rendered with Jinja from warehouse/ and run in local DuckDB over the scratch tables (each built twice). One HEAD request
to the newest archive ZIP (size only). Everything written is removed in `finally`. Deleted before merge.
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import re
import sys
import time
import traceback

import duckdb
from jinja2 import Environment
import requests
import yaml

from ingestion.common import report
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

CAPTURED: list = []
_original = report.annotate


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:6000]}", flush=True)


report.annotate = _capture
from ingestion.to_bronze.gios_air_quality import entities, load as bronze_load  # noqa: E402
import ingestion.to_landing.gios_air_quality as landing  # noqa: E402

bronze_load.annotate = _capture
entities.annotate = _capture

SCRATCH = "zz_dev_gios_air"
LANDING_ROOT = f"{SCRATCH}/01_landing"
STATS: dict = {"head": os.environ.get("GITHUB_SHA", "")[:7]}
POLLUTANTS = ("PM10", "PM2.5", "NO2", "O3", "SO2")


def note(title: str, data, level: str = "notice") -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    _original(level, title, json.loads(text) if len(text) <= 3800 else {"truncated": text[:3700]})


def one(con, q):
    return con.execute(q).fetchone()[0]


def rows(con, q, limit=400):
    return [[None if v is None else str(v) for v in r] for r in con.execute(q).fetchall()][:limit]


def show(con, title, q, limit=200):
    print(f"\n=== {title}", flush=True)
    rel = con.execute(q)
    print("\t".join(d[0] for d in rel.description))
    for r in rel.fetchall()[:limit]:
        print("\t".join("" if v is None else str(v)[:120] for v in r), flush=True)


def render(path: str, sources: dict[str, str]) -> str:
    macros = Path("warehouse/macros/sources/gios_air_quality.sql").read_text()
    env = Environment(extensions=["jinja2.ext.do"])
    return env.from_string(macros + Path(path).read_text()).render(
        config=lambda **_k: "",
        ref=lambda name: sources.get(name, name),
        source=lambda _s, t: sources[t],
        var=lambda *_a: [],
    )


def schema_tests(con, yml: str, model: str, table: str) -> dict:
    spec = yaml.safe_load(Path(yml).read_text())
    m = next(x for x in spec["models"] if x["name"] == model)
    out = {}
    for t in m.get("tests", []):
        if isinstance(t, dict) and "unique" in t:
            expr = t["unique"]["column_name"]
            out["unique"] = one(con, f"select count(*) from (select {expr} k from {table} group by 1 having count(*) > 1)")
    for col in m.get("columns", []):
        for t in col.get("tests", []):
            name = col["name"]
            if t == "not_null":
                out[f"not_null {name}"] = one(con, f"select count(*) from {table} where {name} is null")
            elif t == "unique":
                out[f"unique {name}"] = one(con, f"select count(*) from (select {name} from {table} group by 1 having count(*) > 1)")
            elif isinstance(t, dict) and "accepted_values" in t:
                vals = t["accepted_values"]["arguments"]["values"]
                lst = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)
                bad = rows(con, f"select cast({name} as varchar), count(*) from {table} where {name} is not null "
                                f"and cast({name} as varchar) not in ({lst}) group by 1 order by 2 desc limit 10")
                out[f"accepted_values {name}"] = sum(int(c) for _, c in bad)
                if bad:
                    out[f"accepted_values {name} sample"] = bad
    return out


def landing_runs() -> dict:
    out: dict = {}
    for attempt in (1, 2):
        CAPTURED.clear()
        started = time.monotonic()
        code = landing.main("load", 120, landing_root=LANDING_ROOT)
        data = next((d for t, d in CAPTURED if "landing load" in t), {})
        out[f"extract_{attempt}"] = {"exit": code, "minutes": round((time.monotonic() - started) / 60, 1),
                                     **{k: v for k, v in data.items() if k != "failed_sample"}}
        if data.get("failed_sample"):
            out[f"extract_{attempt}"]["failed_sample"] = data["failed_sample"][:3]
    for mode in ("inspect", "verify"):
        CAPTURED.clear()
        code = landing.main(mode, 10, landing_root=LANDING_ROOT)
        out[mode] = {"exit": code, **next((d for t, d in CAPTURED if f"landing {mode}" in t), {})}
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = landing.receipts(client, bucket, LANDING_ROOT)
    out["files"] = sum(len(v) for v in held.values())
    out["bytes"] = sum(r["size_bytes"] for v in held.values() for r in v)
    out["entities"] = sorted(held)
    return out


def bronze_runs(con) -> dict:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = bronze_load.load(0, landing_root=LANDING_ROOT, namespace=SCRATCH, silver_namespace=SCRATCH)
        b = next((d for t, d in CAPTURED if "Bronze from Landing" in t), {})
        s = next((d for t, d in CAPTURED if "Silver measurements" in t), {})
        out["loads"].append({"exit": code, "candidates": b.get("candidates"), "appended_files": b.get("appended_files"),
                             "rows_added": b.get("rows_added"), "rejected": b.get("rejected"),
                             "rejected_sample": b.get("rejected_sample"),
                             "silver": {k: s.get(k) for k in ("days", "rebuilt", "rows", "failed", "days_held", "rows_held",
                                                              "first_day", "last_day")}})
    cat = catalog()
    for table in ("gios_air_stations", "gios_air_sensors", "gios_air_measurements", "gios_air_hourly"):
        t = cat.load_table((SCRATCH, table))
        arrow = t.scan().to_arrow()
        con.register(f"{table}_arrow", arrow)
        con.execute(f"create table b_{table} as select * from {table}_arrow")
        out[table] = {"rows": arrow.num_rows, "files": len(list(t.scan().plan_files()))}
    try:
        rej = cat.load_table((SCRATCH, "gios_air_quality_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected_rows"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out


def bronze_profile(con) -> dict:
    p: dict = {}
    m = "b_gios_air_measurements"
    p["measurements"] = {
        "rows": one(con, f"select count(*) from {m}"),
        "sensors": one(con, f"select count(distinct sensor_id) from {m}"),
        "stations": one(con, f"select count(distinct station_id) from {m}"),
        "range": one(con, f"select min(measured_at) || ' .. ' || max(measured_at) from {m}"),
        "null_value": one(con, f"select count(*) from {m} where value is null"),
        "null_keys": one(con, f"select count(*) from {m} where sensor_id is null or measured_at is null or raw_sha256 is null"),
        "non_numeric": one(con, f"select count(*) from {m} where value is not null and try_cast(value as double) is null"),
        "zero": one(con, f"select count(*) from {m} where try_cast(value as double) = 0"),
        "negative": one(con, f"select count(*) from {m} where try_cast(value as double) < 0"),
        "dup_key_in_pass": one(con, f"select count(*) from (select sensor_id, measured_at from {m} group by all having count(*) > 1)"),
        "pages_gt_0": one(con, f"select count(*) from {m} where page <> '0'"),
        "sensor_code_shapes": rows(con, f"select regexp_replace(sensor_code, '^[A-Za-z0-9]+-', '<station>-') s, count(*) "
                                        f"from {m} group by 1 order by 2 desc limit 15"),
        "minute_not_00": one(con, f"select count(*) from {m} where substr(measured_at, 15, 5) <> '00:00'"),
    }
    s = "b_gios_air_sensors"
    p["sensors"] = {
        "rows": one(con, f"select count(*) from {s}"),
        "distinct": one(con, f"select count(distinct sensor_id) from {s}"),
        "parameter_codes": rows(con, f"select parameter_code, parameter_formula, parameter_name, count(*) from {s} group by all order by 4 desc"),
        "sensors_without_values": one(con, f"select count(*) from {s} where sensor_id not in (select sensor_id from b_gios_air_measurements)"),
    }
    st = "b_gios_air_stations"
    p["stations"] = {
        "rows": one(con, f"select count(*) from {st}"),
        "distinct": one(con, f"select count(distinct station_id) from {st}"),
        "null_street": one(con, f"select count(*) from {st} where street is null"),
        "null_commune_district": one(con, f"select count(*) from {st} where commune is null or district is null"),
        "voivodeships": rows(con, f"select voivodeship, count(*) from {st} group by 1 order by 1"),
        "lat_lon_range": one(con, f"select min(try_cast(latitude as double)) || '/' || max(try_cast(latitude as double)) || ' ' || "
                                  f"min(try_cast(longitude as double)) || '/' || max(try_cast(longitude as double)) from {st}"),
    }
    show(con, "values per parameter (Bronze)", f"select s.parameter_code, count(*), count(m.value), min(try_cast(m.value as double)), "
              f"max(try_cast(m.value as double)), avg(try_cast(m.value as double)) from {m} m join {s} s using (sensor_id) group by 1 order by 1")
    return p


def silver(con) -> dict:
    out: dict = {}
    srcs = {"gios_air_stations": "b_gios_air_stations", "gios_air_sensors": "b_gios_air_sensors"}
    yml = "warehouse/models/silver/gios_air_quality/_gios_air_quality.yml"
    for model in ("gios_air_stations", "gios_air_sensors"):
        sql = render(f"warehouse/models/silver/gios_air_quality/{model}.sql", srcs)
        counts = []
        for _ in (1, 2):
            con.execute(f"create or replace table {model} as {sql}")
            counts.append(one(con, f"select count(*) || ':' || coalesce(sum(hash(m)), 0) from {model} as m"))
        out[model] = {"rows": int(counts[0].split(":")[0]), "rebuild_identical": counts[0] == counts[1],
                      "tests": schema_tests(con, yml, model, model)}
    for test in ("gios_air_quality_stations_complete", "gios_air_quality_sensors_complete"):
        sql = render(f"warehouse/tests/{test}.sql", {})
        out[test] = len(con.execute(sql).fetchall())
    h = "b_gios_air_hourly"
    out["gios_air_hourly"] = {
        "rows": one(con, f"select count(*) from {h}"),
        "days": rows(con, f"select entity, count(*), count(value), count(distinct sensor_id) from {h} group by 1 order by 1"),
        "dup_keys": one(con, f"select count(*) from (select sensor_id, measured_at from {h} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {h} where sensor_id is null or measured_at is null or station_id is null or raw_sha256 is null"),
        "null_values": one(con, f"select count(*) from {h} where value is null"),
        "status": rows(con, f"select validation_status, count(*) from {h} group by 1"),
        "entity_matches_day": one(con, f"select count(*) from {h} where entity <> strftime(measured_on, '%Y-%m-%d')"),
        "bronze_keys_vs_silver": one(con, "select count(*) from (select distinct sensor_id, measured_at from b_gios_air_measurements)")
        - one(con, f"select count(*) from {h}"),
        "sensors_not_in_dim": one(con, f"select count(distinct sensor_id) from {h} where sensor_id not in (select sensor_id from gios_air_sensors)"),
    }
    return out


def gold(con) -> dict:
    g: dict = {}
    con.execute("create or replace table geo as select 'TERC:' || c as geography_key, c as terc_code, 'wojewodztwo' as level_name "
                "from (values ('02'), ('04'), ('06'), ('08'), ('10'), ('12'), ('14'), ('16'), ('18'), ('20'), ('22'), ('24'), "
                "('26'), ('28'), ('30'), ('32')) as t (c)")
    env = Environment(extensions=["jinja2.ext.do"])
    macros = Path("warehouse/macros/sources/gios_air_quality.sql").read_text()
    mapping = env.from_string(macros + "with {{ poc_geography__gios_air_quality() }} select * from gios_air_quality").render()
    con.execute(f"create or replace table poc_map_geography as {mapping}")
    g["map_rows"] = one(con, "select count(*) from poc_map_geography")
    sql = render("warehouse/models/gold/poc_build_gios_air_quality.sql",
                 {"gios_air_hourly": "b_gios_air_hourly", "poc_map_geography": "poc_map_geography"})
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table build as {sql}")
        counts.append(one(con, "select count(*) || ':' || sum(hash(b)) from build as b"))
    g["rows"] = int(counts[0].split(":")[0])
    g["rebuild_identical"] = counts[0] == counts[1]
    cols = [d[0] for d in con.execute("select * from build limit 0").description]
    expected = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
                "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
                "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
                "source_version", "raw_sha256"]
    g["columns_ok"] = cols == expected
    g["dup_series_date"] = one(con, "select count(*) from (select measure_key || '|' || geography_key || case when geography_key = '_X' "
                                    "then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json, date_key "
                                    "from build group by all having count(*) > 1)")
    g["freq_per_measure"] = one(con, "select count(*) from (select measure_key from build group by 1 having count(distinct frequency) > 1)")
    g["rows_geography_unmatched"] = one(con, "select count(*) from build where geography_key = '_X'")
    g["null_value"] = one(con, "select count(*) from build where value is null")
    g["negative"] = one(con, "select count(*) from build where value < 0")
    g["max_rows_per_measure_area_day"] = one(con, "select coalesce(max(n), 0) from (select measure_key, geography_key, date_key, count(*) n from build group by all)")
    g["measures"] = rows(con, "select measure_codes, count(*), count(distinct geography_key), min(date_key), max(date_key), "
                              "round(min(value), 1), round(max(value), 1) from build group by 1 order by 1")
    g["poland"] = rows(con, "select period_label, measure_codes, round(value, 2) from build where geography_key = 'PL' order by 1, 2")
    g["station_days"] = rows(con, f"""
        with s as (select s.sensor_id, s.parameter_code from gios_air_sensors s where s.parameter_code in {POLLUTANTS})
        select h.measured_on, s.parameter_code, count(*) filter (where n >= 18), count(*) filter (where n < 18)
        from (select sensor_id, measured_on, count(value) n from b_gios_air_hourly group by all) h join s using (sensor_id)
        group by all order by 1, 2""")
    g["spot_mazowieckie"] = rows(con, "select period_label, measure_codes, round(value, 2) from build where geography_source_code = 'MAZOWIECKIE' "
                                      "and measure_codes in ('PM10', 'PM2.5') order by 1, 2")
    # cross-check one Poland value by hand from Silver: PM10, newest full day
    g["pl_pm10_check"] = rows(con, f"""
        with day as (select max(date_key) d from build where measure_codes = 'PM10' and geography_key = 'PL'),
        sd as (select h.sensor_id, avg(h.value) m from b_gios_air_hourly h join gios_air_sensors s using (sensor_id)
               where s.parameter_code = 'PM10' and h.measured_on = (select d from day) group by 1 having count(h.value) >= 18)
        select (select d from day), round(avg(m), 4), count(*), (select round(value, 4) from build where measure_codes = 'PM10'
               and geography_key = 'PL' and date_key = (select d from day)) from sd""")
    return g


def licence() -> dict:
    from warehouse import publish

    src = list(csv.DictReader(open("warehouse/seeds/source.csv", encoding="utf-8")))
    lic = list(csv.DictReader(open("warehouse/seeds/licence.csv", encoding="utf-8")))
    row = next(r for r in src if r["source_key"] == "gios_air_quality")
    terms = next((r for r in lic if r["licence_key"] == row["licence_key"]), None)
    out = {"source_row": {k: row[k] for k in ("source_key", "register_id", "licence_key")},
           "licence_row": terms and {k: terms[k] for k in ("licence_key", "attribution_text", "commercial_ok")}}
    try:
        out["licensed"] = "gios_air_quality" in publish.licensed_sources(src, lic)
    except Exception as exc:  # noqa: BLE001
        out["licensed_error"] = f"{type(exc).__name__}: {exc}"[:200]
    return out


def archive_zip() -> dict:
    r = requests.get("https://powietrze.gios.gov.pl/pjp/archives", headers={"User-Agent": landing.USER_AGENT}, timeout=60)
    links = re.findall(r'href\s*=\s*"(/pjp/archives/downloadFile/\d+)"[\s\S]{0,600}?Wyniki pomiarów z (\d{4}) roku', r.text)
    out = {"years": len(links), "first": links[:1], "last": links[-1:]}
    if links:
        time.sleep(5)
        h = requests.head("https://powietrze.gios.gov.pl" + links[-1][0], headers={"User-Agent": landing.USER_AGENT},
                          timeout=60, allow_redirects=True)
        out["newest_head"] = {"status": h.status_code, "bytes": h.headers.get("Content-Length"),
                              "type": h.headers.get("Content-Type"), "name": h.headers.get("Content-Disposition"),
                              "last_modified": h.headers.get("Last-Modified")}
    return out


def cleanup() -> dict:
    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:  # noqa: BLE001
                cat.drop_table(table)
            removed["tables"].append(table[-1])
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
    return removed


def main(argv: list[str]) -> int:
    status = 0
    try:
        note("gios_air_quality trial landing", {**STATS, **landing_runs()})
        con = duckdb.connect()
        note("gios_air_quality trial bronze", bronze_runs(con))
        note("gios_air_quality trial bronze profile", bronze_profile(con))
        s = silver(con)
        bad = any(v for m in ("gios_air_stations", "gios_air_sensors") for k, v in s[m]["tests"].items() if not k.endswith("sample") and v)
        note("gios_air_quality trial silver", s, "warning" if bad else "notice")
        note("gios_air_quality trial gold", gold(con))
        note("gios_air_quality trial licence and archive", {"licence": licence(), "archive": archive_zip()})
    except Exception as exc:  # noqa: BLE001
        status = 1
        traceback.print_exc()
        note("gios_air_quality trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"}, "error")
    finally:
        note("gios_air_quality trial cleanup", cleanup())
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
