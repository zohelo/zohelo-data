"""Dev trial (scratch only): per-block Gold of the MF local-government budget summaries and ENTSOG gas flows.

Runs warehouse/observations.py for each source into the scratch namespace and prefix zz_dev_goldje: the
first-load guard, the load, a second run with nothing to do, then checks of the scratch tables against production
Silver (rows with a value, rows not loaded by reason), the grain (series_key x date_key), null keys, unmatched
geography codes, statuses, units, Poland's newest values, and every row of the curated build stage
(gold.build_<source>) looked up in the new tables by area, frequency, date and value. Reads production Silver,
gold.map_geography and the curated build tables; writes nothing outside the scratch, which it removes. At most
10 annotations. Deleted before merge.

python -m dev.goldje_trial
"""

from __future__ import annotations

import contextlib
import os
import subprocess
import sys
import tempfile
import time
import traceback

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from warehouse import observations as obs

NS = "zz_dev_goldje"
SOURCES = tuple(sys.argv[1:]) or ("mf_jst_budgets", "entsog")


def connect():
    con = duckdb.connect()
    con.execute("SET memory_limit = '8GB'; SET preserve_insertion_order = false")
    con.execute("INSTALL httpfs; LOAD httpfs")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
    con.execute(
        "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    return con


def files(paths: list[str]) -> str:
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"


def run(source: str, *extra: str) -> list:
    started = time.monotonic()
    out = obs.run(["--source", source, "--trial", NS, "--minutes", "60", *extra])
    brief = [out["result"], out.get("built", 0), out.get("rows", 0), out.get("rejected", 0), out.get("failed", 0)]
    if out.get("rejected_blocks") or out.get("failed_blocks"):
        brief.append({"rejected": out.get("rejected_blocks"), "failed": out.get("failed_blocks")})
    if "coverage" in out:
        c = out["coverage"]
        brief.append([c["blocks_in_gold"], c["blocks_in_silver"], c["series_in_gold"], c["coverage_pct"]])
    brief.append(round(time.monotonic() - started))
    return brief


def cleanup(cat, client) -> dict:
    gone = {"tables": 0, "objects": 0}
    names = [f"{s}_{t}" for s in SOURCES for t in obs.TABLES] + [obs.REJECTED[1]]
    errors = []
    for _ in range(3):
        listed = [t[1] for t in cat.list_tables(NS)] if (NS,) in cat.list_namespaces() else []
        if not listed:
            break
        for name in listed:
            try:
                cat.drop_table((NS, name))
                gone["tables"] += 1
            except Exception as exc:
                errors.append(f"{name}: {type(exc).__name__}")
        time.sleep(5)
    with contextlib.suppress(Exception):
        cat.drop_namespace(NS)
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=NS + "/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            gone["objects"] += len(keys)
    left = sum(
        len(p.get("Contents", []))
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=NS + "/")
    )
    production = {t[1] for t in cat.list_tables("gold")}
    return {
        **gone,
        "objects_left": left,
        "namespace_left": (NS,) in cat.list_namespaces(),
        "drop_errors": errors[:5],
        "production_gold_tables_of_these_loads": sorted(production & set(names) - {obs.REJECTED[1]}),
    }


def views(con, cat, source: str) -> dict:
    state = {name: et.state(cat.load_table(t), obs.ENTITY) for name, t in obs.table_ids(source, NS).items()}
    for name, short in (("observations", "o"), ("series", "s"), ("measures", "m"), ("geography", "g")):
        paths = [p for h in state[name].values() for p in h["paths"]]
        con.execute(f"create or replace view {short} as select * from {files(paths)}")
    silver = obs.silver_blocks(cat, source)
    ((block, item),) = silver.items()
    con.execute(f"create or replace view silver as select * from {files(item['paths'])}")
    return {
        "silver": [block, len(item["paths"]), item["rows"], round(item["bytes"] / 1e6, 1)],
        "gold_mb": {n: round(sum(h["bytes"] for h in s.values()) / 1e6, 2) for n, s in state.items()},
    }


def one(con, sql: str):
    return con.execute(sql).fetchall()[0]


def common(con) -> dict:
    rows, dup = one(
        con,
        "select coalesce(sum(n), 0), count(*) filter (where n > 1) "
        "from (select count(*) as n from o group by series_key, date_key)",
    )
    return {
        "rows": rows,
        "dup_series_x_date": dup,
        "null_keys": one(
            con,
            "select count(*) from o where series_key is null or measure_key is null or geography_key is null "
            "or date_key is null or frequency is null or value is null or observation_status_key is null",
        )[0],
        "series_measures": one(con, "select (select count(*) from s), (select count(*) from m)"),
        "freq": con.execute(
            "select frequency, count(*), min(date_key)::varchar, max(date_key)::varchar from o group by 1 order by 1"
        ).fetchall(),
        "status": con.execute("select observation_status_key, count(*) from o group by 1 order by 2 desc").fetchall(),
        "zero_nan_inf": one(
            con,
            "select count(*) filter (where value = 0), count(*) filter (where isnan(value)), "
            "count(*) filter (where isinf(value)) from o",
        ),
        "geo_codes_unmatched_pts": one(
            con,
            "select count(*), count(*) filter (where geography_key = '_X'), "
            "coalesce(sum(points) filter (where geography_key = '_X'), 0) from g",
        ),
        "unmatched": [
            r[0]
            for r in con.execute(
                "select geography_source_code from g where geography_key = '_X' order by points desc"
            ).fetchall()
        ][:30],
        "units": con.execute("select unit_key, count(*), sum(points) from m group by 1 order by 3 desc").fetchall()[
            :12
        ],
    }


def curated_jst(con, cat) -> list:
    """Every row of the curated build_mf_jst_budgets looked up in the new tables by unit type, quarter and value."""
    arrow = (
        cat.load_table(("gold", "build_mf_jst_budgets"))
        .scan(selected_fields=("measure_key", "breakdown_json", "date_key", "value"))
        .to_arrow()
    )
    con.register("cur", arrow)
    con.execute(
        """create or replace temp table cur_found as
           select c.*, exists (
               select 1 from o as n
               where n.date_key = c.date_key and n.series_key like '%|PL|' || c.breakdown_json
               and abs(n.value - c.value) <= 1e-9 * greatest(1, abs(c.value))) as found
           from cur as c"""
    )
    return [
        one(con, "select count(*), count(*) filter (where found) from cur_found"),
        con.execute(
            "select measure_key, count(*) from cur_found where not found group by all order by 2 desc"
        ).fetchall()[:8],
    ]


def curated_entsog(con, cat) -> list:
    """The curated daily direction totals (breakdown {}) against the sum of the new per-point rows of that day."""
    arrow = (
        cat.load_table(("gold", "build_entsog"))
        .scan(selected_fields=("measure_key", "breakdown_json", "frequency", "date_key", "value"))
        .to_arrow()
    )
    con.register("cur", arrow)
    con.execute(
        """create or replace temp table cur_found as
           with tot as (
               select split_part(measure_key, ':', 4) as direction, date_key, value
               from cur where frequency = 'D' and breakdown_json = '{}'
           ), new as (
               select split_part(split_part(measure_key, '@', 1), ':', 4) as direction, date_key, sum(value) as value,
                      count(*) as points
               from o group by all
           )
           select t.*, n.value as new_value, n.points,
                  n.value is not null and abs(n.value - t.value) <= 1e-6 * greatest(1, abs(t.value)) as found
           from tot as t left join new as n using (direction, date_key)"""
    )
    return [
        one(con, "select count(*), count(*) filter (where found) from cur_found"),
        con.execute(
            "select direction, date_key::varchar, value, new_value, points from cur_found where not found "
            "order by date_key desc limit 6"
        ).fetchall(),
    ]


def check_jst(con) -> dict:
    return {
        "silver": one(
            con,
            "select count(*), count(*) filter (where value_pln is null or isnan(value_pln) or isinf(value_pln)), "
            "count(*) filter (where period_date > try_strptime(left(source_version, 8), '%Y%m%d')), "
            "count(*) filter (where try_strptime(left(source_version, 8), '%Y%m%d') is null), "
            "min(period_date)::varchar, max(period_date)::varchar, min(source_version), max(source_version) "
            "from silver",
        ),
        "silver_by_section_measure": con.execute(
            "select section, measure_code, count(distinct item_label || item_occurrence), count(*) from silver "
            "group by all order by 1, 2"
        ).fetchall(),
        "rows_by_unit_type": con.execute(
            "select json_extract_string(s.breakdown_json, '$.unit_type'), s.is_total, count(distinct s.series_key), "
            "sum(s.points) from s group by all order by 1"
        ).fetchall(),
        "measures_by_freq": con.execute("select frequency, count(*), sum(points) from m group by 1").fetchall(),
        "measure_key_max_len": one(con, "select max(length(measure_key)) from m")[0],
        "keys_with_pipe": one(con, "select count(*) from m where contains(measure_key, '|')")[0],
        "quarters_per_series": one(con, "select min(points), max(points), count(*) from s"),
        "poland_jst_newest": con.execute(
            "select measure_key, date_key::varchar, value from o where series_key like '%unit_type\":\"jst%' "
            "and (measure_key like '%:executed:revenue:DOCHODY OGÓŁEM@Q' or measure_key like '%:executed:expenditure:"
            "WYDATKI OGÓŁEM%' or measure_key like '%:total:liabilities:E@Q') "
            "qualify row_number() over (partition by measure_key order by date_key desc) = 1 order by 1"
        ).fetchall(),
    }


def check_entsog(con) -> dict:
    return {
        "silver": one(
            con,
            "select count(*), count(*) filter (where value_kwh_d is null), "
            "count(*) filter (where value_kwh_d is not null and last_update_utc < period_to_utc), "
            "count(*) filter (where value_kwh_d is not null and last_update_utc < period_to_utc "
            "and period_to_utc < (select max(last_update_utc) from silver) - interval 7 day), "
            "count(distinct point_key || direction_key), min(gas_day)::varchar, max(gas_day)::varchar, "
            "max(last_update_utc)::varchar from silver",
        ),
        "silver_status": con.execute(
            "select flow_status, count(*) from silver where value_kwh_d is not null group by 1 order by 2 desc"
        ).fetchall(),
        "point_days_twice": one(
            con,
            "select count(*) from (select point_key, direction_key, gas_day from silver "
            "group by all having count(*) > 1)",
        )[0],
        "series_points": one(con, "select count(*), min(points), max(points) from s"),
        "label_sample": con.execute("select breakdown_label_json from s order by points desc limit 3").fetchall(),
        "poland_newest": con.execute(
            "select o.measure_key, json_extract_string(s.breakdown_json, '$.point'), o.date_key::varchar, o.value, "
            "o.observation_status_key from o join s using (series_key) "
            "qualify row_number() over (partition by o.series_key order by o.date_key desc) = 1 "
            "order by o.value desc limit 5"
        ).fetchall(),
    }


CHECKS = {"mf_jst_budgets": (check_jst, curated_jst), "entsog": (check_entsog, curated_entsog)}


def main() -> None:
    tmp = "/mnt/duckdb_tmp"
    try:
        subprocess.run(["sudo", "mkdir", "-p", tmp], check=True)
        subprocess.run(["sudo", "chown", str(os.getuid()), tmp], check=True)
    except Exception:
        tmp = tempfile.mkdtemp(prefix="duckdb_tmp")
    os.environ["DUCKDB_TEMP_DIRECTORY"] = tmp
    cat, client = catalog(), s3()
    before = cleanup(cat, client)
    obs.annotate = lambda *a, **k: None  # the trial writes its own compact annotations
    first = {"cleanup_before": before, "model_digests": {s: obs.model_digest(s) for s in SOURCES}}
    try:
        for source in SOURCES:
            try:
                runs = {
                    "guard": run(source, "--continue-only"),
                    "tables_after_guard": (
                        sorted(t[1] for t in cat.list_tables(NS) if t[1].startswith(source + "_"))
                        if (NS,) in cat.list_namespaces()
                        else []
                    ),
                    "load": run(source),
                    "again": run(source),
                    "again_continue_only": run(source, "--continue-only"),
                }
                con = connect()
                head = {**first, "runs": runs, **views(con, cat, source), **common(con)}
                first = {}
                checks = CHECKS[source][0](con)
                checks["curated_rows_found_misses"] = CHECKS[source][1](con, cat)
                annotate("notice", f"goldje trial: {source} gold", head)
                annotate("notice", f"goldje trial: {source} checks", checks)
                con.close()
            except Exception:
                annotate("error", f"goldje trial: {source} failed", {"traceback": traceback.format_exc()[-3000:]})
    finally:
        annotate("notice", "goldje trial: cleanup", cleanup(cat, client))


if __name__ == "__main__":
    main()
