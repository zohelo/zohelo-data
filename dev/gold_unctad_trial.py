"""Dev trial (scratch only): per-block Gold of the nine UNCTAD Data Hub tables (gold lane, #2873).

Runs warehouse/observations.py for unctad into the scratch namespace and prefix zz_dev_gold_unctad: the
first-load guard, the load, a second run with nothing to do, then checks of the scratch tables against
production Silver (cells with a value, cells not loaded by reason, per table), the grain (series_key x date_key),
null keys, one frequency per measure, unmatched geography codes, statuses, units, Poland's newest values, and every
row of the curated build stage (gold.build_unctad) looked up in the new tables by area, frequency, date and value.
Reads production Silver, gold.map_geography and gold.build_unctad; writes nothing outside the scratch, which it
removes. At most 10 annotations. Deleted before merge.

python -m dev.gold_unctad_trial
"""

from __future__ import annotations

import contextlib
import os
import subprocess
import tempfile
import time
import traceback

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from warehouse import observations as obs

NS = "zz_dev_gold_unctad"
SOURCE = "unctad"


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
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "], union_by_name = true)"


def run(*extra: str) -> list:
    started = time.monotonic()
    out = obs.run(["--source", SOURCE, "--trial", NS, "--minutes", "60", *extra])
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
    names = [f"{SOURCE}_{t}" for t in obs.TABLES]
    errors = []
    for _ in range(3):
        listed = [t[1] for t in cat.list_tables(NS)] if (NS,) in cat.list_namespaces() else []
        if not listed:
            break
        for name in listed:
            try:
                cat.drop_table((NS, name))
                gone["tables"] += 1
            except Exception as exc:  # noqa: BLE001 - reported in the annotation
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
        "production_gold_tables_of_this_load": sorted(production & set(names)),
    }


def views(con, cat) -> dict:
    state = {name: et.state(cat.load_table(t), obs.ENTITY) for name, t in obs.table_ids(SOURCE, NS).items()}
    for name, short in (("observations", "o"), ("series", "s"), ("measures", "m"), ("geography", "g")):
        paths = [p for h in state[name].values() for p in h["paths"]]
        con.execute(f"create or replace view {short} as select * from {files(paths)}")
    silver = obs.silver_blocks(cat, SOURCE)
    paths = [p for item in silver.values() for p in item["paths"]]
    con.execute(f"create or replace view silver as select * from {files(paths)}")
    return {
        "silver": {b: [len(i["paths"]), i["rows"], round(i["bytes"] / 1e6, 1)] for b, i in sorted(silver.items())},
        "gold_mb": {n: round(sum(h["bytes"] for h in s.values()) / 1e6, 2) for n, s in state.items()},
    }


def one(con, sql: str):
    return con.execute(sql).fetchone()


CELLS = """
create or replace table cells as
select
    s.entity,
    s.period,
    s.economy,
    m.measure,
    try_cast(json_extract_string(s.measure_values, '$."' || m.measure || '"') as double) as value,
    json_extract_string(s.measure_footnotes, '$."' || m.measure || '"') as footnote,
    json_extract_string(s.measure_missing, '$."' || m.measure || '"') as missing
from silver as s
cross join unnest(json_keys(s.measure_values)) as m (measure)
"""


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
        "measures_with_2_freq": one(
            con, "select count(*) from (select measure_key from s group by 1 having count(distinct frequency) > 1)"
        )[0],
        "series_measures": one(con, "select (select count(*) from s), (select count(*) from m)"),
        "by_block": con.execute(
            "select s.block_id, count(*), count(distinct o.series_key), count(distinct s.measure_key), "
            "min(o.date_key)::varchar, max(o.date_key)::varchar, any_value(o.frequency) "
            "from o inner join s using (series_key) group by 1 order by 1"
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
            "coalesce(sum(points) filter (where geography_key = '_X'), 0), "
            "count(*) filter (where geography_key = '_Z') from g",
        ),
        "mapped": con.execute(
            "select geography_key, count(*) from g where geography_key not in ('_X', '_Z') group by 1 order by 1"
        ).fetchall()[:40],
    }


def checks(con) -> dict:
    con.execute(CELLS)
    return {
        "cells": con.execute(
            """select entity, count(*), count(value),
                      count(*) filter (where value is not null and missing is not null),
                      count(*) filter (where value is not null and (isnan(value) or isinf(value))),
                      count(*) filter (where value is not null and (footnote ilike '%forecast%'
                          or footnote ilike '%projection%' or footnote ilike '%projected%')),
                      count(*) filter (where value is not null and not regexp_matches(period,
                          '^\\d{4}$|^\\d{4}Q0?[1-4]$|^\\d{4}M(0[1-9]|1[0-2])$')),
                      count(*) filter (where value = 0)
               from cells group by 1 order by 1"""
        ).fetchall(),
        "expected_rows": one(
            con,
            """select count(*) from cells where value is not null and not isnan(value) and not isinf(value)
               and missing is null and coalesce(footnote, '') not ilike '%forecast%'
               and coalesce(footnote, '') not ilike '%projection%' and coalesce(footnote, '') not ilike '%projected%'
               and regexp_matches(period, '^\\d{4}$|^\\d{4}Q0?[1-4]$|^\\d{4}M(0[1-9]|1[0-2])$')""",
        )[0],
        "periods_unplaced": con.execute(
            "select entity, period, count(*) from cells where value is not null and not regexp_matches(period, "
            "'^\\d{4}$|^\\d{4}Q0?[1-4]$|^\\d{4}M(0[1-9]|1[0-2])$') group by all order by 3 desc"
        ).fetchall()[:6],
        "missing_reasons": con.execute(
            "select missing, count(*), count(value), count(*) filter (where value = 0) from cells "
            "where missing is not null group by 1 order by 2 desc"
        ).fetchall()[:8],
        "footnotes": con.execute(
            "select left(footnote, 60), count(*) from cells where value is not null and footnote is not null "
            "group by 1 order by 2 desc"
        ).fetchall()[:12],
        "measures": con.execute(
            "select entity, list(distinct measure) from cells group by 1 order by 1"
        ).fetchall(),
        "newest_period": con.execute(
            "select entity, max(period) from cells where value is not null group by 1 order by 1"
        ).fetchall(),
    }


def curated(con, cat) -> list:
    """Every row of the curated build stage, looked up in the new tables by area code, frequency, date and value."""
    table = cat.load_table(("gold", "build_unctad"))
    con.register(
        "cur",
        table.scan(selected_fields=("geography_source_code", "frequency", "date_key", "value")).to_arrow(),
    )
    return list(
        one(
            con,
            """with new as (select s.geography_source_code as area, o.frequency, o.date_key, o.value
                            from o inner join s using (series_key))
               select count(*), count(*) filter (where exists (
                   select 1 from new as n where n.area is not distinct from c.geography_source_code
                   and n.frequency = c.frequency and n.date_key = c.date_key and n.value = c.value)) from cur as c""",
        )
    )


def poland(con) -> dict:
    return {
        "measures_rows": one(
            con,
            "select count(distinct s.measure_key), count(*) from o inner join s using (series_key) "
            "where o.geography_key = 'PL'",
        ),
        "newest": con.execute(
            "select o.measure_key, o.date_key::varchar, o.value, o.observation_status_key "
            "from o inner join s using (series_key) where o.geography_key = 'PL' and ("
            "o.measure_key like 'unctad:unctad_fdi_flows_stock:%:08.1@A' "
            "or o.measure_key like 'unctad:unctad_merchandise_trade:%:02@A' "
            "or o.measure_key like 'unctad:unctad_services_trade:%:02.S@A' "
            "or o.measure_key like 'unctad:unctad_merchant_fleet:%') "
            "qualify row_number() over (partition by o.series_key order by o.date_key desc) = 1 order by 1"
        ).fetchall()[:10],
        "other_tables_pl_keys": con.execute(
            "select s.block_id, count(distinct s.measure_key) from o inner join s using (series_key) "
            "where o.geography_key = 'PL' group by 1 order by 1"
        ).fetchall(),
    }


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
    digests = {s: obs.model_digest(s) for s in sorted(obs.SOURCES)}
    try:
        runs = {
            "guard": run("--continue-only"),
            "tables_after_guard": (
                sorted(t[1] for t in cat.list_tables(NS)) if (NS,) in cat.list_namespaces() else []
            ),
            "load": run(),
            "again": run(),
            "again_continue_only": run("--continue-only"),
        }
        annotate("notice", "unctad trial: runs", {"cleanup_before": before, "model_digests": digests, "runs": runs})
        con = connect()
        annotate("notice", "unctad trial: gold", {**views(con, cat), **common(con)})
        found = checks(con)
        measures = found.pop("measures")
        annotate("notice", "unctad trial: silver cells", found)
        annotate("notice", "unctad trial: measures", {"measures": measures})
        annotate("notice", "unctad trial: poland and curated", {**poland(con), "curated_found": curated(con, cat)})
        con.close()
    except Exception:
        annotate("error", "unctad trial: failed", {"traceback": traceback.format_exc()[-3000:]})
    finally:
        annotate("notice", "unctad trial: cleanup", cleanup(cat, client))


if __name__ == "__main__":
    main()
