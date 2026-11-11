"""Dev trial (scratch only, deleted before merge): UNCTAD rework of PR #2769.

Landing (twice) into prefix zz_dev_unctad_rework/, Bronze (twice) into namespace zz_dev_unctad_rework, then the
dbt-compiled SQL of the nine Silver models, their tests, tests/unctad_areas_present.sql, poc_map_geography,
poc_build_unctad, grain_check and build_check run in local DuckDB (in memory) over that Bronze table. Prod tables
are only read (dimension inputs of map_geography, dim_measure); no prod table is written. Silver and Gold are
built twice to show the rebuild gives the same rows. Every scratch table and object is removed in `finally`.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import time

import duckdb

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.unctad import load as bronze
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_unctad_rework"
COMPILED = ROOT / "warehouse" / "target" / "compiled" / "zohelo"
SILVER = [
    "unctad_fdi_flows_stock", "unctad_merchandise_trade", "unctad_services_trade", "unctad_digital_services_trade",
    "unctad_commodity_prices", "unctad_liner_shipping_connectivity", "unctad_merchant_fleet", "unctad_seaborne_trade",
    "unctad_port_calls",
]


def note(title: str, payload, level: str = "notice") -> None:
    text = json.dumps(payload, ensure_ascii=False, default=str)
    for i in range(0, len(text), 3500):
        annotate(level, f"{title} [{i // 3500 + 1}]" if len(text) > 3500 else title, text[i : i + 3500])
    print(f"### {title}: {text[:3000]}", flush=True)


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET preserve_insertion_order=false;")
    for ext in ("httpfs", "iceberg"):
        con.execute(f"INSTALL {ext}; LOAD {ext};")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").strip("/")
    con.execute(f"""CREATE SECRET r2_catalog (TYPE iceberg, TOKEN '{req("STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN")}')""")
    con.execute(
        f"""CREATE SECRET r2_files (TYPE s3, KEY_ID '{req("STORAGE_WRITE_ACCESS_KEY")}',
        SECRET '{req("STORAGE_WRITE_SECRET_KEY")}', ENDPOINT '{endpoint}', REGION 'auto', URL_STYLE 'path')"""
    )
    con.execute(
        f"""ATTACH '{req("STORAGE_LAKEHOUSE_CATALOG_NAME")}' AS r2 (TYPE iceberg, SECRET r2_catalog,
        ENDPOINT '{req("STORAGE_LAKEHOUSE_CATALOG_URL")}', READ_ONLY)"""
    )
    return con


def compile_sql() -> None:
    env = {**os.environ, "DBT_PROFILES_DIR": str(ROOT / "warehouse")}
    run = subprocess.run(
        ["dbt", "compile", "--target", "prod", "--select", "path:models/silver/unctad", "poc_map_geography",
         "poc_build_unctad", "poc_build_check", "poc_grain_check", "unctad_areas_present"],
        cwd=ROOT / "warehouse", env=env, capture_output=True, text=True,
    )
    note("dbt compile", {"exit": run.returncode, "tail": run.stdout[-1500:]}, "notice" if run.returncode == 0 else "error")
    if run.returncode:
        raise RuntimeError("dbt compile failed")


def rel(name: str) -> str:
    """Regex for a compiled relation r2.<schema>.<name>, quoted or not."""
    parts = name.split(".")
    return r"\.".join(rf'"?{re.escape(p)}"?' for p in ["r2", *parts])


def localize(sql: str) -> str:
    sql = re.sub(rel("bronze.unctad_rows"), f"r2.{SCRATCH}.unctad_rows", sql)
    for m in SILVER:
        sql = re.sub(rel(f"silver.{m}"), m, sql)
    sql = re.sub(rel("gold.build_unctad"), "build_unctad", sql)
    sql = re.sub(rel("gold.map_geography"), "map_geography", sql)
    # other sources' build stages: empty, so grain_check and build_check read only UNCTAD (their rows do not
    # touch UNCTAD's groups; the prod build reads them in full)
    sql = re.sub(r'"?r2"?\."?gold"?\."?(build_[a-z_]+)"?',
                 lambda m: m.group(0) if m.group(1) == "build_check" else "(select * from build_unctad where false)",
                 sql)
    return sql


def compiled(path: str) -> str:
    return localize((COMPILED / path).read_text())


def rows(con, query: str, limit: int = 80) -> list:
    return [[str(v)[:80] for v in r] for r in con.execute(query).fetchmany(limit)]


def build(con, name: str, sql: str) -> dict:
    con.execute(f"create or replace table {name} as {sql}")
    n, digest = con.execute(f"select count(*), sum(hash(t)) from {name} as t").fetchone()
    return {"rows": n, "digest": str(digest)}


def trial() -> None:
    ingest = {}
    for attempt in (1, 2):
        ingest[f"landing {attempt}"] = bulk_file.main("unctad", "load", landing_root=SCRATCH)
    for attempt in (1, 2):
        ingest[f"bronze {attempt}"] = bronze.load(0, landing_root=SCRATCH, namespace=SCRATCH)
    compile_sql()
    con = connect()
    ingest["bronze rows"] = dict(con.execute(
        f"select entity, count(*) from r2.{SCRATCH}.unctad_rows group by 1 order by 1").fetchall())
    note("UNCTAD landing+bronze (exit codes; per-load detail in the loaders' own annotations)", ingest)
    runs = {}
    for attempt in (1, 2):
        runs[attempt] = {m: build(con, m, compiled(f"models/silver/unctad/{m}.sql")) for m in SILVER}
    silver = {
        "rows": {m: v["rows"] for m, v in runs[1].items()},
        "rebuild identical (rows + hash)": runs[1] == runs[2],
        "range, null economy": rows(con, " union all ".join(
            f"select '{m[7:]}', min(period), max(period), count(*) filter (where economy is null) from {m}"
            for m in SILVER)),
        "built dimension codes": rows(con, """
            select 'fdi', json_extract_string(dimensions, '$.Flow'), json_extract_string(dimensions, '$.Direction'),
                count(*) from unctad_fdi_flows_stock group by all
            union all select 'merch', json_extract_string(dimensions, '$.Flow'), null, count(*)
                from unctad_merchandise_trade group by all
            union all select 'services', json_extract_string(dimensions, '$.Flow'), null, count(*)
                from unctad_services_trade group by all order by 1, 2, 3"""),
        "France codes": rows(con, " union all ".join(
            f"select '{m[7:]}', economy, economy_label from (select distinct economy, economy_label from {m}) "
            f"where economy_label ilike 'France%'" for m in SILVER[:3])),
    }
    tests = [*sorted((COMPILED / "models/silver/unctad/_unctad.yml").glob("*.sql")),
             COMPILED / "tests/unctad_areas_present.sql"]
    results, samples = {}, {}
    for path in tests:
        sql = localize(path.read_text())
        failures = con.execute(f"select count(*) from ({sql}) as t").fetchone()[0]
        results[path.stem[:60]] = failures
        if failures:
            samples[path.stem[:40]] = rows(con, f"select * from ({sql}) as t", 8)
    silver["tests"] = {"run": len(results), "failing": {k: v for k, v in results.items() if v}, "samples": samples}
    note("UNCTAD silver", silver, "notice" if not samples else "warning")
    gold = {}
    for attempt in (1, 2):
        gold[attempt] = {
            "map_geography": build(con, "map_geography", compiled("models/gold/poc_map_geography.sql")),
            "build_unctad": build(con, "build_unctad", compiled("models/gold/poc_build_unctad.sql")),
        }
    grain = build(con, "grain_check", compiled("models/gold/poc_grain_check.sql"))
    check = build(con, "build_check", compiled("models/gold/poc_build_check.sql"))
    out = {
        "build_unctad rows": gold[1]["build_unctad"]["rows"],
        "rebuild identical (rows + hash)": gold[1] == gold[2],
        "map_geography unctad (unmatched?, n)": rows(con, """select geography_key = '_X', count(*) from map_geography
            where source_system = 'unctad' group by 1"""),
        "dup series_key x date_key": con.execute("""select count(*) from (select measure_key || '|' || geography_key
            || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end
            || '|' || breakdown_json as series_key, date_key from build_unctad group by all
            having count(*) > 1)""").fetchone()[0],
        "grain_check rows": grain["rows"],
        "grain_check detail": rows(con, "select source_system, block_id, duplicate_keys from grain_check", 10),
        "build_check (block, rows, series, measures, unmatched, total, unplaced, first, last, freq, note)": rows(
            con, """select block_id, row_count, series, measures, rows_geography_unmatched, rows_total_series,
            rows_period_unplaced, first_date, last_date, frequencies, note from build_check
            where source_system = 'unctad'"""),
        "per measure (key, rows, areas, last)": rows(con, """select replace(measure_key, 'unctad:', ''), count(*),
            count(distinct geography_key), max(date_key) from build_unctad group by 1 order by 1"""),
        "PL/FR latest": rows(con, """select geography_key, replace(measure_key, 'unctad:', ''), year(date_key), value
            from build_unctad where geography_key in ('PL', 'FR')
            and measure_key like '%USD_MN' and measure_key not like '%:09:%'
            qualify row_number() over (partition by geography_key, measure_key order by date_key desc) = 1
            order by 1, 2"""),
    }
    note("UNCTAD gold", out)


def cleanup() -> None:
    removed = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            for attempt in range(6):
                try:
                    cat.purge_table(table)
                    break
                except Exception:  # noqa: BLE001 - rate limits: back off
                    time.sleep(10 * (attempt + 1))
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:  # noqa: BLE001 - report what is left
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    buckets = ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
    for bucket_var, label in zip(buckets, ("landing_objects", "lake_objects"), strict=True):
        bucket = req(bucket_var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in buckets
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/")
    )
    note("scratch removed", removed)


def main() -> int:
    failed = 0
    try:
        trial()
    except Exception as exc:  # noqa: BLE001 - report, then clean up
        failed = 1
        note("trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:2500]}"}, "error")
    finally:
        cleanup()
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
