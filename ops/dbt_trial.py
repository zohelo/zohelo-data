#!/usr/bin/env python3
"""Trial: can dbt (dbt-duckdb + DuckDB iceberg) read Bronze and write Iceberg tables to R2?

Runs against a scratch namespace (zz_dbt_trial) that is dropped at the end:
  A  DuckDB alone: attach the R2 catalog, read bronze.teryt_terc, CREATE TABLE AS
  B  dbt: dbt build --target trial (warehouse/ project) -> zz_dbt_trial.teryt_terc
  C  independent readback of the dbt table through R2 SQL
  D  drop the scratch tables and namespace
Each step is published as an annotation. Remove this script when the result is decided.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import time

import duckdb
import requests

from ingestion.common.report import annotate

SCRATCH = "zz_dbt_trial"
SOURCE = "r2.bronze.teryt_terc"
WAREHOUSE_DIR = Path(__file__).resolve().parents[1] / "warehouse"


def connect():
    con = duckdb.connect()
    for sql in ("INSTALL httpfs", "LOAD httpfs", "INSTALL iceberg", "LOAD iceberg"):
        con.execute(sql)
    con.execute("CREATE SECRET r2_catalog (TYPE ICEBERG, TOKEN ?)", [os.environ["R2_DATA_CATALOG_TOKEN"]])
    con.execute(f"ATTACH '{os.environ['R2_WAREHOUSE']}' AS r2 "
                f"(TYPE ICEBERG, ENDPOINT '{os.environ['R2_CATALOG_URI']}', SECRET r2_catalog)")
    return con


def step(results: dict, name: str, fn):
    started = time.monotonic()
    try:
        value = fn()
        results[name] = {"ok": True, "result": value, "seconds": round(time.monotonic() - started, 1)}
    except Exception as exc:
        results[name] = {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:600]}",
                         "seconds": round(time.monotonic() - started, 1)}
    return results[name]["ok"]


def r2_sql(query: str):
    account, bucket = os.environ["CLOUDFLARE_ACCOUNT_ID"], os.environ["R2_LAKEHOUSE_BUCKET"]
    response = requests.post(
        f"https://api.sql.cloudflarestorage.com/api/v1/accounts/{account}/r2-sql/query/{bucket}",
        headers={"Authorization": f"Bearer {os.environ['R2_DATA_CATALOG_TOKEN']}"},
        json={"query": query}, timeout=120)
    response.raise_for_status()
    return str(response.json().get("result"))[:400]


def dbt(*args) -> str:
    env = dict(os.environ, DBT_PROFILES_DIR=str(WAREHOUSE_DIR))
    run = subprocess.run(["dbt", *args, "--project-dir", str(WAREHOUSE_DIR), "--target", "trial"],
                         cwd=WAREHOUSE_DIR, env=env, capture_output=True, text=True, timeout=1800)
    tail = (run.stdout + run.stderr)[-1500:]
    if run.returncode:
        raise RuntimeError(f"dbt {args[0]} exit {run.returncode}: {tail}")
    return tail[-500:]


def main() -> int:
    results: dict = {}
    holder: dict = {}
    con = None
    try:
        if not step(results, "A1_attach", lambda: holder.setdefault("con", connect()) is not None):
            annotate("error", "dbt trial A duckdb", results)
            return 1
        con = holder["con"]
        step(results, "A2_read_bronze", lambda: con.execute(f"SELECT count(*) FROM {SOURCE}").fetchone()[0])
        step(results, "A3_create_namespace", lambda: con.execute(f"CREATE SCHEMA IF NOT EXISTS r2.{SCRATCH}") and True)
        if not step(results, "A4_ctas", lambda: con.execute(
                f"CREATE TABLE r2.{SCRATCH}.duckdb_ctas AS SELECT * FROM {SOURCE}") and
                con.execute(f"SELECT count(*) FROM r2.{SCRATCH}.duckdb_ctas").fetchone()[0]):
            step(results, "A4b_create_then_insert", lambda: (
                con.execute(f"CREATE TABLE r2.{SCRATCH}.duckdb_insert AS SELECT * FROM {SOURCE} LIMIT 0"),
                con.execute(f"INSERT INTO r2.{SCRATCH}.duckdb_insert SELECT * FROM {SOURCE}"),
                con.execute(f"SELECT count(*) FROM r2.{SCRATCH}.duckdb_insert").fetchone()[0])[-1])
        annotate("notice", "dbt trial A duckdb", {k: v for k, v in results.items()})
        step(results, "B1_dbt_debug", lambda: dbt("debug"))
        step(results, "B2_dbt_build", lambda: dbt("build", "--select", "teryt_terc"))
        step(results, "B3_duckdb_readback", lambda: con.execute(
            f"SELECT count(*) FROM r2.{SCRATCH}.teryt_terc").fetchone()[0])
        step(results, "C_r2_sql_readback", lambda: r2_sql(f"SELECT COUNT(*) AS n FROM {SCRATCH}.teryt_terc"))
        annotate("notice", "dbt trial B-C dbt", {k: v for k, v in results.items() if k[0] in "BC"})
    finally:
        if con is not None:
            def cleanup():
                dropped = []
                for (name,) in con.execute(
                        f"SELECT table_name FROM information_schema.tables "
                        f"WHERE table_catalog = 'r2' AND table_schema = '{SCRATCH}'").fetchall():
                    con.execute(f"DROP TABLE r2.{SCRATCH}.{name}")
                    dropped.append(name)
                con.execute(f"DROP SCHEMA IF EXISTS r2.{SCRATCH}")
                return dropped
            step(results, "D_cleanup", cleanup)
            annotate("notice" if results["D_cleanup"]["ok"] else "error", "dbt trial D cleanup",
                     results["D_cleanup"])
    ok = all(results.get(k, {}).get("ok") for k in ("A1_attach", "A2_read_bronze", "B2_dbt_build", "B3_duckdb_readback"))
    annotate("notice" if ok else "warning", "dbt trial verdict",
             {"dbt_writes_iceberg_to_r2": ok, "steps": {k: v["ok"] for k, v in results.items()}})
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
