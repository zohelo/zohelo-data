"""Dev trial (PLG-006 NBP interest rates, GL-003 OECD CLI): Landing → Bronze → Silver into scratch only.

Per source: the bulk_file Landing load into the Landing bucket prefix zz_dev_new_sources/ (twice: the second
must be "unchanged"), the source's Bronze load into Iceberg namespace zz_dev_new_sources and the lake prefix
zz_dev_new_sources/ (twice: the second must append nothing), then the Silver model's SQL (rendered from
warehouse/models/silver/<source>/) in local DuckDB over that Bronze table, with the model's tests as queries.
Everything written is removed in `finally` (tables purged, namespace dropped, both prefixes deleted).
Never writes production namespaces or prefixes; never publishes.
"""

from __future__ import annotations

import importlib
from pathlib import Path
import re
import sys

import duckdb

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

SCRATCH = "zz_dev_new_sources"
MODELS = {
    "nbp_interest_rates": (
        "warehouse/models/silver/nbp_interest_rates/nbp_policy_rates.sql",
        "nbp_interest_rate_items",
        [
            (
                "rows by rate",
                "select rate_code, count(*), min(effective_from), max(effective_from) from silver group by 1",
            ),
            ("in force", "select rate_code, name_en, effective_from, rate_pct from silver where valid_to is null"),
            (
                "unique",
                "select count(*) from (select rate_code, effective_from from silver group by 1, 2 having count(*) > 1)",
            ),
            (
                "nulls",
                "select count(*) from silver where name_en is null or rate_pct is null or effective_from is null",
            ),
            ("bad codes", "select count(*) from silver where rate_code not in ('ref','lom','dep','red','dys')"),
            (
                "ref history",
                "select effective_from, rate_pct from silver where rate_code = 'ref' order by 1 desc limit 8",
            ),
        ],
    ),
    "oecd": (
        "warehouse/models/silver/oecd/oecd_cli.sql",
        "oecd_observations",
        [
            ("rows", "select count(*), count(distinct ref_area), min(time_period), max(time_period) from silver"),
            (
                "unique",
                "select count(*) from (select series_key, time_period from silver group by 1, 2 having count(*) > 1)",
            ),
            ("nulls", "select count(*) from silver where ref_area is null or freq is null or measure is null"),
            ("bad freq", "select count(*) from silver where freq not in ('M','Q','A')"),
            ("value not numeric", "select count(*) from silver where obs_value is null"),
            (
                "LI areas (AA, IX, M)",
                "select list(distinct ref_area order by ref_area) from silver where measure = 'LI' and adjustment = 'AA' and transformation = 'IX' and freq = 'M'",
            ),
            (
                "POL and EU series",
                "select ref_area, measure, adjustment, transformation, freq, count(*), min(time_period), max(time_period) from silver where ref_area in ('POL','EU','EA20','OECD','OECDE') group by all order by all",
            ),
            (
                "POL latest",
                "select measure, time_period, obs_value from silver where ref_area = 'POL' and freq = 'M' qualify row_number() over (partition by measure order by time_period desc) <= 2",
            ),
        ],
    ),
}


def render(path: str, table: str) -> str:
    sql = Path(path).read_text()
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    return re.sub(r"\{\{\s*source\([^)]*\)\s*\}\}", table, sql)


def trial(source: str) -> None:
    sql_path, bronze, checks = MODELS[source]
    for attempt in (1, 2):
        status = bulk_file.main(source, "load", landing_root=SCRATCH)
        annotate("notice", f"{source} landing load {attempt}", {"exit": status})
    loader = importlib.import_module(f"ingestion.to_bronze.{source}.load")
    for attempt in (1, 2):
        status = loader.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        annotate("notice", f"{source} bronze load {attempt}", {"exit": status})
    con = duckdb.connect()
    con.register("bronze", catalog().load_table((SCRATCH, bronze)).scan().to_arrow())
    con.execute(f"create table silver as {render(sql_path, 'bronze')}")
    results = {"bronze_rows": con.execute("select count(*) from bronze").fetchone()[0]}
    results["silver_rows"] = con.execute("select count(*) from silver").fetchone()[0]
    for title, query in checks:
        results[title] = [list(map(str, row)) for row in con.execute(query).fetchall()][:40]
    annotate("notice", f"{source} silver (scratch)", results)


def cleanup() -> None:
    removed = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:
                cat.drop_table(table)
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    for bucket_var, label in (
        ("STORAGE_LANDING_BUCKET_NAME", "landing_objects"),
        ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects"),
    ):
        bucket = req(bucket_var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    annotate("notice", "scratch removed", removed)


def main() -> int:
    sources = sys.argv[1:] or list(MODELS)
    failed = 0
    try:
        for source in sources:
            try:
                trial(source)
            except Exception as exc:
                failed += 1
                annotate("error", f"{source} trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
