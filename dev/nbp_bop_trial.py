"""Dev trial of nbp_bop: Landing (bulk_file) and Bronze into zz_dev_nbp_bop/ (both buckets) and namespace
zz_dev_nbp_bop, twice each (idempotency); then the Silver SQL and the tests nbp_bop_items / nbp_bop_unplaced and
the grain check run on the scratch Bronze in DuckDB. One compact annotation per layer. Everything written is
removed at the end (finally). Deleted before merge."""

from __future__ import annotations

import io
import json
from pathlib import Path
import re
import sys

import duckdb
import jinja2

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.nbp_bop import load
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_nbp_bop"


def render(path: str) -> str:
    sql = (ROOT / path).read_text()
    macro = (ROOT / "warehouse/macros/xlsx_cells.sql").read_text()
    head = "{% macro xlsx_newest_cells(relation) %}"
    newest = macro[macro.index(head) + len(head): macro.index("{% endmacro %}")].replace("{{ relation }}", "cells")
    sql = sql.replace("{{ xlsx_newest_cells(source('nbp_bop', 'nbp_bop_cells')) }}", newest)
    sql = sql.replace("{{ ref('nbp_bop_observations') }}", "silver")
    return jinja2.Environment().from_string(sql).render()


def main() -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    try:
        old = sys.stdout
        for attempt in (1, 2):
            sys.stdout = buf = io.StringIO()
            bulk_file.main("nbp_bop", "load", landing_root=f"{SCRATCH}/01_landing")
            sys.stdout = old
            last = [ln for ln in buf.getvalue().splitlines() if ln.strip()][-1:]
        landing = last
        for attempt in (1, 2):
            load.load(0, landing_root=f"{SCRATCH}/01_landing", namespace=SCRATCH)
        cells = cat.load_table((SCRATCH, "nbp_bop_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "nbp_bop_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # no rejections table when nothing was rejected
            rejected = []
        con = duckdb.connect()
        con.register("cells", cells)
        con.sql("create table silver as " + render("warehouse/models/silver/nbp_bop/nbp_bop_observations.sql"))
        annotate("notice", "nbp_bop Landing and Bronze", {"second_landing": str(landing)[:600],
                 "bronze_cells": cells.num_rows, "rejected": [r.get("error") for r in rejected]})
        per = con.sql("select entity, currency, count(*), count(distinct item_code), min(date_key), max(date_key) "
                      "from silver group by all order by all").fetchall()
        grain = con.sql("select count(*) from (select entity, currency, item_code, date_key from silver "
                        "group by all having count(*) > 1)").fetchall()[0][0]
        items = con.sql(render("warehouse/tests/nbp_bop_items.sql")).fetchall()
        unplaced = con.sql(render("warehouse/tests/nbp_bop_unplaced.sql")).fetchall()
        spot = con.sql("select entity, item_code, strftime(date_key, '%Y-%m'), value from silver where currency='EUR' "
                       "and item_code in ('current_account', 'international_investment_position_net', "
                       "'direct_investment_liabilities') qualify row_number() over (partition by entity, item_code "
                       "order by date_key desc) = 1 order by 1, 2").fetchall()
        annotate("notice" if not (grain or items or unplaced) else "warning", "nbp_bop Silver and tests",
                 json.loads(json.dumps({"rows": con.sql("select count(*) from silver").fetchall()[0][0],
                                        "per_entity_currency": per, "grain_duplicates": grain,
                                        "nbp_bop_items_failures": items[:15], "nbp_bop_unplaced_failures": unplaced[:10],
                                        "spot_eur_latest": spot}, default=str)))
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    removed += 1
        dropped = []
        for ns in (SCRATCH, "zz_dev_gold_stat"):  # the Gold trials' scratch namespace is checked too (review of #2779)
            try:
                for ident in cat.list_tables(ns):
                    cat.drop_table(ident)
                    dropped.append(".".join(ident))
            except Exception as exc:  # a namespace that does not exist
                dropped.append(f"{ns}: {type(exc).__name__}")
        annotate("notice", "nbp_bop trial cleanup", {"objects_removed": removed, "tables_dropped": dropped[:40]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
