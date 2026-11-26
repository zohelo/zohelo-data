"""Diagnostics for the transport trial (#2966, #2999), printed to the job log only (no annotations but one summary):
(1) CEPiK: Landing and Bronze into zz_dev_cepik_diag, the branch's Silver in memory, the rows without a month and the
header and data cells of their blocks; (2) GDDKiA GPR: the attachment links of both edition pages and the response
headers of each spreadsheet link (the file name no longer arrives). Scratch removed at the end. Deleted before merge."""

from __future__ import annotations

import re
import sys
import time

import duckdb
import requests

from dev.cepik_trial import render
from ingestion.common import gus_publications as gp
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing import cepik as landing
from ingestion.to_landing import gddkia_gpr as gpr
from ingestion.to_landing.patterns import bulk_file
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_cepik_diag"
LAND = f"{SCRATCH}/01_landing"


def show(con, title, sql, limit=200):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:120] for v in r))
    print(f"({len(rows)} rows)", flush=True)


def gpr_links() -> dict:
    out = {}
    for edition, page in gpr.EDITIONS.items():
        time.sleep(2)
        links = [(h, t) for h, t in gpr._links(page) if "/attachment/" in h]
        print(f"\n=== GPR {edition} {page}: {len(links)} attachment links", flush=True)
        for h, t in links:
            print(f"  {h}\t{t!r}")
        for h, t in links:
            if not gpr.SPREADSHEET_TEXT.search(t):
                continue
            time.sleep(2)
            r = requests.get(h, headers={"User-Agent": bulk_file.USER_AGENT}, allow_redirects=True, timeout=60,
                             stream=True)
            r.close()
            print(f"  GET {h} -> {r.status_code} final={r.url} history={[x.status_code for x in r.history]}")
            for k, v in r.headers.items():
                print(f"      {k}: {v[:200]}")
            out.setdefault(edition, []).append([t[:60], r.headers.get("Content-Disposition", "")[:80],
                                                r.headers.get("Content-Type", "")[:40]])
    return out


def main(argv) -> int:
    summary = {}
    try:
        summary["gpr"] = gpr_links()
    except Exception as exc:
        summary["gpr_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    cat = catalog()
    con = duckdb.connect()
    try:
        landing.extract(30, landing_root=LAND)
        entities = sorted(landing.held(LAND))
        gp.bronze_load("cepik", entities, landing_root=LAND, namespace=SCRATCH)
        cells = cat.load_table((SCRATCH, "cepik_cells")).scan().to_arrow()
        con.register("cepik_cells_a", cells)
        con.sql("create table cepik_cells as select * from cepik_cells_a")
        sql = render(ROOT / "warehouse/models/silver/cepik/cepik_vehicle_registrations.sql", {"cepik_cells": "cepik_cells"})
        con.sql("create table s as " + sql)
        show(con, "silver rows without a month", "select entity, table_code, group_label, value_text, value from s where period_month is null")
        bad = [r[0] for r in con.sql("select distinct entity from s where period_month is null").fetchall()]
        summary["cepik_null_month_entities"] = bad
        print(f"\ncolumns of cepik_cells: {cells.column_names}")
        for e in bad:
            show(con, f"cells of {e} (rows with any text in col 1 or a month name)",
                 f"""select row_number, string_agg(column_number || ':' || replace(left(value_text, 25), chr(10), ' '), ' | ' order by column_number)
                 from cepik_cells where entity = '{e}' group by row_number order by row_number""", limit=400)
        show(con, "months per entity", "select entity, table_code, string_agg(distinct cast(period_month as varchar), ',') from s group by all order by 1")
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    removed += 1
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
            cat.drop_namespace(SCRATCH)
        except Exception as exc:
            print("drop failed", exc)
        summary["removed"] = removed
        annotate("notice", "transport diag", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
