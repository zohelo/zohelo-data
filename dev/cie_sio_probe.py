"""Dev probe of cie_sio (PLS-022): every workbook the archive links (not only the selection) lands in zz_dev_sio/ and
loads into namespace zz_dev_sio; the Bronze cells are written to DEV_OUTPUT_DIR (run artifact) to profile the layouts.
Everything written to R2 is removed at the end (finally). Deleted before merge."""

from __future__ import annotations

import os
from pathlib import Path
import re
import sys

import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze import cie_sio as bronze
from ingestion.to_landing import cie_sio as landing

SCRATCH = "zz_dev_sio"


def profile(cells) -> None:
    """Print every workbook's layout to the job log: sheets and size, the first rows in full, and per column the
    distinct values (at most 60) with the count of numeric and non-numeric cells."""
    import duckdb

    con = duckdb.connect()
    con.register("c", cells)
    for (entity,) in con.sql("select distinct entity from c order by 1").fetchall():
        print(f"\n##### {entity}", flush=True)
        for row in con.sql(f"""select sheet_index, sheet_name, count(*), max(row_number), max(column_number)
                from c where entity = '{entity}' group by all order by 1""").fetchall():
            print("SHEET", row)
        for row in con.sql(f"""select sheet_index, row_number, string_agg(column_number || ':' || replace(value_text, chr(10), ' '),
                ' | ' order by column_number) from c where entity = '{entity}' and row_number <= 6 group by all order by 1, 2""").fetchall():
            print("ROW", row)
        for row in con.sql(f"""select sheet_index, column_number, count(*) n,
                count(*) filter (where try_cast(replace(value_text, ',', '.') as double) is not null) numeric,
                string_agg(distinct left(value_text, 40), ' ~ ')[:1500] vals
                from c where entity = '{entity}' and row_number > 1 group by all order by 1, 2""").fetchall():
            if row[3] < row[2]:
                print("COL", row)
            else:
                print("COL", row[:4], "numeric")


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    out = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
    out.mkdir(parents=True, exist_ok=True)
    land = f"{SCRATCH}/01_landing"
    try:
        landing.SELECTION = re.compile(".")
        landing.extract(60, landing_root=land)
        entities = sorted(landing.held(land))
        bronze.bronze_load(entities, landing_root=land, namespace=SCRATCH)
        cells = cat.load_table((SCRATCH, "cie_sio_cells")).scan().to_arrow()
        pq.write_table(cells, out / "cells.parquet")
        try:
            rejected = cat.load_table((SCRATCH, "cie_sio_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:
            rejected = []
        profile(cells)
        annotate("notice", "cie_sio probe", {"entities": len(entities), "cells": cells.num_rows,
                                             "rejected": [(r["entity"], r["error"][:100]) for r in rejected]})
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
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
        except Exception as exc:
            print("drop failed", exc)
        annotate("notice", "cie_sio probe cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
