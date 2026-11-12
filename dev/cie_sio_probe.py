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
