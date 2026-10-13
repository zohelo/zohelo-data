#!/usr/bin/env python3
"""Find which decoder step needs the memory for one dataset (dev, deleted before merge)."""
from __future__ import annotations

import gzip
import json
from pathlib import Path
import sys
import tempfile
import time

import duckdb

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.eurostat.batch import download, landing_candidates


def main() -> int:
    code = sys.argv[1] if len(sys.argv) > 1 else "AVIA_TF_ACA"
    client, landing = s3(), req("R2_LANDING_BUCKET")
    item = next(c for c in landing_candidates(client, landing) if c["dataset_id"] == code)
    out = {"dataset": code}
    with tempfile.TemporaryDirectory() as tmp:
        raw = Path(tmp) / "d.tsv.gz"
        download(client, landing, item, raw)
        with gzip.open(raw, "rt") as f:
            header = f.readline().rstrip("\n").split("\t")
            longest = max((len(line) for _, line in zip(range(20000), f)), default=0)
        out.update(columns=len(header), header0=header[0][:80], longest_line_first_20k=longest)
        con = duckdb.connect(str(Path(tmp) / "w.duckdb"))
        con.execute("SET memory_limit='10GB'; SET threads=2; SET preserve_insertion_order=true")
        columns = {f"c{i}": "VARCHAR" for i in range(len(header))}

        def step(name, sql, params=None):
            t = time.monotonic()
            try:
                r = con.execute(sql, params or []).fetchall()
                peak = con.execute("SELECT sum(memory_usage_bytes) FROM duckdb_memory()").fetchone()[0]
                out[name] = {"s": round(time.monotonic() - t, 1), "mem_gb": round((peak or 0) / 1e9, 2),
                             "result": str(r[:1])[:80]}
            except Exception as exc:
                out[name] = {"error": str(exc)[:200]}
            print(json.dumps({name: out[name]}), flush=True)

        step("load", "CREATE TABLE src AS SELECT list_transform(string_split(c0, ','), x -> trim(x)) AS parts, "
             "* EXCLUDE (c0) FROM read_csv(?, auto_detect=false, delim='\t', quote='', escape='', comment='', "
             "header=false, skip=1, columns=?, nullstr='\x1f', strict_mode=true, null_padding=false, "
             "parallel=false, compression='gzip', max_line_size=8000000)", [str(raw), columns])
        step("rows", "SELECT count(*) FROM src")
        step("distinct", "SELECT count(DISTINCT parts) FROM src")
        step("distinct_text", "SELECT count(DISTINCT array_to_string(parts, ',')) FROM src")
        cols = ", ".join(f"c{i}" for i in range(1, len(header)))
        step("unpivot_count", f"SELECT count(*) FROM (UNPIVOT (SELECT rowid + 2 AS line, * FROM src) ON {cols} INTO NAME col VALUE cell)")
        step("copy_ordered", f"COPY (SELECT line, col, cell FROM (UNPIVOT (SELECT rowid + 2 AS line, * FROM src) ON {cols} INTO NAME col VALUE cell)) TO '{tmp}/a.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)")
    annotate("notice", "memory probe", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
