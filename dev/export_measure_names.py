"""Read-only export of the measure catalogue (gold.dim_measure) to the job log, for a Batch API run that
writes bilingual measure names (owner, 27 Nov 2026). Reads one table; writes nothing; no scratch namespace.

Output: one line per measure, `MEASURE<TAB>` + JSON (measure_key, source_system, block_id, block_name,
measure_name, unit_key, status), then one summary annotation.
"""
from __future__ import annotations

import json

import duckdb

from ingestion.common import r2


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    endpoint = r2.req("STORAGE_ENDPOINT_URL").replace("https://", "").strip("/")
    con.execute("CREATE SECRET r2_catalog (TYPE iceberg, TOKEN ?)", [r2.req("STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN")])
    con.execute(
        "CREATE SECRET r2_files (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [r2.req("STORAGE_WRITE_ACCESS_KEY"), r2.req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    con.execute(
        "ATTACH ? AS r2 (TYPE iceberg, SECRET r2_catalog, ENDPOINT ?, READ_ONLY)",
        [r2.req("STORAGE_LAKEHOUSE_CATALOG_NAME"), r2.req("STORAGE_LAKEHOUSE_CATALOG_URL")],
    )
    return con


def main() -> None:
    con = connect()
    cols = [r[0] for r in con.execute("describe r2.gold.dim_measure").fetchall()]
    want = [c for c in ("measure_key", "source_system", "block_id", "block_name", "measure_name", "unit_key", "status") if c in cols]
    rows = con.execute(f"select {', '.join(want)} from r2.gold.dim_measure order by 1").fetchall()
    for r in rows:
        print("MEASURE\t" + json.dumps(dict(zip(want, r)), ensure_ascii=False, default=str))
    by_status: dict[str, int] = {}
    if "status" in want:
        for r in rows:
            k = str(r[want.index("status")])
            by_status[k] = by_status.get(k, 0) + 1
    print(f"::notice title=measure export::{json.dumps({'rows': len(rows), 'columns': want, 'by_status': by_status})}")


if __name__ == "__main__":
    main()
