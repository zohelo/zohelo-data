#!/usr/bin/env python3
"""Read-only storage audit (dev, deleted before merge).

1. 06_control and lakehouse folders by size, with file types and examples.
2. Landing per source by file type (compressed or not).
3. Iceberg tables: location, snapshots, data bytes.
4. Eurostat Bronze: Parquet column-chunk sizes over a sample of data files.
5. Lean-schema trial: rewrite sample Bronze files locally with fewer columns and
   compare sizes. Nothing is written to R2.
"""
from __future__ import annotations

from collections import defaultdict
import json
import os
from pathlib import Path
import random
import tempfile
from urllib.parse import urlsplit

import duckdb
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "."))


def ext(key: str) -> str:
    name = key.rsplit("/", 1)[-1]
    if name.endswith(".receipt.json"):
        return ".receipt.json"
    parts = name.split(".")
    return "." + ".".join(parts[-2:]) if len(parts) > 2 and parts[-1] in {"gz", "zip", "zst"} else (
        "." + parts[-1] if len(parts) > 1 else "(none)")


def folders(client, bucket, prefix, depth):
    out = defaultdict(lambda: {"n": 0, "gb": 0, "latest": "", "types": defaultdict(lambda: [0, 0]), "ex": []})
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            name = "/".join(parts[:depth]) if len(parts) > depth else "/".join(parts[:-1]) + "/(files)"
            e = out[name]
            e["n"] += 1
            e["gb"] += obj["Size"]
            e["latest"] = max(e["latest"], obj["LastModified"].isoformat()[:10])
            t = e["types"][ext(obj["Key"])]
            t[0] += 1
            t[1] += obj["Size"]
            if len(e["ex"]) < 2:
                e["ex"].append(obj["Key"][-90:])
    res = {}
    for k, v in sorted(out.items(), key=lambda kv: -kv[1]["gb"]):
        res[k] = {"n": v["n"], "gb": round(v["gb"] / 1e9, 3), "latest": v["latest"],
                  "types": {t: [c, round(b / 1e9, 3)] for t, (c, b) in
                            sorted(v["types"].items(), key=lambda kv: -kv[1][1])[:4]},
                  "ex": v["ex"]}
    return res


def emit(title, data, limit=3700):
    """Pack a dict into as few annotations as fit."""
    chunk = {}
    for k, v in data.items():
        if chunk and len(json.dumps({**chunk, k: v})) > limit:
            annotate("notice", title, chunk)
            chunk = {}
        chunk[k] = v
    if chunk:
        annotate("notice", title, chunk)


def main() -> int:
    client = s3()
    landing, lake = req("R2_LANDING_BUCKET"), req("R2_LAKEHOUSE_BUCKET")
    result = {}

    control = folders(client, lake, "06_control/", 3)
    result["control"] = control
    result["lake"] = folders(client, lake, "", 3)
    result["landing"] = folders(client, landing, "", 2)

    cat = catalog()
    tables = {}
    for ns in cat.list_namespaces():
        for ident in cat.list_tables(ns):
            t = cat.load_table(ident)
            snap = t.current_snapshot()
            s = (snap.summary if snap and snap.summary else {}) or {}
            tables[".".join(ident)] = {"loc": t.location()[-60:], "snaps": len(t.metadata.snapshots),
                                       "rows": int(s.get("total-records", 0)),
                                       "gb": round(int(s.get("total-files-size", 0)) / 1e9, 2),
                                       "files": int(s.get("total-data-files", 0))}
    result["tables"] = tables

    # Parquet column sizes for Eurostat Bronze
    table = cat.load_table(("bronze", "eurostat_full_observations"))
    files = [(f.file.file_path, f.file.file_size_in_bytes, f.file.record_count) for f in table.scan().plan_files()]
    random.seed(7)
    files.sort(key=lambda x: -x[1])
    sample = files[:10] + random.sample(files[10:], min(30, len(files) - 10))
    fs = pafs.S3FileSystem(access_key=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
                           secret_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
                           endpoint_override=req("R2_S3_ENDPOINT"), region="auto")
    cols = defaultdict(int)
    total = 0
    meta_rows = 0
    for path, size, rows in sample:
        u = urlsplit(path)
        md = pq.ParquetFile(f"{u.netloc}{u.path}", filesystem=fs).metadata
        meta_rows += md.num_rows
        for rg in range(md.num_row_groups):
            g = md.row_group(rg)
            for c in range(g.num_columns):
                cc = g.column(c)
                cols[cc.path_in_schema] += cc.total_compressed_size
                total += cc.total_compressed_size
    result["bronze_columns"] = {
        "sample_files": len(sample), "sample_rows": meta_rows, "sample_gb": round(total / 1e9, 3),
        "table_files": len(files), "bytes_per_row": round(total / max(meta_rows, 1), 2),
        "share_pct": {k: round(100 * v / total, 1) for k, v in sorted(cols.items(), key=lambda kv: -kv[1])}}

    # Lean trial: rewrite 6 mid-sized sample files locally
    trial = []
    mids = sorted(files, key=lambda x: x[1])
    picks = [mids[len(mids) * q // 100] for q in (50, 75, 90, 97, 99)] + [files[0]]
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'")
    with tempfile.TemporaryDirectory() as tmp:
        for path, size, rows in picks:
            u = urlsplit(path)
            local = Path(tmp) / "in.parquet"
            with fs.open_input_stream(f"{u.netloc}{u.path}") as src, local.open("wb") as dst:
                while chunk := src.read(16 << 20):
                    dst.write(chunk)
            base = f"read_parquet('{local}')"
            lean_cols = """dataset_id, source_version, retrieved_at_utc, raw_sha256,
                regexp_replace(dimension_key_json, ',?"time":"[^"]*"', '') AS series_key,
                freq_code, unit_code, geo_code, period_key, value_text, status_code, source_row_number"""
            variants = {}
            for name, select, order in [
                ("lean_zstd", lean_cols, ""),
                ("lean_zstd_l9", lean_cols, ""),
                ("lean_plus_numeric", lean_cols + ", value_numeric", ""),
                ("lean_sorted_series_period", lean_cols, "ORDER BY series_key, period_key"),
            ]:
                out = Path(tmp) / f"{name}.parquet"
                level = ", COMPRESSION_LEVEL 9" if name.endswith("l9") else ""
                con.execute(f"COPY (SELECT {select} FROM {base} {order}) TO '{out}' "
                            f"(FORMAT PARQUET, COMPRESSION ZSTD{level}, ROW_GROUP_SIZE 1000000)")
                variants[name] = out.stat().st_size
                out.unlink()
            trial.append({"rows": rows, "old_mb": round(size / 1e6, 1),
                          **{k: round(v / 1e6, 1) for k, v in variants.items()},
                          "ratio_lean": round(variants["lean_zstd"] / size, 3)})
            local.unlink()
    result["lean_trial"] = trial

    (OUT / "storage-audit.json").write_text(json.dumps(result, indent=2))
    annotate("notice", "bronze columns", result["bronze_columns"])
    annotate("notice", "lean trial", trial)
    emit("control", {k: {kk: vv for kk, vv in v.items() if kk != "ex"} | {"ex": v["ex"][:1]}
                     for k, v in list(control.items())[:25]})
    emit("lake", {k: [v["n"], v["gb"]] for k, v in list(result["lake"].items())[:60] if not k.startswith("06_control")})
    emit("landing", {k: {"n": v["n"], "gb": v["gb"], "types": v["types"]} for k, v in result["landing"].items()})
    emit("tables", {k: [v["rows"], v["gb"], v["files"], v["snaps"]] for k, v in
                    sorted(tables.items(), key=lambda kv: -kv[1]["gb"])[:60]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
