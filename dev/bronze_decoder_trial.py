#!/usr/bin/env python3
"""Trial the lean Eurostat decoder on real Landing files (dev, deleted before merge).

For a sample of the latest Landing files (size percentiles, the largest, TUS/INN files
the old decoder rejected): decode locally with the new decoder, time it, and compare
with the current Bronze rows of the same raw SHA-256 (source row, period, value,
status, geo). Read-only: nothing is written to R2.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import time
from urllib.parse import urlsplit

import duckdb

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.eurostat.batch import download, landing_candidates
from ingestion.to_bronze.eurostat.decode import EurostatBulkDecodeError, decode_landing_distribution


def main() -> int:
    client, landing = s3(), req("R2_LANDING_BUCKET")
    cands = landing_candidates(client, landing)
    by_size = sorted(cands, key=lambda c: c["size"])
    picks = {by_size[len(by_size) * q // 100]["raw_sha256"]: by_size[len(by_size) * q // 100]
             for q in (10, 50, 90, 99)}
    for c in by_size[-2:]:
        picks[c["raw_sha256"]] = c
    for prefix in ("TUS_20SELFSTAT", "TUS_00MONTH", "INN_C711"):
        for c in cands:
            if c["dataset_id"].startswith(prefix):
                picks[c["raw_sha256"]] = c
                break
    old = {}
    table = catalog().load_table(("bronze", "eurostat_full_observations"))
    for f in table.inspect.files().select(["file_path"]).to_pylist():
        old[Path(f["file_path"]).stem] = f["file_path"]

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = req("R2_S3_ENDPOINT").replace("https://", "").rstrip("/")
    con.execute(f"""CREATE SECRET r2 (TYPE s3, KEY_ID '{req("CLOUDFLARE_R2_ACCESS_KEY_ID")}',
        SECRET '{req("CLOUDFLARE_R2_SECRET_ACCESS_KEY")}', ENDPOINT '{endpoint}', REGION 'auto', URL_STYLE 'path')""")
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        for i, item in enumerate(sorted(picks.values(), key=lambda c: c["size"])):
            work = Path(tmp) / str(i)
            work.mkdir()
            raw, rec, out = work / "d.tsv.gz", work / "r.json", work / "o.parquet"
            rec.write_bytes(item["receipt_bytes"])
            download(client, landing, item, raw)
            row = {"dataset": item["dataset_id"], "gz_mb": round(item["size"] / 1e6, 1)}
            t0 = time.monotonic()
            try:
                rep = decode_landing_distribution(raw, rec, out, receipt_uri="r2://x/r",
                                                  raw_uri=f"r2://{landing}/{item['data_key']}")
            except EurostatBulkDecodeError as exc:
                row["rejected"] = str(exc)[:200]
                results.append(row)
                continue
            row.update(seconds=round(time.monotonic() - t0, 1), rows=rep["observation_cells"],
                       new_mb=round(rep["output_bytes"] / 1e6, 2))
            old_path = old.get(item["raw_sha256"])
            if old_path:
                u = urlsplit(old_path)
                src = f"s3://{u.netloc}{u.path}"
                row["old_mb"] = round(int(client.head_object(Bucket=u.netloc, Key=u.path.lstrip('/'))["ContentLength"]) / 1e6, 2)
                cols = "source_row_number, period_key, value_text, coalesce(status_code, '') AS s, coalesce(geo_code, '') AS g"
                diff = con.execute(f"""
                    SELECT (SELECT count(*) FROM (SELECT {cols} FROM read_parquet('{out}')
                                                 EXCEPT ALL SELECT {cols} FROM read_parquet('{src}'))),
                           (SELECT count(*) FROM (SELECT {cols} FROM read_parquet('{src}')
                                                 EXCEPT ALL SELECT {cols} FROM read_parquet('{out}'))),
                           (SELECT count(*) FROM read_parquet('{src}'))""").fetchone()
                row.update(only_new=diff[0], only_old=diff[1], old_rows=diff[2])
            examples = con.execute(f"SELECT value_text, count(*) FROM read_parquet('{out}') "
                                   "WHERE TRY_CAST(value_text AS DOUBLE) IS NULL AND value_text NOT IN ('', ':') "
                                   "GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
            if examples:
                row["non_numeric"] = examples
            results.append(row)
            out.unlink()
            raw.unlink()
            print(json.dumps(row), flush=True)
    (Path(os.environ.get("DEV_OUTPUT_DIR", ".")) / "trial.json").write_text(json.dumps(results, indent=2))
    annotate("notice", "decoder trial", results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
