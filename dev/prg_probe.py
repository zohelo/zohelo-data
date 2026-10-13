"""Dry run: download the PRG package and decode it with the Bronze decoder; no R2 writes."""
import tempfile
import time
import traceback
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.common.report import annotate
from ingestion.to_bronze.gugik_prg.load import decode
from ingestion.to_landing.gugik_prg.extract import download

out = {}
with tempfile.TemporaryDirectory() as t:
    zip_path = Path(t) / "prg.zip"
    started = time.monotonic()
    digest, size, _ = download(zip_path)
    out["download"] = {"seconds": round(time.monotonic() - started), "mb": round(size / 1e6, 1), "sha256": digest}
    try:
        started = time.monotonic()
        output = Path(t) / "out.parquet"
        out["rows_by_layer"] = decode(zip_path, {"entity": "jednostki_administracyjne", "sha256": digest,
                                                 "version_token": "20261013T000000Z",
                                                 "ingested_at": "20261013T000000Z"}, output)
        meta = pq.ParquetFile(output)
        out["parquet"] = {"mb": round(output.stat().st_size / 1e6, 1), "rows": meta.metadata.num_rows,
                          "seconds": round(time.monotonic() - started)}
        first = pq.read_table(output, columns=["layer", "jpt_kod_je", "jpt_nazwa_", "geometry_type", "crs_wkt"]).slice(0, 2).to_pylist()
        out["sample"] = [{k: (str(v)[:80] if v is not None else None) for k, v in r.items()} for r in first]
    except Exception:
        out["error"] = traceback.format_exc()[-1500:]
annotate("notice", "prg dry run", out)
