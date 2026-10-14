"""Dry run: download the WDI archive and decode it with the Bronze decoder; no R2 writes."""
import tempfile
import time
import traceback
from pathlib import Path

from ingestion.common.report import annotate
from ingestion.to_bronze.world_bank_wdi.load import decode
from ingestion.to_landing.world_bank_wdi.extract import download, version_from

out = {}
with tempfile.TemporaryDirectory() as t:
    work = Path(t)
    started = time.monotonic()
    digest, size, headers, final_url = download(work / "wdi.zip")
    out["download"] = {"seconds": round(time.monotonic() - started), "mb": round(size / 1e6, 1),
                       "final_url": final_url, "version": version_from(headers, "none"), "etag": headers.get("ETag")}
    try:
        started = time.monotonic()
        obs, meta, counts = decode(work / "wdi.zip", {"entity": "WDI_CSV", "sha256": digest,
                                                       "version_token": "x", "ingested_at": "20261014T000000Z"}, work)
        out["decode"] = {**counts, "seconds": round(time.monotonic() - started),
                         "obs_mb": round(obs.stat().st_size / 1e6, 1), "meta_mb": round(meta.stat().st_size / 1e6, 1)}
    except Exception:
        out["error"] = traceback.format_exc()[-1500:]
annotate("notice", "wdi dry run", out)
