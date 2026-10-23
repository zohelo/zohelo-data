"""Trial (read-only for R2): DBW inventory over all indicators, then decode a sample of
files into local Bronze and Silver. Writes only $DEV_OUTPUT_DIR. Deleted before merge."""

from __future__ import annotations

from collections import Counter, defaultdict
import json
import os
from pathlib import Path
import random
import tempfile
import time
import traceback

import duckdb
import requests

from ingestion.common.report import annotate
from ingestion.to_bronze.gus_dbw.decode import DbwDecodeError, decode
from ingestion.to_landing.gus_dbw import extract as x

out = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
out.mkdir(parents=True, exist_ok=True)


def run():
    s = requests.Session()
    s.headers["User-Agent"] = x.USER_AGENT
    t0 = time.monotonic()
    raw, stats = x.inventory(s, None, time.monotonic() + 200 * 60)
    stats = Counter(stats)
    samples_body = {}
    versions = defaultdict(set)
    for f in raw:
        versions[f["entity"]].add(f["version"])
    stats["listed_rows"] = len(raw)
    stats["entities"] = len(versions)
    stats["entities_with_two_dates"] = sum(len(v) > 1 for v in versions.values())
    stats["inventory_minutes"] = round((time.monotonic() - t0) / 60, 1)
    unique = sorted({f["entity"]: f for f in raw}.values(), key=lambda f: f["entity"])
    random.seed(7)
    sample = random.sample(unique, min(25, len(unique)))
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        for f in sample:
            time.sleep(x.PAUSE_SECONDS)
            r = {"entity": f["entity"], "periodicity": f["periodicity"], "years": f["years"]}
            try:
                body = x.get(s, f"{x.WEB}/bulk_new/{f['file']}").content
                p = Path(tmp) / "f.zip"
                p.write_bytes(body)
                import hashlib

                rec = {
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "entity": f["entity"],
                    "version_token": f["version"],
                    "ingested_at": "20261024T000000Z",
                }
                r["mb"] = round(len(body) / 1e6, 2)
                r["rows"] = decode(p, rec, Path(tmp) / "b.parquet")["rows"]
                con = duckdb.connect()
                con.execute(f"create view bronze as select * from '{tmp}/b.parquet'")
                sql = Path("warehouse/entity/dbw_observations.sql").read_text()
                r["silver"] = con.execute(
                    f"select count(*), count(value), count(year), count(distinct indicator_id), min(year), max(year), "
                    f"count(dimension_1_id), count(dimension_2_id), count(dimension_3_id) from ({sql})"
                ).fetchone()
                r["text_vs_value"] = con.execute(
                    "select count(*) filter (where nullif(trim(wartosc),'') is not null), "
                    "count(*) filter (where nullif(trim(wartosc),'') is not null and "
                    "try_cast(replace(trim(wartosc), ',', '.') as double) is null), "
                    "any_value(wartosc) filter (where nullif(trim(wartosc),'') is not null and "
                    "try_cast(replace(trim(wartosc), ',', '.') as double) is null), any_value(id_daty) from bronze"
                ).fetchone()
                con.close()
            except DbwDecodeError as exc:
                r["rejected"] = str(exc)[:300]
            except Exception as exc:
                r["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
            results.append(r)
    report = {
        "stats": dict(stats),
        "periodicity": Counter(f["periodicity"] for f in unique).most_common(10),
        "results": results,
        "no_file_bodies": samples_body,
    }
    (out / "dbw_trial.json").write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str))
    print(
        json.dumps(
            {
                "stats": dict(stats),
                "ok": sum("rows" in r for r in results),
                "bad": [r for r in results if "rows" not in r][:5],
            },
            ensure_ascii=False,
            default=str,
        )
    )


try:
    run()
except Exception:
    tb = traceback.format_exc()
    (out / "error.txt").write_text(tb)
    annotate("error", "dbw trial crashed", {"traceback": tb[-3000:]})
    raise
