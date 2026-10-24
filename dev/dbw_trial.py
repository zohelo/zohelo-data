"""Trial (read-only for R2): DBW inventory on a sample of indicators, timing, and decode of a
sample of files into local Bronze and Silver. Bounded to ~50 min. Deleted before merge."""

from __future__ import annotations

from collections import Counter
import hashlib
from pathlib import Path
import random
import tempfile
import threading
import time
import traceback

import duckdb
import requests

from ingestion.common.report import annotate
from ingestion.to_bronze.gus_dbw.decode import DbwDecodeError, decode
from ingestion.to_landing.gus_dbw import extract as x

START = time.monotonic()
progress: dict = {"stage": "start"}


def watchdog():
    for _ in range(3):
        time.sleep(15 * 60)
        annotate(
            "notice",
            "dbw trial progress",
            {**progress, "minutes": round((time.monotonic() - START) / 60), "timing": dict(x.TIMING)},
        )


def run():
    s = requests.Session()
    s.headers["User-Agent"] = x.USER_AGENT
    progress["stage"] = "tree"
    tree = x.get(s, f"{x.WEB}/api_app/wsk/getIndicatorsTree").json()
    inds = x.indicators(tree)
    progress["indicators_total"] = len(inds)
    random.seed(7)
    sample = {i["indicator_id"] for i in random.sample(inds, min(150, len(inds)))}
    progress["stage"] = "inventory"
    files, stats = x.inventory(s, sample, time.monotonic() + 25 * 60)
    stats = dict(stats, files=len(files), periodicity=Counter(f["periodicity"] for f in files).most_common(6))
    annotate(
        "notice",
        "dbw trial inventory",
        {"stats": stats, "timing": dict(x.TIMING), "minutes": round((time.monotonic() - START) / 60)},
    )
    progress["stage"] = "decode"
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        for f in random.sample(files, min(28, len(files))):
            if time.monotonic() - START > 45 * 60:
                break
            r = {"e": f["entity"]}
            try:
                body = x.get(s, f"{x.WEB}/bulk_new/{f['file']}", limit=300).content
                p = Path(tmp) / "f.zip"
                p.write_bytes(body)
                rec = {
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "entity": f["entity"],
                    "version_token": f["version"],
                    "ingested_at": "20261024T000000Z",
                }
                r["mb"] = round(len(body) / 1e6, 2)
                out = Path(tmp) / "b.parquet"
                out.unlink(missing_ok=True)
                res = decode(p, rec, out, Path(tmp) / "d.parquet")
                (Path(tmp) / "d.parquet").unlink(missing_ok=True)
                r["rows"], r["dict"] = res["rows"], res["dictionary_rows"]
                con = duckdb.connect()
                con.execute(f"create view bronze as select * from '{out}'")
                sql = Path("warehouse/entity/dbw_observations.sql").read_text()
                r["s"] = con.execute(
                    "select count(value), count(year), min(year), max(year), "
                    f"count(dimension_1_id), count(dimension_2_id) from ({sql})"
                ).fetchone()
                r["tv"] = con.execute(
                    "select count(*) filter (where nullif(trim(wartosc),'') is not null), "
                    "count(*) filter (where nullif(trim(wartosc),'') is not null and "
                    "try_cast(replace(trim(wartosc), ',', '.') as double) is null), "
                    "any_value(wartosc) filter (where nullif(trim(wartosc),'') is not null and "
                    "try_cast(replace(trim(wartosc), ',', '.') as double) is null), "
                    "count(*) filter (where precyzja is null) from bronze"
                ).fetchone()
                con.close()
            except DbwDecodeError as exc:
                r["rej"] = str(exc)[:200]
            except Exception as exc:
                r["err"] = f"{type(exc).__name__}: {str(exc)[:200]}"
            results.append(r)
            progress["decoded"] = len(results)
    for i in range(0, len(results), 7):
        annotate("notice", f"dbw trial files {i}", results[i : i + 7])


threading.Thread(target=watchdog, daemon=True).start()
try:
    run()
except Exception:
    annotate("error", "dbw trial crashed", {"traceback": traceback.format_exc()[-3000:], **progress})
    raise
annotate("notice", "dbw trial done", {"minutes": round((time.monotonic() - START) / 60), "timing": dict(x.TIMING)})
