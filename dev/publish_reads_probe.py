"""Read-only check of #2944 (test branch only, never merged): gold.publish's preload against production, and the
writers that now take the preloaded tables (catalog_tables, geo, topics) run with a client that records and has
no network. Nothing is written to R2. One annotation."""

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import io
import json
import resource
import time
import traceback

import pyarrow.parquet as pq

from ingestion.common.r2 import catalog, s3
from ingestion.common.report import annotate
from warehouse import publish


class Recorder:
    """Stands in for the S3 client in the writers: keeps what would be written, cannot reach R2."""

    def __init__(self) -> None:
        self.bodies: dict[str, bytes] = {}

    def put_object(self, **kwargs) -> None:
        self.bodies[kwargs["Key"]] = kwargs["Body"]


def main() -> int:
    out: dict = {}
    try:
        cat, client = catalog(), s3()
        measures = publish.seed("kpi_measure")
        licensed = publish.licensed_sources(publish.seed("source"), publish.seed("licence"))
        started = time.monotonic()
        loaded = publish.preload(cat, client, publish.PREFIX, "gold", measures, licensed, publish.published_v2())
        read = loaded["tables"]
        support = publish.support_reads("gold", True)
        out["preload"] = {
            "seconds": round(time.monotonic() - started),
            "tables": len(read),
            "support_wanted": len(support),
            "support_read": sum(1 for key in support if key in read),
            "support_not_read": sorted(".".join(key) for key in support if key not in read),
            "lost": loaded["lost"],
            "arrow_mb": round(sum(t.nbytes for t in read.values()) / 1e6),
            "support_rows": {".".join(key): read[key].num_rows for key in support if key in read},
            "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024),
        }
        recorder = Recorder()
        scratch = "zz_dev_2944/"
        written = publish.catalog_tables(recorder, scratch, "gold", read)
        same = {}
        for name in written["written"]:
            live = pq.read_table(io.BytesIO(publish.get(client, publish.PREFIX + f"catalog/{name}.parquet")))
            mine = pq.read_table(io.BytesIO(recorder.bodies[scratch + f"catalog/{name}.parquet"]))
            same[name] = live.equals(mine)
        out["catalog_tables"] = {**written, "equal_to_live_file": same}
        counts = publish.geo(recorder, scratch, read[publish.GEO_UNITS], read[publish.GEO_NAMES])
        out["geo"] = {
            "counts": counts,
            "sha256_equal_to_live_file": {
                level: hashlib.sha256(recorder.bodies[scratch + f"geo/{level}.geojson"]).hexdigest()
                == hashlib.sha256(publish.get(client, publish.PREFIX + f"geo/{level}.geojson")).hexdigest()
                for level in counts
            },
        }
        topics = publish.topics(recorder, publish.iso(datetime.now(UTC)), read)
        mine = json.loads(recorder.bodies[topics["key"]])
        live = json.loads(publish.get(client, topics["key"]))
        mine.pop("generated_at", None)
        live_stamp = live.pop("generated_at", None)
        out["topics"] = {
            "key": topics["key"],
            "bytes": topics["bytes"],
            "equal_to_live_file_without_generated_at": mine == live,
            "live_generated_at": live_stamp,
            "keys_differing": sorted(k for k in set(mine) | set(live) if mine.get(k) != live.get(k)),
        }
        out["recorded_not_written"] = len(recorder.bodies)
        out["peak_rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
    except Exception as error:  # the probe reports what it reached
        out["error"] = f"{type(error).__name__}: {error}"[:600]
        out["trace"] = traceback.format_exc()[-900:]
    annotate("error" if "error" in out else "notice", "publish reads probe (#2944)", out)
    return 1 if "error" in out else 0


if __name__ == "__main__":
    raise SystemExit(main())
