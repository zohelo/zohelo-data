"""Dev trial (#851): bronze.verify steps for one source, timed per stage; crashes annotated. Removed before merge."""

import sys
import time
import traceback

from ingestion.common import lineage
from ingestion.common.r2 import landing_objects, req, s3
from ingestion.common.report import annotate

source = sys.argv[1] if len(sys.argv) > 1 else "eurostat"
times: dict[str, float] = {}
start = time.monotonic()


def stage(name, func, *args):
    t = time.monotonic()
    try:
        return func(*args)
    finally:
        times[name] = round((time.monotonic() - t) / 60, 1)


try:
    meta = stage("landing_objects", landing_objects, s3(), req("R2_LANDING_BUCKET"), source)
    held, rejected = stage("bronze_hashes", lineage.bronze_hashes, source)
    passed, report = stage("check_latest_only", lineage.check_latest_only, meta, held)
    loaded = set().union(*held.values()) if held else set()
    inspect = lineage.inspect_eurostat if source == "eurostat" else lineage.inspect_snapshot
    result = stage("inspect", inspect, meta, loaded, rejected)
    annotate(
        "notice",
        f"dev verify {source} result",
        {
            "passed_latest": passed,
            "latest_only": report,
            "unexplained": result.get("latest_unexplained"),
            "times": times,
        },
    )
    stage("report_duplicates", lineage.report_duplicates, source)
    annotate(
        "notice", f"dev verify {source} timing", {"times": times, "total": round((time.monotonic() - start) / 60, 1)}
    )
except BaseException as error:
    frames = [
        f"{f.filename.rsplit('/', 1)[-1]}:{f.lineno} {f.name}" for f in traceback.extract_tb(error.__traceback__)[-4:]
    ]
    annotate("error", f"dev verify {source} crashed", {"error": repr(error)[:500], "where": frames, "times": times})
    raise
