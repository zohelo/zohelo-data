"""Dev trial (#851): read-only bronze.verify for one source on this branch, timed. Deleted before merge."""

import sys
import time

from ingestion.common import lineage
from ingestion.common.report import annotate

source = sys.argv[1] if len(sys.argv) > 1 else "eurostat"
start = time.monotonic()
status = lineage.main(["--source", source])
annotate(
    "notice", f"dev verify {source} timing", {"status": status, "minutes": round((time.monotonic() - start) / 60, 1)}
)
