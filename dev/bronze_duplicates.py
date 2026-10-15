"""Read-only probe (#803): Landing files whose rows a Bronze table holds more than once, per source."""

from __future__ import annotations

import sys

from ingestion.common.lineage import report_duplicates

for source in sys.argv[1:]:
    report_duplicates(source)
