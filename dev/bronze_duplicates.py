"""Read-only probe (#803): Landing files whose rows a Bronze table holds more than once, per source."""

from __future__ import annotations

import sys

from ingestion.common.bronze_files import bronze_hashes
from ingestion.common.lineage import duplicates
from ingestion.common.report import annotate

for source in sys.argv[1:]:
    loaded, rejected, duplicated = bronze_hashes(source)
    annotate("notice", f"{source} Bronze duplicates", {"loaded": len(loaded), "tables": duplicates(duplicated)})
