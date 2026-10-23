"""Read-only: do old DBW tables still exist in the catalog? Deleted before merge."""

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

cat = catalog()
found = {}
for ns in ("bronze", "silver", "gold"):
    found[ns] = [".".join(t) for t in cat.list_tables(ns) if "dbw" in t[-1]]
annotate("notice", "dbw catalog probe", found)
