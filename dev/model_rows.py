"""Trial for #868 (read-only): rows of every Silver/Gold table, and row_ceilings() run
against the real catalog as if every model had just been built. Deleted before merge."""

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build, changed

cat = catalog()
rows = {}
for layer in ("silver", "gold"):
    for identifier in cat.list_tables(layer):
        rows[f"{layer}.{identifier[-1]}"] = build.table_rows(cat.load_table(identifier))
annotate("notice", "model rows", rows)
meta = changed.manifest("prod")
results = [{"unique_id": uid, "status": "success"} for uid, n in meta["nodes"].items() if n["resource_type"] == "model"]
annotate("notice", "row_ceilings trial", build.row_ceilings(results))
