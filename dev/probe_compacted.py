"""Read-only probe (#809): index rows the new prepare() derives from compacted Eurostat files."""

from pathlib import Path

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import eurostat_batch as eb

table = catalog().load_table(eb.BRONZE_ID)
files = table.inspect.data_files()
named, compacted = set(), []
for path, partition, size in zip(
    files.column("file_path").to_pylist(),
    files.column("partition").to_pylist(),
    files.column("file_size_in_bytes").to_pylist(),
    strict=True,
):
    if eb.SHA_RE.fullmatch(Path(path).stem):
        named.add((partition["dataset_id"], Path(path).stem))
    else:
        compacted += eb._compacted(table, path, partition["dataset_id"], size)
pairs = {(r["dataset_id"], r["raw_sha256"]) for r in compacted}
annotate(
    "notice",
    "compacted eurostat files",
    {
        "compacted_index_rows": len(compacted),
        "pairs": len(pairs),
        "also_in_named_files": len(pairs & named),
        "datasets": sorted({d for d, _ in pairs})[:40],
        "rows": sum(r["record_count"] for r in compacted),
        "sample": compacted[:3],
    },
)
