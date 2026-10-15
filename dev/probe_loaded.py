"""Read-only probe (#803): per Bronze table, files not named after a Landing SHA-256
(compaction output) and how many loaded hashes the file-name check missed."""

from pathlib import Path

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

cat = catalog()
out = {}
for table_id in sorted(t for tables, _ in bf.SOURCE_TABLES.values() for t in tables):
    try:
        table = cat.load_table(table_id)
    except Exception as exc:
        out[".".join(table_id)] = f"load failed: {type(exc).__name__}"
        continue
    if table.current_snapshot() is None:
        continue
    paths = table.inspect.files().column("file_path").to_pylist()
    named = {Path(p).stem for p in paths if bf.SHA_RE.fullmatch(Path(p).stem)}
    other = len(paths) - len(named)
    entry = {"files": len(paths), "not_sha_named": other}
    if other:
        full = bf.loaded_hashes(table)
        entry.update({"hashes_by_name": len(named), "hashes_full": len(full), "missed": len(full - named)})
    out[".".join(table_id)] = entry
annotate("notice", "loaded hashes probe", out)
