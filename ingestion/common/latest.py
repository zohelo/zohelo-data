"""Latest version only in Bronze (owner data policy #822/#845, 16 Oct 2026).

Bronze, Silver and Gold hold only the latest version of every source entity; superseded
versions live only in the Landing Archive. Bronze loaders append each new Landing file;
``prune`` then removes, from every Bronze table of the source, the rows of Landing files that
are no longer the latest version of their entity.

Latest is decided from the Landing receipts (``r2.landing_objects``, which include files
already moved to the Archive): per (area, entity), every file of the highest version is kept
(a version may come in several parts, e.g. BDL ``__part-NNN``). Files a table holds but the
receipts do not know are kept and reported, never deleted. A file whose bytes are the latest
of another entity is kept too (identical bytes can sit under several entities).
"""

from __future__ import annotations

import contextlib
from typing import Any

from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import In

from ingestion.common.bronze_files import SOURCE_TABLES, loaded_hashes, retry
from ingestion.common.r2 import catalog, landing_objects, req, s3

CHUNK = 300  # raw SHA-256 values per delete commit


def keep_digests(meta: dict[str, dict[str, Any]]) -> set[str]:
    """SHA-256 of every file of the highest version of each (area, entity)."""
    best: dict[tuple[str, str], tuple[str, set[str]]] = {}
    for digest, item in meta.items():
        for path in item.get("snapshot_paths", []):
            key = (path["area"], path["entity"])
            held = best.get(key)
            if held is None or path["version"] > held[0]:
                best[key] = (path["version"], {digest})
            elif path["version"] == held[0]:
                held[1].add(digest)
    return set().union(*(digests for _, digests in best.values())) if best else set()


def superseded_rows(table, hashes: set[str]) -> int:
    """Rows of the superseded files the table holds in files named after them (compacted files not counted)."""
    if table.current_snapshot() is None or not hashes:
        return 0
    files = table.inspect.data_files()
    total = 0
    for path, count in zip(
        files.column("file_path").to_pylist(), files.column("record_count").to_pylist(), strict=True
    ):
        if path.rsplit("/", 1)[-1].split(".", 1)[0] in hashes:
            total += int(count)
    return total


def prune(source: str, apply: bool) -> dict[str, Any]:
    """Per Bronze table of the source: files held, superseded, unknown; with apply, delete the superseded rows."""
    meta = landing_objects(s3(), req("R2_LANDING_BUCKET"), source)
    known, keep = set(meta), keep_digests(meta)
    cat = catalog()
    report: dict[str, Any] = {}
    for table_id in SOURCE_TABLES[source][0]:
        name = ".".join(table_id)
        with contextlib.suppress(NoSuchTableError):
            table = retry(lambda table_id=table_id: cat.load_table(table_id), f"load {name}")
            held = loaded_hashes(table)
            old = sorted((held & known) - keep)
            entry = {
                "files": len(held),
                "superseded": len(old),
                "superseded_rows_in_named_files": superseded_rows(table, set(old)),
                "unknown_kept": len(held - known),
            }
            if apply and old:
                for start in range(0, len(old), CHUNK):
                    chunk = old[start : start + CHUNK]
                    retry(
                        lambda table_id=table_id, chunk=chunk: cat.load_table(table_id).delete(In("raw_sha256", chunk)),
                        f"prune {name}",
                    )
                entry["removed"] = len(old)
            report[name] = entry
    return report
