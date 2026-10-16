"""Latest version only in Bronze (owner data policy #822/#845, 16 Oct 2026).

Bronze, Silver and Gold hold only the latest version of every source entity; superseded
versions live only in the Landing Archive. Bronze loaders append each new Landing file;
``prune`` then removes, from every Bronze table of the source, the rows of Landing files that
are no longer the latest version of their entity.

Latest is decided from the Landing receipts (``r2.landing_objects``, which include files
already moved to the Archive): per (area, entity), the files of the newest download are kept.
Only sources declaring ``bronze_latest_only: true`` are pruned after their loads. GUS BDL uses
the Silver rule read from its table (``keep_bdl``): its Landing mixes two layouts. Files a table holds but the
receipts do not know are kept and reported, never deleted. A file whose bytes are the latest
of another entity is kept too (identical bytes can sit under several entities).
"""

from __future__ import annotations

import contextlib
from typing import Any

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import In

from ingestion.common.bronze_files import SOURCE_TABLES, loaded_hashes, retry
from ingestion.common.r2 import catalog, landing_objects, req, s3

CHUNK = 300  # raw SHA-256 values per delete commit


def keep_digests(meta: dict[str, dict[str, Any]]) -> set[str]:
    """SHA-256 of every file of the newest download of each (area, entity) in the snapshot layout.

    Newest = latest ingest stamp (YYYYMMDDTHHMMSSZ, sortable). Version tokens are not compared:
    their shapes differ between sources and even within one (Last-Modified vs download time,
    "noversion", sanitized hashes). Files of one ingest (parts) are all kept."""
    best: dict[tuple[str, str], tuple[str, set[str]]] = {}
    for digest, item in meta.items():
        for path in item.get("snapshot_paths", []):
            key = (path["area"], path["entity"])
            held = best.get(key)
            if held is None or path["ingested"] > held[0]:
                best[key] = (path["ingested"], {digest})
            elif path["ingested"] == held[0]:
                held[1].add(digest)
    return set().union(*(digests for _, digests in best.values())) if best else set()


BDL_TABLE = ("bronze", "bdl_subgroup_observations")


def keep_bdl(table) -> tuple[set[str], set[str]]:
    """(held, keep) raw SHA-256 of GUS BDL subgroup observations, by the Silver rule (bdl_observations):
    per subgroup the highest ``source_version`` (API lastUpdate, ISO), and per part of it the newest
    ingest. BDL Landing mixes an earlier ``web_bulk/`` layout with snapshots, so the rule is read from
    the table itself, where every row carries its file's subgroup, version, part and ingest time."""
    files: dict[str, tuple[str, str, int, str]] = {}
    columns = ("raw_sha256", "subgroup_id", "source_version", "part_index", "ingested_at")
    for batch in table.scan(selected_fields=columns).to_arrow_batch_reader():
        for row in pa.Table.from_batches([batch]).group_by(list(columns)).aggregate([]).to_pylist():
            files[row["raw_sha256"]] = (
                row["subgroup_id"],
                row["source_version"] or "",
                int(row["part_index"] or 0),
                str(row["ingested_at"]),
            )
    newest_version: dict[str, str] = {}
    for subgroup, version, _, _ in files.values():
        newest_version[subgroup] = max(version, newest_version.get(subgroup, ""))
    best: dict[tuple[str, int], tuple[str, str]] = {}
    for digest, (subgroup, version, part, ingested) in files.items():
        if version != newest_version[subgroup]:
            continue
        held = best.get((subgroup, part))
        # newest ingest; on a tie the smallest raw_sha256, exactly as Silver orders them
        if held is None or ingested > held[0] or (ingested == held[0] and digest < held[1]):
            best[(subgroup, part)] = (ingested, digest)
    return set(files), {digest for _, digest in best.values()}


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
    if source == "gus_bdl":
        return prune_bdl(apply)
    meta = landing_objects(s3(), req("R2_LANDING_BUCKET"), source)
    # Only snapshot-layout files can be superseded; content-addressed files of the earlier layout
    # have no entity or version, so they are kept and reported.
    known = {d for d, item in meta.items() if item.get("snapshot_paths")}
    keep = keep_digests(meta)
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
                "unknown_kept": len(held - known),  # not in the receipts, or earlier content-addressed layout
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


def prune_bdl(apply: bool) -> dict[str, Any]:
    """GUS BDL subgroup observations: remove files that are not the current version (``keep_bdl``).
    The metadata tables (variables, units) are dictionaries reloaded whole and not pruned here."""
    cat = catalog()
    table = retry(lambda: cat.load_table(BDL_TABLE), "load bdl")
    held, keep = keep_bdl(table)
    old = sorted(held - keep)
    entry: dict[str, Any] = {
        "files": len(held),
        "superseded": len(old),
        "superseded_rows_in_named_files": superseded_rows(table, set(old)),
        "unknown_kept": 0,
    }
    if apply and old:
        for start in range(0, len(old), CHUNK):
            chunk = old[start : start + CHUNK]
            retry(lambda chunk=chunk: cat.load_table(BDL_TABLE).delete(In("raw_sha256", chunk)), "prune bdl")
        entry["removed"] = len(old)
    return {".".join(BDL_TABLE): entry}
