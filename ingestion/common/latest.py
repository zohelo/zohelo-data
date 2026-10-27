"""Latest version only in Bronze (owner data policy #822/#845, 16 Oct 2026).

Bronze, Silver and Gold hold only the latest version of every source entity; superseded
versions live only in the Landing Archive. Bronze loaders append each new Landing file;
``prune`` then removes, from every Bronze table of the source, the rows of Landing files that
are no longer the latest version of their entity.

Latest is decided from the Landing receipts (``r2.landing_objects``, which include files
already moved to the Archive): per (area, entity), the files of the newest download are kept.
Only sources declaring ``bronze_latest_only: true`` are pruned after their loads (the small
sources; Eurostat and GUS BDL replace whole entities instead, #875). Files a table holds but the
receipts do not know are kept and reported, never deleted. A file whose bytes are the latest
of another entity is kept too (identical bytes can sit under several entities).
"""

from __future__ import annotations

import contextlib
from typing import Any

import pyarrow.compute as pc
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, EqualTo, In

from ingestion.common.bronze_files import SHA_RE, SOURCE_TABLES, loaded_hashes, read_columns, retry
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


def partition_groups(table, hashes: list[str]) -> list[tuple[dict[str, Any], list[str]]]:
    """Superseded hashes grouped by the identity-partition values of the files that hold them.

    A delete scoped to one partition reads and rewrites only that partition's files: an unscoped
    delete of hundreds of hashes made pyiceberg read compacted files across the whole table and
    the runner was killed (Eurostat, 16 Oct). A file named after a hash holds that hash; a file
    with another name (written by catalog compaction before it was switched off, #875) is read
    for its raw_sha256 values, and every compacted file lies in one partition, so its hashes are
    deleted in that partition (NBP kept one superseded file only in a compacted file, 22 Oct).
    Hashes found in no file form one group without partition values."""
    spec, schema = table.spec(), table.schema()
    columns = {f.name: schema.find_field(f.source_id).name for f in spec.fields if str(f.transform) == "identity"}
    wanted: set[str] = set(hashes)
    where: dict[str, set[tuple]] = {}
    files = table.inspect.data_files()
    compacted = []
    for path, partition in zip(
        files.column("file_path").to_pylist(), files.column("partition").to_pylist(), strict=True
    ):
        # a null partition value cannot scope a delete (EqualTo(col, None)); such files count as unpartitioned
        key = tuple(sorted((columns[k], v) for k, v in (partition or {}).items() if k in columns and v is not None))
        stem = path.rsplit("/", 1)[-1].split(".", 1)[0]
        if stem in wanted:
            where.setdefault(stem, set()).add(key)
        elif not SHA_RE.fullmatch(stem):
            compacted.append((path, key))
    for path, key in compacted:
        found = set(pc.unique(read_columns(table, path, ["raw_sha256"]).column("raw_sha256")).to_pylist())
        for digest in found & wanted:
            where.setdefault(digest, set()).add(key)
    groups: dict[tuple, list[str]] = {}
    for digest in hashes:
        for key in where.get(digest) or {()}:
            groups.setdefault(key, []).append(digest)
    return [(dict(key), sorted(group)) for key, group in groups.items()]


def remove(cat, table_id: tuple[str, str], table, hashes: list[str]) -> int:
    """Delete the rows of these Landing files, one partition at a time, in chunks; returns the count removed.

    Hashes found in no file of a partitioned table are skipped (an unscoped delete reads the table
    whole); they stay listed as superseded."""
    removed: set[str] = set()
    partitioned = bool(table.spec().fields)
    for values, group in partition_groups(table, hashes):
        if partitioned and not values:
            continue
        removed.update(group)
        for start in range(0, len(group), CHUNK):
            condition = In("raw_sha256", group[start : start + CHUNK])
            for column, value in values.items():
                condition = And(EqualTo(column, value), condition)
            retry(lambda condition=condition: cat.load_table(table_id).delete(condition), f"prune {'.'.join(table_id)}")
    return len(removed)


def prune(source: str, apply: bool) -> dict[str, Any]:
    """Per Bronze table of the source: files held, superseded, unknown; with apply, delete the superseded rows."""
    meta = landing_objects(s3(), req("STORAGE_LANDING_BUCKET_NAME"), source)
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
                entry["removed"] = remove(cat, table_id, table, old)
            report[name] = entry
    return report
