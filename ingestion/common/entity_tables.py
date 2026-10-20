"""Entity tables: Bronze and Silver tables that hold exactly one file set per source entity (design #875).

An entity is one unit the source publishes as a whole (an Eurostat dataset, a GUS BDL
subgroup). Each table is partitioned by its entity column, catalog compaction is off for it,
and every data file is named ``<recipe>.parquet`` (the Landing SHA-256, or for derived files a
digest of their inputs), so the table's file list alone tells what it holds:

- ``state``: entity -> {recipes, rows, bytes}, from table metadata only (no data is read);
- ``replace``: a new version replaces the entity's partition: one transaction drops the
  entity's files from the file list and appends the new ones. Nothing is read or rewritten
  row by row, a rerun is a no-op, and duplicates cannot arise;
- ``drop_entities``: entities that left the source;
- ``configure``: compaction off and one snapshot kept (owner policy #822), per table.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import time
from typing import Any

from pyiceberg.expressions import In
from pyiceberg.io.pyarrow import parquet_file_to_data_file
import requests

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import req

CHUNK = 200  # entities per commit: one catalog commit per chunk keeps commits few and small
SNAPSHOT_POLICY = {"state": "enabled", "min_snapshots_to_keep": 1, "max_snapshot_age": "1h"}


def recipe_of(path: str) -> str:
    """The recipe (file name without extension) of a data file."""
    return Path(path).name.removesuffix(".parquet")


def state(table, entity_column: str) -> dict[str, dict[str, Any]]:
    """Entity -> {"recipes": set, "rows": int, "bytes": int, "paths": list}, from metadata only."""
    out: dict[str, dict[str, Any]] = defaultdict(lambda: {"recipes": set(), "rows": 0, "bytes": 0, "paths": []})
    if table.current_snapshot() is None:
        return {}
    files = table.inspect.files()
    for path, rows, size, partition in zip(  # noqa: B905 - columns of one arrow table
        files.column("file_path").to_pylist(),
        files.column("record_count").to_pylist(),
        files.column("file_size_in_bytes").to_pylist(),
        files.column("partition").to_pylist(),
    ):
        item = out[(partition or {}).get(entity_column)]
        item["recipes"].add(recipe_of(path))
        item["rows"] += rows
        item["bytes"] += size
        item["paths"].append(path)
    return dict(out)


def _data_files(table, uris: list[str], stats_columns: list[str]):
    stats = {f"write.metadata.metrics.column.{c}": "full" for c in stats_columns}
    metadata = table.metadata.model_copy(update={"properties": {**table.metadata.properties, **stats}})
    return [parquet_file_to_data_file(table.io, metadata, uri) for uri in uris]


def replace(
    cat,
    table_id: tuple[str, str],
    entity_column: str,
    wanted: dict[str, list[str]],
    stats_columns: list[str],
    operation: str,
) -> dict[str, int]:
    """Make each entity in ``wanted`` hold exactly the given file URIs (an empty list removes it).

    Entities already holding exactly those files are skipped, so a rerun, or a retry after a
    lost commit response, changes nothing. Returns counts of entities replaced and skipped."""
    replaced = skipped = 0
    entities = sorted(wanted)
    for start in range(0, len(entities), CHUNK):
        chunk = entities[start : start + CHUNK]
        first = None
        for attempt in range(len(bf.RETRY_DELAYS) + 1):
            table = bf.retry(lambda: cat.load_table(table_id), "load_table")
            held = state(table, entity_column)
            todo = [e for e in chunk if set(held.get(e, {}).get("paths", [])) != set(wanted[e])]
            first = todo if first is None else first
            if not todo:
                break
            files = _data_files(table, [u for e in todo for u in wanted[e]], stats_columns)
            gone = [e for e in todo if e in held]
            try:
                with table.transaction() as tx:
                    if gone:
                        tx.delete(In(entity_column, gone))
                    if files:
                        with tx.update_snapshot(
                            snapshot_properties={"zohelo.operation": operation}
                        ).fast_append() as add:
                            for data_file in files:
                                add.append_data_file(data_file)
            except Exception as exc:  # a lost response is re-checked against the table, never resent blindly
                if not bf.transient(exc) or attempt == len(bf.RETRY_DELAYS):
                    raise
                time.sleep(bf.RETRY_DELAYS[attempt])
        held = state(bf.retry(lambda: cat.load_table(table_id), "load_table"), entity_column)
        wrong = [e for e in chunk if set(held.get(e, {}).get("paths", [])) != set(wanted[e])]
        if wrong:
            raise RuntimeError(f"{'.'.join(table_id)}: entities not as intended after commit: {wrong[:5]}")
        replaced += len(first)
        skipped += len(chunk) - len(first)
    return {"replaced": replaced, "skipped": skipped}


def configure(table_id: tuple[str, str]) -> dict[str, Any]:
    """Per-table maintenance: compaction off (file names are the checkpoint), one snapshot kept."""
    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{req('CLOUDFLARE_ACCOUNT_ID')}/r2-catalog/"
        f"{req('R2_LAKEHOUSE_BUCKET')}/namespaces/{table_id[0]}/tables/{table_id[1]}/maintenance-configs"
    )
    headers = {"Authorization": f"Bearer {req('R2_DATA_CATALOG_TOKEN')}"}
    body = {"compaction": {"state": "disabled"}, "snapshot_expiration": SNAPSHOT_POLICY}
    response = requests.post(url, headers=headers, json=body, timeout=30)
    payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
    if response.status_code != 200 or not payload.get("success", True):
        raise RuntimeError(
            f"maintenance config of {'.'.join(table_id)} failed: {response.status_code} {response.text[:300]}"
        )
    return payload.get("result") or {}
