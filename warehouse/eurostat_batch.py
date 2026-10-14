"""Batching of the incremental Eurostat Silver build (design: warehouse/README.md).

Scanning Bronze to find the newest version of each dataset reads billions of rows, so
the batch is planned from Iceberg metadata instead:

  <ns>.eurostat_file_index   one row per Bronze file (= one Landing file): dataset_id (the
                             partition value), raw_sha256 (the file name), ingested_at (the
                             column's upper bound), record_count, is_current (newest
                             ingested_at of its dataset). Rebuilt on every build.
  <ns>.eurostat_loaded_files the Landing files whose rows are in Silver, with load time.

``prepare`` returns the dbt vars of one build: the next current files not yet loaded (in
dataset order, up to a row budget) and the loaded files that have been superseded;
``finish`` records them after the model has run. Both tables are small (one row per file)
and derived, so they are overwritten.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime
from pathlib import Path
import re

import pyarrow as pa
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
import yaml

from ingestion.common.r2 import catalog

BRONZE_ID = ("bronze", "eurostat_full_observations")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
INDEX_SCHEMA = pa.schema(
    [
        ("dataset_id", pa.string()),
        ("raw_sha256", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("record_count", pa.int64()),
        ("file_size_bytes", pa.int64()),
        ("is_current", pa.bool_()),
    ]
)
LOG_SCHEMA = pa.schema(
    [("dataset_id", pa.string()), ("raw_sha256", pa.string()), ("loaded_at", pa.timestamp("us", tz="UTC"))]
)
MODEL = "eurostat_observations"
# The batch travels as dbt --vars (one command-line argument, ≤128 KB on Linux): at most
# MAX_PAIRS (dataset, file) pairs per list, ~100 bytes each.
SETTINGS = yaml.safe_load((Path(__file__).resolve().parent / "warehouse.yaml").read_text())["eurostat_silver"]
MAX_PAIRS = int(SETTINGS["max_versions"])
OVERSIZED_ROWS = int(SETTINGS["oversized_rows"])


def wanted(selectors: list[str]) -> bool:
    return not selectors or any(
        s.strip("+").split(".")[-1] in (MODEL, "eurostat_dataset_versions", "eurostat", "silver") for s in selectors
    )


def _overwrite(cat, table_id: tuple[str, str], rows: pa.Table) -> None:
    try:
        table = cat.load_table(table_id)
    except NoSuchTableError:
        with contextlib.suppress(NamespaceAlreadyExistsError):
            cat.create_namespace(table_id[0])
        table = cat.create_table(table_id, schema=rows.schema)
    table.overwrite(rows)


def _read(cat, table_id: tuple[str, str], schema: pa.Schema) -> pa.Table:
    try:
        table = cat.load_table(table_id)
    except NoSuchTableError:
        return schema.empty_table()
    if table.current_snapshot() is None:
        return schema.empty_table()
    return table.scan().to_arrow().cast(schema)


def _drop_legacy(cat, namespace: str) -> bool:
    """A table of the model's name without its columns is the legacy passthrough copy, which
    new models replace (owner decision, 13 Oct 2026); without the drop, the incremental
    model would append to it."""
    try:
        names = {f.name for f in cat.load_table((namespace, MODEL)).schema().fields}
    except NoSuchTableError:
        return False
    if {"raw_sha256", "breakdown_codes", "time_period"} <= names:
        return False
    cat.drop_table((namespace, MODEL))
    return True


def prepare(namespace: str, row_budget: int) -> dict:
    cat = catalog()
    dropped_legacy = _drop_legacy(cat, namespace)
    files = cat.load_table(BRONZE_ID).inspect.files()
    rows = []
    for path, partition, count, size, metrics in zip(
        files.column("file_path").to_pylist(),
        files.column("partition").to_pylist(),
        files.column("record_count").to_pylist(),
        files.column("file_size_in_bytes").to_pylist(),
        files.column("readable_metrics").to_pylist(),
        strict=False,
    ):
        sha = Path(path).stem
        if not SHA_RE.fullmatch(sha):
            continue
        ingested = ((metrics or {}).get("ingested_at") or {}).get("upper_bound")
        rows.append(
            {
                "dataset_id": partition["dataset_id"],
                "raw_sha256": sha,
                "ingested_at": ingested,
                "record_count": count,
                "file_size_bytes": size,
            }
        )
    newest: dict[str, tuple] = {}
    for r in rows:
        key = (r["ingested_at"] or datetime.min.replace(tzinfo=UTC), r["raw_sha256"])
        if r["dataset_id"] not in newest or key > newest[r["dataset_id"]]:
            newest[r["dataset_id"]] = key
    for r in rows:
        r["is_current"] = newest[r["dataset_id"]][1] == r["raw_sha256"]
    _overwrite(cat, (namespace, "eurostat_file_index"), pa.Table.from_pylist(rows, schema=INDEX_SCHEMA))

    # Keyed by (dataset_id, raw_sha256): Eurostat publishes some byte-identical files under
    # several dataset codes, so a file hash alone does not identify a dataset version.
    current = {(r["dataset_id"], r["raw_sha256"]): r for r in rows if r["is_current"]}
    log = _read(cat, (namespace, "eurostat_loaded_files"), LOG_SCHEMA).to_pylist()
    loaded = {(r["dataset_id"], r["raw_sha256"]) for r in log}
    superseded = sorted(loaded - set(current))[:MAX_PAIRS]
    batch, total, oversized = [], 0, []
    for key in sorted(k for k in current if k not in loaded):
        r = current[key]
        # A single file above OVERSIZED_ROWS exhausts the runner's temp disk (1.48B rows
        # failed at 75 GB); those are deferred until a sliced load exists.
        if r["record_count"] > OVERSIZED_ROWS:
            oversized.append(r)
            continue
        if batch and (total + r["record_count"] > row_budget or len(batch) >= MAX_PAIRS):
            break
        batch.append(r)
        total += r["record_count"]
    remaining = sum(1 for k in current if k not in loaded) - len(batch) - len(oversized)
    return {
        "vars": {
            "eurostat_batch": [[r["dataset_id"], r["raw_sha256"]] for r in batch],
            "eurostat_superseded": [list(k) for k in superseded],
        },
        "summary": {
            "datasets_current": len(current),
            "batch_datasets": len(batch),
            "batch_rows": total,
            "superseded": len(superseded),
            "remaining_after_batch": remaining,
            "dropped_legacy_table": dropped_legacy,
            "deferred_oversized": [[r["dataset_id"], r["record_count"]] for r in oversized][:20],
            "deferred_oversized_count": len(oversized),
        },
    }


def finish(namespace: str, plan: dict) -> None:
    cat = catalog()
    log_id = (namespace, "eurostat_loaded_files")
    gone = {tuple(k) for k in plan["vars"]["eurostat_superseded"]}
    log = [r for r in _read(cat, log_id, LOG_SCHEMA).to_pylist() if (r["dataset_id"], r["raw_sha256"]) not in gone]
    now = datetime.now(UTC)
    log += [{"dataset_id": d, "raw_sha256": h, "loaded_at": now} for d, h in plan["vars"]["eurostat_batch"]]
    _overwrite(cat, log_id, pa.Table.from_pylist(log, schema=LOG_SCHEMA))
