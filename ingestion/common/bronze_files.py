"""Shared Bronze plumbing for sources loaded one Parquet file per Landing file.

The convention of docs/playbook.md: every Landing file (SHA-256 ``raw_sha256``) becomes
``02_bronze/<source>/<table>/<entity>/<raw_sha256>.parquet``, registered in an Iceberg
table partitioned by the entity column, with column statistics computed client-side
(R2 Data Catalog rejects the metrics table properties). The table's file list is the
checkpoint: file names, or the ``raw_sha256`` column of files that catalog compaction
rewrote under other names; decoder rejections go to ``bronze.<source>_rejected_files``.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime
from hashlib import sha256
import json
from pathlib import Path
import re
import time
from typing import Any

import botocore.exceptions
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.io.pyarrow import parquet_file_to_data_file
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)
import requests

SHA_RE = re.compile(r"^[0-9a-f]{64}$")
RETRY_DELAYS = (2, 5, 10, 20, 40)
TYPES = {
    "VARCHAR": StringType(),
    "BIGINT": LongType(),
    "INTEGER": IntegerType(),
    "TIMESTAMPTZ": TimestamptzType(),
    "BLOB": BinaryType(),
    "DOUBLE": DoubleType(),
    "BOOLEAN": BooleanType(),
}
THROTTLED = {"ServiceUnavailable", "SlowDown", "InternalError", "RequestTimeout", "TooManyRequests", "503", "500"}


def rejected_schema(entity_column: str = "entity") -> Schema:
    """Rejected-files table schema. New tables use ``entity``; the Eurostat and BDL tables
    were created with ``dataset_id`` in that position and keep it (same meaning: the
    Landing entity — dataset code or subgroup id)."""
    return Schema(
        NestedField(1, entity_column, StringType(), required=False),
        NestedField(2, "raw_sha256", StringType(), required=False),
        NestedField(3, "landing_key", StringType(), required=False),
        NestedField(4, "error", StringType(), required=False),
        NestedField(5, "recorded_at", TimestamptzType(), required=False),
        NestedField(6, "code_sha", StringType(), required=False),
    )


REJECTED_SCHEMA = rejected_schema()


def transient(exc: Exception | None) -> bool:
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(
            exc,
            (
                requests.ConnectionError,
                requests.Timeout,
                botocore.exceptions.ConnectionError,
                botocore.exceptions.ReadTimeoutError,
            ),
        ):
            return True
        if isinstance(exc, botocore.exceptions.ClientError):
            return str(exc.response.get("Error", {}).get("Code", "")) in THROTTLED
        if isinstance(exc, requests.HTTPError):
            return exc.response is not None and exc.response.status_code in {429, 500, 502, 503, 504}
        exc = exc.__cause__
    return False


def retry(operation, label: str):
    """Retry transient catalog/storage reads (never use for an append)."""
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            return operation()
        except Exception as exc:
            if not transient(exc) or attempt == len(RETRY_DELAYS):
                raise
            print(json.dumps({"status": "transient_retry", "operation": label, "attempt": attempt + 1}), flush=True)
            time.sleep(RETRY_DELAYS[attempt])


def ensure_table(cat, table_id: tuple[str, str], columns: list[tuple[str, str]], partition: str):
    """Load the table, creating it (all columns optional, identity partition) if absent."""
    try:
        table = cat.load_table(table_id)
    except NoSuchTableError:
        with contextlib.suppress(NamespaceAlreadyExistsError):
            cat.create_namespace(table_id[0])
        schema = Schema(
            *[NestedField(i, name, TYPES[kind], required=False) for i, (name, kind) in enumerate(columns, start=1)]
        )
        source_id = [name for name, _ in columns].index(partition) + 1
        spec = PartitionSpec(
            PartitionField(source_id=source_id, field_id=1000, transform=IdentityTransform(), name=partition)
        )
        table = cat.create_table(table_id, schema=schema, partition_spec=spec)
        with table.transaction() as tx:
            tx.set_properties(**{"schema.name-mapping.default": table.schema().name_mapping.model_dump_json()})
        table = cat.load_table(table_id)
    if [f.name for f in table.schema().fields] != [name for name, _ in columns]:
        raise RuntimeError(f"{'.'.join(table_id)} schema differs from the decoder")
    return table


def rejected_table(cat, table_id: tuple[str, str], entity_column: str = "entity"):
    """Load the rejected-files table, creating it if absent."""
    try:
        return cat.load_table(table_id)
    except NoSuchTableError:
        return cat.create_table(table_id, schema=rejected_schema(entity_column))


def loaded_hashes(table) -> set[str]:
    """Raw SHA-256 of every Landing file already in the table.

    A loader names each Bronze file after its Landing file's SHA-256. R2 Data Catalog
    compaction rewrites small files under other names, and a check by file name alone then
    treats their Landing files as new and loads them again (#803: NBP doubled). The hashes
    of every other file are read from its `raw_sha256` column."""
    if table.current_snapshot() is None:
        return set()
    hashes: set[str] = set()
    for path in table.inspect.data_files().column("file_path").to_pylist():
        stem = Path(path).stem
        if SHA_RE.fullmatch(stem):
            hashes.add(stem)
            continue
        column = read_columns(table, path, ["raw_sha256"]).column("raw_sha256")
        hashes.update(h for h in pc.unique(column).to_pylist() if h)
    return hashes


def duplicated_hashes(table) -> dict[str, set[str]]:
    """Landing files (raw SHA-256) whose rows the table holds twice; read by bronze.verify only.

    A Landing file is identified by (partition value, raw SHA-256): Eurostat publishes some
    byte-identical files under several dataset codes. ``loaded_twice``: in more than one file
    named after it, in such a file and in a compacted file (compaction removes the files it
    rewrites), or with a row repeated in full (every column) inside one compacted file — a
    reload repeats rows exactly, while rows of one Landing file differ at least in their
    period, field or source row. ``several_compacted``: in more than one compacted file, which
    compaction may cause by splitting one file's rows; to check, not proof."""
    if table.current_snapshot() is None:
        return {"loaded_twice": set(), "several_compacted": set()}
    named: dict[tuple, int] = {}
    compacted: dict[tuple, int] = {}
    twice: set[tuple] = set()
    files = table.inspect.data_files()
    paths = files.column("file_path").to_pylist()
    for path, partition in zip(paths, files.column("partition").to_pylist(), strict=True):
        entity = next(iter((partition or {}).values()), None)
        stem = Path(path).stem
        if SHA_RE.fullmatch(stem):
            named[(entity, stem)] = named.get((entity, stem), 0) + 1
            continue
        data = read_columns(table, path, None)
        for h in pc.unique(data.column("raw_sha256")).to_pylist():
            if h:
                compacted[(entity, h)] = compacted.get((entity, h), 0) + 1
        counts = data.group_by(data.column_names).aggregate([([], "count_all")])
        repeated = counts.filter(pc.greater(counts.column("count_all"), 1)).column("raw_sha256")
        twice |= {(entity, h) for h in pc.unique(repeated).to_pylist() if h}
    twice |= {k for k, n in named.items() if n > 1 or k in compacted}
    several = {k for k, n in compacted.items() if n > 1 and k not in twice}
    return {"loaded_twice": {h for _, h in twice}, "several_compacted": {h for _, h in several}}


def read_columns(table, path: str, columns: list[str] | None) -> pa.Table:
    """Some columns (None: all) of one data file of the table (a file not named after a SHA-256)."""

    def read() -> pa.Table:
        with table.io.new_input(path).open() as handle:
            return pq.read_table(handle, columns=columns)

    return retry(read, "read_file")


def rejected_hashes(cat, table_id: tuple[str, str]) -> set[str]:
    try:
        table = cat.load_table(table_id)
    except NoSuchTableError:
        return set()
    if table.current_snapshot() is None:
        return set()
    return set(table.scan(selected_fields=("raw_sha256",)).to_arrow().column("raw_sha256").to_pylist())


def _missing(exc: botocore.exceptions.ClientError) -> bool:
    return str(exc.response.get("Error", {}).get("Code", "")) in {"404", "NoSuchKey", "NotFound"}


def upload(client, bucket: str, key: str, path: Path, raw_sha256: str) -> str:
    """Upload once and verify; an existing object must be byte-identical (same SHA-256 metadata).

    An existing object with other bytes is an unregistered leftover of a failed run and is
    replaced. Any error other than "not found" is raised, never treated as absent.
    """
    digest = sha256(path.read_bytes()).hexdigest()
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        if (head.get("Metadata") or {}).get("sha256") == digest:
            return f"s3://{bucket}/{key}"
        client.delete_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as exc:
        if not _missing(exc):
            raise
    client.upload_file(
        str(path),
        bucket,
        key,
        ExtraArgs={
            "ContentType": "application/vnd.apache.parquet",
            "Metadata": {"sha256": digest, "raw-sha256": raw_sha256},
        },
    )
    if (client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Bronze upload verification failed: {key}")
    return f"s3://{bucket}/{key}"


def download(client, bucket: str, source: str, key: str, raw_sha256: str, size: int | None, destination: Path) -> None:
    """The Landing file's exact bytes, from Landing or, once moved, from the Archive;
    size (when known) and SHA-256 are verified against the receipt."""
    from ingestion.common.r2 import archive_key, raw_chunks

    try:
        client.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as exc:
        if not _missing(exc):
            raise
        key = archive_key(source, raw_sha256, key)
    digest, written = sha256(), 0
    with destination.open("xb") as handle:
        for block in raw_chunks(client, bucket, key):
            written += len(block)
            digest.update(block)
            handle.write(block)
    if (size is not None and written != size) or digest.hexdigest() != raw_sha256:
        raise RuntimeError(f"{source} raw bytes do not match their receipt: {key}")


def commit(cat, table_id: tuple[str, str], uris: list[str], stats_columns: list[str], operation: str) -> None:
    """Register the files; a lost commit response is re-checked, never blindly resent."""
    stats = {f"write.metadata.metrics.column.{c}": "full" for c in stats_columns}
    for attempt in range(len(RETRY_DELAYS) + 1):
        table = retry(lambda: cat.load_table(table_id), "reload_table")
        present = set(table.inspect.files().column("file_path").to_pylist()) if table.current_snapshot() else set()
        missing = [u for u in uris if u not in present]
        if not missing:
            return
        metadata = table.metadata.model_copy(update={"properties": {**table.metadata.properties, **stats}})
        files = [parquet_file_to_data_file(table.io, metadata, uri) for uri in missing]
        try:
            with (
                table.transaction() as tx,
                tx.update_snapshot(snapshot_properties={"zohelo.operation": operation}).fast_append() as append,
            ):
                for data_file in files:
                    append.append_data_file(data_file)
            return
        except Exception as exc:
            if not transient(exc) or attempt == len(RETRY_DELAYS):
                raise
            time.sleep(RETRY_DELAYS[attempt])


def record_rejections(
    cat, table_id: tuple[str, str], rejected: list[dict[str, Any]], entity_column: str = "entity"
) -> None:
    """Append decoder rejections (dicts with entity, raw_sha256, landing_key, error)."""
    if not rejected:
        return
    now = datetime.now(UTC)
    rows = [
        {
            **{k: v for k, v in r.items() if k != "entity"},
            entity_column: r.get("entity", r.get(entity_column)),
            "recorded_at": r.get("recorded_at") or now,
        }
        for r in rejected
    ]
    schema = rejected_schema(entity_column)
    rejected_table(cat, table_id, entity_column).append(pa.Table.from_pylist(rows, schema=schema.as_arrow()))


def source_tables() -> dict[str, tuple[list[tuple[str, str]], list[tuple[str, str]]]]:
    """Bronze tables per rebuilt source, from the ``bronze:`` list in ingestion/sources/<source>.yaml:
    (tables holding one file per Landing SHA-256, rejected-files tables). Only sources whose
    Pipeline loads Bronze (``bronze.load`` in ``activities``) are included."""
    from ingestion.common.r2 import sources

    out = {}
    for source, config in sources().items():
        if "bronze.load" not in (config.get("activities") or []):
            continue
        names = [tuple(str(n).split(".", 1)) for n in config.get("bronze") or []]
        out[source] = (
            [n for n in names if not n[1].endswith("rejected_files")],
            [n for n in names if n[1].endswith("rejected_files")],
        )
    return out


SOURCE_TABLES = source_tables()


def proven_hashes(source: str) -> tuple[set[str], set[str]]:
    """(Landing SHA-256 loaded into Bronze, Landing SHA-256 rejected with a reason)."""
    from ingestion.common.r2 import catalog

    tables, rejections = SOURCE_TABLES[source]
    cat = catalog()
    loaded: set[str] = set()
    for table_id in tables:
        with contextlib.suppress(NoSuchTableError):
            loaded |= loaded_hashes(cat.load_table(table_id))
    rejected: set[str] = set()
    for table_id in rejections:
        rejected |= rejected_hashes(cat, table_id)
    return loaded, rejected


def source_duplicates(source: str) -> dict[str, dict[str, set[str]]]:
    """Per Bronze table of the source: ``duplicated_hashes``."""
    from ingestion.common.r2 import catalog

    cat = catalog()
    found: dict[str, dict[str, set[str]]] = {}
    for table_id in SOURCE_TABLES[source][0]:
        with contextlib.suppress(NoSuchTableError):
            found[".".join(table_id)] = duplicated_hashes(cat.load_table(table_id))
    return found
