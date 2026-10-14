"""Shared Bronze plumbing for sources loaded one Parquet file per Landing file.

The convention of docs/playbook.md: every Landing file (SHA-256 ``raw_sha256``) becomes
``02_bronze/<source>/<table>/<entity>/<raw_sha256>.parquet``, registered in an Iceberg
table partitioned by the entity column, with column statistics computed client-side
(R2 Data Catalog rejects the metrics table properties). The table's file list is the
checkpoint; decoder rejections go to ``bronze.<source>_rejected_files``.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import time
from typing import Any

import botocore.exceptions
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import parquet_file_to_data_file
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import BinaryType, LongType, NestedField, StringType, TimestamptzType
import requests

SHA_RE = re.compile(r"^[0-9a-f]{64}$")
RETRY_DELAYS = (2, 5, 10, 20, 40)
TYPES = {"VARCHAR": StringType(), "BIGINT": LongType(), "TIMESTAMPTZ": TimestamptzType(), "BLOB": BinaryType()}
THROTTLED = {"ServiceUnavailable", "SlowDown", "InternalError", "RequestTimeout", "TooManyRequests", "503", "500"}
REJECTED_SCHEMA = Schema(
    NestedField(1, "entity", StringType(), required=False),
    NestedField(2, "raw_sha256", StringType(), required=False),
    NestedField(3, "landing_key", StringType(), required=False),
    NestedField(4, "error", StringType(), required=False),
    NestedField(5, "recorded_at", TimestamptzType(), required=False),
    NestedField(6, "code_sha", StringType(), required=False),
)


def transient(exc: Exception | None) -> bool:
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, (requests.ConnectionError, requests.Timeout,
                            botocore.exceptions.ConnectionError, botocore.exceptions.ReadTimeoutError)):
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
        try:
            cat.create_namespace(table_id[0])
        except Exception:
            pass
        schema = Schema(*[NestedField(i, name, TYPES[kind], required=False)
                          for i, (name, kind) in enumerate(columns, start=1)])
        source_id = [name for name, _ in columns].index(partition) + 1
        spec = PartitionSpec(PartitionField(source_id=source_id, field_id=1000,
                                            transform=IdentityTransform(), name=partition))
        table = cat.create_table(table_id, schema=schema, partition_spec=spec)
        with table.transaction() as tx:
            tx.set_properties(**{"schema.name-mapping.default": table.schema().name_mapping.model_dump_json()})
        table = cat.load_table(table_id)
    if [f.name for f in table.schema().fields] != [name for name, _ in columns]:
        raise RuntimeError(f"{'.'.join(table_id)} schema differs from the decoder")
    return table


def rejected_table(cat, table_id: tuple[str, str]):
    try:
        return cat.load_table(table_id)
    except NoSuchTableError:
        return cat.create_table(table_id, schema=REJECTED_SCHEMA)


def loaded_hashes(table) -> set[str]:
    """Raw SHA-256 of every Landing file already in the table (from the file names)."""
    if table.current_snapshot() is None:
        return set()
    paths = table.inspect.files().column("file_path").to_pylist()
    return {Path(p).stem for p in paths if SHA_RE.fullmatch(Path(p).stem)}


def rejected_hashes(cat, table_id: tuple[str, str]) -> set[str]:
    try:
        table = cat.load_table(table_id)
    except NoSuchTableError:
        return set()
    if table.current_snapshot() is None:
        return set()
    return set(table.scan(selected_fields=("raw_sha256",)).to_arrow().column("raw_sha256").to_pylist())


def upload(client, bucket: str, key: str, path: Path, raw_sha256: str) -> str:
    """Upload once; an existing object must be byte-identical (same SHA-256 metadata)."""
    digest = sha256(path.read_bytes()).hexdigest()
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        if (head.get("Metadata") or {}).get("sha256") != digest:
            client.delete_object(Bucket=bucket, Key=key)    # unregistered leftover of a failed run
            raise FileNotFoundError
    except Exception:
        client.upload_file(str(path), bucket, key, ExtraArgs={
            "ContentType": "application/vnd.apache.parquet",
            "Metadata": {"sha256": digest, "raw-sha256": raw_sha256}})
        if (client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256") != digest:
            raise RuntimeError(f"Bronze upload verification failed: {key}")
    return f"s3://{bucket}/{key}"


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
            with table.transaction() as tx:
                with tx.update_snapshot(snapshot_properties={"zohelo.operation": operation}).fast_append() as append:
                    for data_file in files:
                        append.append_data_file(data_file)
            return
        except Exception as exc:
            if not transient(exc) or attempt == len(RETRY_DELAYS):
                raise
            time.sleep(RETRY_DELAYS[attempt])


def record_rejections(cat, table_id: tuple[str, str], rejected: list[dict[str, Any]]) -> None:
    if not rejected:
        return
    now = datetime.now(timezone.utc)
    rows = [{**r, "recorded_at": r.get("recorded_at") or now} for r in rejected]
    rejected_table(cat, table_id).append(pa.Table.from_pylist(rows, schema=REJECTED_SCHEMA.as_arrow()))


# Bronze tables that hold one file per Landing SHA-256, by source: (data tables, rejections).
SOURCE_TABLES = {
    "eurostat": ([("bronze", "eurostat_full_observations")], ("bronze", "eurostat_rejected_files")),
    "nbp": ([("bronze", "nbp_observations")], ("bronze", "nbp_rejected_files")),
    "gus_bdl": ([("bronze", "bdl_subgroup_observations"), ("bronze", "bdl_metadata_variables"),
                 ("bronze", "bdl_metadata_units")], ("bronze", "bdl_rejected_files")),
    "gus_teryt": ([("bronze", "teryt_records")], ("bronze", "teryt_rejected_files")),
    "gugik_prg": ([("bronze", "prg_boundaries")], ("bronze", "prg_rejected_files")),
    "world_bank_wdi": ([("bronze", "wdi_bulk_observations"), ("bronze", "wdi_bulk_metadata")],
                       ("bronze", "wdi_rejected_files")),
}


def proven_hashes(source: str) -> tuple[set[str], set[str]]:
    """(Landing SHA-256 loaded into Bronze, Landing SHA-256 rejected with a reason)."""
    from ingestion.common.r2 import catalog
    tables, rejections = SOURCE_TABLES[source]
    cat = catalog()
    loaded: set[str] = set()
    for table_id in tables:
        try:
            loaded |= loaded_hashes(cat.load_table(table_id))
        except NoSuchTableError:
            pass
    return loaded, rejected_hashes(cat, rejections)
