"""Decode one retained Eurostat full-distribution TSV into source-shaped Bronze.

Landing remains the exact native ``.tsv.gz`` object.  This module is a downstream,
replayable transformation: it streams the retained file, preserves every native
dimension code, period, value lexeme and status flag, and writes an independently
verifiable Parquet relation without changing the Landing object.
"""
from __future__ import annotations

import csv
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import gzip
from hashlib import sha256
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping

import duckdb
import pandas as pd


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MD5_RE = re.compile(r"^[0-9a-f]{32}$")
_DIMENSION_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
_DRIVE_FILE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,1024}$")
_BATCH_ROWS = 10_000

BRONZE_COLUMNS = (
    ("dataset_id", "VARCHAR"),
    ("distribution_id", "VARCHAR"),
    ("partition_id", "VARCHAR"),
    ("partition_selection_json", "VARCHAR"),
    ("period_key", "VARCHAR"),
    ("freq_code", "VARCHAR"),
    ("unit_code", "VARCHAR"),
    ("geo_code", "VARCHAR"),
    ("dimension_key_json", "VARCHAR"),
    ("dimension_key_sha256", "VARCHAR"),
    ("value_text", "VARCHAR"),
    ("value_numeric", "DOUBLE"),
    ("status_code", "VARCHAR"),
    ("is_missing", "BOOLEAN"),
    ("missing_reason", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("retrieved_at_utc", "TIMESTAMPTZ"),
    ("raw_sha256", "VARCHAR"),
    ("raw_file_id", "VARCHAR"),
    ("receipt_sha256", "VARCHAR"),
    ("receipt_file_id", "VARCHAR"),
    ("receipt_size_bytes", "BIGINT"),
    ("source_row_number", "BIGINT"),
    ("source_period_position", "BIGINT"),
)
_BRONZE_COLUMN_NAMES = tuple(name for name, _kind in BRONZE_COLUMNS)


class EurostatBulkDecodeError(ValueError):
    """A retained Eurostat distribution cannot satisfy the Bronze contract."""


def decode_landing_distribution(
    source: Path,
    receipt: Path,
    output: Path,
    *,
    receipt_uri: str,
    raw_uri: str,
) -> dict[str, Any]:
    """Decode one Landing data file; identity comes from its receipt (docs/landing.md)."""
    source = Path(source)
    receipt = Path(receipt)
    output = Path(output)
    identity = _validated_landing_identity(
        source=source, receipt=receipt, output=output,
        receipt_uri=receipt_uri, raw_uri=raw_uri,
    )
    return _decode_with_identity(source, output, identity)


def _decode_with_identity(source: Path, output: Path, identity: Mapping[str, Any]) -> dict[str, Any]:
    dataset_id = identity["dataset_id"]
    if _file_sha256(source) != identity["raw_sha256"]:
        raise EurostatBulkDecodeError("Eurostat distribution SHA-256 does not match its receipt")

    output.parent.mkdir(parents=True, exist_ok=True)
    database = output.parent / f".{output.name}.duckdb"
    if database.exists():
        raise EurostatBulkDecodeError("Eurostat decoder working database already exists")

    measurements = {
        "dataset_id": dataset_id,
        "distribution_id": identity["distribution_id"],
        "partition_id": identity["partition_id"],
        "empty_partition": identity["empty_partition"],
        "source_rows": 0,
        "observation_cells": 0,
        "missing_cells": 0,
        "empty_cells": 0,
        "colon_cells": 0,
        "flagged_cells": 0,
        "periods": 0,
    }
    completed = False
    try:
        with duckdb.connect(str(database)) as connection:
            # Eurostat headers require unique periods and every source series emits the
            # same ordered period set. A repeated complete observation key therefore
            # implies a repeated series key, so checking only period position 1 is exact
            # while reducing the aggregation by the number of periods in the dataset.
            connection.execute("SET memory_limit = '4GB'")
            connection.execute("SET threads = 1")
            connection.execute("SET preserve_insertion_order = false")
            definitions = ", ".join(
                f'"{name}" {kind}' for name, kind in BRONZE_COLUMNS
            )
            connection.execute(f"CREATE TABLE bronze ({definitions})")
            if not identity["empty_partition"]:
                _load_observations(connection, source, identity, measurements)

            duplicate = connection.execute(
                """
                SELECT dimension_key_sha256, count(*)
                FROM bronze
                WHERE source_period_position = 1
                GROUP BY dimension_key_sha256
                HAVING count(*) > 1
                LIMIT 1
                """
            ).fetchone()
            if duplicate is not None:
                raise EurostatBulkDecodeError(
                    "Eurostat TSV repeats a complete dimensional observation key"
                )
            escaped = str(output).replace("'", "''")
            connection.execute(
                f"COPY bronze TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        if not output.is_file():
            raise EurostatBulkDecodeError("Eurostat Bronze output was not created")
        measurements["output_bytes"] = output.stat().st_size
        measurements["output_sha256"] = _file_sha256(output)
        completed = True
    except EurostatBulkDecodeError:
        raise
    except (OSError, EOFError, UnicodeError, csv.Error, duckdb.Error) as exc:
        raise EurostatBulkDecodeError(f"invalid Eurostat full distribution: {exc}") from exc
    finally:
        database.unlink(missing_ok=True)
        if not completed and output.exists():
            output.unlink()

    return measurements


def _load_observations(
    connection: duckdb.DuckDBPyConnection,
    source: Path,
    identity: Mapping[str, Any],
    measurements: dict[str, Any],
) -> None:
    """Decode the native TSV body after receipt and raw identity are accepted."""
    with gzip.open(
        source, "rt", encoding="utf-8-sig", errors="strict", newline=""
    ) as stream:
        reader = csv.reader(stream, delimiter="\t")
        try:
            header = next(reader)
        except StopIteration:
            raise EurostatBulkDecodeError("Eurostat TSV is empty") from None
        dimension_names, periods = _parse_header(header)
        measurements["periods"] = len(periods)
        batch: list[tuple[Any, ...]] = []
        for line_number, row in enumerate(reader, start=2):
            if len(row) != len(header):
                raise EurostatBulkDecodeError(
                    f"Eurostat TSV row {line_number} has {len(row)} fields; "
                    f"expected {len(header)}"
                )
            dimension_codes = [part.strip() for part in row[0].split(",")]
            if len(dimension_codes) != len(dimension_names) or any(
                not code for code in dimension_codes
            ):
                raise EurostatBulkDecodeError(
                    f"Eurostat TSV row {line_number} has an invalid series key"
                )
            series_dimensions = dict(zip(dimension_names, dimension_codes))
            measurements["source_rows"] += 1
            for period_position, (period, cell) in enumerate(
                zip(periods, row[1:]), start=1
            ):
                dimensions = {**series_dimensions, "time": period}
                key_json = json.dumps(
                    dimensions,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                parsed = _parse_cell(cell, line_number, period)
                if parsed[3]:
                    measurements["missing_cells"] += 1
                    measurements[f"{parsed[4]}_cells"] += 1
                if parsed[2] is not None:
                    measurements["flagged_cells"] += 1
                batch.append(
                    (
                        identity["dataset_id"],
                        identity["distribution_id"],
                        identity["partition_id"],
                        identity["partition_selection_json"],
                        period,
                        series_dimensions.get("freq"),
                        series_dimensions.get("unit"),
                        series_dimensions.get("geo"),
                        key_json,
                        sha256(key_json.encode("utf-8")).hexdigest(),
                        parsed[0],
                        parsed[1],
                        parsed[2],
                        parsed[3],
                        parsed[4],
                        identity["source_version"],
                        identity["retrieved_at_utc"],
                        identity["raw_sha256"],
                        identity["raw_file_id"],
                        identity["receipt_sha256"],
                        identity["receipt_file_id"],
                        identity["receipt_size_bytes"],
                        line_number,
                        period_position,
                    )
                )
                measurements["observation_cells"] += 1
                if len(batch) >= _BATCH_ROWS:
                    _append_batch(connection, batch)
        if batch:
            _append_batch(connection, batch)


def _append_batch(
    connection: duckdb.DuckDBPyConnection,
    batch: list[tuple[Any, ...]],
) -> None:
    """Append one typed frame instead of binding every row through executemany."""
    frame = pd.DataFrame.from_records(
        batch,
        columns=_BRONZE_COLUMN_NAMES,
        coerce_float=False,
    )
    connection.append("bronze", frame)
    batch.clear()


def _parse_header(header: list[str]) -> tuple[list[str], list[str]]:
    if not header or "\\TIME_PERIOD" not in header[0].upper():
        raise EurostatBulkDecodeError(
            "Eurostat TSV header has no series key/TIME_PERIOD column"
        )
    series_header, marker = header[0].split("\\", 1)
    if marker.strip().upper() != "TIME_PERIOD":
        raise EurostatBulkDecodeError("Eurostat TSV has an unsupported observation axis")
    dimensions = [value.strip().lower() for value in series_header.split(",")]
    if (
        not dimensions
        or any(not _DIMENSION_RE.fullmatch(value) for value in dimensions)
        or len(set(dimensions)) != len(dimensions)
        or "time" in dimensions
    ):
        raise EurostatBulkDecodeError("Eurostat TSV has invalid series dimensions")
    periods = [value.strip() for value in header[1:]]
    if not periods or any(not value for value in periods) or len(set(periods)) != len(periods):
        raise EurostatBulkDecodeError("Eurostat TSV has invalid or duplicate periods")
    return dimensions, periods


def _parse_cell(
    raw: str, line_number: int, period: str
) -> tuple[str | None, float | None, str | None, bool, str | None]:
    stripped = raw.strip()
    if not stripped:
        return "", None, None, True, "empty"
    parts = stripped.split(maxsplit=1)
    value_text = parts[0]
    status = parts[1].strip() if len(parts) == 2 and parts[1].strip() else None
    if value_text == ":":
        return ":", None, status, True, "colon"
    try:
        numeric = Decimal(value_text)
    except InvalidOperation:
        raise EurostatBulkDecodeError(
            f"Eurostat TSV row {line_number} period {period!r} has a non-numeric value"
        ) from None
    if not numeric.is_finite():
        raise EurostatBulkDecodeError(
            f"Eurostat TSV row {line_number} period {period!r} has a non-finite value"
        )
    numeric_float = float(numeric)
    if not math.isfinite(numeric_float):
        raise EurostatBulkDecodeError(
            f"Eurostat TSV row {line_number} period {period!r} exceeds numeric range"
        )
    return value_text, numeric_float, status, False, None


def _validated_landing_identity(
    *,
    source: Path,
    receipt: Path,
    output: Path,
    receipt_uri: str,
    raw_uri: str,
) -> dict[str, Any]:
    if not source.is_file() or not receipt.is_file():
        raise EurostatBulkDecodeError("Eurostat Landing file or receipt is not a regular file")
    if output.exists():
        raise EurostatBulkDecodeError("Eurostat Bronze output must be a fresh path")
    for label, uri in (("receipt", receipt_uri), ("raw", raw_uri)):
        if not isinstance(uri, str) or not uri.startswith("r2://") or len(uri) > 1024:
            raise EurostatBulkDecodeError(f"Eurostat Landing {label} URI is invalid")
    receipt_bytes = receipt.read_bytes()
    try:
        accepted = json.loads(receipt_bytes.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise EurostatBulkDecodeError(f"Eurostat Landing receipt is not valid JSON: {exc}") from exc
    if (
        not isinstance(accepted, dict)
        or accepted.get("schema_version") != 2
        or accepted.get("kind") != "eurostat_landing"
        or accepted.get("area") != "data"
        or accepted.get("source_id") != "eurostat"
    ):
        raise EurostatBulkDecodeError("receipt is not a Eurostat Landing receipt")
    dataset_id = accepted.get("dataset_code")
    if not _bounded_text(dataset_id, 2048):
        raise EurostatBulkDecodeError("Eurostat dataset identity is invalid")
    source_version = accepted.get("eurostat_version") or None
    if source_version is not None and not _bounded_text(source_version, 1024):
        raise EurostatBulkDecodeError("Eurostat source version is invalid")
    raw_sha256 = accepted.get("sha256")
    raw_size = accepted.get("size_bytes")
    if not isinstance(raw_sha256, str) or not _SHA256_RE.fullmatch(raw_sha256):
        raise EurostatBulkDecodeError("Eurostat distribution SHA-256 is invalid")
    if type(raw_size) is not int or raw_size <= 0 or source.stat().st_size != raw_size:
        raise EurostatBulkDecodeError("Eurostat distribution size does not match its receipt")
    obj = accepted.get("object")
    if not isinstance(obj, dict) or not raw_uri.endswith("/" + str(obj.get("key"))):
        raise EurostatBulkDecodeError("Eurostat receipt object does not match the raw file")
    retrieved_at = accepted.get("ingested_at")
    try:
        parsed = datetime.fromisoformat(str(retrieved_at).replace("Z", "+00:00"))
    except ValueError:
        raise EurostatBulkDecodeError("Eurostat retrieval timestamp is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise EurostatBulkDecodeError("Eurostat retrieval timestamp must be UTC")
    return {
        "dataset_id": dataset_id,
        "distribution_id": dataset_id,
        "partition_id": None,
        "partition_selection_json": None,
        "empty_partition": False,
        "source_version": source_version,
        "retrieved_at_utc": retrieved_at,
        "raw_sha256": raw_sha256,
        "raw_file_id": raw_uri,
        "receipt_sha256": sha256(receipt_bytes).hexdigest(),
        "receipt_file_id": receipt_uri,
        "receipt_size_bytes": len(receipt_bytes),
    }


def _bounded_text(value: Any, limit: int) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= limit
        and not any(ord(character) < 32 for character in value)
    )


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
