"""Decode one Landing Eurostat ``.tsv.gz`` into source-shaped Bronze Parquet.

Bronze keeps the source as published: one row per series × period with the native
series key, period, value lexeme and status flag. Nothing is interpreted — numbers stay
text (``value_text``), ``:`` stays ``:``, an empty cell stays empty — so every file the
source publishes can be loaded; casting is a Silver rule. Provenance per row is the
Landing raw SHA-256 (plus dataset and version); the receipt in Landing holds the rest.

The TSV is read and unpivoted by DuckDB (no per-cell Python), written as ZSTD Parquet in
source order (series by series), which keeps repeated values cheap to store.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import gzip
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any

import duckdb

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DIMENSION_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
_NULL_MARKER = "\x1f__zohelo_never_null__"

BRONZE_COLUMNS = (
    ("dataset_id", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("raw_sha256", "VARCHAR"),
    ("series_dimensions", "VARCHAR"),
    ("series_key", "VARCHAR"),
    ("freq_code", "VARCHAR"),
    ("unit_code", "VARCHAR"),
    ("geo_code", "VARCHAR"),
    ("period_key", "VARCHAR"),
    ("value_text", "VARCHAR"),
    ("status_code", "VARCHAR"),
    ("source_row_number", "BIGINT"),
)
COLUMN_NAMES = tuple(name for name, _ in BRONZE_COLUMNS)


class EurostatBulkDecodeError(ValueError):
    """A Landing file cannot satisfy the Bronze contract (recorded, never dropped silently)."""


def decode_landing_distribution(
    source: Path, receipt: Path, output: Path, *, receipt_uri: str, raw_uri: str, memory_limit: str = "10GB"
) -> dict[str, Any]:
    """Decode one Landing data file; identity comes from its receipt (docs/landing.md)."""
    source, receipt, output = Path(source), Path(receipt), Path(output)
    identity = _validated_landing_identity(
        source=source, receipt=receipt, output=output, receipt_uri=receipt_uri, raw_uri=raw_uri
    )
    if _file_sha256(source) != identity["raw_sha256"]:
        raise EurostatBulkDecodeError("Eurostat distribution SHA-256 does not match its receipt")
    try:
        with gzip.open(source, "rt", encoding="utf-8-sig", errors="strict", newline="") as stream:
            header_line = stream.readline().rstrip("\r\n")
            first_series = stream.readline()
    except (OSError, EOFError, UnicodeError) as exc:
        raise EurostatBulkDecodeError(f"invalid Eurostat full distribution: {exc}") from exc
    if not header_line:
        raise EurostatBulkDecodeError("Eurostat TSV is empty")
    header = header_line.split("\t")
    dimensions, periods = _parse_header(header)
    if not first_series.strip():
        raise EurostatBulkDecodeError("Eurostat TSV has no series")

    output.parent.mkdir(parents=True, exist_ok=True)
    database = output.parent / f".{output.name}.duckdb"
    if database.exists():
        raise EurostatBulkDecodeError("Eurostat decoder working database already exists")
    completed = False
    try:
        with duckdb.connect(str(database)) as con:
            con.execute(f"SET memory_limit = '{memory_limit}'")
            con.execute("SET threads = 2")
            con.execute("SET preserve_insertion_order = true")
            columns = {f"c{i}": "VARCHAR" for i in range(len(header))}
            # One pass over the file: the split series key per row; the table keeps file
            # order, so rowid + 2 is the source line. No window, join or sort anywhere, so
            # memory stays bounded by DuckDB's streaming operators.
            con.execute(
                "CREATE TABLE src AS SELECT "
                "list_transform(string_split(c0, ','), lambda x: trim(x)) AS parts, * EXCLUDE (c0) "
                "FROM read_csv(?, auto_detect = false, delim = '\t', quote = '', escape = '', "
                "comment = '', header = false, skip = 1, columns = ?, nullstr = ?, strict_mode = true, "
                "null_padding = false, parallel = false, compression = 'gzip', max_line_size = 8000000)",
                [str(source), columns, _NULL_MARKER],
            )
            source_rows, nulls, bad, distinct = con.execute(
                "SELECT count(*), count(*) FILTER (WHERE "
                + " OR ".join(f"c{i} IS NULL" for i in range(1, len(header)))
                + "), min(rowid + 2) FILTER (WHERE len(parts) <> ? OR list_contains(parts, '')), "
                "count(DISTINCT hash(parts)) FROM src",
                [len(dimensions)],
            ).fetchone()
            if source_rows == 0:
                raise EurostatBulkDecodeError("Eurostat TSV has no series")
            if nulls:
                raise EurostatBulkDecodeError("Eurostat TSV has rows with missing fields")
            if bad is not None:
                raise EurostatBulkDecodeError(f"Eurostat TSV row {bad} has an invalid series key")
            # hash(): a 64-bit fingerprint keeps the distinct check within memory on the
            # largest files (AVIA_*: DISTINCT over the key lists ran out of 9.3 GB).
            if distinct != source_rows:
                raise EurostatBulkDecodeError("Eurostat TSV repeats a complete dimensional observation key")

            def code(name: str) -> str:
                return f"parts[{dimensions.index(name) + 1}]" if name in dimensions else "NULL::VARCHAR"

            value_columns = ", ".join(f"c{i}" for i in range(1, len(header)))
            # The unpivot need not keep file order (source_row_number records it); without
            # it DuckDB streams the largest files instead of buffering them.
            con.execute("SET preserve_insertion_order = false")
            escaped = str(output).replace("'", "''")
            con.execute(
                f"""
                COPY (
                  SELECT ?::VARCHAR AS dataset_id, ?::VARCHAR AS source_version,
                         ?::TIMESTAMPTZ AS ingested_at, ?::VARCHAR AS raw_sha256,
                         ?::VARCHAR AS series_dimensions,
                         array_to_string(parts, ',') AS series_key,
                         {code("freq")} AS freq_code, {code("unit")} AS unit_code,
                         {code("geo")} AS geo_code,
                         (?::VARCHAR[])[substr(col, 2)::INTEGER] AS period_key,
                         regexp_extract(trim(cell), '^(\\S*)', 1) AS value_text,
                         nullif(trim(regexp_extract(trim(cell), '^\\S*\\s+(.*)$', 1)), '') AS status_code,
                         line AS source_row_number
                  FROM (UNPIVOT (SELECT rowid + 2 AS line, * FROM src)
                        ON {value_columns} INTO NAME col VALUE cell)
                ) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)
            """,
                [
                    identity["dataset_id"],
                    identity["source_version"],
                    identity["ingested_at"],
                    identity["raw_sha256"],
                    ",".join(dimensions),
                    periods,
                ],
            )
            cells = con.execute(f"SELECT count(*) FROM read_parquet('{escaped}')").fetchone()[0]
        if cells != source_rows * len(periods):
            raise EurostatBulkDecodeError("Eurostat Bronze row count differs from series × periods")
        completed = True
    except (
        duckdb.OutOfMemoryException,
        duckdb.IOException,
        duckdb.HTTPException,
        duckdb.InterruptException,
        duckdb.FatalException,
        duckdb.InternalException,
    ) as exc:
        raise RuntimeError(f"Eurostat decode could not run (not a verdict on the file): {str(exc)[:600]}") from exc
    except duckdb.Error as exc:
        raise EurostatBulkDecodeError(f"invalid Eurostat full distribution: {str(exc)[:600]}") from exc
    finally:
        database.unlink(missing_ok=True)
        Path(str(database) + ".wal").unlink(missing_ok=True)
        if not completed and output.exists():
            output.unlink()
    return {
        "dataset_id": identity["dataset_id"],
        "raw_sha256": identity["raw_sha256"],
        "source_rows": source_rows,
        "periods": len(periods),
        "observation_cells": cells,
        "output_bytes": output.stat().st_size,
    }


def _parse_header(header: list[str]) -> tuple[list[str], list[str]]:
    if not header or "\\TIME_PERIOD" not in header[0].upper():
        raise EurostatBulkDecodeError("Eurostat TSV header has no series key/TIME_PERIOD column")
    series_header, marker = header[0].split("\\", 1)
    if marker.strip().upper() != "TIME_PERIOD":
        raise EurostatBulkDecodeError("Eurostat TSV has an unsupported observation axis")
    dimensions = [value.strip().lower() for value in series_header.split(",")]
    if (
        not dimensions
        or any(not _DIMENSION_RE.fullmatch(v) for v in dimensions)
        or len(set(dimensions)) != len(dimensions)
        or "time" in dimensions
    ):
        raise EurostatBulkDecodeError("Eurostat TSV has invalid series dimensions")
    periods = [value.strip() for value in header[1:]]
    if not periods or any(not v for v in periods) or len(set(periods)) != len(periods):
        raise EurostatBulkDecodeError("Eurostat TSV has invalid or duplicate periods")
    return dimensions, periods


def _validated_landing_identity(
    *, source: Path, receipt: Path, output: Path, receipt_uri: str, raw_uri: str
) -> dict[str, Any]:
    if not source.is_file() or not receipt.is_file():
        raise EurostatBulkDecodeError("Eurostat Landing file or receipt is not a regular file")
    if output.exists():
        raise EurostatBulkDecodeError("Eurostat Bronze output must be a fresh path")
    for label, uri in (("receipt", receipt_uri), ("raw", raw_uri)):
        if not isinstance(uri, str) or not uri.startswith("r2://") or len(uri) > 1024:
            raise EurostatBulkDecodeError(f"Eurostat Landing {label} URI is invalid")
    try:
        accepted = json.loads(receipt.read_bytes().decode("utf-8"))
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
    raw_sha256, raw_size = accepted.get("sha256"), accepted.get("size_bytes")
    if not isinstance(raw_sha256, str) or not _SHA256_RE.fullmatch(raw_sha256):
        raise EurostatBulkDecodeError("Eurostat distribution SHA-256 is invalid")
    if type(raw_size) is not int or raw_size <= 0 or source.stat().st_size != raw_size:
        raise EurostatBulkDecodeError("Eurostat distribution size does not match its receipt")
    obj = accepted.get("object")
    if not isinstance(obj, dict) or not raw_uri.endswith("/" + str(obj.get("key"))):
        raise EurostatBulkDecodeError("Eurostat receipt object does not match the raw file")
    ingested_at = accepted.get("ingested_at")
    try:
        parsed = datetime.fromisoformat(str(ingested_at).replace("Z", "+00:00"))
    except ValueError:
        raise EurostatBulkDecodeError("Eurostat ingestion timestamp is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise EurostatBulkDecodeError("Eurostat ingestion timestamp must be UTC")
    return {
        "dataset_id": dataset_id,
        "source_version": source_version,
        "ingested_at": parsed.isoformat(),
        "raw_sha256": raw_sha256,
    }


def _bounded_text(value: Any, limit: int) -> bool:
    return isinstance(value, str) and 0 < len(value) <= limit and not any(ord(character) < 32 for character in value)


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
