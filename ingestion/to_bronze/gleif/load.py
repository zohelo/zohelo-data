#!/usr/bin/env python3
"""GLEIF bronze.load: Landing golden-copy CSV files → one Bronze table per file kind.

The newest Landing version of each declared file (lei2, rr, repex; one ZIP with one CSV each)
becomes one Parquet file ``02_bronze/gleif/<table>/<raw_sha256>.parquet``: every CSV column
as text, named after the CSV header (lower case, dots → underscores), plus provenance. The
first loaded file fixes a table's columns; a file with another header is rejected (a new
GLEIF CDF version is a deliberate schema change, not a silent one). Superseded versions are
removed after the load (``bronze_latest_only``). Rejections: bronze.gleif_rejected_files.
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
from typing import Any
import zipfile

import duckdb
from pyiceberg.exceptions import NoSuchTableError

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import archive_key, catalog, raw_chunks, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file

SOURCE = "gleif"
TABLES = {  # declared entity → Bronze table
    "lei2": "gleif_lei_records",
    "rr": "gleif_relationship_records",
    "repex": "gleif_reporting_exceptions",
}
REJECTED_ID = ("bronze", "gleif_rejected_files")
PROVENANCE = [
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("source_row_number", "BIGINT"),
]
NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class GleifDecodeError(ValueError):
    """A Landing file cannot satisfy the Bronze contract (recorded, never dropped silently)."""


def column_name(header: str) -> str:
    """CSV header → Bronze column: ``Entity.LegalName.xmllang`` → ``entity_legalname_xmllang``."""
    name = re.sub(r"[^a-z0-9]+", "_", header.strip().lower()).strip("_")
    if not NAME_RE.fullmatch(name):
        raise GleifDecodeError(f"cannot name a column after the header {header!r}")
    return name


def fetch(client, bucket: str, receipt: dict[str, Any], path: Path) -> None:
    """Landing (or Archive) bytes of a receipt to ``path``, checked against its SHA-256 and size."""
    key = receipt["object"]["key"]
    try:
        client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError:
        key = archive_key(SOURCE, receipt["sha256"], key)
    digest, size = sha256(), 0
    with path.open("wb") as handle:
        for block in raw_chunks(client, bucket, key):
            digest.update(block)
            size += len(block)
            handle.write(block)
    if digest.hexdigest() != receipt["sha256"] or size != receipt["size_bytes"]:
        raise RuntimeError(f"GLEIF raw bytes do not match their receipt: {key}")


def decode(raw: Path, receipt: dict[str, Any], output: Path, expected: list[str] | None) -> tuple[int, list[str]]:
    """One ZIP → Parquet with the CSV columns as text plus provenance; returns (rows, columns)."""
    try:
        with zipfile.ZipFile(raw) as archive:
            members = [m for m in archive.infolist() if m.filename.lower().endswith(".csv")]
            if len(members) != 1:
                raise GleifDecodeError(f"expected one CSV in the ZIP, found {[m.filename for m in members]}")
            csv_path = output.parent / "data.csv"
            with archive.open(members[0]) as src, csv_path.open("wb") as dst:
                shutil.copyfileobj(src, dst, 16 << 20)
    except zipfile.BadZipFile as exc:
        raise GleifDecodeError(f"not a readable ZIP: {exc}") from exc
    raw.unlink()  # disk: the CSV of lei2 is ~5 GB
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit = '10GB'")
        con.execute(f"SET temp_directory = '{output.parent}'")
        con.execute("SET preserve_insertion_order = true")
        try:
            con.execute(
                "CREATE VIEW src AS SELECT * FROM read_csv(?, delim=',', quote='\"', escape='\"', header=true, "
                "all_varchar=true, strict_mode=true, encoding='utf-8', sample_size=-1)",
                [str(csv_path)],
            )
            headers = [r[0] for r in con.execute("DESCRIBE src").fetchall()]
        except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:
            raise GleifDecodeError(f"CSV cannot be read: {str(exc)[:400]}") from exc
        columns = [column_name(h) for h in headers]
        if len(set(columns)) != len(columns):
            raise GleifDecodeError("two CSV headers give the same column name")
        if expected is not None and columns != expected:
            changed = sorted(set(columns) ^ set(expected))[:10]
            raise GleifDecodeError(f"CSV header differs from the Bronze table (CDF change?): {changed}")
        ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        select = ", ".join(
            [f'"{h.replace(chr(34), chr(34) * 2)}" AS {c}' for h, c in zip(headers, columns, strict=True)]
            + [
                "? AS raw_sha256",
                "? AS source_version",
                "?::TIMESTAMPTZ AS ingested_at",
                "(row_number() OVER ())::BIGINT AS source_row_number",
            ]
        )
        target = str(output).replace("'", "''")
        try:
            con.execute(
                f"COPY (SELECT {select} FROM src) TO '{target}' "
                "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 500000)",
                [receipt["sha256"], receipt["version_token"], ingested],
            )
        except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:
            raise GleifDecodeError(f"CSV cannot be read: {str(exc)[:400]}") from exc
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
        if not rows:
            raise GleifDecodeError("the CSV has no rows")
        return int(rows), columns
    finally:
        con.close()
        csv_path.unlink(missing_ok=True)


def load(budget_minutes: float) -> int:
    deadline = time.monotonic() + budget_minutes * 60
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    refused = bf.rejected_hashes(cat, REJECTED_ID)
    totals: dict[str, Any] = {"loaded": [], "current": [], "rejected": [], "not_started": []}
    rejected = []
    for spec in bulk_file.declaration(SOURCE):
        entity, table_id = spec["entity"], ("bronze", TABLES[spec["entity"]])
        receipt = bulk_file.newest(client, landing_bucket, SOURCE, entity)
        try:
            table = bf.retry(lambda t=table_id: cat.load_table(t), "load_table")
            expected = [f.name for f in table.schema().fields][: -len(PROVENANCE)]
            done = bf.loaded_hashes(table)
        except NoSuchTableError:
            table, expected, done = None, None, set()
        if receipt["sha256"] in done:
            totals["current"].append(entity)
            continue
        if receipt["sha256"] in refused:
            totals["rejected"].append(entity)
            continue
        if time.monotonic() > deadline:
            totals["not_started"].append(entity)
            continue
        started = time.monotonic()
        with tempfile.TemporaryDirectory(prefix="zohelo-gleif-bronze-") as temporary:
            raw, output = Path(temporary) / "raw.zip", Path(temporary) / f"{receipt['sha256']}.parquet"
            fetch(client, landing_bucket, receipt, raw)
            try:
                rows, columns = decode(raw, receipt, output, expected)
            except GleifDecodeError as exc:
                rejected.append(
                    {
                        "entity": entity,
                        "raw_sha256": receipt["sha256"],
                        "landing_key": receipt["object"]["key"],
                        "error": f"decoder: {exc}"[:1000],
                        "code_sha": os.environ.get("GITHUB_SHA", ""),
                    }
                )
                totals["rejected"].append(entity)
                continue
            if table is None:
                schema = [(c, "VARCHAR") for c in columns] + PROVENANCE
                bf.retry(lambda t=table_id, c=schema: bf.ensure_table(cat, t, c, "raw_sha256"), "create_table")
            key = f"02_bronze/{SOURCE}/{TABLES[entity]}/{receipt['sha256']}.parquet"
            uri = bf.upload(client, lake, key, output, receipt["sha256"])
            stats = [columns[0], "raw_sha256"]
            bf.commit(cat, table_id, [uri], stats, f"gleif-bronze-load-{entity}")
        totals["loaded"].append(
            {
                "entity": entity,
                "rows": rows,
                "version": receipt["version_token"],
                "minutes": round((time.monotonic() - started) / 60, 1),
            }
        )
    bf.record_rejections(cat, REJECTED_ID, rejected)
    result = "complete" if not totals["not_started"] else "incomplete_time_budget"
    annotate(
        "notice" if result == "complete" and not rejected else "warning",
        "gleif Bronze from Landing",
        {"result": result, **totals},
    )
    step_summary(f"## gleif · bronze load: {result}\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0 if result == "complete" else 2
