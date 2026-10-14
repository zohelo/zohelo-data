#!/usr/bin/env python3
"""GUS TERYT bronze.load: Landing full files → bronze.teryt_records.

The newest Landing version of every catalogue variant (TERC_Urzedowy … WMRODZ) becomes
one Parquet file ``02_bronze/gus_teryt/records/<entity>/<raw_sha256>.parquet``: the CSV
member of the ZIP read by DuckDB with every column as text, mapped onto the union of
the catalogue columns (absent columns stay null), plus provenance. Partitioned by entity;
the table's file list is the checkpoint. Rejections: bronze.teryt_rejected_files.
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any
import zipfile

import duckdb

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import archive_key, catalog, raw_chunks, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.gus_teryt.extract import landed

TABLE_ID = ("bronze", "teryt_records")
REJECTED_ID = ("bronze", "teryt_rejected_files")
OUTPUT_PREFIX = "02_bronze/gus_teryt/records/"
# Union of the CSV columns of all eight catalogue variants (as published, October 2026).
SOURCE_COLUMNS = [
    "WOJ",
    "POW",
    "GMI",
    "RODZ",
    "RODZ_GMI",
    "RM",
    "MZ",
    "NMST",
    "NMSK",
    "SYMBM",
    "SYMSTAT",
    "NAZWA",
    "NAZWA_DOD",
    "NAZWA_RM",
    "SYM",
    "SYMPOD",
    "SYM_UL",
    "CECHA",
    "NAZWA_1",
    "NAZWA_2",
    "STAN_NA",
]
COLUMNS = (
    [("entity", "VARCHAR"), ("catalogue", "VARCHAR")]
    + [(c.lower(), "VARCHAR") for c in SOURCE_COLUMNS]
    + [
        ("raw_sha256", "VARCHAR"),
        ("source_version", "VARCHAR"),
        ("ingested_at", "TIMESTAMPTZ"),
        ("source_row_number", "BIGINT"),
    ]
)


def decode(body: bytes, receipt: dict[str, Any], output: Path) -> int:
    """CSV member → Parquet with the fixed Bronze columns; returns the row count.

    TERYT CSV fields are not quoted (street names contain bare '"'), so quoting is off;
    a field never contains ';'. Strict mode still rejects rows with a wrong field count."""
    archive = zipfile.ZipFile(io.BytesIO(body))
    members = [i for i in archive.infolist() if i.filename.lower().endswith(".csv")]
    if len(members) != 1:
        raise ValueError(f"expected one CSV member, found {[i.filename for i in archive.infolist()]}")
    csv_path = output.with_suffix(".csv")
    csv_path.write_bytes(archive.read(members[0]))
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE src AS SELECT * FROM read_csv(?, delim=';', header=true, all_varchar=true, "
            "quote='', escape='', nullstr='\x01', strict_mode=true, encoding='utf-8')",
            [str(csv_path)],
        )
        found = [r[0] for r in con.execute("DESCRIBE src").fetchall()]
        unknown = [c for c in found if c.upper() not in SOURCE_COLUMNS]
        if unknown or "STAN_NA" not in [c.upper() for c in found]:
            raise ValueError(f"unexpected TERYT columns: {found}")
        present = {c.upper(): c for c in found}
        ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        select = ", ".join(
            [
                "? AS entity",
                "? AS catalogue",
                *[
                    f'src."{present[c]}" AS {c.lower()}' if c in present else f"NULL::VARCHAR AS {c.lower()}"
                    for c in SOURCE_COLUMNS
                ],
                "? AS raw_sha256",
                "? AS source_version",
                "?::TIMESTAMPTZ AS ingested_at",
                "(rowid + 2)::BIGINT AS source_row_number",
            ]
        )
        target = str(output).replace("'", "''")  # a temporary path, quoted for SQL
        con.execute(
            f"COPY (SELECT {select} FROM src ORDER BY rowid) TO '{target}' (FORMAT parquet, COMPRESSION zstd)",
            [receipt["entity"], receipt["entity"].split("_")[0], receipt["sha256"], receipt["version_token"], ingested],
        )
        return con.execute("SELECT count(*) FROM src").fetchone()[0]
    finally:
        con.close()
        csv_path.unlink(missing_ok=True)


def read_raw(client, bucket: str, receipt: dict[str, Any]) -> bytes:
    key = receipt["object"]["key"]
    try:
        client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError:
        key = archive_key("gus_teryt", receipt["sha256"], key)
    body = b"".join(raw_chunks(client, bucket, key))
    if sha256(body).hexdigest() != receipt["sha256"] or len(body) != receipt["size_bytes"]:
        raise RuntimeError(f"TERYT raw bytes do not match their receipt: {key}")
    return body


def load(budget_minutes: float) -> int:
    deadline = time.monotonic() + budget_minutes * 60
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    bf.retry(lambda: bf.ensure_table(cat, TABLE_ID, COLUMNS, "entity"), "load_table")
    table = cat.load_table(TABLE_ID)
    done = bf.loaded_hashes(table) | bf.rejected_hashes(cat, REJECTED_ID)
    todo = [rows[-1] for rows in landed(client, landing_bucket).values() if rows[-1]["sha256"] not in done]
    totals = {"candidates": len(todo), "appended_files": 0, "rows_added": 0, "rejected": 0}
    uris, rejected = [], []
    with tempfile.TemporaryDirectory(prefix="zohelo-teryt-bronze-") as temporary:
        for receipt in sorted(todo, key=lambda r: r["entity"]):
            if time.monotonic() > deadline:
                break
            body = read_raw(client, landing_bucket, receipt)
            path = Path(temporary) / f"{receipt['sha256']}.parquet"
            try:
                rows = decode(body, receipt, path)
            except (ValueError, duckdb.Error, zipfile.BadZipFile) as exc:
                rejected.append(
                    {
                        "entity": receipt["entity"],
                        "raw_sha256": receipt["sha256"],
                        "landing_key": receipt["object"]["key"],
                        "error": f"{type(exc).__name__}: {exc}"[:1000],
                        "code_sha": os.environ.get("GITHUB_SHA", ""),
                    }
                )
                continue
            key = f"{OUTPUT_PREFIX}{receipt['entity']}/{receipt['sha256']}.parquet"
            uris.append(bf.upload(client, lake, key, path, receipt["sha256"]))
            totals["rows_added"] += rows
    if uris:
        bf.commit(cat, TABLE_ID, uris, ["entity", "catalogue", "raw_sha256"], "gus-teryt-bronze-load")
    totals["appended_files"] = len(uris)
    bf.record_rejections(cat, REJECTED_ID, rejected)
    totals["rejected"] = len(rejected)
    totals["remaining"] = totals["candidates"] - totals["appended_files"] - totals["rejected"]
    result = "complete" if totals["remaining"] == 0 else "incomplete_time_budget"
    annotate(
        "notice" if result == "complete" else "warning", "gus_teryt Bronze from Landing", {"result": result, **totals}
    )
    step_summary(f"## gus_teryt · bronze load: {result}\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0 if result == "complete" else 2
