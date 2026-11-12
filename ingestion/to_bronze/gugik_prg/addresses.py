#!/usr/bin/env python3
"""GUGiK PRG bronze.load, address package (PL-ENV-007, #2870): newest Landing copy → bronze.prg_address_points.

The national package (POLSKA.zip, ~564 MB) holds four shapefile layers: address points, streets, localities and
squares. Only the address point layer is loaded (the streets and localities are the TERYT registers of
gus_teryt). Its members are Deflate64, which Python's zipfile cannot read, so they are extracted with the 7-Zip
tool; DuckDB spatial (GDAL) reads the shapefile in a stream and writes one Parquet file
``02_bronze/gugik_prg/address_points/punkty_adresowe/<raw_sha256>.parquet``: one row per address point, every
attribute as text as published, the point's coordinates as text (x, y in the published CRS, the .prj text in
crs_wkt; no geometry blob) and provenance. The row count must equal the DBF header's record count. An unknown or
missing attribute, a missing member or a non-point geometry rejects the file into bronze.prg_rejected_files.
``landing_root``, ``namespace`` and ``lake_root`` exist for Dev trials (zz_dev_ scratch locations).
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
from typing import Any
import zipfile

import duckdb

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import archive_key, catalog, raw_chunks, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file

SOURCE = "gugik_prg"
ENTITY = "punkty_adresowe"
TABLE = "prg_address_points"
REJECTED = "prg_rejected_files"
FOLDER = "address_points"
LAYER = "PRG_PunktyAdresowe_POLSKA"
MEMBERS = (".shp", ".shx", ".dbf", ".prj")
ENCODING = "UTF-8"  # the DBF has no .cpg; its text is UTF-8 (Dev trial #2870)
MEMORY = "6GB"
# Attributes of the address point layer as published (Dev profile #2870, November 2026).
ATTRIBUTES = [
    "ID_IIP",  # PRG identifier of the point (namespace, UUID, version)
    "NUMER_PORZ",  # house number as published
    "KOD_POCZT",
    "DATA_NAD",  # date the number was assigned (often empty)
    "NAZWA_ULC",
    "ID_ULIC",  # TERYT ULIC street id; empty for addresses without a street
    "NAZWA_GMI",
    "TERYT_GMI",  # the gmina's 7-digit TERC code
    "NAZWA_MSC",
    "ID_SIMC",  # TERYT SIMC locality id
]
FID = "OGC_FID"  # the feature's record number in the shapefile, as GDAL reads it: source_row_number
COLUMNS = (
    [("entity", "VARCHAR")]
    + [(a.lower(), "VARCHAR") for a in ATTRIBUTES]
    + [
        ("x", "VARCHAR"),
        ("y", "VARCHAR"),
        ("crs_wkt", "VARCHAR"),
        ("raw_sha256", "VARCHAR"),
        ("source_version", "VARCHAR"),
        ("ingested_at", "TIMESTAMPTZ"),
        ("source_row_number", "BIGINT"),
    ]
)


class AddressDecodeError(ValueError):
    """The package does not satisfy the Bronze contract (recorded as a rejection, never guessed)."""


def dbf_records(path: Path) -> int:
    """Record count from the DBF header (bytes 4-7, little endian)."""
    with path.open("rb") as handle:
        return struct.unpack("<I", handle.read(8)[4:8])[0]


def extract(zip_path: Path, folder: Path) -> dict[str, Path]:
    """The address layer's members, extracted with 7-Zip (Deflate64); the ZIP is deleted afterwards (disk)."""
    try:
        with zipfile.ZipFile(zip_path) as archive:  # the central directory is readable whatever the method
            names = [i.filename for i in archive.infolist()]
    except zipfile.BadZipFile as exc:
        raise AddressDecodeError(f"not a readable ZIP: {exc}") from exc
    wanted = {suffix: next((n for n in names if n == LAYER + suffix), None) for suffix in MEMBERS}
    missing = [LAYER + s for s, n in wanted.items() if n is None]
    if missing:
        raise AddressDecodeError(f"address layer members missing: {missing}; members {names[:20]}")
    tool = shutil.which("7z") or shutil.which("7za")
    if not tool:
        raise RuntimeError("7z is not installed")
    done = subprocess.run(
        [tool, "x", "-bd", "-y", f"-o{folder}", str(zip_path), *wanted.values()],
        capture_output=True,
        text=True,
        errors="replace",
        timeout=3600,
    )
    if done.returncode != 0:
        raise AddressDecodeError(f"7z could not extract the address layer: {(done.stdout + done.stderr)[-300:]}")
    zip_path.unlink()
    return {suffix: folder / name for suffix, name in wanted.items()}


def decode(zip_path: Path, receipt: dict[str, Any], output: Path) -> dict[str, Any]:
    """The address layer → one Parquet file; returns counts."""
    files = extract(zip_path, output.parent / "unzipped")
    shp, crs = files[".shp"], files[".prj"].read_text(errors="replace").strip()
    expected_rows = dbf_records(files[".dbf"])
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{MEMORY}'")
    con.execute(f"SET temp_directory = '{output.parent}'")
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        path = str(shp).replace("'", "''")  # a temporary path, quoted for SQL (a view takes no parameters)
        con.execute(f"CREATE VIEW src AS SELECT * FROM ST_Read('{path}', open_options = ['ENCODING={ENCODING}'])")
        found = [r[0] for r in con.execute("DESCRIBE src").fetchall() if r[0] not in ("geom", FID)]
        if sorted(found) != sorted(ATTRIBUTES):
            raise AddressDecodeError(
                f"attributes differ: unexpected {sorted(set(found) - set(ATTRIBUTES))}, "
                f"missing {sorted(set(ATTRIBUTES) - set(found))}"
            )
        columns = ", ".join(f'CAST("{a}" AS VARCHAR) AS {a.lower()}' for a in ATTRIBUTES)
        ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        target = str(output).replace("'", "''")
        con.execute(
            f"COPY (SELECT ? AS entity, {columns}, CAST(ST_X(geom) AS VARCHAR) AS x, CAST(ST_Y(geom) AS VARCHAR) AS y, "
            "? AS crs_wkt, ? AS raw_sha256, ? AS source_version, ?::TIMESTAMPTZ AS ingested_at, "
            f"CAST({FID} AS BIGINT) AS source_row_number FROM src) "
            f"TO '{target}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)",
            [receipt["entity"], crs, receipt["sha256"], receipt["version_token"], ingested],
        )
        rows, not_points = con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE x IS NULL OR y IS NULL) FROM read_parquet('{target}')"
        ).fetchone()
        written = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{target}')").fetchall()]
    except (duckdb.InvalidInputException, duckdb.ConversionException, duckdb.BinderException) as exc:
        # a verdict on the file (unreadable layer, a missing column, a non-point geometry); storage, memory and
        # network errors propagate and the next run tries again
        raise AddressDecodeError(f"DuckDB could not read the address layer: {str(exc)[:500]}") from exc
    finally:
        con.close()
        shutil.rmtree(output.parent / "unzipped", ignore_errors=True)
    if written != [c for c, _ in COLUMNS]:
        raise RuntimeError(f"PRG address Parquet columns differ from the table: {written}")
    if rows != expected_rows:
        raise AddressDecodeError(f"{rows} rows read, the DBF header says {expected_rows}")
    if not_points:
        raise AddressDecodeError(f"{not_points} features without point coordinates")
    return {"rows": rows, "dbf_records": expected_rows}


def load(
    budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze", lake_root: str = "02_bronze"
) -> int:
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    table_id, rejected_id = (namespace, TABLE), (namespace, REJECTED)
    table = bf.retry(lambda: bf.ensure_table(cat, table_id, COLUMNS, "entity"), "load_table")
    done = bf.loaded_hashes(table) | bf.rejected_hashes(cat, rejected_id)
    held = bulk_file.landed(client, landing_bucket, bulk_file.prefix(SOURCE, ENTITY, landing_root))
    totals: dict[str, Any] = {"landed": bool(held), "candidates": 0, "appended_files": 0, "rejected": 0}
    receipt = held[-1] if held else None
    if receipt and receipt["sha256"] not in done:
        totals["candidates"] = 1
        with tempfile.TemporaryDirectory(prefix="zohelo-prg-addresses-") as temporary:
            zip_path, output = Path(temporary) / "addresses.zip", Path(temporary) / "out.parquet"
            key = receipt["object"]["key"]
            try:
                client.head_object(Bucket=landing_bucket, Key=key)
            except client.exceptions.ClientError:
                key = archive_key(SOURCE, receipt["sha256"], key)
            digest = sha256()
            with zip_path.open("wb") as handle:
                for block in raw_chunks(client, landing_bucket, key):
                    handle.write(block)
                    digest.update(block)
            if digest.hexdigest() != receipt["sha256"]:
                raise RuntimeError(f"PRG address raw bytes do not match their receipt: {key}")
            try:
                totals.update(decode(zip_path, receipt, output))
            except AddressDecodeError as exc:
                bf.record_rejections(
                    cat,
                    rejected_id,
                    [
                        {
                            "entity": receipt["entity"],
                            "raw_sha256": receipt["sha256"],
                            "landing_key": receipt["object"]["key"],
                            "error": f"{type(exc).__name__}: {exc}"[:1000],
                            "code_sha": os.environ.get("GITHUB_SHA", ""),
                        }
                    ],
                )
                totals["rejected"] = 1
            else:
                uri = bf.upload(
                    client,
                    lake,
                    f"{lake_root}/{SOURCE}/{FOLDER}/{ENTITY}/{receipt['sha256']}.parquet",
                    output,
                    receipt["sha256"],
                )
                bf.commit(cat, table_id, [uri], ["entity", "raw_sha256"], "gugik-prg-addresses-bronze-load")
                totals["appended_files"] = 1
                totals["parquet_mb"] = round(output.stat().st_size / 1e6, 1)
    annotate("notice" if not totals["rejected"] else "warning", "gugik_prg address points Bronze from Landing", totals)
    step_summary(f"## gugik_prg · address points bronze load\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0
