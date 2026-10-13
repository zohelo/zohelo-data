#!/usr/bin/env python3
"""GUGiK PRG bronze.load: newest Landing package → bronze.prg_boundaries.

The ZIP's shapefile layers (A00 … A06) are read with DuckDB spatial and written as one
Parquet file ``02_bronze/gugik_prg/boundaries/<entity>/<raw_sha256>.parquet``: one row per
boundary feature, the layer name, every attribute as text (the source's own values,
numbers cast to text), the geometry as WKB in the source CRS (geographic ETRS89 per the .prj, October 2026), the
CRS text from the layer's .prj, and provenance. Partitioned by entity; the file list is
the checkpoint. Rejections: bronze.prg_rejected_files.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any
import zipfile

import duckdb

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import archive_key, catalog, raw_chunks, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.gugik_prg.extract import landed

TABLE_ID = ("bronze", "prg_boundaries")
REJECTED_ID = ("bronze", "prg_rejected_files")
OUTPUT_PREFIX = "02_bronze/gugik_prg/boundaries/"
# Attributes of the PRG layers (union over A00–A06, as published in October 2026).
ATTRIBUTES = ["OGC_FID", "JPT_KOD_JE", "JPT_SJR_KO", "JPT_NAZWA_", "JPT_POWIER", "JPT_ORGAN_", "JPT_KOD__1",
              "JPT_NAZWA1", "JPT_ORGAN1", "JPT_WAZNA_", "IIP_PRZEST", "IIP_IDENTY", "JPT_KJ_IIP", "JPT_KJ_I_1",
              "JPT_KJ_I_2", "JPT_KJ_I_3", "JPT_SPS_KO", "JPT_POWI_1", "JPT_JOR_ID", "WERSJA_OD", "WERSJA_DO",
              "WAZNY_OD", "WAZNY_DO", "IIP_WERSJA", "JPT_OPIS", "JPT_ID", "REGON", "Shape_Leng", "Shape_Le_1",
              "Shape_Area"]
COLUMNS = ([("entity", "VARCHAR"), ("layer", "VARCHAR")] + [(a.lower(), "VARCHAR") for a in ATTRIBUTES]
           + [("geometry_wkb", "BLOB"), ("geometry_type", "VARCHAR"), ("crs_wkt", "VARCHAR"),
              ("raw_sha256", "VARCHAR"), ("source_version", "VARCHAR"), ("ingested_at", "TIMESTAMPTZ"),
              ("source_row_number", "BIGINT")])


def decode(zip_path: Path, receipt: dict[str, Any], output: Path) -> dict[str, int]:
    """All layers → one Parquet file; returns rows per layer."""
    folder = zip_path.parent / "unzipped"
    zipfile.ZipFile(zip_path).extractall(folder)
    layers = sorted(folder.rglob("*.shp"))
    if not layers:
        raise ValueError("PRG package without shapefiles")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    counts, selects = {}, []
    for shp in layers:
        found = [r[0] for r in con.execute("DESCRIBE SELECT * FROM ST_Read(?)", [str(shp)]).fetchall()]
        unknown = [c for c in found if c != "geom" and c not in ATTRIBUTES]
        if unknown:
            raise ValueError(f"{shp.stem}: unexpected attributes {unknown}")
        prj = shp.with_suffix(".prj")
        crs = prj.read_text(errors="replace").strip() if prj.exists() else None
        path = str(shp).replace("'", "''")
        crs_sql = "NULL" if crs is None else "'" + crs.replace("'", "''") + "'"
        layer = shp.stem.replace("'", "''")
        columns = ", ".join(f'CAST("{a}" AS VARCHAR) AS {a.lower()}' if a in found else f"NULL::VARCHAR AS {a.lower()}"
                            for a in ATTRIBUTES)
        selects.append(f"SELECT '{layer}' AS layer, {columns}, ST_AsWKB(geom) AS geometry_wkb, "
                       f"ST_GeometryType(geom)::VARCHAR AS geometry_type, {crs_sql} AS crs_wkt, "
                       f"(row_number() OVER ())::BIGINT AS source_row_number FROM ST_Read('{path}')")
        counts[shp.stem] = con.execute("SELECT count(*) FROM ST_Read(?)", [str(shp)]).fetchone()[0]
    union = " UNION ALL ".join(selects)
    target = str(output).replace("'", "''")
    con.execute(f"COPY (SELECT ? AS entity, u.* EXCLUDE (source_row_number), ? AS raw_sha256, ? AS source_version, "
                f"?::TIMESTAMPTZ AS ingested_at, u.source_row_number FROM ({union}) u) "
                f"TO '{target}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 20000)",
                [receipt["entity"], receipt["sha256"], receipt["version_token"], ingested])
    written = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{target}')").fetchall()]
    if written != [c for c, _ in COLUMNS]:
        raise ValueError(f"PRG Parquet columns differ from the table: {written}")
    con.close()
    return counts


def load(budget_minutes: float) -> int:
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    bf.retry(lambda: bf.ensure_table(cat, TABLE_ID, COLUMNS, "entity"), "load_table")
    done = bf.loaded_hashes(cat.load_table(TABLE_ID)) | bf.rejected_hashes(cat, REJECTED_ID)
    held = landed(client, landing_bucket)
    totals: dict[str, Any] = {"candidates": 0, "appended_files": 0, "rejected": 0}
    if held and held[-1]["sha256"] not in done:
        receipt = held[-1]
        totals["candidates"] = 1
        with tempfile.TemporaryDirectory(prefix="zohelo-prg-bronze-") as temporary:
            zip_path, output = Path(temporary) / "prg.zip", Path(temporary) / "out.parquet"
            key = receipt["object"]["key"]
            try:
                client.head_object(Bucket=landing_bucket, Key=key)
            except client.exceptions.ClientError:
                key = archive_key("gugik_prg", receipt["sha256"], key)
            digest = sha256()
            with zip_path.open("wb") as handle:
                for block in raw_chunks(client, landing_bucket, key):
                    handle.write(block)
                    digest.update(block)
            if digest.hexdigest() != receipt["sha256"]:
                raise RuntimeError(f"PRG raw bytes do not match their receipt: {key}")
            try:
                totals["rows_by_layer"] = decode(zip_path, receipt, output)
            except (ValueError, duckdb.Error, zipfile.BadZipFile) as exc:
                bf.record_rejections(cat, REJECTED_ID, [{
                    "entity": receipt["entity"], "raw_sha256": receipt["sha256"], "landing_key": receipt["object"]["key"],
                    "error": f"{type(exc).__name__}: {exc}"[:1000], "code_sha": os.environ.get("GITHUB_SHA", "")}])
                totals["rejected"] = 1
            else:
                uri = bf.upload(client, lake, f"{OUTPUT_PREFIX}{receipt['entity']}/{receipt['sha256']}.parquet",
                                output, receipt["sha256"])
                bf.commit(cat, TABLE_ID, [uri], ["entity", "layer", "raw_sha256"], "gugik-prg-bronze-load")
                totals["appended_files"] = 1
                totals["parquet_mb"] = round(output.stat().st_size / 1e6, 1)
    result = "complete"
    annotate("notice", "gugik_prg Bronze from Landing", {"result": result, **totals})
    step_summary(f"## gugik_prg · bronze load: {result}\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0
