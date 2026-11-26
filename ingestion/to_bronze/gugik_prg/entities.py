"""GUGiK PRG address points (register PL-ENV-007, #3106): Landing → Bronze → Silver, one package at a time.

Entity = the national address package ``punkty_adresowe`` (POLSKA.zip, SHP); its newest Landing copy is the entity
version. Bronze and Silver hold one file for it, replaced whole (entity tables, design #875), so a rerun writes
nothing and a new package replaces both files in one commit per table:

  02_bronze/gugik_prg/prg_address_points/punkty_adresowe/<raw_sha256>.parquet
      the address-point layer PRG_PunktyAdresowe_POLSKA, one row per point: every attribute as text, the point's
      coordinates as printed (x, y in the published CRS), the layer's .prj text, provenance
  03_silver/gugik_prg/prg_address_points/punkty_adresowe/<raw_sha256>.<model>.parquet
      warehouse/entity/prg_address_points.sql (<model> = digest of the SQL: a changed model recomputes Silver)

The package's members are Deflate64, which Python's zipfile cannot read: only the address layer is unpacked, with 7z
(CRC checked), to the runner's disk (~7.4 GB), and read with DuckDB spatial in bounded memory. The package's other
layers (localities, streets, squares) are not loaded. A package whose layer has another attribute set, another row
count than its .shx, or whose Silver breaks a check (a null key, a repeated PRG address id) is rejected into
bronze.prg_rejected_files with the reason; nothing is uploaded and Silver keeps the previous package (Bronze's copy of
it is superseded and removed by the latest-only prune after the load, #845). Prints one JSON result line.
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from typing import Any

import duckdb
from pyiceberg.exceptions import NoSuchTableError

from ingestion.common import bronze_files as bf
from ingestion.common import entity_tables as et
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

SOURCE = "gugik_prg"
ENTITY = "punkty_adresowe"
LAYER = "PRG_PunktyAdresowe_POLSKA"
TABLE = "prg_address_points"  # the Bronze and the Silver table
REJECTED = "prg_rejected_files"
MODEL = "prg_address_points"
MEMORY = "8GB"
STATS = ["entity", "raw_sha256"]
# Attributes of the address-point layer, in the published order (Dev probes 45810275130 and 45811745316, package of
# 26 Nov 2026: 8,647,096 points; OGC_FID is the package's own feature number, ID_IIP the IIP identifier).
ATTRIBUTES = [
    "OGC_FID",
    "ID_IIP",
    "NUMER_PORZ",
    "KOD_POCZT",
    "DATA_NAD",
    "NAZWA_ULC",
    "ID_ULIC",
    "NAZWA_GMI",
    "TERYT_GMI",
    "NAZWA_MSC",
    "ID_SIMC",
]
COLUMNS = (
    [("entity", "VARCHAR")]
    + [(a.lower(), "VARCHAR") for a in ATTRIBUTES]
    + [
        ("x", "VARCHAR"),
        ("y", "VARCHAR"),
        ("geometry_type", "VARCHAR"),
        ("crs_wkt", "VARCHAR"),
        ("raw_sha256", "VARCHAR"),
        ("source_version", "VARCHAR"),
        ("ingested_at", "TIMESTAMPTZ"),
        ("source_row_number", "BIGINT"),
    ]
)
NOT_NULL = ("entity", "address_id", "gmina_code", "house_number", "x_epsg2180", "y_epsg2180", "raw_sha256")
KEY = ("address_id",)
# A gmina code that is not 7 digits, or a point outside Poland's extent in EPSG:2180 (published x 172,099–860,888,
# y 139,337–774,828), rejects the package.
REJECT_IF = (
    "not regexp_full_match(gmina_code, '[0-9]{7}') or x_epsg2180 not between 100000 and 950000 "
    "or y_epsg2180 not between 50000 and 850000"
)


class AddressDecodeError(ValueError):
    """The package cannot satisfy the Bronze contract (recorded, never dropped silently)."""


def model_digest() -> str:
    from warehouse import entity

    return sha256((Path(entity.__file__).parent / "entity" / f"{MODEL}.sql").read_bytes()).hexdigest()[:12]


def unpack(raw: Path, folder: Path) -> Path:
    """The address layer's members of the package → folder; returns the .shp path."""
    tool = shutil.which("7z") or shutil.which("7za")
    if not tool:
        raise RuntimeError("7z is not installed")
    try:
        done = subprocess.run(
            [tool, "x", "-bd", "-y", f"-o{folder}", str(raw), f"{LAYER}.*"],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=1800,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("7z took longer than 30 minutes to unpack") from exc
    if done.returncode != 0:
        raise AddressDecodeError(f"7z cannot unpack the package: {(done.stdout + done.stderr)[-300:]}")
    found = {p.suffix.lower() for p in folder.glob(f"{LAYER}.*")}
    if not {".shp", ".shx", ".dbf", ".prj"} <= found:
        raise AddressDecodeError(f"the package has no complete {LAYER} layer (found {sorted(found)})")
    return folder / f"{LAYER}.shp"


def decode(raw: Path, receipt: dict[str, Any], output: Path) -> int:
    """Package → one Bronze Parquet file; returns the rows (= the .shx record count)."""
    shp = unpack(raw, output.parent / "layer")
    raw.unlink()  # disk: the layer's .dbf alone is ~7 GB
    expected = (shp.with_suffix(".shx").stat().st_size - 100) // 8
    crs = shp.with_suffix(".prj").read_text(errors="replace").strip()
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit = '{MEMORY}'")
        con.execute(f"SET temp_directory = '{output.parent}'")
        con.execute("INSTALL spatial; LOAD spatial;")
        path = str(shp).replace("'", "''")  # a temporary path, quoted for SQL
        try:
            found = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ST_Read('{path}')").fetchall()]
        except duckdb.Error as exc:
            raise AddressDecodeError(f"the layer cannot be read: {str(exc)[:300]}") from exc
        attributes = [c for c in found if c != "geom"]
        if attributes != ATTRIBUTES:
            changed = sorted(set(attributes) ^ set(ATTRIBUTES))
            raise AddressDecodeError(f"the layer's attributes differ from the declared set: {changed or attributes}")
        ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        columns = ", ".join(f'CAST("{a}" AS VARCHAR) AS {a.lower()}' for a in ATTRIBUTES)
        target = str(output).replace("'", "''")
        try:
            con.execute(
                f"COPY (SELECT ? AS entity, {columns}, CAST(ST_X(geom) AS VARCHAR) AS x, "
                "CAST(ST_Y(geom) AS VARCHAR) AS y, ST_GeometryType(geom)::VARCHAR AS geometry_type, "
                "? AS crs_wkt, ? AS raw_sha256, ? AS source_version, ?::TIMESTAMPTZ AS ingested_at, "
                f"(row_number() OVER ())::BIGINT AS source_row_number FROM ST_Read('{path}')) "
                f"TO '{target}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)",
                [receipt["entity"], crs, receipt["sha256"], receipt["version_token"], ingested],
            )
        except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:
            raise AddressDecodeError(f"the layer cannot be read: {str(exc)[:300]}") from exc
        written = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{target}')").fetchall()]
        if written != [c for c, _ in COLUMNS]:
            raise RuntimeError(f"PRG address Parquet columns differ from the table: {written}")
        rows, not_points = con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE geometry_type IS DISTINCT FROM 'POINT') "
            f"FROM read_parquet('{target}')"
        ).fetchone()
    finally:
        con.close()
        shutil.rmtree(shp.parent, ignore_errors=True)
    if rows != expected:
        raise AddressDecodeError(f"{rows} rows read for {expected} records in the .shx")
    if not_points:
        raise AddressDecodeError(f"{not_points} features are not points")
    return int(rows)


def state(cat, table_id: tuple[str, str]) -> dict[str, dict[str, Any]]:
    try:
        return et.state(bf.retry(lambda: cat.load_table(table_id), "load_table"), "entity")
    except NoSuchTableError:
        return {}


def silver_columns() -> list[tuple[str, str]]:
    """The Silver table's columns, from the model's output types (DuckDB → Iceberg)."""
    from warehouse import entity

    sql = (Path(entity.__file__).parent / "entity" / f"{MODEL}.sql").read_text(encoding="utf-8")
    con = duckdb.connect()
    try:
        empty = ", ".join(f"CAST(NULL AS {kind}) AS {name}" for name, kind in COLUMNS)
        con.execute(f"CREATE VIEW bronze AS SELECT {empty} WHERE false")
        described = con.execute(f"DESCRIBE {sql}").fetchall()
    finally:
        con.close()
    kinds = {"VARCHAR": "VARCHAR", "DOUBLE": "DOUBLE", "BIGINT": "BIGINT", "INTEGER": "INTEGER", "DATE": "DATE"}
    kinds["TIMESTAMP WITH TIME ZONE"] = "TIMESTAMPTZ"
    return [(name, kinds[str(kind)]) for name, kind, *_ in described]


def ensure(cat, table_id: tuple[str, str], columns: list[tuple[str, str]]) -> None:
    """Create the table on first use (partitioned by entity, entity-table maintenance); an existing table must
    have the declared columns (a changed model or layout needs a deliberate migration)."""
    try:
        table = bf.retry(lambda: cat.load_table(table_id), "load_table")
    except NoSuchTableError:
        bf.retry(lambda: bf.ensure_table(cat, table_id, columns, "entity"), "create_table")
        et.configure(table_id)
        return
    held = [(f.name, str(f.field_type)) for f in table.schema().fields]
    if held != [(n, str(bf.TYPES[k])) for n, k in columns]:
        raise RuntimeError(f"{'.'.join(table_id)}: the table's columns differ from the loader's")


def run(
    client,
    cat,
    landing: str,
    lake: str,
    landing_root: str = "01_landing",
    namespace: str = "bronze",
    silver_namespace: str = "silver",
    minutes: float = 120,
) -> dict[str, Any]:
    """Load the newest address package into Bronze and Silver unless both already hold it."""
    started = time.monotonic()
    digest = model_digest()
    bronze_root = "02_bronze" if namespace == "bronze" else namespace
    silver_root = "03_silver" if silver_namespace == "silver" else silver_namespace
    bronze_id, silver_id, rejected_id = (namespace, TABLE), (silver_namespace, TABLE), (namespace, REJECTED)
    out: dict[str, Any] = {"entity": ENTITY, "result": "current"}
    held = bulk_file.landed(client, landing, bulk_file.prefix(SOURCE, ENTITY, landing_root))
    if not held:
        out["result"] = "not landed"
        annotate("notice", f"{SOURCE} address points Bronze and Silver", out)
        return out
    receipt = held[-1]
    sha = receipt["sha256"]
    out["version"] = receipt["version_token"]
    refused = bf.rejected_hashes(cat, rejected_id)
    held_b, held_s = state(cat, bronze_id), state(cat, silver_id)
    need_bronze = held_b.get(ENTITY, {}).get("recipes") != {sha}
    need_silver = need_bronze or held_s.get(ENTITY, {}).get("recipes") != {f"{sha}.{digest}"}
    if sha in refused or f"{sha}.{digest}" in refused:
        out["result"] = "rejected before"
    elif need_silver and minutes < 30:
        out["result"] = "left for the next run (time budget)"
    elif need_silver:
        ids, roots = (bronze_id, silver_id, rejected_id), (bronze_root, silver_root)
        out.update(load(client, cat, (landing, lake), receipt, digest, held_b if not need_bronze else None, ids, roots))
    after_b, after_s = state(cat, bronze_id), state(cat, silver_id)
    out["bronze_rows_held"] = after_b.get(ENTITY, {}).get("rows")
    out["silver_rows_held"] = after_s.get(ENTITY, {}).get("rows")
    out["minutes"] = round((time.monotonic() - started) / 60, 1)
    level = "warning" if out["result"] in {"rejected", "rejected before"} else "notice"
    annotate(level, f"{SOURCE} address points Bronze and Silver", out)
    print(json.dumps(out, default=str), flush=True)
    return out


def load(client, cat, buckets, receipt, digest, held_b, ids, roots) -> dict[str, Any]:
    """Decode (when Bronze does not hold the package: ``held_b`` None), apply the Silver model, then replace both
    tables' files."""
    from warehouse import entity as entity_model

    landing, lake = buckets
    need_bronze = held_b is None
    sha = receipt["sha256"]
    bronze_id, silver_id, rejected_id = ids
    bronze_root, silver_root = roots
    reject = {"entity": ENTITY, "raw_sha256": sha, "landing_key": receipt["object"]["key"]}
    reject["code_sha"] = os.environ.get("GITHUB_SHA", "")
    out: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="zohelo-prg-addresses-") as temporary:
        work = Path(temporary)
        local, silver = work / f"{sha}.parquet", work / "silver.parquet"
        if need_bronze:
            raw = work / "package.zip"
            bf.download(client, landing, SOURCE, receipt["object"]["key"], sha, receipt["size_bytes"], raw)
            began = time.monotonic()
            try:
                out["bronze_rows"] = decode(raw, receipt, local)
            except AddressDecodeError as exc:
                bf.record_rejections(cat, rejected_id, [{**reject, "error": f"decoder: {exc}"[:1000]}])
                return {"result": "rejected", "error": str(exc)[:300]}
            out["decode_minutes"] = round((time.monotonic() - began) / 60, 1)
            source = str(local)
        else:
            source = held_b[ENTITY]["paths"][0]
        began = time.monotonic()
        try:
            report = entity_model.apply(
                MODEL,
                source,
                silver,
                not_null=NOT_NULL,
                unique_key=KEY,
                same_rows=True,
                reject_if=REJECT_IF,
                memory_limit=MEMORY,
                temp_directory=str(work),
            )
        except entity_model.EntityModelError as exc:  # verdict of this model on this package: <sha>.<model>
            bf.record_rejections(
                cat, rejected_id, [{**reject, "raw_sha256": f"{sha}.{digest}", "error": f"silver: {exc}"[:1000]}]
            )
            return {"result": "rejected", "error": str(exc)[:300]}
        out["silver_minutes"] = round((time.monotonic() - began) / 60, 1)
        # Uploads only after both files passed: a rejection leaves no unreferenced object and keeps the previous
        # version in both tables.
        ensure(cat, bronze_id, COLUMNS)
        ensure(cat, silver_id, silver_columns())
        if need_bronze:
            uri = bf.upload(client, lake, f"{bronze_root}/{SOURCE}/{TABLE}/{ENTITY}/{sha}.parquet", local, sha)
            et.replace(cat, bronze_id, "entity", {ENTITY: [uri]}, STATS, f"{SOURCE}-addresses")
            out["bronze_parquet_mb"] = round(local.stat().st_size / 1e6, 1)
        key = f"{silver_root}/{SOURCE}/{TABLE}/{ENTITY}/{sha}.{digest}.parquet"
        et.replace(
            cat,
            silver_id,
            "entity",
            {ENTITY: [bf.upload(client, lake, key, silver, sha)]},
            STATS,
            f"{SOURCE}-addresses",
        )
        out.update(
            {"result": "loaded", "silver_rows": report["rows"], "silver_parquet_mb": round(report["bytes"] / 1e6, 1)}
        )
    return out
