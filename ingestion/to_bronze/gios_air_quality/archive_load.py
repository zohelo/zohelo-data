"""GIOŚ yearly archive (#3119): Landing → Bronze → Silver, one year file at a time (entity tables, design #875).

Entities (ingestion/to_landing/gios_air_quality/archive.py): ``archive_<year>`` (ZIP of workbooks) and
``archive_stations`` (station metadata workbook); the newest Landing copy of each is its version. Each table holds one
file per entity, replaced whole (entity_tables), so a rerun writes nothing and a replaced year replaces its files:

  02_bronze/gios_air_quality/gios_air_archive_values/<year entity>/<raw_sha256>.parquet   archive.decode
  03_silver/gios_air_quality/gios_air_archive_values/<year entity>/<raw_sha256>.<model>.parquet
      warehouse/entity/gios_air_archive_values.sql (<model> = digest of the SQL: a changed model recomputes Silver)
  02_bronze/gios_air_quality/gios_air_archive_stations/archive_stations/<raw_sha256>.parquet   stations()

The Silver stations are a dbt model (models/silver/gios_air_quality/gios_air_archive_stations.sql). A year whose
file is not of a declared layout, or whose Silver breaks a check (a null key, a repeated station × indicator ×
averaging time × stamp, a value cell that is not a number), is rejected into bronze.gios_air_quality_rejected_files
with the reason and the tables keep the previous version. Years are committed one by one, newest first; a run that
reaches its time budget stops and the next run continues.
"""

from __future__ import annotations

from hashlib import sha256
import io
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any
import zipfile

import duckdb
from pyiceberg.exceptions import NoSuchTableError

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common import entity_tables as et
from ingestion.common.r2 import get_with_backoff
from ingestion.common.report import annotate
from ingestion.to_bronze.gios_air_quality import archive

SOURCE = "gios_air_quality"
VALUES, STATIONS, REJECTED = "gios_air_archive_values", "gios_air_archive_stations", "gios_air_quality_rejected_files"
MODEL = "gios_air_archive_values"
YEAR = re.compile(r"archive_\d{4}")
STATIONS_ENTITY = "archive_stations"  # ingestion/to_landing/gios_air_quality/archive.py STATIONS
RECEIPT = re.compile(r"(archive_[a-z0-9]+)/([0-9TZ]+)__([0-9TZ]+)\.receipt\.json")
NOT_NULL = ("station_code", "indicator", "averaging_time", "measured_at", "measured_on", "raw_sha256")
KEY = ("station_code", "indicator", "averaging_time", "measured_at")
# A value cell that is not a number; an hourly stamp off the full hour.
REJECT_IF = "value is null or (averaging_time = '1g' and minute(measured_at) <> 0)"
STATS = ["entity", "raw_sha256"]
# Header row of the station sheet STACJE (Dev probe 45747293591, whitespace collapsed) → Bronze column, in order.
STATION_FIELDS = {
    "Nr": "row_number",
    "Kod stacji": "station_code",
    "Kod międzynarodowy": "international_code",
    "Nazwa stacji": "station_name",
    "Stary Kod stacji (o ile inny od aktualnego)": "old_station_codes",
    "Data uruchomienia": "opened_on",
    "Data zamknięcia": "closed_on",
    "Typ stacji": "station_type",
    "Typ obszaru": "area_type",
    "Rodzaj stacji": "station_kind",
    "Województwo": "voivodeship",
    "Miejscowość": "locality",
    "Adres": "address",
    "WGS84 φ N": "latitude",
    "WGS84 λ E": "longitude",
}
STATION_SHEET = "STACJE"
STATION_COLUMNS = [
    ("entity", "VARCHAR"),
    *[(c, "VARCHAR") for c in STATION_FIELDS.values()],
    *bulk_bronze.PROVENANCE,
    ("sheet_row", "BIGINT"),
]


def model_digest() -> str:
    from warehouse import entity

    return sha256((Path(entity.__file__).parent / "entity" / f"{MODEL}.sql").read_bytes()).hexdigest()[:12]


def newest_receipts(client, bucket: str, landing_root: str) -> dict[str, dict[str, Any]]:
    """Archive entity → receipt of its newest landed copy (newest ingest stamp in the key name)."""
    prefix = f"{landing_root}/{SOURCE}/snapshot/data/"
    newest: dict[str, tuple[str, str]] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "archive_"):
        for item in page.get("Contents", []):
            match = RECEIPT.fullmatch(item["Key"].removeprefix(prefix))
            if match and (match.group(1) not in newest or match.group(3) > newest[match.group(1)][1]):
                newest[match.group(1)] = (item["Key"], match.group(3))
    return {e: {**json.loads(get_with_backoff(client, bucket, k)), "receipt_key": k} for e, (k, _) in newest.items()}


def stations(body: bytes, receipt: dict[str, Any], output: Path) -> int:
    """The station sheet (STACJE) of the metadata workbook → one Bronze Parquet file: row 1 must be the declared
    header (STATION_FIELDS, matched by name), then one row per station; a row with cells but no station code, or
    a cell beyond the declared columns, rejects the file. The sheet STANOWISKA (sensors) is not loaded."""
    from ingestion.common import xlsx_cells

    with zipfile.ZipFile(io.BytesIO(body)) as workbook:
        parts = dict(xlsx_cells.sheets(workbook))
        if STATION_SHEET not in parts:
            raise archive.ArchiveDecodeError(f"no sheet {STATION_SHEET!r}, found {sorted(parts)}")
        used = archive._dimension(workbook, parts[STATION_SHEET])
    with tempfile.TemporaryDirectory(prefix="zohelo-gios-stations-") as temporary:
        path = Path(temporary) / "stations.xlsx"
        path.write_bytes(body)
        con = duckdb.connect()
        try:
            con.execute("install excel; load excel; set preserve_insertion_order = true")
            rng = f", range = '{used}'" if used else ""
            con.execute(
                "create temp table raw as select cast(row_number() over () as bigint) as sheet_row, * from "
                f"read_xlsx('{path}', sheet = '{STATION_SHEET}', header = false, all_varchar = true, stop_at_empty = false{rng})"
            )
            cols = [r[0] for r in con.execute("describe raw").fetchall()][1:]
            head = con.execute(f"select {', '.join(f'"{c}"' for c in cols)} from raw where sheet_row = 1").fetchone()
            found = [" ".join((h or "").split()) for h in head]
            n = len(STATION_FIELDS)
            if found[:n] != list(STATION_FIELDS) or any(found[n:]):
                raise archive.ArchiveDecodeError(f"station sheet header {found} is not the declared layout")
            extra = " or ".join(f'"{c}" is not null' for c in cols[n:]) or "false"
            any_cell = " or ".join(f'"{c}" is not null' for c in cols[:n])
            bad = con.execute(
                f'select count(*) from raw where sheet_row > 1 and (({extra}) or ("{cols[1]}" is null and ({any_cell})))'
            ).fetchone()[0]
            if bad:
                raise archive.ArchiveDecodeError(f"station sheet: {bad} rows without a station code or beyond the header")
            prov = bulk_bronze.provenance(receipt)
            con.execute(
                f"""copy (select ? as entity, {", ".join(f'"{c}" as {f}' for c, f in zip(cols, STATION_FIELDS.values()))},
                    ? as raw_sha256, ? as source_version, cast(? as timestamptz) as ingested_at, sheet_row
                from raw where sheet_row > 1 and "{cols[1]}" is not null order by sheet_row)
                to '{output}' (format parquet, compression zstd)""",
                [prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"]],
            )
            rows = con.execute(f"select count(*) from read_parquet('{output}')").fetchone()[0]
        except duckdb.Error as exc:
            raise archive.ArchiveDecodeError(f"station sheet: {str(exc)[:300]}") from exc
        finally:
            con.close()
    if not rows:
        raise archive.ArchiveDecodeError("station sheet: no rows")
    return rows


def state(cat, table_id) -> dict[str, dict[str, Any]]:
    try:
        return et.state(bf.retry(lambda: cat.load_table(table_id), "load_table"), "entity")
    except NoSuchTableError:
        return {}


SILVER_COLUMNS = [
    ("entity", "VARCHAR"),
    ("archive_year", "INTEGER"),
    ("member", "VARCHAR"),
    ("station_code", "VARCHAR"),
    ("sensor_code", "VARCHAR"),
    ("indicator", "VARCHAR"),
    ("averaging_time", "VARCHAR"),
    ("indicator_published", "VARCHAR"),
    ("averaging_time_published", "VARCHAR"),
    ("unit", "VARCHAR"),
    ("measured_at", "TIMESTAMP"),
    ("measured_on", "DATE"),
    ("value", "DOUBLE"),
    ("value_text", "VARCHAR"),
    ("validation_status", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("sheet_row", "BIGINT"),
]


def ensure(cat, table_id: tuple[str, str], columns: list[tuple[str, str]]) -> None:
    """Create the table on first use (all columns optional, identity partition on entity; the types of
    bronze_files.TYPES plus TIMESTAMP for local times), then set the entity-table maintenance (et.configure); an
    existing table must have the declared columns."""
    import contextlib

    from pyiceberg.exceptions import NamespaceAlreadyExistsError
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import NestedField, TimestampType

    try:
        table = cat.load_table(table_id)
    except NoSuchTableError:
        kinds = {**bf.TYPES, "TIMESTAMP": TimestampType()}
        with contextlib.suppress(NamespaceAlreadyExistsError):
            cat.create_namespace(table_id[0])
        fields = [NestedField(i, n, kinds[k], required=False) for i, (n, k) in enumerate(columns, start=1)]
        spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="entity"))
        table = bf.retry(lambda: cat.create_table(table_id, schema=Schema(*fields), partition_spec=spec), "create")
        with table.transaction() as tx:
            tx.set_properties(**{"schema.name-mapping.default": table.schema().name_mapping.model_dump_json()})
        et.configure(table_id)
        return
    if [f.name for f in table.schema().fields] != [name for name, _ in columns]:
        raise RuntimeError(f"{'.'.join(table_id)} schema differs from the decoder")


def replace(cat, table_id, columns, entity: str, uri: str) -> None:
    ensure(cat, table_id, columns)
    et.replace(cat, table_id, "entity", {entity: [uri]}, STATS, f"{SOURCE}-archive")


def run(client, cat, landing: str, lake: str, landing_root: str, namespace: str, silver_namespace: str,
        minutes: float) -> dict[str, Any]:
    from warehouse import entity as entity_model

    started = time.monotonic()
    digest = model_digest()
    bronze_root = "02_bronze" if namespace == "bronze" else namespace
    silver_root = "03_silver" if silver_namespace == "silver" else silver_namespace
    values_b, values_s, stations_b = (namespace, VALUES), (silver_namespace, VALUES), (namespace, STATIONS)
    receipts = newest_receipts(client, landing, landing_root)
    held_b, held_s, held_st = state(cat, values_b), state(cat, values_s), state(cat, stations_b)
    refused = bf.rejected_hashes(cat, (namespace, REJECTED))
    out: dict[str, Any] = {"entities_landed": len(receipts), "loaded": [], "bronze_rows": 0, "silver_rows": 0}
    out.update({"rejected": [], "skipped_members": {}, "left_for_next_run": []})
    rejected: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="zohelo-gios-archive-") as temporary:
        work = Path(temporary)
        for name in sorted(receipts, key=lambda e: (e != STATIONS_ENTITY, -int(e[-4:]) if YEAR.fullmatch(e) else 0)):
            receipt, sha = receipts[name], receipts[name]["sha256"]
            if sha in refused or f"{sha}.{digest}" in refused:
                continue
            reject = {"entity": name, "raw_sha256": sha, "landing_key": receipt["object"]["key"],
                      "code_sha": os.environ.get("GITHUB_SHA", "")}
            if name == STATIONS_ENTITY:
                if held_st.get(name, {}).get("recipes") == {sha}:
                    continue
                local = work / f"{sha}.parquet"
                try:
                    rows = stations(bulk_bronze.read_raw(client, landing, SOURCE, receipt), receipt, local)
                except archive.ArchiveDecodeError as exc:
                    rejected.append({**reject, "error": f"decoder: {exc}"[:1000]})
                    continue
                uri = bf.upload(client, lake, f"{bronze_root}/{SOURCE}/{STATIONS}/{name}/{sha}.parquet", local, sha)
                replace(cat, stations_b, STATION_COLUMNS, name, uri)
                out["loaded"].append(name)
                out["bronze_rows"] += rows
                continue
            if not YEAR.fullmatch(name):
                continue
            need_bronze = held_b.get(name, {}).get("recipes") != {sha}
            if not need_bronze and held_s.get(name, {}).get("recipes") == {f"{sha}.{digest}"}:
                continue
            if time.monotonic() - started > minutes * 60:
                out["left_for_next_run"].append(name)
                continue
            local, silver = work / f"{name}.bronze.parquet", work / f"{name}.silver.parquet"
            if need_bronze:
                try:
                    report = archive.decode(bulk_bronze.read_raw(client, landing, SOURCE, receipt), receipt, local)
                except archive.ArchiveDecodeError as exc:
                    rejected.append({**reject, "error": f"decoder: {exc}"[:1000]})
                    continue
                if report["skipped"]:
                    out["skipped_members"][name] = sorted(report["skipped"])
                source = str(local)
            else:
                source = held_b[name]["paths"][0]
            try:
                result = entity_model.apply(MODEL, source, silver, not_null=NOT_NULL, unique_key=KEY, same_rows=True,
                                            reject_if=REJECT_IF, memory_limit="8GB", temp_directory=str(work))
            except entity_model.EntityModelError as exc:
                rejected.append({**reject, "raw_sha256": f"{sha}.{digest}", "error": f"silver: {exc}"[:1000]})
                local.unlink(missing_ok=True)
                continue
            if need_bronze:
                uri = bf.upload(client, lake, f"{bronze_root}/{SOURCE}/{VALUES}/{name}/{sha}.parquet", local, sha)
                replace(cat, values_b, archive.COLUMNS, name, uri)
                out["bronze_rows"] += result["bronze_rows"]
            uri = bf.upload(client, lake, f"{silver_root}/{SOURCE}/{VALUES}/{name}/{sha}.{digest}.parquet", silver, sha)
            replace(cat, values_s, SILVER_COLUMNS, name, uri)
            out["loaded"].append(name)
            out["silver_rows"] += result["rows"]
            local.unlink(missing_ok=True)
            silver.unlink(missing_ok=True)
    bf.rejected_table(cat, (namespace, REJECTED))
    bf.record_rejections(cat, (namespace, REJECTED), rejected)
    out["rejected"] = [f"{r['entity']}: {r['error'][:300]}" for r in rejected]
    after = state(cat, values_s)
    out["silver_years_held"] = len(after)
    out["silver_rows_held"] = sum(s["rows"] for s in after.values())
    out["minutes"] = round((time.monotonic() - started) / 60, 1)
    annotate("warning" if rejected else "notice", f"{SOURCE} archive Bronze and Silver", out)
    return out

