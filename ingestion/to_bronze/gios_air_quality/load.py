#!/usr/bin/env python3
"""GIOŚ air quality bronze.load: daily pass ZIPs of API answers → three Bronze tables, then the Silver measurements.

Every Landing file is one daily pass ``pass_<YYYYMMDD>``, a ZIP of exact API v1 answers
(ingestion/to_landing/gios_air_quality/). Each table gets one Parquet file per Landing file,
``02_bronze/gios_air_quality/<table>/<entity>/<raw_sha256>.parquet``, partitioned by entity:

  station/page-<n>.json                     bronze.gios_air_stations       one row per station of the list
  sensors/<station>/page-<n>.json           bronze.gios_air_sensors        one row per sensor of a station
  data/<station>/<sensor>/page-<n>.json     bronze.gios_air_measurements   one row per sensor × hour

Values stay text exactly as the JSON prints them (numbers are not re-formatted, a null value stays null). An unknown
member name or field, a value that is not a JSON number, string or null, a measurement value that is not a number,
a sensor list naming another station, or a measurement time more than WINDOW_DAYS before the pass day rejects the
whole file into bronze.gios_air_quality_rejected_files. Only receipts of passes Bronze does not hold yet are read
(R2 reads). After the load, entities.py rebuilds the measurement days the passes cover.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import io
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.exceptions import NoSuchTableError

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

SOURCE = "gios_air_quality"
REJECTED = "gios_air_quality_rejected_files"
WINDOW_DAYS = 5  # a pass holds the last three days; an older value means the API changed its window
TABLES = {  # table: (member pattern, columns from the member name, list key, JSON field → column)
    "gios_air_stations": (
        re.compile(r"station/page-(\d+)\.json"),
        [],
        "Lista stacji pomiarowych",
        {
            "Identyfikator stacji": "station_id",
            "Kod stacji": "station_code",
            "Nazwa stacji": "station_name",
            "WGS84 φ N": "latitude",
            "WGS84 λ E": "longitude",
            "Identyfikator miasta": "city_id",
            "Nazwa miasta": "city_name",
            "Gmina": "commune",
            "Powiat": "district",
            "Województwo": "voivodeship",
            "Ulica": "street",
        },
    ),
    "gios_air_sensors": (
        re.compile(r"sensors/(\d+)/page-(\d+)\.json"),
        ["list_station_id"],
        "Lista stanowisk pomiarowych dla podanej stacji",
        {
            "Identyfikator stanowiska": "sensor_id",
            "Identyfikator stacji": "station_id",
            "Wskaźnik": "parameter_name",
            "Wskaźnik - wzór": "parameter_formula",
            "Wskaźnik - kod": "parameter_code",
            "Id wskaźnika": "parameter_id",
        },
    ),
    "gios_air_measurements": (
        re.compile(r"data/(\d+)/(\d+)/page-(\d+)\.json"),
        ["station_id", "sensor_id"],
        "Lista danych pomiarowych",
        {"Kod stanowiska": "sensor_code", "Data": "measured_at", "Wartość": "value"},
    ),
}
TAIL = [*bulk_bronze.PROVENANCE, ("page", "VARCHAR"), ("source_row_number", "BIGINT")]
MEASURED_AT = re.compile(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d")


def columns(table: str) -> list[tuple[str, str]]:
    _, path, _, fields = TABLES[table]
    return [("entity", "VARCHAR"), *[(c, "VARCHAR") for c in [*path, *fields.values()]], *TAIL]


def pass_day(entity: str) -> date:
    return datetime.strptime(entity.split("_", 1)[1], "%Y%m%d").date()


def text(value: Any, where: str) -> str | None:
    """A JSON scalar as text (numbers as printed: parsed with parse_float/parse_int=str); anything else is refused."""
    if value is None or isinstance(value, str):
        return value
    raise ValueError(f"{where}: unexpected value type {type(value).__name__}")


def decode(body: bytes, receipt: dict[str, Any]) -> dict[str, pa.Table]:
    """One pass → {table: rows}; any surprise raises (the file is rejected whole)."""
    base = bulk_bronze.provenance(receipt)
    earliest = (pass_day(receipt["entity"]) - timedelta(days=WINDOW_DAYS)).isoformat()
    rows: dict[str, list[dict[str, Any]]] = {t: [] for t in TABLES}
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        for name in archive.namelist():
            table, match = next(((t, p.fullmatch(name)) for t, (p, *_r) in TABLES.items() if p.fullmatch(name)), (0, 0))
            if not match:
                raise ValueError(f"unknown member {name!r}")
            _, path, list_key, fields = TABLES[table]
            parsed = json.loads(archive.read(name), parse_float=str, parse_int=str)
            if list_key not in parsed:
                raise ValueError(f"{name}: no {list_key!r}")
            for n, item in enumerate(parsed[list_key], start=1):
                unknown = set(item) - set(fields)
                if unknown:
                    raise ValueError(f"{name} item {n}: unknown fields {sorted(unknown)}")
                row = {**base, "page": match.groups()[-1], "source_row_number": len(rows[table]) + 1}
                row.update(dict(zip(path, match.groups()[:-1], strict=True)))
                row.update({fields[k]: text(item.get(k), f"{name} item {n} {k}") for k in fields})
                if table == "gios_air_sensors" and row["station_id"] != row["list_station_id"]:
                    raise ValueError(f"{name} item {n}: sensor of station {row['station_id']}")
                if table == "gios_air_measurements":
                    when = row["measured_at"] or ""
                    if not MEASURED_AT.fullmatch(when) or when < earliest:
                        raise ValueError(f"{name} item {n}: measurement time {when!r} outside the pass window")
                    if row["value"] is not None:
                        float(row["value"])  # a value that is not a number rejects the file
                rows[table].append(row)
    if not rows["gios_air_stations"] or not rows["gios_air_sensors"]:
        raise ValueError("a pass without stations or sensors")
    return {t: pa.Table.from_pylist(r, schema=bulk_bronze.arrow_schema(columns(t))) for t, r in rows.items()}


def candidates(client, bucket: str, landing_root: str, cat, namespace: str) -> list[dict[str, Any]]:
    """Receipts of the passes that every Bronze table does not hold yet and that were not rejected."""
    keys: dict[str, list[str]] = {}
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{landing_root}/{SOURCE}/snapshot/data/"
    ):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".receipt.json"):
                keys.setdefault(o["Key"].rsplit("/", 2)[-2], []).append(o["Key"])
    held: set[str] | None = None
    for table in TABLES:
        try:
            state = set(et.state(cat.load_table((namespace, table)), "entity"))
        except NoSuchTableError:  # a table that does not exist yet holds nothing
            state = set()
        held = state if held is None else held & state
    rejected = bf.rejected_hashes(cat, (namespace, REJECTED))
    out = []
    for entity, entity_keys in sorted(keys.items()):
        if entity in (held or set()):
            continue
        newest = sorted(entity_keys)[-1]  # key = <ingested>__<ingested>: the newest copy sorts last
        receipt = {**json.loads(get_with_backoff(client, bucket, newest)), "receipt_key": newest}
        if receipt["sha256"] not in rejected:
            out.append(receipt)
    return out


def load(
    budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze", silver_namespace: str = "silver"
) -> int:
    """budget_minutes is unused: one pass a day."""
    from ingestion.to_bronze.gios_air_quality import entities

    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    todo = candidates(client, landing_bucket, landing_root, cat, namespace)
    totals: dict[str, Any] = {"candidates": len(todo), "appended_files": 0, "rows_added": {}, "rejected": 0}
    uris: dict[str, list[str]] = {t: [] for t in TABLES}
    rejected = []
    with tempfile.TemporaryDirectory(prefix=f"zohelo-{SOURCE}-bronze-") as temporary:
        for receipt in todo:
            body = bulk_bronze.read_raw(client, landing_bucket, SOURCE, receipt)
            try:
                decoded = decode(body, receipt)
            except (ValueError, KeyError, TypeError, zipfile.BadZipFile, UnicodeDecodeError) as exc:
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
            for table, rows in decoded.items():
                path = Path(temporary) / f"{table}.{receipt['sha256']}.parquet"
                pq.write_table(rows, path, compression="zstd")
                key = f"{lake_root}/{SOURCE}/{table}/{receipt['entity']}/{receipt['sha256']}.parquet"
                uris[table].append(bf.upload(client, lake, key, path, receipt["sha256"]))
                totals["rows_added"][table] = totals["rows_added"].get(table, 0) + rows.num_rows
    for table in TABLES:
        bf.retry(lambda table=table: bf.ensure_table(cat, (namespace, table), columns(table), "entity"), table)
        if uris[table]:
            bf.commit(cat, (namespace, table), uris[table], ["entity", "raw_sha256"], f"{SOURCE}-bronze-load")
            totals["appended_files"] += len(uris[table])
    bf.rejected_table(cat, (namespace, REJECTED))
    bf.record_rejections(cat, (namespace, REJECTED), rejected)
    totals["rejected"] = len(rejected)
    totals["rejected_sample"] = [r["error"][:200] for r in rejected[:3]]
    annotate("warning" if rejected else "notice", f"{SOURCE} Bronze from Landing", totals)
    step_summary(f"## {SOURCE} · bronze load\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    built = entities.build(cat, client, lake, namespace, silver_namespace, today=datetime.now(UTC).date())
    return 1 if built.get("failed") else 0
