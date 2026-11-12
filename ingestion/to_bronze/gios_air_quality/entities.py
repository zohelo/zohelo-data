"""GIOŚ air quality Silver measurements: silver.gios_air_hourly, one file per measurement day (design #875).

The hourly values grow by about 26,000 rows a day (some 1,100 sensors × 24 hours), past the 1 M ceiling of a
full-rebuild dbt model within weeks, so they are an entity table written here, right after Bronze: entity =
measurement day (local date of the value, 'YYYY-MM-DD'); its file is warehouse/entity/gios_air_hourly.sql
applied to the Bronze files of the passes that can hold the day (pass days ``day`` to ``day + WINDOW_DAYS``; Bronze
rejects a pass holding older values). The file is named after its recipe, a digest of the SQL and of those Bronze
files, and replaces the day's partition only when the recipe changed (entity_tables.replace), so a rerun writes
nothing. Checks before a day is accepted (warehouse/entity.py): sensor_id, measured_at, station_id and raw_sha256
never null, sensor_id × measured_at unique. A day that fails keeps its previous file and fails the run.
"""

from __future__ import annotations

import contextlib
from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path
import tempfile
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common import bronze_files as bf
from ingestion.common import entity_tables as et
from ingestion.common.report import annotate

SOURCE = "gios_air_quality"
MODEL = "gios_air_hourly"
BRONZE = "gios_air_measurements"
WINDOW_DAYS = 5
COLUMNS = [
    ("entity", "string"),
    ("measured_on", "date"),
    ("measured_at", "timestamp"),
    ("station_id", "long"),
    ("sensor_id", "long"),
    ("sensor_code", "string"),
    ("value", "double"),
    ("validation_status", "string"),
    ("raw_sha256", "string"),
    ("source_version", "string"),
    ("ingested_at", "timestamptz"),
]


def ensure(cat, table_id: tuple[str, str]):
    """The Silver table (created with the model's types on first use, identity partition on entity)."""
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import (
        DateType,
        DoubleType,
        LongType,
        NestedField,
        StringType,
        TimestampType,
        TimestamptzType,
    )

    kinds = {
        "string": StringType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "long": LongType(),
        "double": DoubleType(),
        "timestamptz": TimestamptzType(),
    }
    try:
        table = cat.load_table(table_id)
    except NoSuchTableError:
        with contextlib.suppress(NamespaceAlreadyExistsError):
            cat.create_namespace(table_id[0])
        schema = Schema(*[NestedField(i, n, kinds[k], required=False) for i, (n, k) in enumerate(COLUMNS, start=1)])
        spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="entity"))
        table = cat.create_table(table_id, schema=schema, partition_spec=spec)
        with table.transaction() as tx:
            tx.set_properties(**{"schema.name-mapping.default": table.schema().name_mapping.model_dump_json()})
        table = cat.load_table(table_id)
    if [f.name for f in table.schema().fields] != [n for n, _ in COLUMNS]:
        raise RuntimeError(f"{'.'.join(table_id)} schema differs from the model")
    return table


def plan(bronze: dict[str, dict[str, Any]], today: date) -> dict[str, tuple[str, list[str]]]:
    """Measurement day → (recipe, Bronze file URIs) for every day a held pass can cover, up to today."""
    from ingestion.to_bronze.gios_air_quality.load import pass_day

    sql = (Path(__file__).resolve().parents[3] / "warehouse" / "entity" / f"{MODEL}.sql").read_bytes()
    passes = {pass_day(e): sorted(s["paths"]) for e, s in bronze.items() if e.startswith("pass_")}
    days = {p - timedelta(days=d) for p in passes for d in range(WINDOW_DAYS + 1)}
    out = {}
    for day in sorted(d for d in days if d <= today):
        files = [u for p in sorted(passes) if day <= p <= day + timedelta(days=WINDOW_DAYS) for u in passes[p]]
        digest = sha256(sql)
        for uri in files:
            digest.update(et.recipe_of(uri).encode())
        out[day.isoformat()] = (digest.hexdigest()[:32], files)
    return out


def build(cat, client, lake: str, namespace: str, silver_namespace: str, today: date) -> dict[str, Any]:
    from warehouse import entity

    bronze = et.state(bf.retry(lambda: cat.load_table((namespace, BRONZE)), "load bronze"), "entity")
    table_id = (silver_namespace, MODEL)
    held = et.state(bf.retry(lambda: ensure(cat, table_id), "silver table"), "entity")
    wanted_days = plan(bronze, today)
    todo = {d: v for d, v in wanted_days.items() if held.get(d, {}).get("recipes") != {v[0]}}
    lake_root = "03_silver" if silver_namespace == "silver" else silver_namespace
    wanted: dict[str, list[str]] = {}
    totals: dict[str, Any] = {"days": len(wanted_days), "rebuilt": 0, "rows": 0, "failed": []}
    with tempfile.TemporaryDirectory(prefix=f"zohelo-{SOURCE}-silver-") as temporary:
        for day, (recipe, files) in todo.items():
            marker = Path(temporary) / f"{day}.day.parquet"
            pq.write_table(pa.table({"day": pa.array([date.fromisoformat(day)], pa.date32())}), marker)
            output = Path(temporary) / f"{recipe}.parquet"
            try:
                result = entity.apply(
                    MODEL,
                    files,
                    output,
                    not_null=("sensor_id", "measured_at", "station_id", "raw_sha256"),
                    same_rows=False,
                    unique_key=("sensor_id", "measured_at"),
                    lookups={"target_day": str(marker)},
                    memory_limit="4GB",
                )
            except (entity.EntityModelError, entity.EntityRunError) as exc:
                totals["failed"].append({"day": day, "error": str(exc)[:400]})
                continue
            if not result["rows"]:
                continue
            key = f"{lake_root}/{SOURCE}/{MODEL}/{day}/{recipe}.parquet"
            wanted[day] = [bf.upload(client, lake, key, output, recipe)]
            totals["rebuilt"] += 1
            totals["rows"] += result["rows"]
    if wanted:
        et.replace(cat, table_id, "entity", wanted, ["entity", "sensor_id", "measured_at"], f"{SOURCE}-silver")
    after = et.state(cat.load_table(table_id), "entity")
    totals.update(
        {
            "table": ".".join(table_id),
            "days_held": len(after),
            "rows_held": sum(s["rows"] for s in after.values()),
            "first_day": min(after, default=None),
            "last_day": max(after, default=None),
        }
    )
    annotate("error" if totals["failed"] else "notice", f"{SOURCE} Silver measurements", totals)
    return totals
