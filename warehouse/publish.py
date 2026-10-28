"""gold.publish: the sole writer of the published layer ``06_published/v1/`` (decision D5).

Reads Gold tables from the R2 Data Catalog and the KPI seeds in the repository and writes the
files described in docs/published.md (schemas in warehouse/published/schemas/). Today it writes
``kpi/<kpi_key>/latest.json``; the other files follow in later Tasks.
"""

from __future__ import annotations

import csv
from datetime import UTC, date, datetime
import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

PROJECT = Path(__file__).resolve().parent
PREFIX = "06_published/v1/"
LEVELS = {"country": "country", "wojewodztwo": "voivodeship"}  # Gold geography_level → contract level
FREQ_RANK = {"D": 1, "B": 2, "W": 3, "M": 4, "Q": 5, "S": 6, "A": 7}
SKIP = {"fx_rate": "one value per currency; latest.json has no currency field"}
LATEST_LIMIT = 100_000  # bytes, docs/published.md size limits (D22)


def gold(cat, namespace: str, name: str) -> pa.Table:
    return cat.load_table((namespace, name)).scan().to_arrow()


def seed(name: str) -> list[dict[str, str]]:
    with (PROJECT / "seeds" / f"{name}.csv").open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def period_label(date_key: date, frequency: str) -> str:
    if frequency == "A":
        return f"{date_key.year}"
    if frequency == "S":
        return f"{date_key.year}-H{1 if date_key.month <= 6 else 2}"
    if frequency == "Q":
        return f"{date_key.year}-Q{(date_key.month - 1) // 3 + 1}"
    if frequency == "M":
        return f"{date_key.year}-{date_key.month:02d}"
    return date_key.strftime("%Y-%m-%d")


def put(client, key: str, body: bytes) -> int:
    from ingestion.common.r2 import req

    client.put_object(
        Bucket=req("STORAGE_LAKEHOUSE_BUCKET_NAME"),
        Key=key,
        Body=body,
        ContentType="application/json",
        CacheControl="no-cache",
    )
    return len(body)


def kpi_latest(
    metrics: pa.Table,
    measures: list[dict],
    checked_at: dict[str, str],
    kpi_key: str,
    unit_key: str,
    generated_at: str,
) -> dict[str, Any]:
    """latest.json of one KPI from its gold.metrics_<kpi_key> rows; pure (no R2)."""
    measure_rows = pa.table(
        {
            "kpi_key": pa.array([m["kpi_key"] for m in measures], pa.string()),
            "source_system": pa.array([m["source_system"] for m in measures], pa.string()),
            "measure_key_pattern": pa.array([m["measure_key_pattern"] for m in measures], pa.string()),
            "priority": pa.array([int(m["priority"]) for m in measures], pa.int64()),
        }
    )
    ranks = pa.table({"frequency": list(FREQ_RANK), "freq_rank": list(FREQ_RANK.values())})
    con = duckdb.connect()
    con.register("metrics", metrics)
    con.register("measures", measure_rows)
    con.register("ranks", ranks)
    rows = con.execute(
        """
        with kept as (
            select * from metrics
            where geography_level in (select unnest(?)) and value is not null and geography_key <> '_X'
        ),
        prioritised as (
            select k.*, coalesce((
                select min(m.priority) from measures m
                where m.kpi_key = ? and m.source_system = k.source_system
                    and k.measure_key like m.measure_key_pattern
            ), 999) as priority
            from kept k
        ),
        series as (
            select source_system, geography_key, measure_key, frequency,
                min(r.freq_rank) as freq_rank, min(priority) as priority
            from prioritised p join ranks r using (frequency)
            group by all
        ),
        chosen as (
            select * from series
            qualify row_number() over (
                partition by source_system, geography_key order by freq_rank, priority, measure_key
            ) = 1
        ),
        ranked as (
            select p.source_system, p.geography_key, p.geography_level, p.date_key, p.frequency,
                p.value, p.observation_status_key,
                row_number() over (partition by p.source_system, p.geography_key order by p.date_key desc) as n
            from prioritised p join chosen c using (source_system, geography_key, measure_key, frequency)
        )
        select * from ranked where n <= 2
        order by source_system, geography_key, n
        """,
        [list(LEVELS), kpi_key],
    ).fetchall()
    columns = ["source_system", "geography_key", "geography_level", "date_key", "frequency", "value", "status", "n"]
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for row in (dict(zip(columns, r, strict=True)) for r in rows):
        key = (row["source_system"], row["geography_key"])
        if row["n"] == 1:
            latest[key] = {
                "source_key": row["source_system"],
                "geography_key": row["geography_key"],
                "geography_level": LEVELS[row["geography_level"]],
                "period": period_label(row["date_key"], row["frequency"]),
                "value": float(row["value"]),
                "flags": [] if row["status"] in (None, "ok") else [row["status"]],
                "checked_at": checked_at.get(row["source_system"], generated_at),
            }
        else:
            latest[key]["previous_period"] = period_label(row["date_key"], row["frequency"])
            latest[key]["previous_value"] = float(row["value"])
    level_order = {level: i for i, level in enumerate(LEVELS.values())}
    values = sorted(
        latest.values(), key=lambda v: (v["source_key"], level_order[v["geography_level"]], v["geography_key"])
    )
    return {"kpi_key": kpi_key, "unit_key": unit_key, "generated_at": generated_at, "values": values}


def checked(coverage: pa.Table) -> dict[str, str]:
    """Per source of gold.coverage, when it was last checked."""
    result: dict[str, str] = {}
    for row in coverage.select(["source", "checked_at"]).to_pylist():
        if row["checked_at"] is None:
            continue
        stamp = iso(row["checked_at"])
        result[row["source"]] = max(result.get(row["source"], stamp), stamp)
    return result


def main(prefix: str = PREFIX, namespace: str = "gold") -> dict[str, Any]:
    """Write kpi/<kpi_key>/latest.json for every published KPI under prefix."""
    from pyiceberg.exceptions import NoSuchTableError

    from ingestion.common.r2 import catalog, s3
    from ingestion.common.report import annotate

    generated_at = iso(datetime.now(UTC))
    cat = catalog()
    client = s3()
    measures = seed("kpi_measure")
    checked_at = checked(gold(cat, "gold", "coverage"))
    written: dict[str, int] = {}
    missing: list[str] = []
    for kpi in seed("kpi"):
        kpi_key = kpi["kpi_key"]
        if kpi["status"] != "published" or kpi_key in SKIP:
            continue
        try:
            metrics = gold(cat, namespace, f"metrics_{kpi_key}")
        except NoSuchTableError:
            missing.append(kpi_key)
            continue
        latest = kpi_latest(metrics, measures, checked_at, kpi_key, kpi["unit_key"], generated_at)
        body = json.dumps(latest, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(body) > LATEST_LIMIT:
            raise ValueError(f"kpi/{kpi_key}/latest.json is {len(body)} bytes, over {LATEST_LIMIT}")
        put(client, prefix + f"kpi/{kpi_key}/latest.json", body)
        written[kpi_key] = len(latest["values"])
    result = {"written": written, "skipped": sorted(SKIP), "missing": missing}
    annotate("notice", "gold.publish: latest", result)
    return result
