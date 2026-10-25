"""gold.coverage: what each source publishes and what the platform holds, block by block.

One row per block a source publishes (a Eurostat dataset, a BDL subgroup, a DBW file, a declared
file of a bulk source…) or that the platform holds, with its versions, its layers and a status:

  loaded                  newest landed version is in Bronze and Silver
  loading                 landed, but Bronze or Silver does not hold that version yet
  outdated                in Silver, but the source publishes a newer version than landed
  waiting                 published by the source, not landed yet
  rejected                the decoder or the Silver model rejected the newest version
  published without data  the source lists it, with no values

Each source's ``bronze.verify`` writes its own rows after its run (one file per source, replaced
whole: entity_tables), so the table is current after every pipeline run of that source. Built
from the source's catalogue and table metadata only, never from the data.
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
from pathlib import Path
import tempfile
from typing import Any

TABLE = ("gold", "coverage")
PREFIX = "04_gold/coverage/"
COLUMNS = [
    ("source", "VARCHAR"),
    ("block_id", "VARCHAR"),
    ("block_name", "VARCHAR"),
    ("published_version", "VARCHAR"),
    ("landed_version", "VARCHAR"),
    ("in_bronze", "BOOLEAN"),
    ("in_silver", "BOOLEAN"),
    ("silver_rows", "BIGINT"),
    ("status", "VARCHAR"),
    ("reason", "VARCHAR"),
    ("checked_at", "TIMESTAMPTZ"),
]


def rows(
    source: str,
    catalogue: dict[str, str | None],
    expected: dict[str, str],
    expected_silver: dict[str, str],
    bronze: dict[str, dict[str, Any]],
    silver: dict[str, dict[str, Any]],
    landed: dict[str, str] | None = None,
    published: dict[str, str] | None = None,
    rejected: set[str] | None = None,
    without_data: set[str] | None = None,
    reasons: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Coverage rows of one source.

    ``catalogue``: block id -> name, every block the source publishes (an empty dict when the
    source has no catalogue: then the landed blocks are the list); ``expected`` /
    ``expected_silver``: block -> recipe of its newest landed version in Bronze / Silver (as for
    entity_tables.completeness); ``bronze`` / ``silver``: entity_tables.state of the tables;
    ``landed`` / ``published``: block -> version token; ``rejected`` / ``without_data``: blocks."""
    landed, published, reasons = landed or {}, published or {}, reasons or {}
    rejected, without_data = rejected or set(), without_data or set()
    now = datetime.now(UTC)
    out = []
    for block in sorted(set(catalogue) | set(expected) | rejected | without_data):
        in_bronze = block in expected and bronze.get(block, {}).get("recipes") == {expected[block]}
        in_silver = in_bronze and silver.get(block, {}).get("recipes") == {expected_silver.get(block)}
        if block in without_data:
            status = "published without data"
        elif block in rejected:
            status = "rejected"
        elif block not in expected:
            status = "waiting"
        elif not in_silver:
            status = "loading"
        elif published.get(block) and landed.get(block) and published[block] > landed[block]:
            status = "outdated"
        else:
            status = "loaded"
        out.append(
            {
                "source": source,
                "block_id": block,
                "block_name": catalogue.get(block),
                "published_version": published.get(block),
                "landed_version": landed.get(block),
                "in_bronze": in_bronze,
                "in_silver": in_silver,
                "silver_rows": silver.get(block, {}).get("rows") if in_silver else None,
                "status": status,
                "reason": reasons.get(block),
                "checked_at": now,
            }
        )
    return out


def lineage_rows(
    source: str, meta: dict[str, dict[str, Any]], latest: set[str], loaded: set[str], rejected: set[str]
) -> list[dict[str, Any]]:
    """Coverage rows of a lineage source (NBP, WDI, TERYT, PRG): one per Landing entity, its
    newest version against Bronze. Their Silver models rebuild whole from Bronze, so the Silver
    columns stay empty (not tracked per file) and ``loaded`` means in Bronze."""
    now = datetime.now(UTC)
    out = []
    for digest in sorted(latest, key=lambda d: str(meta[d].get("entity"))):
        item = meta[digest]
        in_bronze = digest in loaded
        status = "loaded" if in_bronze else "rejected" if digest in rejected else "loading"
        out.append(
            {
                "source": source,
                "block_id": str(item.get("entity") or item["key"]),
                "block_name": item["key"].rsplit("/", 1)[-1],
                "published_version": None,
                "landed_version": item.get("version"),
                "in_bronze": in_bronze,
                "in_silver": None,
                "silver_rows": None,
                "status": status,
                "reason": "Silver rebuilt whole from Bronze: not tracked per file",
                "checked_at": now,
            }
        )
    return out


def write(source: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    """Replace the source's rows in gold.coverage; returns the counts by status (for annotations)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from ingestion.common import bronze_files as bf
    from ingestion.common import entity_tables as et
    from ingestion.common.r2 import catalog, req, s3

    schema = pa.schema(
        [
            (
                name,
                {"VARCHAR": pa.string(), "BOOLEAN": pa.bool_(), "BIGINT": pa.int64()}.get(
                    kind, pa.timestamp("us", tz="UTC")
                ),
            )
            for name, kind in COLUMNS
        ]
    )
    table = pa.Table.from_pylist(records, schema=schema)
    digest = sha256(
        json.dumps(
            [{k: v for k, v in r.items() if k != "checked_at"} for r in records], default=str, sort_keys=True
        ).encode()
        + datetime.now(UTC).strftime("%Y%m%dT%H").encode()
    ).hexdigest()
    cat, client = bf.retry(catalog, "catalog"), s3()
    bf.retry(lambda: bf.ensure_table(cat, TABLE, COLUMNS, "source"), "coverage_table")
    et.configure(TABLE)
    with tempfile.TemporaryDirectory(prefix="coverage-") as tmp:
        path = Path(tmp) / "coverage.parquet"
        pq.write_table(table, path, compression="zstd")
        uri = bf.upload(client, req("R2_LAKEHOUSE_BUCKET"), f"{PREFIX}{source}/{digest}.parquet", path, digest)
    et.replace(cat, TABLE, "source", {source: [uri]}, ["source"], "coverage")
    counts: dict[str, int] = {}
    for r in records:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return {"blocks": len(records), **dict(sorted(counts.items()))}


def publish(source: str, records: list[dict[str, Any]]) -> None:
    """Write the rows and annotate the counts; a failure is reported, never fails the verify."""
    from ingestion.common.report import annotate

    try:
        annotate("notice", f"{source} coverage", write(source, records))
    except Exception as exc:  # coverage is a report: it must not turn a passed verify into a failure
        annotate("warning", f"{source} coverage not written", {"error": f"{type(exc).__name__}: {str(exc)[:300]}"})
