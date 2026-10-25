"""gold.coverage: what each source publishes and what the platform holds, block by block.

One row per block a source publishes (a Eurostat dataset, a BDL subgroup, a DBW file, a declared
file of a bulk source…) or that the platform holds, with its versions, its layers and a status:

  loaded                  newest landed version is in Bronze and Silver
  loading                 landed, but Bronze or Silver does not hold that version yet
  outdated                in Silver, but the source publishes a newer version than landed
  waiting                 published by the source, not landed yet
  rejected                the decoder or the Silver model rejected the newest version
  published without data  the source lists it, with no values
  excluded                published, but outside the platform's scope (reason says why)
  no longer published     held by the platform, but the source's full catalogue no longer lists it

Each source's ``bronze.verify`` writes its own rows after its run (one file per source, replaced
whole: entity_tables), so the table is current after every pipeline run of that source. Built
from the source's catalogue and table metadata only, never from the data.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
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
    excluded: dict[str, str] | None = None,
    catalogue_complete: bool = False,
) -> list[dict[str, Any]]:
    """Coverage rows of one source.

    ``catalogue``: block id -> name, every block the source publishes (an empty dict when the
    source has no catalogue: then the landed blocks are the list); ``expected`` /
    ``expected_silver``: block -> recipe of its newest landed version in Bronze / Silver (as for
    entity_tables.completeness); ``bronze`` / ``silver``: entity_tables.state of the tables;
    ``landed`` / ``published``: block -> version token; ``rejected`` / ``without_data``: blocks;
    ``excluded``: block -> why it is outside the scope (a block landed anyway is reported as held);
    ``catalogue_complete``: the catalogue is the source's whole list, so a held block missing from
    it is ``no longer published`` (the platform keeps its data: policy #822 decides deletions)."""
    landed, published, reasons = landed or {}, published or {}, dict(reasons or {})
    rejected, without_data, excluded = rejected or set(), without_data or set(), excluded or {}
    now = datetime.now(UTC)
    out = []
    blocks = set(catalogue) | set(expected) | rejected | without_data | set(excluded)
    for block in sorted(b for b in blocks if b):
        in_bronze = block in expected and bronze.get(block, {}).get("recipes") == {expected[block]}
        in_silver = in_bronze and silver.get(block, {}).get("recipes") == {expected_silver.get(block)}
        if block in without_data:
            status = "published without data"
        elif block in rejected:
            status = "rejected"
        elif block in excluded and block not in expected:
            status = "excluded"
            reasons.setdefault(block, excluded[block])
        elif block not in expected:
            status = "waiting"
        elif catalogue_complete and catalogue and block not in catalogue:
            status = "no longer published"
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
    columns stay empty (not tracked per file) and ``loaded`` means in Bronze.

    The newest version is chosen per snapshot path, as ``r2.latest_snapshot`` does: identical
    bytes can sit under several entities, so one digest can be the newest of more than one."""
    del latest  # recomputed per path below (the digest set alone loses the entity)
    newest: dict[str, tuple[tuple[str, str], str, dict[str, Any]]] = {}
    for digest, item in meta.items():
        for path in item.get("snapshot_paths", []):
            if path["area"] != "data":
                continue
            order = (path["version"], path["ingested"])
            if path["entity"] not in newest or order > newest[path["entity"]][0]:
                newest[path["entity"]] = (order, digest, path)
    now = datetime.now(UTC)
    out = []
    for entity in sorted(newest, key=str):
        (version, _), digest, path = newest[entity]
        in_bronze = digest in loaded
        status = "loaded" if in_bronze else "rejected" if digest in rejected else "loading"
        out.append(
            {
                "source": source,
                "block_id": str(entity),
                "block_name": str(path.get("key") or meta[digest]["key"]).rsplit("/", 1)[-1],
                "published_version": None,
                "landed_version": version,
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
    from pyiceberg.exceptions import NoSuchTableError

    cat, client = bf.retry(catalog, "catalog"), s3()
    try:
        bf.retry(lambda: cat.load_table(TABLE), "coverage_table")
    except NoSuchTableError:  # first write: create and configure once, not on every verify
        bf.retry(lambda: bf.ensure_table(cat, TABLE, COLUMNS, "source"), "coverage_table")
        et.configure(TABLE)
    with tempfile.TemporaryDirectory(prefix="coverage-") as tmp:
        path = Path(tmp) / "coverage.parquet"
        pq.write_table(table, path, compression="zstd")
        # named by the bytes written (checked_at included): a rerun never overwrites a file the
        # table already references, and the same bytes are the same object
        digest = sha256(path.read_bytes()).hexdigest()
        uri = bf.upload(client, req("R2_LAKEHOUSE_BUCKET"), f"{PREFIX}{source}/{digest}.parquet", path, digest)
    et.replace(cat, TABLE, "source", {source: [uri]}, ["source"], "coverage")
    counts: dict[str, int] = {}
    for r in records:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return {"blocks": len(records), **dict(sorted(counts.items()))}


def publish(source: str, build: Callable[[], list[dict[str, Any]]]) -> None:
    """Build the rows (``build``: reads the catalogue, calls ``rows``), write them and annotate the
    counts. Any failure, in building or writing, is a warning: it never fails the verify."""
    from ingestion.common.report import annotate

    try:
        annotate("notice", f"{source} coverage", write(source, build()))
    except Exception as exc:  # coverage is a report: it must not turn a passed verify into a failure
        annotate("warning", f"{source} coverage not written", {"error": f"{type(exc).__name__}: {str(exc)[:300]}"})
