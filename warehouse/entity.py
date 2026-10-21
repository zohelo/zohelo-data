"""Apply a per-entity Silver model (warehouse/entity/<model>.sql) to one Bronze file (design #875).

The model is plain SQL that selects from the relation ``bronze`` (the one Bronze file of the
entity version); small lookup tables can be passed as extra relations. The result is written
as one ZSTD Parquet file, which the loader then puts in place of the entity's partition in
Silver. Checks run on the result before it is accepted: the row count matches the rule of the
model (``same_rows``: one Silver row per Bronze row), the listed columns are never null, the
grain (``unique_key``) holds, and no row meets ``reject_if``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from ingestion.common.r2 import req

MODELS = Path(__file__).parent / "entity"


class EntityModelError(ValueError):
    """The model's result breaks its contract for this entity (the entity is rejected, not loaded)."""


class EntityRunError(RuntimeError):
    """The model could not run (storage, memory, I/O): not a verdict on the data; retried next run."""


def _connect(memory_limit: str, temp_directory: str | None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute("SET preserve_insertion_order = false")
    if temp_directory:
        con.execute(f"SET temp_directory = '{temp_directory}'")
    return con


def _remote(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSTALL httpfs; LOAD httpfs")
    endpoint = req("R2_S3_ENDPOINT").replace("https://", "").rstrip("/")
    con.execute(
        "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [req("CLOUDFLARE_R2_ACCESS_KEY_ID"), req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"), endpoint],
    )


def apply(
    model: str,
    bronze: str,
    output: Path,
    *,
    not_null: tuple[str, ...] = (),
    same_rows: bool = True,
    unique_key: tuple[str, ...] = (),
    reject_if: str = "",
    lookups: dict[str, str] | None = None,
    memory_limit: str = "10GB",
    temp_directory: str | None = None,
) -> dict[str, Any]:
    """Run warehouse/entity/<model>.sql over one Bronze file (local path or s3:// URI)."""
    sql = (MODELS / f"{model}.sql").read_text(encoding="utf-8")
    output = Path(output)
    if output.exists():
        raise EntityModelError(f"{output} must be a fresh path")
    con = _connect(memory_limit, temp_directory)
    try:
        if bronze.startswith("s3://") or any(str(v).startswith("s3://") for v in (lookups or {}).values()):
            _remote(con)
        con.execute(f"CREATE VIEW bronze AS SELECT * FROM read_parquet('{bronze.replace(chr(39), '')}')")
        for name, path in (lookups or {}).items():
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{str(path).replace(chr(39), '')}')")
        target = str(output).replace("'", "''")
        con.execute(f"COPY ({sql}) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)")
        bronze_rows = con.execute("SELECT count(*) FROM bronze").fetchone()[0]
        checks = ", ".join([f"count(*) FILTER (WHERE {c} IS NULL)" for c in not_null] or ["0"])
        rows, *nulls = con.execute(f"SELECT count(*), {checks} FROM read_parquet('{target}')").fetchone()
        keys = (
            con.execute(
                f"SELECT count(*) FROM (SELECT DISTINCT {', '.join(unique_key)} FROM read_parquet('{target}'))"
            ).fetchone()[0]
            if unique_key
            else rows
        )
        flagged = (
            con.execute(f"SELECT count(*) FROM read_parquet('{target}') WHERE {reject_if}").fetchone()[0]
            if reject_if
            else 0
        )
        sample = (
            con.execute(f"SELECT * FROM read_parquet('{target}') WHERE {reject_if} LIMIT 3").fetchall()
            if flagged
            else []
        )
    except duckdb.Error as exc:
        output.unlink(missing_ok=True)
        raise EntityRunError(f"{model}: {str(exc)[:600]}") from exc
    finally:
        con.close()
    problems = [f"{c} null in {n} rows" for c, n in zip(not_null, nulls[: len(not_null)], strict=True) if n]
    if same_rows and rows != bronze_rows:
        problems.append(f"{rows} Silver rows for {bronze_rows} Bronze rows")
    if keys != rows:
        problems.append(f"{rows - keys} rows repeat the key ({', '.join(unique_key)})")
    if flagged:
        problems.append(f"{flagged} rows where {reject_if}, e.g. {[list(map(str, r)) for r in sample]}")
    if problems:
        output.unlink(missing_ok=True)
        raise EntityModelError(f"{model}: " + "; ".join(problems))
    return {"rows": rows, "bronze_rows": bronze_rows, "bytes": output.stat().st_size}
