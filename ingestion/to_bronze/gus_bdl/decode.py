"""Decode one BDL subgroup export (website ZIP with one ';' CSV) into source-shaped Bronze.

CSV ("CSV – tablica relacyjna"): Kod;Nazwa;<subgroup dimensions…>;Rok;Wartosc;Jednostka miary;Atrybut;
Bronze keeps the source as published: one row per CSV row, dimension labels as JSON arrays
(names once per file, values per row), the value lexeme as text (decimal comma kept) and
the attribute label. No casting; Silver maps rows to API variable ids and types values.
"""

from __future__ import annotations

import csv
from hashlib import sha256
import io
import json
from pathlib import Path
from typing import Any
import zipfile

import duckdb

BRONZE_COLUMNS = (
    ("subgroup_id", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("raw_sha256", "VARCHAR"),
    ("channel", "VARCHAR"),
    ("part_index", "INTEGER"),
    ("unit_code", "VARCHAR"),
    ("unit_name", "VARCHAR"),
    ("dimension_names", "VARCHAR"),
    ("dimension_values", "VARCHAR"),
    ("variable_id", "BIGINT"),
    ("year", "VARCHAR"),
    ("value_text", "VARCHAR"),
    ("measure_unit", "VARCHAR"),
    ("attribute", "VARCHAR"),
    ("source_row_number", "BIGINT"),
)
COLUMN_NAMES = tuple(name for name, _ in BRONZE_COLUMNS)
FIXED_HEAD, FIXED_TAIL = ["Kod", "Nazwa"], ["Rok", "Wartosc", "Jednostka miary", "Atrybut"]


class BdlDecodeError(ValueError):
    """The file cannot satisfy the Bronze contract (recorded with the reason, never dropped)."""


class BdlEmptyExport(BdlDecodeError):
    """A valid export with no rows: its territory selection has no data (a part, or a whole subgroup)."""


def decode_web_export(source: Path, receipt: dict[str, Any], output: Path, memory_limit: str = "8GB") -> dict:
    data = source.read_bytes()
    if sha256(data).hexdigest() != receipt["sha256"] or len(data) != receipt["size_bytes"]:
        raise BdlDecodeError("BDL export does not match its receipt")
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as exc:
        raise BdlDecodeError(f"not a ZIP: {exc}") from exc
    members = [m for m in archive.infolist() if m.filename.lower().endswith(".csv")]
    if len(members) != 1:
        raise BdlDecodeError(f"expected one CSV in the export, found {len(members)}")
    work = output.parent / "export.csv"
    with archive.open(members[0]) as src, work.open("wb") as dst:
        while block := src.read(8 << 20):
            dst.write(block)
    with work.open("r", encoding="utf-8-sig", newline="") as handle:
        header = next(csv.reader(handle, delimiter=";"))
    if header and header[-1] == "":
        header = header[:-1]
    if header[:2] != FIXED_HEAD or header[-4:] != FIXED_TAIL or len(header) < 6:
        raise BdlDecodeError(f"unexpected BDL CSV header: {header[:8]}")
    dims = header[2:-4]
    names = {f"c{i}": "VARCHAR" for i in range(len(header) + 1)}  # + trailing empty column
    database = output.parent / "work.duckdb"
    try:
        with duckdb.connect(str(database)) as con:
            con.execute(f"SET memory_limit='{memory_limit}'; SET threads=2; SET preserve_insertion_order=true")
            con.execute(
                "CREATE TABLE src AS SELECT * FROM read_csv(?, auto_detect=false, delim=';', quote='\"', "
                "escape='\"', header=false, skip=1, columns=?, nullstr='\x1f', null_padding=true, "
                "strict_mode=false, parallel=false, max_line_size=8000000)",
                [str(work), names],
            )
            n = len(header)
            dim_values = (
                "to_json([" + ", ".join(f"c{i}" for i in range(2, 2 + len(dims))) + "])::VARCHAR" if dims else "'[]'"
            )
            escaped = str(output).replace("'", "''")
            con.execute(
                f"""
                COPY (SELECT ?::VARCHAR AS subgroup_id, ?::VARCHAR AS source_version,
                             ?::TIMESTAMPTZ AS ingested_at, ?::VARCHAR AS raw_sha256,
                             'web_export' AS channel, ?::INTEGER AS part_index,
                             c0 AS unit_code, c1 AS unit_name, ?::VARCHAR AS dimension_names,
                             {dim_values} AS dimension_values, NULL::BIGINT AS variable_id,
                             c{n - 4} AS year, c{n - 3} AS value_text, c{n - 2} AS measure_unit,
                             nullif(c{n - 1}, '') AS attribute, rowid + 2 AS source_row_number
                      FROM src)
                TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)""",
                [
                    receipt["subgroup_id"],
                    receipt.get("version_token"),
                    receipt["ingested_at"],
                    receipt["sha256"],
                    int((receipt.get("part") or {}).get("index", 1)),
                    json.dumps(dims, ensure_ascii=False, separators=(",", ":")),
                ],
            )
            # A row with more fields than the header names (a dimension column without a
            # header, e.g. P3018 of 2014) shifts every column after it: the year column then
            # holds dimension labels. Such files are rejected, never loaded shifted (#875).
            rows, bad, shifted = con.execute(
                f"SELECT count(*), count(*) FILTER (WHERE c0 IS NULL OR c{n - 4} IS NULL), "
                f"count(*) FILTER (WHERE NOT regexp_full_match(trim(c{n - 4}), '[0-9]{{4}}') "
                f"OR coalesce(c{n}, '') <> '') FROM src"
            ).fetchone()
    except (
        duckdb.OutOfMemoryException,
        duckdb.IOException,
        duckdb.HTTPException,
        duckdb.InterruptException,
        duckdb.FatalException,
        duckdb.InternalException,
    ) as exc:
        raise RuntimeError(f"BDL decode could not run (not a verdict on the file): {str(exc)[:500]}") from exc
    except duckdb.Error as exc:
        raise BdlDecodeError(f"invalid BDL CSV: {str(exc)[:500]}") from exc
    finally:
        database.unlink(missing_ok=True)
        work.unlink(missing_ok=True)
    if rows == 0:
        raise BdlEmptyExport("BDL export has no rows")
    if bad:
        raise BdlDecodeError(f"{bad} rows without unit code or year")
    if shifted:
        raise BdlDecodeError(f"{shifted} rows do not match the header (more fields than named, or year not YYYY)")
    return {"rows": rows, "dimensions": dims, "output_bytes": output.stat().st_size}


def _timestamp(value: str):
    from datetime import UTC, datetime

    text = str(value)
    if len(text) == 16 and text.endswith("Z") and "T" in text:  # YYYYMMDDTHHMMSSZ (API refresh)
        return datetime.strptime(text, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def decode_api_page(source: Path, receipt: dict[str, Any], output: Path) -> dict:
    """One BDL API page (/data/by-variable, landed by to_landing/gus_bdl/refresh.py) → the same Bronze
    shape as a website export: one row per unit × year of the page's variable. Dimension labels,
    measure unit and dimension names come from the receipt (stored when the page was landed)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    data = source.read_bytes()
    if sha256(data).hexdigest() != receipt["sha256"] or len(data) != receipt["size_bytes"]:
        raise BdlDecodeError("BDL API page does not match its receipt")
    try:
        body = json.loads(data)
    except json.JSONDecodeError as exc:
        raise BdlDecodeError(f"BDL API page is not JSON: {exc}") from exc
    variable = receipt.get("variable") or {}
    if not variable.get("id"):
        raise BdlDecodeError("BDL API receipt has no variable descriptor")
    dims = json.dumps(receipt.get("dimension_names") or [], ensure_ascii=False, separators=(",", ":"))
    labels = json.dumps(variable.get("labels") or [], ensure_ascii=False, separators=(",", ":"))
    ingested = _timestamp(receipt["ingested_at"])
    rows, n = [], 0
    for unit in body.get("results") or []:
        for value in unit.get("values") or []:
            n += 1
            year, val = value.get("year"), value.get("val", value.get("value"))
            if not unit.get("id") or year is None:
                raise BdlDecodeError(f"BDL API row {n} without unit id or year")
            rows.append(
                {
                    "subgroup_id": receipt["subgroup_id"],
                    "source_version": receipt.get("version_token"),
                    "ingested_at": ingested,
                    "raw_sha256": receipt["sha256"],
                    "channel": "api",
                    "part_index": int((receipt.get("part") or {}).get("index", 1)),
                    "unit_code": str(unit["id"]),
                    "unit_name": unit.get("name"),
                    "dimension_names": dims,
                    "dimension_values": labels,
                    "variable_id": int(variable["id"]),
                    "year": str(year),
                    "value_text": None if val is None else (str(int(val)) if float(val).is_integer() else str(val)),
                    "measure_unit": variable.get("measure_unit"),
                    # API attribute id as text (1 = no attribute, dropped); the website export carries its
                    # label instead, so Silver must not compare attributes across channels
                    "attribute": None if value.get("attrId") in (None, 1) else str(value.get("attrId")),
                    "source_row_number": n,
                }
            )
    if not rows:
        raise BdlEmptyExport("BDL API page has no rows")
    types = {
        "VARCHAR": pa.string(),
        "BIGINT": pa.int64(),
        "INTEGER": pa.int32(),
        "TIMESTAMPTZ": pa.timestamp("us", tz="UTC"),
    }
    schema = pa.schema([(name, types[kind]) for name, kind in BRONZE_COLUMNS])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), output, compression="zstd")
    return {"rows": len(rows), "output_bytes": output.stat().st_size}
