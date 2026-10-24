#!/usr/bin/env python3
"""GLEIF Landing → Bronze → Silver, one golden-copy file at a time (design #875, as Eurostat).

Entity = one declared file (lei2, rr, repex); its newest Landing version is the entity version.
Each Bronze and Silver table holds one entity, as one file, replaced whole (entity_tables):

  02_bronze/gleif/<bronze table>/<raw_sha256>.parquet            every CSV column as text
  03_silver/gleif/<silver table>/<raw_sha256>.<model>.parquet    warehouse/entity/<model>.sql

Bronze columns are named after the CSV header (lower case, non-alphanumerics → underscore); the
first loaded file fixes a table's columns, and a file with another header is rejected (a new
GLEIF CDF version is a deliberate schema change). A rejected file (decoder or Silver model) is
recorded in bronze.gleif_rejected_files and the tables keep the previous version, so a bad
publish never empties them. ``<model>`` is a digest of the Silver SQL: a changed model recomputes
Silver from unchanged Bronze. Prints one JSON result line.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
from typing import Any
import zipfile

import duckdb
import pyarrow.parquet as pq
from pyiceberg.exceptions import NoSuchTableError

from ingestion.common import bronze_files as bf
from ingestion.common import entity_tables as et
from ingestion.common.r2 import archive_key, catalog, raw_chunks, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file
from warehouse import entity

SOURCE = "gleif"
ENTITY = "entity"
# declared file → (Bronze table, Silver table and model, Silver not-null columns, Silver key)
FILES = {
    "lei2": ("gleif_lei_records", "gleif_legal_entities", ("lei", "legal_name", "registered_at"), ("lei",)),
    "rr": ("gleif_relationship_records", "gleif_relationships", ("child_lei", "parent_lei", "relationship_type"), ()),
    "repex": ("gleif_reporting_exceptions", "gleif_reporting_exceptions", ("lei", "exception_category"), ()),
}
REJECTED_ID = ("bronze", "gleif_rejected_files")
PROVENANCE = [
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("source_row_number", "BIGINT"),
]
NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
ARROW_TYPES = {"string": "VARCHAR", "large_string": "VARCHAR", "double": "DOUBLE", "int64": "BIGINT"}
MEMORY = "8GB"


class GleifDecodeError(ValueError):
    """A Landing file cannot satisfy the Bronze contract (recorded, never dropped silently)."""


def column_name(header: str) -> str:
    """CSV header → Bronze column: ``Entity.LegalName.xmllang`` → ``entity_legalname_xmllang``."""
    name = re.sub(r"[^a-z0-9]+", "_", header.strip().lower()).strip("_")
    if not NAME_RE.fullmatch(name):
        raise GleifDecodeError(f"cannot name a column after the header {header!r}")
    return name


def model_digest(model: str) -> str:
    return sha256((Path(entity.__file__).parent / "entity" / f"{model}.sql").read_bytes()).hexdigest()[:12]


def fetch(client, bucket: str, receipt: dict[str, Any], path: Path) -> None:
    """Landing (or Archive) bytes of a receipt to ``path``, checked against its SHA-256 and size."""
    key = receipt["object"]["key"]
    try:
        client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError:
        key = archive_key(SOURCE, receipt["sha256"], key)
    digest, size = sha256(), 0
    with path.open("wb") as handle:
        for block in raw_chunks(client, bucket, key):
            digest.update(block)
            size += len(block)
            handle.write(block)
    if digest.hexdigest() != receipt["sha256"] or size != receipt["size_bytes"]:
        raise RuntimeError(f"GLEIF raw bytes do not match their receipt: {key}")


def decode(raw: Path, receipt: dict[str, Any], output: Path, expected: list[str] | None) -> tuple[int, list[str]]:
    """One ZIP → Parquet: entity, the CSV columns as text, provenance; returns (rows, CSV columns)."""
    try:
        with zipfile.ZipFile(raw) as archive:
            members = [m for m in archive.infolist() if m.filename.lower().endswith(".csv")]
            if len(members) != 1:
                raise GleifDecodeError(f"expected one CSV in the ZIP, found {[m.filename for m in members]}")
            csv_path = output.parent / "data.csv"
            with archive.open(members[0]) as src, csv_path.open("wb") as dst:
                shutil.copyfileobj(src, dst, 16 << 20)
    except zipfile.BadZipFile as exc:
        raise GleifDecodeError(f"not a readable ZIP: {exc}") from exc
    raw.unlink()  # disk: the lei2 CSV is ~5 GB
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit = '{MEMORY}'")
        con.execute(f"SET temp_directory = '{output.parent}'")
        try:
            source = str(csv_path).replace("'", "''")  # a temporary path, quoted for SQL
            con.execute(
                f"CREATE VIEW src AS SELECT * FROM read_csv('{source}', delim=',', quote='\"', escape='\"', "
                "header=true, all_varchar=true, strict_mode=true, encoding='utf-8', sample_size=-1)"
            )
            headers = [r[0] for r in con.execute("DESCRIBE src").fetchall()]
        except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:
            raise GleifDecodeError(f"CSV cannot be read: {str(exc)[:400]}") from exc
        columns = [column_name(h) for h in headers]
        if len(set(columns)) != len(columns) or ENTITY in columns:
            raise GleifDecodeError("CSV headers do not give distinct column names")
        if expected is not None and columns != expected:
            changed = sorted(set(columns) ^ set(expected))[:10]
            raise GleifDecodeError(f"CSV header differs from the Bronze table (CDF change?): {changed}")
        ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        select = ", ".join(
            ["? AS entity"]
            + [f'"{h.replace(chr(34), chr(34) * 2)}" AS {c}' for h, c in zip(headers, columns, strict=True)]
            + [
                "? AS raw_sha256",
                "? AS source_version",
                "?::TIMESTAMPTZ AS ingested_at",
                "(row_number() OVER ())::BIGINT AS source_row_number",
            ]
        )
        target = str(output).replace("'", "''")
        try:  # row groups of 100k rows: a 338-column group of 500k rows does not fit in memory
            con.execute(
                f"COPY (SELECT {select} FROM src) TO '{target}' "
                "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)",
                [receipt["entity"], receipt["sha256"], receipt["version_token"], ingested],
            )
        except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:
            raise GleifDecodeError(f"CSV cannot be read: {str(exc)[:400]}") from exc
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
        if not rows:
            raise GleifDecodeError("the CSV has no rows")
        return int(rows), columns
    finally:
        con.close()
        csv_path.unlink(missing_ok=True)


def silver_columns(path: Path) -> list[tuple[str, str]]:
    """Silver table columns from the first Silver file (types from the model's output)."""
    out = []
    for field in pq.read_schema(path):
        kind = "TIMESTAMPTZ" if str(field.type).startswith("timestamp") else ARROW_TYPES.get(str(field.type))
        if kind is None:
            raise RuntimeError(f"Silver column {field.name} has an unsupported type {field.type}")
        out.append((field.name, kind))
    return out


def held(cat, table_id: tuple[str, str]):
    """(table or None, its recipes for the entity column)."""
    try:
        table = bf.retry(lambda: cat.load_table(table_id), "load_table")
    except NoSuchTableError:
        return None, {}
    return table, et.state(table, ENTITY)


def run_entity(cat, client, lake: str, landing: str, name: str, refused: set[str], out: dict) -> dict | None:
    """Bronze and Silver for one declared file; returns a rejection row or None."""
    bronze_name, silver_name, not_null, key = FILES[name]
    bronze_id, silver_id = ("bronze", bronze_name), ("silver", silver_name)
    digest = model_digest(silver_name)
    receipt = bulk_file.newest(client, landing, SOURCE, name)
    sha = receipt["sha256"]
    bronze, b_state = held(cat, bronze_id)
    silver, s_state = held(cat, silver_id)
    need_bronze = b_state.get(name, {}).get("recipes") != {sha}
    need_silver = need_bronze or s_state.get(name, {}).get("recipes") != {f"{sha}.{digest}"}
    if sha in refused or f"{sha}.{digest}" in refused:
        out["rejected_before"].append(name)
        return None
    if not need_silver:
        out["current"].append(name)
        return None
    started = time.monotonic()
    rejection = {
        "entity": name,
        "raw_sha256": sha,
        "landing_key": receipt["object"]["key"],
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    bronze_key = f"02_bronze/{SOURCE}/{bronze_name}/{sha}.parquet"
    silver_key = f"03_silver/{SOURCE}/{silver_name}/{sha}.{digest}.parquet"
    with tempfile.TemporaryDirectory(prefix="zohelo-gleif-") as temporary:
        work = Path(temporary)
        local_bronze = work / f"{sha}.parquet"
        if need_bronze:
            raw = work / "raw.zip"
            fetch(client, landing, receipt, raw)
            expected = [f.name for f in bronze.schema().fields][1 : -len(PROVENANCE)] if bronze else None
            try:
                _, columns = decode(raw, receipt, local_bronze, expected)
            except GleifDecodeError as exc:
                return {**rejection, "error": f"decoder: {exc}"[:1000]}
        else:
            bronze_uri = next(iter(b_state[name]["paths"]))
        silver_file = work / "silver.parquet"
        try:
            report = entity.apply(
                silver_name,
                str(local_bronze) if need_bronze else bronze_uri,
                silver_file,
                not_null=not_null,
                unique_key=key,
                memory_limit=MEMORY,
                temp_directory=str(work),
            )
        except entity.EntityModelError as exc:  # verdict of this model on this file: <sha>.<model>
            return {**rejection, "raw_sha256": f"{sha}.{digest}", "error": f"silver: {exc}"[:1000]}
        columns_silver = silver_columns(silver_file)
        if silver is None:
            bf.retry(lambda: bf.ensure_table(cat, silver_id, columns_silver, ENTITY), "create_table")
            et.configure(silver_id)
        elif [(f.name, str(f.field_type)) for f in silver.schema().fields] != [
            (n, str(bf.TYPES[k])) for n, k in columns_silver
        ]:  # a changed model needs a deliberate table migration; nothing is uploaded
            raise RuntimeError(f"{'.'.join(silver_id)}: the model's columns differ from the table")
        # Uploads only after both files passed: a rejection leaves no unreferenced object.
        if need_bronze:
            if bronze is None:
                schema = [(ENTITY, "VARCHAR")] + [(c, "VARCHAR") for c in columns] + PROVENANCE
                bf.retry(lambda: bf.ensure_table(cat, bronze_id, schema, ENTITY), "create_table")
                et.configure(bronze_id)
            bronze_uri = bf.upload(client, lake, bronze_key, local_bronze, sha)
        silver_uri = bf.upload(client, lake, silver_key, silver_file, sha)
        stats = [ENTITY, "raw_sha256"]
        # Bronze and Silver are replaced together only once both files exist: a rejection keeps
        # the previous version in both.
        et.replace(cat, bronze_id, ENTITY, {name: [bronze_uri]}, stats, "gleif-entities")
        et.replace(cat, silver_id, ENTITY, {name: [silver_uri]}, stats, "gleif-entities")
    out["loaded"].append(
        {
            "entity": name,
            "rows": report["rows"],
            "version": receipt["version_token"],
            "bronze": "decoded" if need_bronze else "kept",
            "minutes": round((time.monotonic() - started) / 60, 1),
        }
    )
    return None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=100, help="time budget; files not started wait")
    args = parser.parse_args(argv)
    deadline = time.monotonic() + args.minutes * 60
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    refused = bf.rejected_hashes(cat, REJECTED_ID)
    out: dict[str, Any] = {"loaded": [], "current": [], "rejected": [], "rejected_before": [], "not_started": []}
    out["failed"] = []
    rejected = []
    try:
        for spec in bulk_file.declaration(SOURCE):
            name = spec["entity"]
            if time.monotonic() > deadline:
                out["not_started"].append(name)
                continue
            try:
                row = run_entity(cat, client, lake, landing, name, refused, out)
            except Exception as exc:  # not a verdict on the data: the next run tries again
                out["failed"].append([name, f"{type(exc).__name__}: {str(exc)[:300]}"])
                continue
            if row:
                rejected.append(row)
                out["rejected"].append([name, row["error"][:300]])
    finally:
        bf.record_rejections(cat, REJECTED_ID, rejected)
    status = 1 if out["failed"] else 2 if out["not_started"] else 0
    level = "error" if out["failed"] else "warning" if rejected or out["rejected_before"] or status else "notice"
    annotate(level, "gleif entities", out)
    step_summary(f"## gleif · Landing → Bronze → Silver\n\n```json\n{json.dumps(out, indent=2)}\n```\n")
    print(json.dumps(out, default=str))
    return status


def verify() -> int:
    """Each declared file: its newest Landing version is in Bronze and Silver, or rejected."""
    client, cat = s3(), bf.retry(catalog, "catalog")
    landing = req("R2_LANDING_BUCKET")
    refused = bf.rejected_hashes(cat, REJECTED_ID)
    report, failed = {}, False
    for name, (bronze_name, silver_name, _, _) in FILES.items():
        sha = bulk_file.newest(client, landing, SOURCE, name)["sha256"]
        digest = model_digest(silver_name)
        _, b_state = held(cat, ("bronze", bronze_name))
        _, s_state = held(cat, ("silver", silver_name))
        b_ok = b_state.get(name, {}).get("recipes") == {sha}
        s_ok = s_state.get(name, {}).get("recipes") == {f"{sha}.{digest}"}
        rejected = sha in refused or f"{sha}.{digest}" in refused
        report[name] = {
            "bronze": b_ok,
            "silver": s_ok,
            "rejected": rejected,
            "bronze_rows": b_state.get(name, {}).get("rows"),
            "silver_rows": s_state.get(name, {}).get("rows"),
        }
        failed |= not (b_ok and s_ok) and not rejected
    annotate("error" if failed else "notice", "gleif completeness (Landing → Bronze → Silver)", report)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
