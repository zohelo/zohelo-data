#!/usr/bin/env python3
"""GUNB RWDZ Landing → Bronze → Silver, one producer file at a time (design #875, as GLEIF).

Entity = one declared file of https://wyszukiwarka.gunb.gov.pl/pobranie.html (one voivodeship's applications and
decisions on building permits); its newest Landing version is the entity version, replaced whole in each table
(entity_tables):

  02_bronze/gunb_rwdz/gunb_rwdz_records/<raw_sha256>.parquet               decode(): allowed columns as text
  03_silver/gunb_rwdz/gunb_rwdz_records/<raw_sha256>.<model>.parquet       warehouse/entity/gunb_rwdz_records.sql

Personal data stays out of Bronze: the decoder keeps only the columns in KEEP (record numbers, dates, authority,
kind of works, object category, TERYT codes) and checks that the file has exactly the columns of HEADER, so a new
column (possibly naming a person) rejects the file instead of reaching Bronze. Investor and designer names, street
addresses, postcodes, plot and precinct numbers and the free-text name of the works are never read into a table.
A rejected file (decoder or Silver model) is recorded in bronze.gunb_rwdz_rejected_files and the tables keep the
previous version. ``<model>`` is a digest of the Silver SQL: a changed model recomputes Silver from unchanged
Bronze. ``landing_root`` and ``namespace`` exist for Dev trials (zz_dev_ scratch locations). Prints one JSON line.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
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

SOURCE = "gunb_rwdz"
ENTITY = "entity"
BRONZE, SILVER, REJECTED = "gunb_rwdz_records", "gunb_rwdz_records", "gunb_rwdz_rejected_files"
# The producer's header (pobranie.html: '#' separates the columns, UTF-8), in order. A file with another header
# is rejected: a new column could name a person.
HEADER: list[str] = []
# Columns read into Bronze (every other column is dropped before Bronze; see the module docstring).
KEEP: list[str] = []
NOT_NULL = ("record_id", "raw_sha256")
KEY = ("record_id",)
PROVENANCE = [
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("source_row_number", "BIGINT"),
]
BRONZE_COLUMNS = [(ENTITY, "VARCHAR"), *[(c, "VARCHAR") for c in KEEP], *PROVENANCE]
ARROW_TYPES = {"string": "VARCHAR", "large_string": "VARCHAR", "double": "DOUBLE", "int64": "BIGINT",
               "int32": "INTEGER", "date32[day]": "DATE", "bool": "BOOLEAN"}  # fmt: skip
MEMORY = "6GB"


class RwdzDecodeError(ValueError):
    """A Landing file cannot satisfy the Bronze contract (recorded, never dropped silently)."""


def model_digest() -> str:
    return sha256((Path(entity.__file__).parent / "entity" / f"{SILVER}.sql").read_bytes()).hexdigest()[:12]


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
        raise RuntimeError(f"RWDZ raw bytes do not match their receipt: {key}")


def decode(raw: Path, receipt: dict[str, Any], output: Path) -> int:
    """One ZIP (one CSV) → Parquet with the KEEP columns as text plus provenance; returns the row count."""
    try:
        with zipfile.ZipFile(raw) as archive:
            members = [m for m in archive.infolist() if not m.is_dir()]
            if len(members) != 1 or not members[0].filename.lower().endswith(".csv"):
                raise RwdzDecodeError(f"expected one CSV in the ZIP, found {[m.filename for m in members]}")
            csv_path = output.parent / "data.csv"
            with archive.open(members[0]) as src, csv_path.open("wb") as dst:
                while block := src.read(16 << 20):
                    dst.write(block)
    except zipfile.BadZipFile as exc:
        raise RwdzDecodeError(f"not a readable ZIP: {exc}") from exc
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit = '{MEMORY}'")
        con.execute(f"SET temp_directory = '{output.parent}'")
        source = str(csv_path).replace("'", "''")
        try:
            con.execute(
                f"CREATE VIEW src AS SELECT * FROM read_csv('{source}', delim='#', quote='', escape='', header=true, "
                "all_varchar=true, strict_mode=true, encoding='utf-8', sample_size=-1, null_padding=false)"
            )
            headers = [r[0] for r in con.execute("DESCRIBE src").fetchall()]
        except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:
            raise RwdzDecodeError(f"CSV cannot be read: {str(exc)[:400]}") from exc
        if headers != HEADER:
            changed = sorted(set(headers) ^ set(HEADER))[:10]
            raise RwdzDecodeError(f"unknown layout: the header differs from the known RWDZ header {changed or headers[:5]}")
        ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        select = ", ".join(
            ["? AS entity"]
            + [f'nullif(trim("{c}"), \'\') AS "{c}"' for c in KEEP]
            + [
                "? AS raw_sha256",
                "? AS source_version",
                "?::TIMESTAMPTZ AS ingested_at",
                "(row_number() OVER ())::BIGINT AS source_row_number",
            ]
        )
        target = str(output).replace("'", "''")
        try:
            con.execute(
                f"COPY (SELECT {select} FROM src) TO '{target}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 200000)",
                [receipt["entity"], receipt["sha256"], receipt["version_token"], ingested],
            )
        except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:
            raise RwdzDecodeError(f"CSV cannot be read: {str(exc)[:400]}") from exc
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
        if not rows:
            raise RwdzDecodeError("the CSV has no rows")
        return int(rows)
    finally:
        con.close()
        csv_path.unlink(missing_ok=True)


def table_columns(path: Path) -> list[tuple[str, str]]:
    out = []
    for field in pq.read_schema(path):
        kind = "TIMESTAMPTZ" if str(field.type).startswith("timestamp") else ARROW_TYPES.get(str(field.type))
        if kind is None or kind not in bf.TYPES:
            raise RuntimeError(f"Silver column {field.name} has an unsupported type {field.type}")
        out.append((field.name, kind))
    return out


def held(cat, table_id: tuple[str, str]):
    try:
        table = bf.retry(lambda: cat.load_table(table_id), "load_table")
    except NoSuchTableError:
        return None, {}
    return table, et.state(table, ENTITY)


class Where:
    """Production locations, or a Dev trial's scratch ones (namespace and prefixes zz_dev_*)."""

    def __init__(self, landing_root: str = "01_landing", namespace: str | None = None):
        self.landing_root = landing_root
        self.bronze = (namespace or "bronze", BRONZE)
        self.silver = (namespace or "silver", SILVER)
        self.rejected = (namespace or "bronze", REJECTED)
        self.bronze_key = f"{namespace}/02_bronze" if namespace else "02_bronze"
        self.silver_key = f"{namespace}/03_silver" if namespace else "03_silver"

    def newest(self, client, landing: str, name: str) -> dict[str, Any] | None:
        held_copies = bulk_file.landed(client, landing, bulk_file.prefix(SOURCE, name, self.landing_root))
        return held_copies[-1] if held_copies else None


def run_entity(cat, client, lake: str, landing: str, name: str, refused: set[str], out: dict, where: Where):
    """Bronze and Silver for one declared file; returns a rejection row or None."""
    digest = model_digest()
    receipt = where.newest(client, landing, name)
    if receipt is None:
        out["not_landed"].append(name)
        return None
    sha = receipt["sha256"]
    bronze, b_state = held(cat, where.bronze)
    silver, s_state = held(cat, where.silver)
    need_bronze = b_state.get(name, {}).get("recipes") != {sha}
    need_silver = need_bronze or s_state.get(name, {}).get("recipes") != {f"{sha}.{digest}"}
    if sha in refused or f"{sha}.{digest}" in refused:
        out["rejected_before"].append(name)
        return None
    if not need_silver:
        out["current"].append(name)
        return None
    started = time.monotonic()
    rejection = {"entity": name, "raw_sha256": sha, "landing_key": receipt["object"]["key"],
                 "code_sha": os.environ.get("GITHUB_SHA", "")}  # fmt: skip
    with tempfile.TemporaryDirectory(prefix="zohelo-rwdz-") as temporary:
        work = Path(temporary)
        local_bronze = work / f"{sha}.parquet"
        if need_bronze:
            raw = work / "raw.zip"
            fetch(client, landing, receipt, raw)
            try:
                decode(raw, receipt, local_bronze)
            except RwdzDecodeError as exc:
                return {**rejection, "error": f"decoder: {exc}"[:1000]}
            finally:
                raw.unlink(missing_ok=True)
            bronze_input = str(local_bronze)
        else:
            bronze_input = next(iter(b_state[name]["paths"]))
        silver_file = work / "silver.parquet"
        try:
            report = entity.apply(SILVER, bronze_input, silver_file, not_null=NOT_NULL, unique_key=KEY,
                                  memory_limit=MEMORY, temp_directory=str(work))  # fmt: skip
        except entity.EntityModelError as exc:  # verdict of this model on this file: <sha>.<model>
            return {**rejection, "raw_sha256": f"{sha}.{digest}", "error": f"silver: {exc}"[:1000]}
        columns = table_columns(silver_file)
        if silver is None:
            bf.retry(lambda: bf.ensure_table(cat, where.silver, columns, ENTITY), "create_table")
            et.configure(where.silver)
        elif [(f.name, str(f.field_type)) for f in silver.schema().fields] != [
            (n, str(bf.TYPES[k])) for n, k in columns
        ]:  # a changed model needs a deliberate table migration; nothing is uploaded
            raise RuntimeError(f"{'.'.join(where.silver)}: the model's columns differ from the table")
        # Uploads only after both files passed: a rejection leaves no unreferenced object.
        if need_bronze:
            if bronze is None:
                bf.retry(lambda: bf.ensure_table(cat, where.bronze, BRONZE_COLUMNS, ENTITY), "create_table")
                et.configure(where.bronze)
            bronze_input = bf.upload(client, lake, f"{where.bronze_key}/{SOURCE}/{BRONZE}/{sha}.parquet",
                                     local_bronze, sha)  # fmt: skip
        silver_uri = bf.upload(client, lake, f"{where.silver_key}/{SOURCE}/{SILVER}/{sha}.{digest}.parquet",
                               silver_file, sha)  # fmt: skip
        stats = [ENTITY, "raw_sha256"]
        # Bronze and Silver are replaced together only once both files exist: a rejection keeps both.
        et.replace(cat, where.bronze, ENTITY, {name: [bronze_input]}, stats, "gunb-rwdz-entities")
        et.replace(cat, where.silver, ENTITY, {name: [silver_uri]}, stats, "gunb-rwdz-entities")
    out["loaded"].append({"entity": name, "rows": report["rows"], "version": receipt["version_token"],
                          "bronze": "decoded" if need_bronze else "kept",
                          "minutes": round((time.monotonic() - started) / 60, 1)})  # fmt: skip
    return None


def main(argv=None, where: Where | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=100, help="time budget; files not started wait")
    args = parser.parse_args(argv)
    where = where or Where()
    deadline = time.monotonic() + args.minutes * 60
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    refused = bf.rejected_hashes(cat, where.rejected)
    out: dict[str, Any] = {k: [] for k in ("loaded", "current", "rejected", "rejected_before", "not_landed",
                                           "not_started", "failed")}  # fmt: skip
    rejected = []
    try:
        for spec in bulk_file.declaration(SOURCE):
            name = spec["entity"]
            if time.monotonic() > deadline:
                out["not_started"].append(name)
                continue
            try:
                row = run_entity(cat, client, lake, landing, name, refused, out, where)
            except Exception as exc:  # not a verdict on the data: the next run tries again
                out["failed"].append([name, f"{type(exc).__name__}: {str(exc)[:300]}"])
                continue
            if row:
                rejected.append(row)
                out["rejected"].append([name, row["error"][:300]])
    finally:
        bf.record_rejections(cat, where.rejected, rejected)
    status = 1 if out["failed"] else 2 if out["not_started"] else 0
    level = "error" if out["failed"] else "warning" if rejected or out["rejected_before"] or status else "notice"
    annotate(level, "gunb_rwdz entities", out)
    step_summary(f"## gunb_rwdz · Landing → Bronze → Silver\n\n```json\n{json.dumps(out, indent=2)}\n```\n")
    print(json.dumps(out, default=str))
    return status


def verify(where: Where | None = None) -> int:
    """Each declared file: its newest Landing version is in Bronze and Silver, or rejected."""
    where = where or Where()
    client, cat = s3(), bf.retry(catalog, "catalog")
    landing = req("STORAGE_LANDING_BUCKET_NAME")
    refused = bf.rejected_hashes(cat, where.rejected)
    digest = model_digest()
    _, b_all = held(cat, where.bronze)
    _, s_all = held(cat, where.silver)
    report, failed = {}, False
    cov: dict[str, dict[str, Any]] = {"expected": {}, "expected_silver": {}, "landed": {}}
    for spec in bulk_file.declaration(SOURCE):
        name = spec["entity"]
        newest = where.newest(client, landing, name)
        if newest is None:
            report[name] = {"landed": False}
            failed = True
            continue
        sha = newest["sha256"]
        b_ok = b_all.get(name, {}).get("recipes") == {sha}
        s_ok = s_all.get(name, {}).get("recipes") == {f"{sha}.{digest}"}
        rejected = sha in refused or f"{sha}.{digest}" in refused
        report[name] = {"bronze": b_ok, "silver": s_ok, "rejected": rejected,
                        "silver_rows": s_all.get(name, {}).get("rows")}  # fmt: skip
        failed |= not (b_ok and s_ok) and not rejected
        cov["expected"][name], cov["expected_silver"][name] = sha, f"{sha}.{digest}"
        cov["landed"][name] = str(newest.get("version_token") or "")
    annotate("error" if failed else "notice", "gunb_rwdz completeness (Landing → Bronze → Silver)", report)
    if where.bronze[0] == "bronze":  # gold.coverage is written by production runs only
        from ingestion.common import coverage

        rejected_names = {n for n, r in report.items() if r.get("rejected")}
        coverage.publish(
            SOURCE,
            lambda: coverage.rows(SOURCE, {n: n for n in report}, cov["expected"], cov["expected_silver"], b_all,
                                  s_all, landed=cov["landed"], rejected=rejected_names),  # fmt: skip
        )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
