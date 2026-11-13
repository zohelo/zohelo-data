#!/usr/bin/env python3
"""IMGW observation archive Landing → Bronze → Silver, one daily synop ZIP at a time (design #875).

Entity = one ZIP of dane_meteorologiczne/dobowe/synop (``synop_daily_<period>_<station>`` or, for the current year,
``synop_daily_<year>_<month>``, ingestion/to_landing/imgw_archive/); its newest Landing version is the entity version.
Bronze and Silver hold one file per entity, replaced whole (entity_tables):

  02_bronze/imgw_archive/synop_daily/<entity>/<raw_sha256>.parquet         s_d member as published, values as text
  03_silver/imgw_archive/synop_daily/<entity>/<raw_sha256>.<model>.parquet warehouse/entity/imgw_archive_synop_daily.sql

``<model>`` is a digest of the Silver SQL: a changed model recomputes Silver from unchanged Bronze. The s_d member is
the daily synop file (65 columns, no header; header names from IMGW's s_d_nagłówek.csv); the ZIP's s_d_t member
(daily means of the terms, published until mid-2024) is not loaded. A ZIP without exactly one s_d member, a row with
another number of fields, or a Silver check that fails (unknown status code, a value that is not a number, a repeated
station × day × parameter) rejects the entity into bronze.imgw_archive_rejected_files and the tables keep the
previous version.

When a year closes, IMGW replaces the year's monthly files (all stations) by one file per station (docs Opis.txt):
a monthly entity of a year for which Landing holds a station file is superseded and is removed from Bronze and Silver
(its rows are in the station files), so no station-day is held twice. Work runs in a few threads and is committed in
chunks; a run stopped by its budget resumes with what is left. Prints one JSON result line.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
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
import pyarrow.parquet as pq
from pyiceberg.exceptions import NoSuchTableError

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate, step_summary
from warehouse import entity

SOURCE = "imgw_archive"
ENTITY = "entity"
BRONZE = ("bronze", "imgw_archive_synop_daily")
SILVER = ("silver", "imgw_archive_synop_daily")
REJECTED = ("bronze", "imgw_archive_rejected_files")
MODEL = "imgw_archive_synop_daily"
# Storage roots; a Dev trial points them at zz_dev_ scratch locations.
LANDING_ROOT, BRONZE_ROOT, SILVER_ROOT = "01_landing", "02_bronze", "03_silver"
RECEIPT_KEY = re.compile(r"[^/]+/[a-z0-9_]+/snapshot/data/([a-z0-9_]+)/([0-9TZ]+)__([0-9TZ]+)\.receipt\.json")
MONTHLY = re.compile(r"^synop_daily_(\d{4})_(\d{2})$")
STATION_YEAR = re.compile(r"^synop_daily_(\d{4})_\d{3}$")
# s_d_nagłówek.csv (dobowe/synop, read 13 Nov 2026, Dev probe 43336937240): 65 fields, value/status pairs.
# fmt: off
HEADER = [
    "NSP", "POST", "ROK", "MC", "DZ", "TMAX", "WTMAX", "TMIN", "WTMIN", "STD", "WSTD", "TMNG", "WTMNG", "SMDB",
    "WSMDB", "ROOP", "PKSN", "WPKSN", "RWSN", "WRWSN", "USL", "WUSL", "DESZ", "WDESZ", "SNEG", "WSNEG", "DISN",
    "WDISN", "GRAD", "WGRAD", "MGLA", "WMGLA", "ZMGL", "WZMGL", "SADZ", "WSADZ", "GOLO", "WGOLO", "ZMNI", "WZMNI",
    "ZMWS", "WZMWS", "ZMET", "WZMET", "FF10", "WFF10", "FF15", "WFF15", "BRZA", "WBRZA", "ROSA", "WROSA", "SZRO",
    "WSZRO", "DZPS", "WDZPS", "DZBL", "WDZBL", "SGR", "IZD", "WIZD", "IZG", "WIZG", "AKTN", "WAKTN",
]
# fmt: on
BRONZE_COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name.lower(), "VARCHAR") for name in HEADER],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
SILVER_NOT_NULL = ("station_code", "observation_date", "parameter", "observation_status", "raw_sha256")
SILVER_KEY = ("station_code", "observation_date", "parameter")
SILVER_REJECT = "observation_status = 'unknown' or (observation_status = 'ok' and value is null)"
STATS = [ENTITY, "raw_sha256"]
THREADS = 6
COMMIT_EVERY = 400  # entities per commit (two tables, a catalog transaction each)
MEMORY = "1GB"  # per thread; one file is a few hundred rows to 22,000 rows


class ArchiveDecodeError(ValueError):
    """The Landing file is not a daily synop ZIP of the known layout (the entity is rejected)."""


def model_digest() -> str:
    return sha256((Path(entity.__file__).parent / "entity" / f"{MODEL}.sql").read_bytes()).hexdigest()[:12]


def decode(body: bytes, receipt: dict[str, Any], output: Path) -> int:
    """The ZIP's s_d member → one Bronze Parquet file (every field as text, provenance added); returns rows."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ArchiveDecodeError(f"not a ZIP: {exc}") from exc
    members = [n for n in archive.namelist() if re.fullmatch(r"s_d_\d[\d_]*\.csv", Path(n).name)]
    if len(members) != 1:
        raise ArchiveDecodeError(f"expected one s_d member, found {archive.namelist()}")
    try:
        text = archive.read(members[0]).decode("cp1250")  # IMGW files are Windows-1250
    except UnicodeDecodeError as exc:
        raise ArchiveDecodeError(f"{members[0]} is not Windows-1250 text: {exc}") from exc
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-imgw-") as temporary:
        path = Path(temporary) / "s_d.csv"
        path.write_text(text, encoding="utf-8")
        columns = "{" + ", ".join(f"'{h}': 'VARCHAR'" for h in HEADER) + "}"
        con = duckdb.connect()
        try:
            con.execute("set preserve_insertion_order = true")
            con.execute(
                f"""
                copy (
                    select
                        ? as entity,
                        {", ".join(f'"{h}" as {h.lower()}' for h in HEADER)},
                        ? as raw_sha256,
                        ? as source_version,
                        cast(? as timestamptz) as ingested_at,
                        cast(row_number() over () as bigint) as source_row_number
                    from read_csv(?, header = false, columns = {columns}, quote = '"', escape = '"', delim = ',',
                                  strict_mode = true, null_padding = false, all_varchar = true)
                ) to '{output}' (format parquet, compression zstd)
                """,
                [prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"], str(path)],
            )
            rows = con.execute(f"select count(*) from read_parquet('{output}')").fetchone()[0]
        except duckdb.Error as exc:
            raise ArchiveDecodeError(f"{members[0]} not readable as 65 fields: {str(exc)[:300]}") from exc
        finally:
            con.close()
    if not rows:
        raise ArchiveDecodeError(f"{members[0]} has no rows")
    return rows


def landing_newest(client, bucket: str) -> dict[str, dict[str, Any]]:
    """Entity -> receipt of its newest landed copy (newest ingest stamp in the key name; that receipt is read)."""
    newest: dict[str, tuple[str, str]] = {}
    prefix = f"{LANDING_ROOT}/{SOURCE}/snapshot/data/"
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            match = RECEIPT_KEY.fullmatch(item["Key"])
            if match and (match.group(1) not in newest or match.group(3) > newest[match.group(1)][1]):
                newest[match.group(1)] = (item["Key"], match.group(3))

    def read(key: str) -> dict[str, Any]:
        body = bf.retry(lambda: client.get_object(Bucket=bucket, Key=key)["Body"].read(), "read_receipt")
        return json.loads(body)

    with ThreadPoolExecutor(max_workers=8) as pool:  # R2 throttles larger bursts of reads
        receipts = list(pool.map(read, [key for key, _ in newest.values()]))
    return {r["entity"]: r for r in receipts}


def superseded(entities: set[str]) -> set[str]:
    """Monthly entities of a year for which a station file is landed (the year was closed and regrouped)."""
    closed = {STATION_YEAR.match(e).group(1) for e in entities if STATION_YEAR.match(e)}
    return {e for e in entities if MONTHLY.match(e) and MONTHLY.match(e).group(1) in closed}


def table_state(cat, table_id) -> tuple[Any, dict[str, dict[str, Any]]]:
    try:
        table = bf.retry(lambda: cat.load_table(table_id), "load_table")
    except NoSuchTableError:
        return None, {}
    return table, et.state(table, ENTITY)


def silver_columns(path: Path) -> list[tuple[str, str]]:
    kinds = {"string": "VARCHAR", "large_string": "VARCHAR", "double": "DOUBLE", "date32[day]": "DATE"}
    out = []
    for field in pq.read_schema(path):
        kind = kinds.get(str(field.type))
        if kind is None:
            raise RuntimeError(f"Silver column {field.name} has an unsupported type {field.type}")
        out.append((field.name, kind))
    return out


def process(
    client, landing: str, lake: str, receipt: dict, need_bronze: bool, bronze_path: str, digest: str, work: Path
):
    """One entity: Bronze (decoded unless current) and Silver files uploaded; returns a result or a rejection."""
    name, sha = receipt["entity"], receipt["sha256"]
    local = work / f"{name}.bronze.parquet"
    rejection = {
        "entity": name,
        "raw_sha256": sha,
        "landing_key": receipt["object"]["key"],
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    bronze_rows = None
    if need_bronze:
        body = bulk_bronze.read_raw(client, landing, SOURCE, receipt)
        try:
            bronze_rows = decode(body, receipt, local)
        except ArchiveDecodeError as exc:
            return {"rejected": {**rejection, "error": f"decoder: {exc}"[:1000]}}
        source = str(local)
    else:
        source = bronze_path
    silver = work / f"{name}.silver.parquet"
    try:
        report = entity.apply(
            MODEL,
            source,
            silver,
            not_null=SILVER_NOT_NULL,
            unique_key=SILVER_KEY,
            same_rows=False,
            reject_if=SILVER_REJECT,
            memory_limit=MEMORY,
            temp_directory=str(work),
        )
    except entity.EntityModelError as exc:
        return {"rejected": {**rejection, "raw_sha256": f"{sha}.{digest}", "error": f"silver: {exc}"[:1000]}}
    out = {"entity": name, "silver_rows": report["rows"], "bronze_rows": bronze_rows}
    if need_bronze:
        out["bronze_uri"] = bf.upload(
            client, lake, f"{BRONZE_ROOT}/{SOURCE}/synop_daily/{name}/{sha}.parquet", local, sha
        )
    out["silver_columns"] = silver_columns(silver)
    out["silver_uri"] = bf.upload(
        client, lake, f"{SILVER_ROOT}/{SOURCE}/synop_daily/{name}/{sha}.{digest}.parquet", silver, sha
    )
    local.unlink(missing_ok=True)
    silver.unlink(missing_ok=True)
    return out


def commit(cat, results: list[dict], tables: dict[str, Any]) -> None:
    """Create missing tables, then replace each entity's file in Bronze and in Silver."""
    if not results:
        return
    bronze = {r["entity"]: [r["bronze_uri"]] for r in results if r.get("bronze_uri")}
    if bronze:
        if tables["bronze"] is None:
            tables["bronze"] = bf.retry(lambda: bf.ensure_table(cat, BRONZE, BRONZE_COLUMNS, ENTITY), "create_table")
            et.configure(BRONZE)
        et.replace(cat, BRONZE, ENTITY, bronze, STATS, "imgw-archive-entities")
    if tables["silver"] is None:
        columns = results[0]["silver_columns"]
        tables["silver"] = bf.retry(lambda: bf.ensure_table(cat, SILVER, columns, ENTITY), "create_table")
        et.configure(SILVER)
    et.replace(cat, SILVER, ENTITY, {r["entity"]: [r["silver_uri"]] for r in results}, STATS, "imgw-archive-entities")


def run(minutes: float) -> dict[str, Any]:
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    started = time.monotonic()
    digest = model_digest()
    receipts = landing_newest(client, landing)
    gone = superseded(set(receipts))
    wanted = {e: r for e, r in receipts.items() if e not in gone}
    bronze_table, b_state = table_state(cat, BRONZE)
    silver_table, s_state = table_state(cat, SILVER)
    refused = bf.rejected_hashes(cat, REJECTED)
    todo, skipped_rejected = [], 0
    for name, receipt in sorted(wanted.items(), reverse=True):  # newest periods first
        sha = receipt["sha256"]
        if sha in refused or f"{sha}.{digest}" in refused:
            skipped_rejected += 1
            continue
        need_bronze = b_state.get(name, {}).get("recipes") != {sha}
        need_silver = need_bronze or s_state.get(name, {}).get("recipes") != {f"{sha}.{digest}"}
        if need_silver:
            paths = b_state.get(name, {}).get("paths") or [""]
            todo.append((receipt, need_bronze, paths[0]))
    out: dict[str, Any] = {
        "entities_landed": len(receipts),
        "superseded_monthly": len(gone),
        "candidates": len(todo),
        "skipped_rejected_before": skipped_rejected,
        "loaded": 0,
        "bronze_rows_added": 0,
        "silver_rows_written": 0,
        "rejected": 0,
        "removed_superseded": 0,
    }
    tables = {"bronze": bronze_table, "silver": silver_table}
    rejected: list[dict] = []
    deadline = started + max(minutes, 5) * 60
    try:
        with tempfile.TemporaryDirectory(prefix="zohelo-imgw-archive-") as temporary:
            work = Path(temporary)
            for start in range(0, len(todo), COMMIT_EVERY):
                if time.monotonic() > deadline:
                    out["stopped_at_budget"] = start
                    break
                chunk = todo[start : start + COMMIT_EVERY]
                with ThreadPoolExecutor(max_workers=THREADS) as pool:
                    results = list(
                        pool.map(lambda t: process(client, landing, lake, t[0], t[1], t[2], digest, work), chunk)
                    )
                good = [r for r in results if "rejected" not in r]
                rejected += [r["rejected"] for r in results if "rejected" in r]
                commit(cat, good, tables)
                out["loaded"] += len(good)
                out["bronze_rows_added"] += sum(r["bronze_rows"] or 0 for r in good)
                out["silver_rows_written"] += sum(r["silver_rows"] for r in good)
        drop = {e: [] for e in gone if e in b_state or e in s_state}
        if drop:
            if tables["bronze"] is not None:
                et.replace(cat, BRONZE, ENTITY, {e: [] for e in drop if e in b_state}, STATS, "imgw-archive-superseded")
            if tables["silver"] is not None:
                et.replace(cat, SILVER, ENTITY, {e: [] for e in drop if e in s_state}, STATS, "imgw-archive-superseded")
            out["removed_superseded"] = len(drop)
    finally:
        bf.record_rejections(cat, REJECTED, rejected)
    out["rejected"] = len(rejected)
    out["rejected_sample"] = [r["error"][:300] for r in rejected[:3]]
    out["minutes"] = round((time.monotonic() - started) / 60, 1)
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=60)
    args = parser.parse_args(argv)
    try:
        out = run(args.minutes)
        failed = None
    except Exception as exc:  # not a verdict on the data: the next run tries again
        out, failed = {}, f"{type(exc).__name__}: {str(exc)[:300]}"
    out["failed"] = failed
    level = "error" if failed else "warning" if out.get("rejected") else "notice"
    print(json.dumps(out, sort_keys=True), flush=True)
    annotate(level, f"{SOURCE} entities", out)
    step_summary(f"## {SOURCE} · Landing → Bronze → Silver\n\n```json\n{json.dumps(out, indent=2)}\n```\n")
    return 1 if failed else 0


def completeness() -> dict[str, Any]:
    """Bronze and Silver against Landing, per entity, from metadata (et.completeness); superseded monthly entities
    must be held nowhere, rejected ones are counted apart."""
    client, cat = s3(), bf.retry(catalog, "catalog")
    receipts = landing_newest(client, req("STORAGE_LANDING_BUCKET_NAME"))
    gone = superseded(set(receipts))
    digest = model_digest()
    refused = bf.rejected_hashes(cat, REJECTED)
    expected = {
        e: r["sha256"]
        for e, r in receipts.items()
        if e not in gone and r["sha256"] not in refused and f"{r['sha256']}.{digest}" not in refused
    }
    b_state = table_state(cat, BRONZE)[1]
    s_state = table_state(cat, SILVER)[1]
    report = et.completeness(expected, b_state, s_state, {e: f"{s}.{digest}" for e, s in expected.items()})
    report["superseded_monthly"] = len(gone)
    report["superseded_still_held"] = sorted(e for e in gone if e in b_state or e in s_state)[:10]
    report["rejected_entities"] = len(receipts) - len(gone) - len(expected)
    return report


def verify() -> int:
    report = completeness()
    ok = report["bronze_pct"] >= 99 and report["silver_pct"] >= 99 and not report["superseded_still_held"]
    report["result"] = "pass" if ok else "fail"
    annotate("notice" if ok else "error", f"{SOURCE} bronze.verify", report)
    step_summary(f"## {SOURCE} · bronze.verify\n\n```json\n{json.dumps(report, indent=2)}\n```\n")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
