#!/usr/bin/env python3
"""Companies House Landing → Bronze → Silver, one monthly snapshot file at a time (design #875).

Entity = the declared file (``basic_company_data``); its newest Landing version is the entity version:

  02_bronze/companies_house/companies_house_companies/<raw_sha256>.parquet     every company of the snapshot,
      13 of the file's 55 columns as text (replaced whole: Bronze holds the newest snapshot only)
  03_silver/companies_house/companies_house_poland_origin/<raw_sha256>.<digest>.parquet    replaced whole
  03_silver/companies_house/companies_house_snapshot_counts/<raw_sha256>.<digest>.parquet  entity = the snapshot
      date: one small file per snapshot, kept, so the monthly history builds up from the first Landing

Personal data: the file's street lines, postcode, care-of line, PO box, county and previous names are not
loaded (a registered office can be a director's home, a previous name a person's); neither are the accounts,
returns, mortgage and partnership columns and the URI (not used by the selection). The header must still be the
known 55 columns: another header rejects the file (bronze.companies_house_rejected_files) and every table keeps
its previous version. Uploads happen only after every check passed. ``<digest>`` covers both Silver models.
``landing_root``, ``namespace`` and ``lake_root`` exist for Dev trials (zz_dev_ scratch locations).
"""

from __future__ import annotations

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

SOURCE = "companies_house"
ENTITY = "entity"
NAME = "basic_company_data"
BRONZE = "companies_house_companies"
REJECTED = "companies_house_rejected_files"
POLAND, COUNTS = "companies_house_poland_origin", "companies_house_snapshot_counts"
SILVER = {  # model = Silver table: (not-null columns, key)
    POLAND: (("company_number", "company_name", "snapshot_date", "raw_sha256"), ("company_number",)),
    COUNTS: (
        ("snapshot_date", "reference_month", "company_category", "company_status", "companies", "raw_sha256"),
        ("snapshot_date", "country_of_origin", "company_category", "company_status"),
    ),
}
SNAPSHOT_RE = re.compile(r"BasicCompanyDataAsOneFile-(\d{4}-\d{2}-\d{2})\.zip$")
HEADER = [  # the file's header (names stripped: some follow a space), snapshot of 1 November 2026
    "CompanyName", "CompanyNumber", "RegAddress.CareOf", "RegAddress.POBox", "RegAddress.AddressLine1",
    "RegAddress.AddressLine2", "RegAddress.PostTown", "RegAddress.County", "RegAddress.Country",
    "RegAddress.PostCode", "CompanyCategory", "CompanyStatus", "CountryOfOrigin", "DissolutionDate",
    "IncorporationDate", "Accounts.AccountRefDay", "Accounts.AccountRefMonth", "Accounts.NextDueDate",
    "Accounts.LastMadeUpDate", "Accounts.AccountCategory", "Returns.NextDueDate", "Returns.LastMadeUpDate",
    "Mortgages.NumMortCharges", "Mortgages.NumMortOutstanding", "Mortgages.NumMortPartSatisfied",
    "Mortgages.NumMortSatisfied", "SICCode.SicText_1", "SICCode.SicText_2", "SICCode.SicText_3",
    "SICCode.SicText_4", "LimitedPartnerships.NumGenPartners", "LimitedPartnerships.NumLimPartners", "URI",
    *[f"PreviousName_{n}.{part}" for n in range(1, 11) for part in ("CONDATE", "CompanyName")],
    "ConfStmtNextDueDate", "ConfStmtLastMadeUpDate",
]  # fmt: skip
KEPT = {  # CSV column → Bronze column (text, as published)
    "CompanyNumber": "company_number",
    "CompanyName": "company_name",
    "CompanyCategory": "company_category",
    "CompanyStatus": "company_status",
    "CountryOfOrigin": "country_of_origin",
    "IncorporationDate": "incorporation_date",
    "DissolutionDate": "dissolution_date",
    "SICCode.SicText_1": "sic_text_1",
    "SICCode.SicText_2": "sic_text_2",
    "SICCode.SicText_3": "sic_text_3",
    "SICCode.SicText_4": "sic_text_4",
    "RegAddress.PostTown": "post_town",
    "RegAddress.Country": "address_country",
}
PROVENANCE = [
    ("snapshot_date", "VARCHAR"),
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
]
BRONZE_COLUMNS = [(ENTITY, "VARCHAR")] + [(c, "VARCHAR") for c in KEPT.values()] + PROVENANCE
SILVER_TYPES = {"string": "VARCHAR", "large_string": "VARCHAR", "int64": "BIGINT", "date32[day]": "DATE"}
MEMORY = "8GB"


class DecodeError(ValueError):
    """A Landing file cannot satisfy the Bronze contract (recorded, never dropped silently)."""


def model_digest() -> str:
    """One digest over both Silver models: a change of either recomputes the newest snapshot's Silver files."""
    h = sha256()
    for model in sorted(SILVER):
        h.update((Path(entity.__file__).parent / "entity" / f"{model}.sql").read_bytes())
    return h.hexdigest()[:12]


def snapshot_date(receipt: dict[str, Any]) -> str:
    """The snapshot date in the file name the listing page linked (YYYY-MM-DD)."""
    found = SNAPSHOT_RE.search(str(receipt.get("source_url") or ""))
    try:
        return datetime.strptime(found.group(1), "%Y-%m-%d").date().isoformat() if found else ""
    except ValueError:
        return ""


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
        raise RuntimeError(f"Companies House raw bytes do not match their receipt: {key}")


def decode(raw: Path, receipt: dict[str, Any], snapshot: str, output: Path) -> int:
    """One ZIP → Parquet: entity, the kept CSV columns as text, snapshot date, provenance; returns the rows."""
    try:
        with zipfile.ZipFile(raw) as archive:
            members = [m for m in archive.infolist() if m.filename.lower().endswith(".csv")]
            if len(members) != 1:
                raise DecodeError(f"expected one CSV in the ZIP, found {[m.filename for m in members]}")
            csv_path = output.parent / "data.csv"
            with archive.open(members[0]) as src, csv_path.open("wb") as dst:
                shutil.copyfileobj(src, dst, 16 << 20)
    except zipfile.BadZipFile as exc:
        raise DecodeError(f"not a readable ZIP: {exc}") from exc
    raw.unlink()  # disk: the CSV is about 2.8 GB
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit = '{MEMORY}'")
        con.execute(f"SET temp_directory = '{output.parent}'")
        try:
            con.execute(
                f"CREATE VIEW src AS SELECT * FROM read_csv('{csv_path}', delim=',', quote='\"', escape='\"', "
                "header=true, all_varchar=true, strict_mode=true, encoding='utf-8', sample_size=-1)"
            )
            headers = [r[0] for r in con.execute("DESCRIBE src").fetchall()]
            if [h.strip() for h in headers] != HEADER:
                changed = sorted({h.strip() for h in headers} ^ set(HEADER))[:10]
                raise DecodeError(f"CSV header differs from the known 55 columns: {changed or 'order'}")
            named = {h.strip(): '"' + h.replace('"', '""') + '"' for h in headers}
            select = ", ".join(
                ["? AS entity"]
                + [f"{named[column]} AS {name}" for column, name in KEPT.items()]
                + ["? AS snapshot_date", "? AS raw_sha256", "? AS source_version", "?::TIMESTAMPTZ AS ingested_at"]
            )
            ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
            con.execute(
                f"COPY (SELECT {select} FROM src) TO '{output}' (FORMAT parquet, COMPRESSION zstd)",
                [NAME, snapshot, receipt["sha256"], receipt["version_token"], ingested],
            )
        except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:
            raise DecodeError(f"CSV cannot be read: {str(exc)[:400]}") from exc
        rows, numbers = con.execute(
            f"SELECT count(*), count(DISTINCT company_number) FROM read_parquet('{output}')"
        ).fetchone()
        if not rows or rows != numbers:
            raise DecodeError(f"{rows} rows for {numbers} company numbers (none, empty or repeated)")
        return int(rows)
    finally:
        con.close()
        csv_path.unlink(missing_ok=True)


def table_columns(path: Path) -> list[tuple[str, str]]:
    """Silver table columns from the model's output file."""
    out = []
    for field in pq.read_schema(path):
        kind = "TIMESTAMPTZ" if str(field.type).startswith("timestamp") else SILVER_TYPES.get(str(field.type))
        if kind is None:
            raise RuntimeError(f"Silver column {field.name} has an unsupported type {field.type}")
        out.append((field.name, kind))
    return out


def held(cat, table_id: tuple[str, str]):
    """(table or None, entity → state from the table's file list)."""
    try:
        table = bf.retry(lambda: cat.load_table(table_id), "load_table")
    except NoSuchTableError:
        return None, {}
    return table, et.state(table, ENTITY)


def state(cat, namespace: str | None, sha: str, snapshot: str) -> dict[str, Any]:
    """What the tables hold of this file: the Bronze table and state, and per Silver model its table and
    whether it holds the file made by the current models (the counts table under the snapshot date)."""
    recipe = f"{sha}.{model_digest()}"
    bronze, b_state = held(cat, (namespace or "bronze", BRONZE))
    out: dict[str, Any] = {"bronze": bronze, "b_state": b_state.get(NAME, {}), "recipe": recipe, "silver": {}}
    out["bronze_ok"] = out["b_state"].get("recipes") == {sha}
    for model in SILVER:
        table, s_state = held(cat, (namespace or "silver", model))
        mine = s_state.get(snapshot if model == COUNTS else NAME, {})
        out["silver"][model] = {"table": table, "ok": mine.get("recipes") == {recipe}, "rows": mine.get("rows")}
    return out


def run(cat, client, out: dict, landing_root: str, namespace: str | None, lake_root: str) -> dict | None:
    """Bronze and both Silver tables for the newest snapshot file; returns a rejection row or None."""
    lake, landing = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    landed = bulk_file.landed(client, landing, bulk_file.prefix(SOURCE, NAME, landing_root))
    if not landed:
        raise RuntimeError(f"{SOURCE}: no Landing receipts")
    receipt = landed[-1]
    sha, snapshot = receipt["sha256"], snapshot_date(receipt)
    out["snapshot_date"] = snapshot
    rejection = {
        "entity": NAME,
        "raw_sha256": sha,
        "landing_key": receipt["object"]["key"],
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    now = state(cat, namespace, sha, snapshot) if snapshot else {}
    refused = bf.rejected_hashes(cat, (namespace or "bronze", REJECTED))
    if sha in refused or now.get("recipe") in refused:
        out["rejected_before"] = True
        return None
    if not snapshot:
        return {**rejection, "error": f"decoder: no snapshot date in the file name {receipt.get('source_url')!r}"}
    todo = [m for m in SILVER if not now["bronze_ok"] or not now["silver"][m]["ok"]]
    if not todo:
        out["current"] = True
        return None
    bronze_id = (namespace or "bronze", BRONZE)
    with tempfile.TemporaryDirectory(prefix="zohelo-companies-house-") as temporary:
        work = Path(temporary)
        local_bronze = work / f"{sha}.parquet"
        if not now["bronze_ok"]:
            raw = work / "raw.zip"
            fetch(client, landing, receipt, raw)
            try:
                out["bronze_rows"] = decode(raw, receipt, snapshot, local_bronze)
            except DecodeError as exc:
                return {**rejection, "error": f"decoder: {exc}"[:1000]}
            source = str(local_bronze)
        else:
            source = next(iter(now["b_state"]["paths"]))
        files = {}
        for model in todo:
            not_null, key = SILVER[model]
            path = work / f"{model}.parquet"
            try:
                report = entity.apply(
                    model, source, path, not_null=not_null, unique_key=key, same_rows=False,
                    memory_limit=MEMORY, temp_directory=str(work),
                )  # fmt: skip
            except entity.EntityModelError as exc:
                return {**rejection, "raw_sha256": now["recipe"], "error": f"silver {model}: {exc}"[:1000]}
            columns = table_columns(path)
            table = now["silver"][model]["table"]
            if table is not None and [(f.name, str(f.field_type)) for f in table.schema().fields] != [
                (c, str(bf.TYPES[k])) for c, k in columns
            ]:  # a changed model needs a deliberate table migration; nothing is uploaded
                raise RuntimeError(f"silver.{model}: the model's columns differ from the table")
            files[model] = (path, columns, report["rows"])
        # Every check passed: create missing tables, upload, then replace each table's entity.
        stats = [ENTITY, "raw_sha256"]
        if not now["bronze_ok"]:
            if now["bronze"] is None:
                bf.retry(lambda: bf.ensure_table(cat, bronze_id, BRONZE_COLUMNS, ENTITY), "create_table")
                et.configure(bronze_id)
            uri = bf.upload(client, lake, f"{lake_root}02_bronze/{SOURCE}/{BRONZE}/{sha}.parquet", local_bronze, sha)
            et.replace(cat, bronze_id, ENTITY, {NAME: [uri]}, stats, "companies-house-entities")
        for model, (path, columns, rows) in files.items():
            silver_id = (namespace or "silver", model)
            if now["silver"][model]["table"] is None:
                bf.retry(lambda i=silver_id, c=columns: bf.ensure_table(cat, i, c, ENTITY), "create_table")
                et.configure(silver_id)
            uri = bf.upload(client, lake, f"{lake_root}03_silver/{SOURCE}/{model}/{now['recipe']}.parquet", path, sha)
            # the counts of a snapshot live under its date and stay; a file published again for the same date
            # replaces them. The Poland-origin selection is the newest snapshot only.
            wanted = {snapshot if model == COUNTS else NAME: [uri]}
            et.replace(cat, silver_id, ENTITY, wanted, stats, "companies-house-entities")
            out["silver"][model] = rows
    out["loaded"] = receipt["version_token"]
    return None


def main(landing_root: str = "01_landing", namespace: str | None = None, lake_root: str = "") -> int:
    client, cat = s3(), bf.retry(catalog, "catalog")
    out: dict[str, Any] = {
        "loaded": None,
        "current": False,
        "rejected": None,
        "rejected_before": False,
        "bronze_rows": None,
        "silver": {},
        "failed": None,
    }
    started = time.monotonic()
    rejected = []
    try:
        row = run(cat, client, out, landing_root, namespace, lake_root)
        if row:
            rejected.append(row)
            out["rejected"] = row["error"][:300]
    except Exception as exc:  # not a verdict on the data: the next run tries again
        out["failed"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    finally:
        bf.record_rejections(cat, (namespace or "bronze", REJECTED), rejected)
    out["minutes"] = round((time.monotonic() - started) / 60, 1)
    level = "error" if out["failed"] else "warning" if out["rejected"] or out["rejected_before"] else "notice"
    annotate(level, "companies_house entities", out)
    step_summary(f"## companies_house · Landing → Bronze → Silver\n\n```json\n{json.dumps(out, indent=2)}\n```\n")
    return 1 if out["failed"] else 0


def verify(landing_root: str = "01_landing", namespace: str | None = None) -> int:
    """The newest Landing file is in Bronze and in both Silver tables (current models), or rejected."""
    client, cat = s3(), bf.retry(catalog, "catalog")
    newest = bulk_file.landed(client, req("STORAGE_LANDING_BUCKET_NAME"), bulk_file.prefix(SOURCE, NAME, landing_root))
    if not newest:
        raise RuntimeError(f"{SOURCE}: no Landing receipts")
    sha, snapshot = newest[-1]["sha256"], snapshot_date(newest[-1])
    now = state(cat, namespace, sha, snapshot)
    refused = bf.rejected_hashes(cat, (namespace or "bronze", REJECTED))
    report = {"snapshot_date": snapshot, "bronze": now["bronze_ok"], "bronze_rows": now["b_state"].get("rows")}
    report.update({model: s["ok"] for model, s in now["silver"].items()})
    report["rejected"] = sha in refused or now["recipe"] in refused
    in_silver = all(s["ok"] for s in now["silver"].values())
    failed = not (now["bronze_ok"] and in_silver) and not report["rejected"]
    annotate("error" if failed else "notice", "companies_house completeness (Landing → Bronze → Silver)", report)
    if namespace is None:
        from ingestion.common import coverage

        rows = sum(s["rows"] or 0 for s in now["silver"].values())
        silver = {NAME: {"recipes": {now["recipe"]}, "rows": rows}} if in_silver else {}
        coverage.publish(
            SOURCE,
            lambda: coverage.rows(
                SOURCE,
                {NAME: "Free Company Data Product, basic company data (monthly snapshot)"},
                {NAME: sha},
                {NAME: now["recipe"]},
                {NAME: now["b_state"]},
                silver,
                landed={NAME: str(newest[-1].get("version_token") or "")},
                rejected={NAME} if report["rejected"] else set(),
            ),
        )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
