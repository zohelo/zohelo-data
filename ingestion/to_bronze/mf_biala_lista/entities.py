#!/usr/bin/env python3
"""MF VAT white list Landing → Bronze → Silver, one flat file at a time (design #875).

Entity = the declared flat file (``flat_file``); its newest Landing version is the entity
version, replaced whole in each table (entity_tables):

  02_bronze/mf_biala_lista/biala_lista_records/<raw_sha256>.parquet           decode.py
  03_silver/mf_biala_lista/<model>/<raw_sha256>.<digest>.parquet              warehouse/entity/<model>.sql

A rejected file (decoder or a Silver model) is recorded in bronze.biala_lista_rejected_files
and every table keeps the previous version. Uploads happen only after every check passed.
"""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any

import pyarrow.parquet as pq
from pyiceberg.exceptions import NoSuchTableError

from ingestion.common import bronze_files as bf
from ingestion.common import entity_tables as et
from ingestion.common.r2 import archive_key, catalog, raw_chunks, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_bronze.mf_biala_lista.decode import BRONZE_COLUMNS, WhiteListDecodeError, decode
from ingestion.to_landing.patterns import bulk_file
from warehouse import entity

SOURCE = "mf_biala_lista"
ENTITY = "entity"
NAME = "flat_file"
BRONZE_ID = ("bronze", "biala_lista_records")
REJECTED_ID = ("bronze", "biala_lista_rejected_files")
SILVER = {  # model = Silver table: (not-null columns, key)
    "biala_lista_taxpayer_hashes": (("hash_sha512", "taxpayer_status", "file_date"), ()),
    "biala_lista_masks": (("account_mask",), ()),
}
ARROW_TYPES = {
    "string": "VARCHAR",
    "large_string": "VARCHAR",
    "double": "DOUBLE",
    "int64": "BIGINT",
    "int32": "INTEGER",
    "date32[day]": "DATE",
}
MEMORY = "8GB"


def model_digest(models) -> str:
    """One digest over every Silver model: a change of any recomputes all (files stay consistent)."""
    h = sha256()
    for model in sorted(models):
        h.update((Path(entity.__file__).parent / "entity" / f"{model}.sql").read_bytes())
    return h.hexdigest()[:12]


def table_columns(path: Path) -> list[tuple[str, str]]:
    out = []
    for field in pq.read_schema(path):
        kind = "TIMESTAMPTZ" if str(field.type).startswith("timestamp") else ARROW_TYPES.get(str(field.type))
        if kind is None or kind not in bf.TYPES:
            raise RuntimeError(f"column {field.name} has an unsupported type {field.type}")
        out.append((field.name, kind))
    return out


def held(cat, table_id):
    try:
        table = bf.retry(lambda: cat.load_table(table_id), "load_table")
    except NoSuchTableError:
        return None, {}
    return table, et.state(table, ENTITY)


def fetch(client, bucket: str, receipt: dict[str, Any], path: Path) -> None:
    key = receipt["object"]["key"]
    try:
        client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError:
        key = archive_key(SOURCE, receipt["sha256"], key)
    with path.open("wb") as handle:
        for block in raw_chunks(client, bucket, key):
            handle.write(block)


def run(cat, client, lake: str, landing: str, refused: set[str], out: dict) -> dict | None:
    """Bronze and both Silver tables for the newest flat file; returns a rejection row or None."""
    digest = model_digest(SILVER)
    receipt = bulk_file.newest(client, landing, SOURCE, NAME)
    sha = receipt["sha256"]
    if sha in refused or f"{sha}.{digest}" in refused:
        out["rejected_before"] = True
        return None
    bronze, b_state = held(cat, BRONZE_ID)
    silver = {m: held(cat, ("silver", m)) for m in SILVER}
    need_bronze = b_state.get(NAME, {}).get("recipes") != {sha}
    # Per model: a Silver file a table already references is never rebuilt (its bytes would differ).
    todo = [m for m, (_, s) in silver.items() if need_bronze or s.get(NAME, {}).get("recipes") != {f"{sha}.{digest}"}]
    if not todo:
        out["current"] = True
        return None
    rejection = {
        "entity": NAME,
        "raw_sha256": sha,
        "landing_key": receipt["object"]["key"],
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    with tempfile.TemporaryDirectory(prefix="zohelo-mf-") as temporary:
        work = Path(temporary)
        local_bronze = work / f"{sha}.parquet"
        if need_bronze:
            raw = work / "raw.7z"
            fetch(client, landing, receipt, raw)
            try:
                out["bronze"] = decode(raw, receipt, local_bronze, MEMORY)
            except WhiteListDecodeError as exc:
                return {**rejection, "error": f"decoder: {exc}"[:1000]}
            source = str(local_bronze)
        else:
            source = next(iter(b_state[NAME]["paths"]))
        files = {}
        for model in todo:
            not_null, key = SILVER[model]
            path = work / f"{model}.parquet"
            try:
                report = entity.apply(
                    model,
                    source,
                    path,
                    not_null=not_null,
                    unique_key=key,
                    same_rows=False,
                    memory_limit=MEMORY,
                    temp_directory=str(work),
                )
            except entity.EntityModelError as exc:
                return {**rejection, "raw_sha256": f"{sha}.{digest}", "error": f"silver {model}: {exc}"[:1000]}
            columns = table_columns(path)
            table = silver[model][0]
            if table is not None and [(f.name, str(f.field_type)) for f in table.schema().fields] != [
                (c, str(bf.TYPES[k])) for c, k in columns
            ]:
                raise RuntimeError(f"silver.{model}: the model's columns differ from the table")
            files[model] = (path, columns, report["rows"])
        # Every check passed: create missing tables, upload, then replace each table's entity.
        stats = [ENTITY, "raw_sha256"]
        if need_bronze:
            if bronze is None:
                bf.retry(lambda: bf.ensure_table(cat, BRONZE_ID, BRONZE_COLUMNS, ENTITY), "create_table")
                et.configure(BRONZE_ID)
            uri = bf.upload(client, lake, f"02_bronze/{SOURCE}/biala_lista_records/{sha}.parquet", local_bronze, sha)
            et.replace(cat, BRONZE_ID, ENTITY, {NAME: [uri]}, stats, "mf-entities")
        for model, (path, columns, rows) in files.items():
            if silver[model][0] is None:
                bf.retry(lambda m=model, c=columns: bf.ensure_table(cat, ("silver", m), c, ENTITY), "create_table")
                et.configure(("silver", model))
            uri = bf.upload(client, lake, f"03_silver/{SOURCE}/{model}/{sha}.{digest}.parquet", path, sha)
            et.replace(cat, ("silver", model), ENTITY, {NAME: [uri]}, stats, "mf-entities")
            out["silver"][model] = rows
    out["loaded"] = receipt["version_token"]
    return None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=60)
    parser.parse_args(argv)
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    out: dict[str, Any] = {
        "loaded": None,
        "current": False,
        "rejected": None,
        "rejected_before": False,
        "bronze": None,
        "silver": {},
        "failed": None,
    }
    started = time.monotonic()
    rejected = []
    try:
        row = run(cat, client, lake, landing, bf.rejected_hashes(cat, REJECTED_ID), out)
        if row:
            rejected.append(row)
            out["rejected"] = row["error"][:300]
    except Exception as exc:  # not a verdict on the data: the next run tries again
        out["failed"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    finally:
        bf.record_rejections(cat, REJECTED_ID, rejected)
    out["minutes"] = round((time.monotonic() - started) / 60, 1)
    level = "error" if out["failed"] else "warning" if out["rejected"] or out["rejected_before"] else "notice"
    annotate(level, "mf_biala_lista entities", out)
    step_summary(f"## mf_biala_lista · Landing → Bronze → Silver\n\n```json\n{json.dumps(out, indent=2)}\n```\n")
    return 1 if out["failed"] else 0


def verify() -> int:
    client, cat = s3(), bf.retry(catalog, "catalog")
    newest = bulk_file.newest(client, req("R2_LANDING_BUCKET"), SOURCE, NAME)
    sha = newest["sha256"]
    digest = model_digest(SILVER)
    refused = bf.rejected_hashes(cat, REJECTED_ID)
    b_state = held(cat, BRONZE_ID)[1].get(NAME, {})
    report = {"bronze": b_state.get("recipes") == {sha}}
    silver_rows = 0
    for model in SILVER:
        s_state = held(cat, ("silver", model))[1].get(NAME, {})
        report[model] = s_state.get("recipes") == {f"{sha}.{digest}"}
        silver_rows += s_state.get("rows") or 0
    report["rejected"] = sha in refused or f"{sha}.{digest}" in refused
    failed = not all(v for k, v in report.items() if k != "rejected") and not report["rejected"]
    annotate("error" if failed else "notice", "mf_biala_lista completeness (Landing → Bronze → Silver)", report)
    from ingestion.common import coverage

    # one block (the daily flat file) feeding several Silver models: in Silver when all of them hold it
    in_silver = all(report[m] for m in SILVER)
    silver = {NAME: {"recipes": {f"{sha}.{digest}"}, "rows": silver_rows}} if in_silver else {}
    coverage.publish(
        SOURCE,
        lambda: coverage.rows(
            SOURCE,
            {NAME: NAME},
            {NAME: sha},
            {NAME: f"{sha}.{digest}"},
            {NAME: b_state},
            silver,
            landed={NAME: str(newest.get("version_token") or "")},
            rejected={NAME} if report["rejected"] else set(),
        ),
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
