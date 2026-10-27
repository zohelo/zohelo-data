#!/usr/bin/env python3
"""OpenData.org Landing → Bronze → Silver, one member file of the snapshot ZIP at a time (design #875).

The newest Landing snapshot is one ZIP of 2,496 newline-delimited Senzing JSON files
(Organization/, PeopleBusiness/, Locations/; ~45 MB each). Entity = one member file
(``Organization.bq_organization_000000000039``); its file in each table is replaced whole:

  02_bronze/opendata_org/records/<entity>/<zip sha256>.<member crc32>.parquet
      bronze.opendata_records: DATA_SOURCE, RECORD_ID, bq_dataset, FEATURES (JSON text) as published
  03_silver/opendata_org/<model>/<entity>/<zip sha256>.<member crc32>.<model digest>.parquet
      silver.opendata_organizations / opendata_people / opendata_locations (by folder;
      warehouse/entity/<model>.sql types the Senzing features)

The ZIP is downloaded once per run to the runner's disk; members are decoded in parallel worker
processes and committed in chunks, so a run stopped by its budget resumes. Decoder or Silver
rejections go to bronze.opendata_rejected_files (by member recipe); other failures are retried
next run. Prints one JSON result line.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any
import zipfile

from pyiceberg.exceptions import NoSuchTableError

from ingestion.common import bronze_files as bf
from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.opendata_org.extract import landed

SOURCE = "opendata_org"
ENTITY = "entity"
BRONZE = ("bronze", "opendata_records")
REJECTED = ("bronze", "opendata_rejected_files")
MODELS = {
    "Organization": "opendata_organizations",
    "PeopleBusiness": "opendata_people",
    "Locations": "opendata_locations",
}
BRONZE_COLUMNS = [
    ("entity", "VARCHAR"),
    ("data_source", "VARCHAR"),
    ("record_id", "VARCHAR"),
    ("bq_dataset", "VARCHAR"),
    ("features", "VARCHAR"),
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("member", "VARCHAR"),
    ("source_row_number", "BIGINT"),
]
STATS = [ENTITY, "raw_sha256"]
COMMIT_EVERY = 250
ARROW_TYPES = {
    "string": "VARCHAR",
    "large_string": "VARCHAR",
    "double": "DOUBLE",
    "int64": "BIGINT",
    "int32": "INTEGER",
}


def model_digest(model: str) -> str:
    from warehouse.entity import MODELS as MODEL_DIR

    return sha256((MODEL_DIR / f"{model}.sql").read_bytes()).hexdigest()[:12]


def members(zip_file) -> list[dict[str, Any]]:
    """The JSON members of a snapshot ZIP (a path or an open seekable file)."""
    out = []
    with zipfile.ZipFile(zip_file) as archive:
        for info in archive.infolist():
            if not info.filename.endswith(".json") or info.file_size == 0:
                continue
            folder = info.filename.split("/", 1)[0]
            if folder not in MODELS:
                raise RuntimeError(f"unexpected folder in the snapshot: {info.filename}")
            out.append(
                {
                    "member": info.filename,
                    "entity": info.filename[: -len(".json")].replace("/", "."),
                    "folder": folder,
                    "crc": f"{info.CRC:08x}",
                    "size": info.file_size,
                }
            )
    return out


KEYS = ["DATA_SOURCE", "RECORD_ID", "bq_dataset", "FEATURES"]


def work(m: dict[str, Any], zip_path: str, receipt: dict[str, Any], digests: dict[str, str], tmp_root: str) -> dict:
    """One member: its Bronze file (decoded, or the registered one when only Silver is due) and its
    Silver file, uploaded but not yet registered. Worker process. A registered Bronze file is never
    written again (new bytes under a registered name would not match the table's metadata)."""
    import duckdb

    client, lake = s3(), req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    sha, model = receipt["sha256"], MODELS[m["folder"]]
    recipe = f"{sha}.{m['crc']}"
    out = {"entity": m["entity"], "recipe": recipe, "model": model, "member": m["member"], "rows": 0}
    work_dir = Path(tempfile.mkdtemp(prefix="opendata-", dir=tmp_root))
    try:
        if m.get("bronze_uri"):  # Bronze is current: Silver only, from the registered file
            out["bronze"] = m["bronze_uri"]
            return _silver(out, m, m["bronze_uri"], None, sha, model, recipe, digests, work_dir, client, lake)
        raw = work_dir / "member.json"
        with zipfile.ZipFile(zip_path) as archive, archive.open(m["member"]) as src, raw.open("wb") as dst:
            shutil.copyfileobj(src, dst, 16 << 20)
        bronze = work_dir / "bronze.parquet"
        source = str(raw).replace("'", "''")
        target = str(bronze).replace("'", "''")
        known = "[" + ", ".join(f"'{k}'" for k in KEYS) + "]"
        con = duckdb.connect()
        try:
            con.execute("SET memory_limit = '3GB'")
            con.execute(f"SET temp_directory = '{work_dir}'")
            other = con.execute(
                f"SELECT count(*) FROM read_ndjson_objects('{source}', maximum_object_size=67108864) "
                f"WHERE NOT list_has_all({known}, json_keys(json))"
            ).fetchone()[0]
            if other:  # Bronze keeps every published key: a new one needs the decoder changed first
                out.update(status="rejected", error=f"decoder: {other} records carry keys beyond {KEYS}")
                return out
            con.execute(
                f"COPY (SELECT ? AS entity, DATA_SOURCE AS data_source, RECORD_ID AS record_id, bq_dataset, "
                f"CAST(FEATURES AS VARCHAR) AS features, ? AS raw_sha256, ? AS source_version, ? AS member, "
                f"CAST(row_number() OVER () AS BIGINT) AS source_row_number "
                f"FROM read_json('{source}', format='newline_delimited', maximum_object_size=67108864, "
                f"columns={{'DATA_SOURCE': 'VARCHAR', 'RECORD_ID': 'VARCHAR', 'bq_dataset': 'VARCHAR', "
                f"'FEATURES': 'JSON'}})) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)",
                [m["entity"], sha, receipt["version_token"], m["member"]],
            )
            rows = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
        except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:
            out.update(status="rejected", error=f"decoder: {str(exc)[:600]}")
            return out
        finally:
            con.close()
        raw.unlink()
        if not rows:
            out.update(status="rejected", error="decoder: the member has no records")
            return out
        out["rows"] = int(rows)
        return _silver(out, m, str(bronze), bronze, sha, model, recipe, digests, work_dir, client, lake)
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def _silver(out, m, bronze_source, local_bronze, sha, model, recipe, digests, work_dir, client, lake) -> dict:
    """Silver file from a Bronze file (local, or registered in R2); uploads Bronze only when it is new."""
    import pyarrow.parquet as pq

    from warehouse import entity

    silver = work_dir / "silver.parquet"
    try:
        report = entity.apply(
            model,
            bronze_source,
            silver,
            not_null=("entity", "record_id"),
            memory_limit="3GB",
            temp_directory=str(work_dir),
        )
    except entity.EntityRunError as exc:
        text = str(exc)
        if any(t in text for t in (*bf.NETWORK_MARKERS, "HTTP", "Out of Memory", "No space", "IO Error")):
            raise  # storage, network or runner trouble: the next run tries again
        out.update(status="rejected", error=f"silver: {exc}"[:1000], recipe=f"{recipe}.{digests[model]}")
        return out
    except entity.EntityModelError as exc:  # a verdict on this file and model
        out.update(status="rejected", error=f"silver: {exc}"[:1000], recipe=f"{recipe}.{digests[model]}")
        return out
    out["silver_schema"] = [(f.name, str(f.type)) for f in pq.read_schema(silver)]
    if local_bronze is not None:
        key = f"02_bronze/{SOURCE}/records/{m['entity']}/{recipe}.parquet"
        out["bronze"] = bf.upload(client, lake, key, local_bronze, sha)
    key = f"03_silver/{SOURCE}/{model}/{m['entity']}/{recipe}.{digests[model]}.parquet"
    out["silver"] = bf.upload(client, lake, key, silver, sha)
    out.update(status="ok", silver_rows=report["rows"])
    out["rows"] = out["rows"] or report["rows"]
    return out


def table_columns(schema: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out = []
    for name, kind in schema:
        mapped = "TIMESTAMPTZ" if kind.startswith("timestamp") else ARROW_TYPES.get(kind)
        if mapped is None:
            raise RuntimeError(f"Silver column {name} has an unsupported type {kind}")
        out.append((name, mapped))
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=280)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)
    started = time.monotonic()
    deadline = started + args.minutes * 60
    client, cat = s3(), bf.retry(catalog, "catalog")
    landing_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    held = landed(client, landing_bucket)
    if not held:
        raise RuntimeError("no OpenData snapshot in Landing")
    receipt = held[-1]
    sha = receipt["sha256"]
    digests = {model: model_digest(model) for model in MODELS.values()}
    bronze = bf.retry(lambda: bf.ensure_table(cat, BRONZE, BRONZE_COLUMNS, ENTITY), "bronze_table")
    et.configure(BRONZE)
    b_state = et.state(bronze, ENTITY)
    silver_state, silver_tables = {}, {}
    for model in MODELS.values():
        try:
            silver_tables[model] = bf.retry(lambda m=model: cat.load_table(("silver", m)), "silver_table")
            silver_state[model] = et.state(silver_tables[model], ENTITY)
        except NoSuchTableError:  # created from the first Silver file's schema
            silver_tables[model], silver_state[model] = None, {}
    refused = bf.rejected_hashes(cat, REJECTED)
    out: dict[str, Any] = {"snapshot": receipt["version_token"], "loaded": 0, "rows": 0, "rejected": [], "failed": []}
    tmp_root = os.environ.get("DUCKDB_TEMP_DIRECTORY") or tempfile.gettempdir()
    Path(tmp_root).mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="opendata-zip-", dir=tmp_root) as tmp:
        zip_path = Path(tmp) / "snapshot.zip"
        todo: list[dict[str, Any]] = []
        bf.download(client, landing_bucket, SOURCE, receipt["object"]["key"], sha, receipt["size_bytes"], zip_path)
        listing = members(zip_path)
        for m in listing:
            recipe = f"{sha}.{m['crc']}"
            model = MODELS[m["folder"]]
            silver_recipe = f"{recipe}.{digests[model]}"
            if recipe in refused or silver_recipe in refused:
                continue
            if b_state.get(m["entity"], {}).get("recipes") != {recipe}:
                todo.append(m)
            elif silver_state[model].get(m["entity"], {}).get("recipes") != {silver_recipe}:
                todo.append({**m, "bronze_uri": b_state[m["entity"]]["paths"][0]})
        out["members"], out["todo"] = len(listing), len(todo)
        pending: list[dict[str, Any]] = []

        def commit() -> None:
            nonlocal pending
            batch, pending = pending, []
            ok = []
            first: dict[str, list] = {}
            for r in (r for r in batch if r["status"] == "ok"):
                # a Silver table's columns are fixed by its first file; a file that differs is not registered
                table = silver_tables[r["model"]]
                columns = table_columns(r["silver_schema"])
                held = (
                    [(f.name, str(f.field_type)) for f in table.schema().fields]
                    if table is not None
                    else first.setdefault(r["model"], [(c, str(bf.TYPES[k])) for c, k in columns])
                )
                if held != [(c, str(bf.TYPES[k])) for c, k in columns]:
                    out["failed"].append([r["entity"], f"silver.{r['model']}: columns differ from the table"])
                    continue
                ok.append(r)
            if ok:
                et.replace(cat, BRONZE, ENTITY, {r["entity"]: [r["bronze"]] for r in ok}, STATS, "opendata-entities")
                for model in MODELS.values():
                    rows = [r for r in ok if r["model"] == model]
                    if not rows:
                        continue
                    if silver_tables[model] is None:
                        columns = table_columns(rows[0]["silver_schema"])
                        silver_tables[model] = bf.retry(
                            lambda m=model, c=columns: bf.ensure_table(cat, ("silver", m), c, ENTITY), "create_table"
                        )
                        et.configure(("silver", model))
                    et.replace(
                        cat,
                        ("silver", model),
                        ENTITY,
                        {r["entity"]: [r["silver"]] for r in rows},
                        STATS,
                        "opendata-entities",
                    )
                out["loaded"] += len(ok)
                out["rows"] += sum(r["rows"] for r in ok)
            bad = [r for r in batch if r["status"] == "rejected"]
            bf.record_rejections(
                cat,
                REJECTED,
                [
                    {
                        "entity": r["entity"],
                        "raw_sha256": r["recipe"],
                        "landing_key": receipt["object"]["key"],
                        "error": r["error"],
                        "code_sha": os.environ.get("GITHUB_SHA", ""),
                    }
                    for r in bad
                ],
            )
            out["rejected"] += [[r["entity"], r["error"][:200]] for r in bad]

        queue = list(todo)
        running: dict = {}
        try:
            with ProcessPoolExecutor(max_workers=args.workers) as pool:
                while queue or running:
                    while queue and len(running) < args.workers and time.monotonic() < deadline:
                        m = queue.pop(0)
                        running[pool.submit(work, m, str(zip_path), receipt, digests, tmp_root)] = m
                    if not running:
                        break
                    done = next(as_completed(running))
                    m = running.pop(done)
                    try:
                        pending.append(done.result())
                    except BrokenProcessPool:
                        raise
                    except Exception as exc:  # not a verdict on the data: retried next run
                        out["failed"].append([m["entity"], f"{type(exc).__name__}: {str(exc)[:200]}"])
                    if len(pending) >= COMMIT_EVERY:
                        commit()
        except BrokenProcessPool as exc:  # a worker was killed (memory): keep what finished, stop
            out["failed"].append(["worker pool", f"BrokenProcessPool: {str(exc)[:200]}"])
            for future, m in running.items():
                if future.done() and not future.exception():
                    pending.append(future.result())
                else:
                    queue.append(m)
        finally:
            commit()
        out["remaining"] = len(queue)
    out["minutes"] = round((time.monotonic() - started) / 60, 1)
    out["rejected"] = out["rejected"][:10]
    out["failed_count"], out["failed"] = len(out["failed"]), out["failed"][:10]
    status = 1 if out["failed_count"] else 2 if out["remaining"] else 0
    annotate("error" if status == 1 else "warning" if status else "notice", "opendata_org entities", out)
    step_summary(f"## opendata_org · Landing → Bronze → Silver\n\n```json\n{json.dumps(out, indent=2)}\n```\n")
    print(json.dumps(out, default=str))
    return status


def snapshot_members(client, bucket: str, receipt: dict[str, Any]) -> list[dict[str, Any]]:
    """The members of a Landing snapshot from its ZIP directory (ranged reads of a few MB)."""
    import s3fs

    from ingestion.common.r2 import archive_key

    key = receipt["object"]["key"]
    try:
        client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError:
        key = archive_key(SOURCE, receipt["sha256"], key)
    fs = s3fs.S3FileSystem(
        key=req("STORAGE_WRITE_ACCESS_KEY"),
        secret=req("STORAGE_WRITE_SECRET_KEY"),
        client_kwargs={"endpoint_url": req("STORAGE_ENDPOINT_URL")},
    )
    with fs.open(f"{bucket}/{key}", "rb", block_size=8 << 20) as handle:
        return members(handle)


def verify() -> int:
    """Every member of the newest snapshot (from its ZIP directory) is current in Bronze and its
    Silver table, or rejected for this snapshot; table state from metadata, not the data."""
    client, cat = s3(), bf.retry(catalog, "catalog")
    bucket = req("STORAGE_LANDING_BUCKET_NAME")
    held = landed(client, bucket)
    if not held:
        annotate("error", "opendata_org completeness (Landing → Bronze → Silver)", {"snapshot": None})
        return 1
    receipt = held[-1]
    sha = receipt["sha256"]
    listing = snapshot_members(client, bucket, receipt)
    digests = {model: model_digest(model) for model in MODELS.values()}
    refused = bf.rejected_hashes(cat, REJECTED)
    b_state = et.state(bf.retry(lambda: cat.load_table(BRONZE), "bronze_table"), ENTITY)
    s_states = {}
    for model in MODELS.values():
        try:
            s_states[model] = et.state(cat.load_table(("silver", model)), ENTITY)
        except NoSuchTableError:
            s_states[model] = {}
    counts = {"members": len(listing), "complete": 0, "rejected": 0, "bronze_missing": 0, "silver_missing": 0}
    cov: dict[str, dict[str, Any]] = {"expected": {}, "expected_silver": {}, "silver": {}, "names": {}}
    rejected: set[str] = set()
    for m in listing:
        recipe, model = f"{sha}.{m['crc']}", MODELS[m["folder"]]
        cov["expected"][m["entity"]], cov["expected_silver"][m["entity"]] = recipe, f"{recipe}.{digests[model]}"
        cov["silver"][m["entity"]] = s_states[model].get(m["entity"], {})
        cov["names"][m["entity"]] = f"{m['folder']} → silver.{model}"
        if recipe in refused or f"{recipe}.{digests[model]}" in refused:
            rejected.add(m["entity"])
            counts["rejected"] += 1
        elif b_state.get(m["entity"], {}).get("recipes") != {recipe}:
            counts["bronze_missing"] += 1
        elif s_states[model].get(m["entity"], {}).get("recipes") != {f"{recipe}.{digests[model]}"}:
            counts["silver_missing"] += 1
        else:
            counts["complete"] += 1
    names = {m["entity"] for m in listing}
    report = {
        "snapshot": receipt["version_token"],
        **counts,
        "bronze_rows": sum(s["rows"] for e, s in b_state.items() if e in names),
        "entities_not_in_snapshot": len(set(b_state) - names),
        **{model: sum(s["rows"] for s in st.values()) for model, st in s_states.items()},
    }
    failed = counts["bronze_missing"] or counts["silver_missing"]
    annotate("error" if failed else "notice", "opendata_org completeness (Landing → Bronze → Silver)", report)
    from ingestion.common import coverage

    version = str(receipt["version_token"])
    coverage.publish(
        "opendata_org",
        lambda: coverage.rows(
            "opendata_org",
            cov["names"],
            cov["expected"],
            cov["expected_silver"],
            b_state,
            cov["silver"],
            landed=dict.fromkeys(cov["expected"], version),
            rejected=rejected,
        ),
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
