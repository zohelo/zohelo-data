#!/usr/bin/env python3
"""Eurostat Landing → Bronze → Silver, one dataset at a time (design #875).

For every Eurostat dataset the newest Landing version is the entity version. Bronze and
Silver each hold exactly one file per dataset:

  02_bronze/eurostat/observations/<dataset>/<raw_sha256>.parquet           (decode.py, as published)
  03_silver/eurostat/observations/<dataset>/<raw_sha256>.<model>.parquet   (warehouse/entity/eurostat_observations.sql)

``<model>`` is a digest of the Silver SQL, so a changed model marks every dataset's Silver as
out of date and is recomputed (a full rebuild of Silver from unchanged Bronze, reported as
``redo_rows``); the daily refresh otherwise touches only datasets with a new version.
What each table holds is read from its file list (entity_tables.state): nothing is scanned.
A dataset whose Bronze file already exists in R2
(same Landing SHA-256 in the object metadata) is registered as it is, not decoded again.
Work runs in parallel processes; finished datasets are committed in chunks, so a run that
stops on its time budget resumes where it stopped. Decoder or Silver-model rejections are
recorded in bronze.eurostat_rejected_files (decoder: by Landing SHA-256; Silver model: by
``<sha>.<model>``, so a fixed model retries them) and reported. A dataset that fails for another
reason (storage, memory) is reported and retried by the next run. Prints one JSON result line.
"""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any

import botocore.exceptions

from ingestion.common import bronze_files as bf
from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.eurostat.decode import BRONZE_COLUMNS, EurostatBulkDecodeError, decode_landing_distribution

ENTITY = "dataset_id"
BRONZE = ("bronze", "eurostat_observations")
SILVER = ("silver", "eurostat_observations")
REJECTED = ("bronze", "eurostat_rejected_files")
BRONZE_PREFIX = "02_bronze/eurostat/observations/"
SILVER_PREFIX = "03_silver/eurostat/observations/"
SILVER_MODEL = "eurostat_observations"
STATS = [ENTITY, "raw_sha256"]
COMMIT_EVERY = 200  # datasets
COMMIT_SECONDS = 600
BIG_BYTES = 50 << 20  # Landing files above this decode alone, with the full memory limit
SILVER_COLUMNS = [
    ("dataset_id", "VARCHAR"),
    ("freq_code", "VARCHAR"),
    ("unit_code", "VARCHAR"),
    ("geo_code", "VARCHAR"),
    ("time_period", "VARCHAR"),
    ("value", "DOUBLE"),
    ("status_code", "VARCHAR"),
    ("breakdown_dimensions", "VARCHAR"),
    ("breakdown_codes", "VARCHAR"),
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
]


LANDING_DATA_PREFIX = "01_landing/eurostat/snapshot/data/"


def landing_candidates(client, landing_bucket: str) -> list[dict[str, Any]]:
    """The latest Landing version of every dataset (highest source version, then ingest)."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=landing_bucket, Prefix=LANDING_DATA_PREFIX):
        keys += [item["Key"] for item in page.get("Contents", []) if item["Key"].endswith(".receipt.json")]

    def read(key: str) -> dict[str, Any]:
        body = bf.retry(lambda: client.get_object(Bucket=landing_bucket, Key=key)["Body"].read(), "read_receipt")
        receipt = json.loads(body)
        sha, size, obj = receipt.get("sha256"), receipt.get("size_bytes"), receipt.get("object") or {}
        if (
            receipt.get("schema_version") != 2
            or receipt.get("kind") != "eurostat_landing"
            or receipt.get("area") != "data"
            or not isinstance(sha, str)
            or not bf.SHA_RE.fullmatch(sha)
            or type(size) is not int
            or size <= 0
            or obj.get("bucket") != landing_bucket
            or not str(obj.get("key", "")).startswith(LANDING_DATA_PREFIX)
        ):
            raise RuntimeError(f"invalid Eurostat Landing receipt: {key}")
        return {
            "raw_sha256": sha,
            "size": size,
            "receipt_key": key,
            "receipt_bytes": body,
            "data_key": obj["key"],
            "dataset_id": receipt.get("dataset_code"),
            "order": (str(receipt.get("version_token") or ""), str(receipt.get("ingested_at") or "")),
        }

    with ThreadPoolExecutor(max_workers=8) as pool:  # R2 throttles bursts of reads
        receipts = list(pool.map(read, sorted(keys)))
    latest: dict[str, dict[str, Any]] = {}
    for item in receipts:
        held = latest.get(item["dataset_id"])
        if held is None or item["order"] > held["order"]:
            latest[item["dataset_id"]] = item
    return sorted(latest.values(), key=lambda item: item["dataset_id"])


def model_digest() -> str:
    from warehouse.entity import MODELS

    return sha256((MODELS / f"{SILVER_MODEL}.sql").read_bytes()).hexdigest()[:12]


def _existing_bronze(client, bucket: str, key: str, raw_sha256: str) -> bool:
    """A Bronze file of this Landing version is already in R2 (from an earlier load)."""
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as exc:
        if bf._missing(exc):
            return False
        raise
    return (head.get("Metadata") or {}).get("raw-sha256") == raw_sha256


def work(
    item: dict[str, Any], need_bronze: bool, digest: str, memory: str, tmp_root: str, paths: dict[str, str]
) -> dict[str, Any]:
    """One dataset: Bronze file (registered as found, or decoded) and its Silver file. Runs in a worker process.

    ``paths``: ``bronze`` (where Bronze files are written and found), ``silver``; a trial adds
    ``bronze_found`` (production prefix whose existing files it may register, read-only)."""
    from warehouse import entity

    client = s3()
    bucket, landing_bucket = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    ds, sha = item[ENTITY], item["raw_sha256"]
    bronze_key = f"{paths['bronze']}{ds}/{sha}.parquet"
    found_key = f"{paths.get('bronze_found', paths['bronze'])}{ds}/{sha}.parquet"
    silver_key = f"{paths['silver']}{ds}/{sha}.{digest}.parquet"
    out: dict[str, Any] = {
        ENTITY: ds,
        "raw_sha256": sha,
        "landing_key": item["data_key"],
        "bronze": item.get("bronze_uri") or f"s3://{bucket}/{bronze_key}",
    }
    work_dir = Path(tempfile.mkdtemp(prefix="eurostat-", dir=tmp_root))
    started = time.monotonic()
    try:
        local_bronze = None
        if need_bronze:
            if _existing_bronze(client, bucket, found_key, sha):
                out["bronze"] = f"s3://{bucket}/{found_key}"
                out["bronze_action"] = "registered"
            else:
                raw, receipt, local_bronze = (
                    work_dir / "data.tsv.gz",
                    work_dir / "receipt.json",
                    work_dir / "bronze.parquet",
                )
                receipt.write_bytes(item["receipt_bytes"])
                if shutil.disk_usage(work_dir).free < item["size"] * 6 + (2 << 30):
                    raise RuntimeError("runner disk headroom is insufficient for Eurostat decode")
                bf.download(client, landing_bucket, "eurostat", item["data_key"], sha, item["size"], raw)
                try:
                    decode_landing_distribution(
                        raw,
                        receipt,
                        local_bronze,
                        receipt_uri=f"r2://{landing_bucket}/{item['receipt_key']}",
                        raw_uri=f"r2://{landing_bucket}/{item['data_key']}",
                        memory_limit=memory,
                    )
                except EurostatBulkDecodeError as exc:  # verdict on the file: keyed by its SHA-256
                    out.update(status="rejected", error=f"decoder: {exc}"[:1000])
                    return out
                raw.unlink()
                bf.upload(client, bucket, bronze_key, local_bronze, sha)
                out["bronze_action"] = "decoded"
        silver = work_dir / "silver.parquet"
        try:
            report = entity.apply(
                SILVER_MODEL,
                str(local_bronze) if local_bronze else out["bronze"],
                silver,
                not_null=("dataset_id", "time_period"),
                memory_limit=memory,
                temp_directory=str(work_dir),
            )
        except entity.EntityModelError as exc:  # verdict of this model: keyed by <sha>.<model>
            out.update(status="rejected", error=f"silver: {exc}"[:1000], raw_sha256=f"{sha}.{digest}")
            return out
        out["silver"] = bf.upload(client, bucket, silver_key, silver, sha)
        out.update(rows=report["rows"], silver_bytes=report["bytes"], status="ok")
        return out
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
        out["seconds"] = round(time.monotonic() - started, 1)


def refused_keys(cat, rejected_id: tuple[str, str], create: bool) -> set[str]:
    """Keys recorded as rejected: Landing SHA-256 (decoder) or <sha>.<model> (Silver model)."""
    from pyiceberg.exceptions import NoSuchTableError

    try:
        table = bf.rejected_table(cat, rejected_id, ENTITY) if create else cat.load_table(rejected_id)
    except NoSuchTableError:
        return set()
    if table.current_snapshot() is None:
        return set()
    return set(table.scan(selected_fields=("raw_sha256",)).to_arrow().column("raw_sha256").to_pylist())


def plan(
    cat,
    client,
    digest: str,
    only: set[str] | None,
    silver_id: tuple[str, str] = SILVER,
    bronze_id=BRONZE,
    rejected_id=REJECTED,
    create: bool = True,
) -> dict[str, Any]:
    """What each dataset needs: Bronze (new Landing version) and/or Silver (new Bronze or model).
    ``create=False`` (checks): reads only, never creates a table."""
    if create:
        bronze = bf.retry(lambda: bf.ensure_table(cat, bronze_id, list(BRONZE_COLUMNS), ENTITY), "bronze_table")
        silver = bf.retry(lambda: bf.ensure_table(cat, silver_id, SILVER_COLUMNS, ENTITY), "silver_table")
    else:
        bronze = bf.retry(lambda: cat.load_table(bronze_id), "bronze_table")
        silver = bf.retry(lambda: cat.load_table(silver_id), "silver_table")
    refused = refused_keys(cat, rejected_id, create)
    b_state, s_state = et.state(bronze, ENTITY), et.state(silver, ENTITY)
    latest = landing_candidates(client, req("R2_LANDING_BUCKET"))
    if only:
        latest = [c for c in latest if c[ENTITY] in only]
    todo, expected, expected_silver, rejected, redo_rows, landed = [], {}, {}, [], 0, {}
    for c in latest:
        ds, sha = c[ENTITY], c["raw_sha256"]
        landed[ds] = c["order"][0]
        if sha in refused or f"{sha}.{digest}" in refused:
            rejected.append(ds)
            continue
        expected[ds], expected_silver[ds] = sha, f"{sha}.{digest}"
        need_bronze = b_state.get(ds, {}).get("recipes") != {sha}
        need_silver = need_bronze or s_state.get(ds, {}).get("recipes") != {f"{sha}.{digest}"}
        redo = need_silver and not need_bronze  # Silver recomputed from unchanged Bronze (model change)
        if redo:
            redo_rows += b_state[ds]["rows"]
        if need_bronze or need_silver:
            current = b_state.get(ds, {}).get("paths", [])
            todo.append(
                {**c, "need_bronze": need_bronze, "bronze_uri": None if need_bronze else current[0], "redo": redo}
            )
    return {
        "todo": todo,
        "latest": len(latest),
        "expected": expected,
        "expected_silver": expected_silver,
        "rejected": rejected,
        "redo_rows": redo_rows,
        "bronze": b_state,
        "silver": s_state,
        "landed": landed,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--minutes", type=float, default=50, help="time budget; unfinished datasets wait for the next run"
    )
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--only", nargs="*", help="dataset ids (trials)")
    parser.add_argument("--memory", default="4GB", help="DuckDB memory per worker for small files")
    parser.add_argument(
        "--silver-table", default=".".join(SILVER), help="namespace.table (the checkpoint builds beside the old table)"
    )
    parser.add_argument("--trial", default="", help="scratch namespace and prefix (Dev trials): writes only there")
    args = parser.parse_args(argv)
    silver_id = tuple(args.silver_table.split(".", 1))
    bronze_id, rejected_id = BRONZE, REJECTED
    paths = {"bronze": BRONZE_PREFIX, "silver": SILVER_PREFIX}
    if args.trial:
        bronze_id, silver_id, rejected_id = (
            (args.trial, "bronze_" + BRONZE[1]),
            (args.trial, "silver_" + SILVER[1]),
            (args.trial, REJECTED[1]),
        )
        paths = {"bronze": f"{args.trial}/bronze/", "silver": f"{args.trial}/silver/", "bronze_found": BRONZE_PREFIX}
    started = time.monotonic()
    deadline = started + args.minutes * 60
    cat, client = bf.retry(catalog, "catalog"), s3()
    digest = model_digest()
    p = plan(cat, client, digest, set(args.only) if args.only else None, silver_id, bronze_id, rejected_id)
    for table_id in (bronze_id, silver_id):
        et.configure(table_id)
    todo = sorted(p["todo"], key=lambda c: c["size"])
    totals: dict[str, Any] = {"bronze_decoded": 0, "bronze_registered": 0, "silver_written": 0, "rows": 0}
    rejected_now: list[str] = []
    failed: list[dict[str, str]] = []
    pending: list[dict[str, Any]] = []
    last_commit = time.monotonic()
    tmp_root = os.environ.get("DUCKDB_TEMP_DIRECTORY") or tempfile.gettempdir()
    Path(tmp_root).mkdir(parents=True, exist_ok=True)

    def commit() -> None:
        nonlocal pending, last_commit
        ok = [r for r in pending if r["status"] == "ok"]
        if ok:
            et.replace(cat, bronze_id, ENTITY, {r[ENTITY]: [r["bronze"]] for r in ok}, STATS, "eurostat-entities")
            et.replace(cat, silver_id, ENTITY, {r[ENTITY]: [r["silver"]] for r in ok}, STATS, "eurostat-entities")
        bad = [r for r in pending if r["status"] == "rejected"]
        bf.record_rejections(
            cat,
            rejected_id,
            [
                {
                    "entity": r[ENTITY],
                    "raw_sha256": r["raw_sha256"],
                    "landing_key": r["landing_key"],
                    "error": r["error"],
                    "recorded_at": datetime.now(UTC),
                    "code_sha": os.environ.get("GITHUB_SHA", "unknown"),
                }
                for r in bad
            ],
            ENTITY,
        )
        pending, last_commit = [], time.monotonic()

    def account(result: dict[str, Any]) -> None:
        if result["status"] == "failed":
            failed.append({"dataset": result[ENTITY], "error": result["error"][:200]})
            return
        pending.append(result)
        key = {"decoded": "bronze_decoded", "registered": "bronze_registered"}.get(result.get("bronze_action", ""))
        if key:
            totals[key] += 1
        if result["status"] == "ok":
            totals["silver_written"] += 1
            totals["rows"] += result["rows"]
        else:
            rejected_now.append(result[ENTITY])

    def failure(c: dict[str, Any], exc: BaseException) -> dict[str, Any]:
        return {ENTITY: c[ENTITY], "status": "failed", "error": f"{type(exc).__name__}: {exc}"}

    small = [c for c in todo if c["size"] <= BIG_BYTES]
    serial = [c for c in todo if c["size"] > BIG_BYTES]  # alone, with the runner's full memory
    queue = list(small)
    while queue and time.monotonic() < deadline - (3600 if serial else 0):
        broken = False
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            running: dict = {}
            while (queue or running) and not broken:
                # the serial phase (big files, retries) keeps its hour; small files stop before it
                stop = deadline - (3600 if serial else 0)
                while queue and len(running) < args.workers and time.monotonic() < stop:
                    c = queue.pop(0)
                    running[pool.submit(work, c, c["need_bronze"], digest, args.memory, tmp_root, paths)] = c
                if not running:
                    break
                finished, _ = wait(running, timeout=60, return_when=FIRST_COMPLETED)
                for f in finished:
                    c = running.pop(f)
                    try:
                        account(f.result())
                    except BrokenProcessPool:  # a worker was killed (memory): unknown which; retry alone
                        serial.append(c)
                        broken = True
                    except Exception as exc:  # one dataset failed for a non-data reason: next run retries it
                        account(failure(c, exc))
                if len(pending) >= COMMIT_EVERY or (pending and time.monotonic() - last_commit > COMMIT_SECONDS):
                    commit()
            if broken:  # the other datasets of the broken pool go alone too; a new pool takes the rest
                serial += list(running.values())
                pool.shutdown(wait=False, cancel_futures=True)
        commit()
    for c in serial:  # each in its own one-worker pool: a kill fails that dataset only
        if time.monotonic() + (3600 if c["size"] > BIG_BYTES else 300) > deadline:
            continue  # a smaller one may still fit
        with ProcessPoolExecutor(max_workers=1) as alone:
            try:
                account(alone.submit(work, c, c["need_bronze"], digest, "10GB", tmp_root, paths).result())
            except Exception as exc:
                account(failure(c, exc))
        commit()
    left = len(todo) - totals["silver_written"] - len(rejected_now)
    summary = {
        "operation": "eurostat-entities",
        "result": "complete" if left == 0 else ("failures" if failed else "incomplete_time_budget"),
        "latest_versions": p["latest"],
        "todo": len(todo),
        "redo_rows": p["redo_rows"],
        "remaining": left,
        **totals,
        "rejected": len(rejected_now),
        "rejected_datasets": rejected_now[:20],
        "rejected_earlier": len(p["rejected"]),
        "failed": len(failed),
        "failed_datasets": failed[:10],
        "minutes": round((time.monotonic() - started) / 60, 1),
    }
    print(json.dumps(summary, sort_keys=True), flush=True)
    annotate("notice" if left == 0 and not rejected_now else "warning", "eurostat entities", summary)
    return 0 if left == 0 else 2


def completeness_report(plan_out: dict[str, Any] | None = None) -> dict[str, Any]:
    """Landing → Bronze → Silver completeness per dataset, from table metadata only (R5, #875)."""
    started = time.monotonic()
    cat, client = bf.retry(catalog, "catalog"), s3()
    p = plan(cat, client, model_digest(), None, create=False)
    if plan_out is not None:
        plan_out.update(p)
    report = et.completeness(p["expected"], p["bronze"], p["silver"], p["expected_silver"])
    report["rejected_count"] = len(p["rejected"])
    report["rejected_latest"] = sorted(p["rejected"])[:20]
    report["minutes"] = round((time.monotonic() - started) / 60, 1)
    return report


def verify(min_pct: float = 99.0) -> int:
    """bronze.verify: fails when a layer is below ``min_pct`` complete against the layer below or a
    dataset holds more than one file (design #875). Rejected datasets are listed, not counted."""
    p: dict[str, Any] = {}
    report = completeness_report(p)
    failed = report["bronze_pct"] < min_pct or report["silver_pct"] < min_pct or report["entities_with_several_files"]
    annotate("error" if failed else "notice", "eurostat completeness (Landing → Bronze → Silver)", report)
    from ingestion.common import coverage

    coverage.publish("eurostat", lambda: coverage_rows(p))
    return 1 if failed else 0


def coverage_rows(p: dict[str, Any]) -> list[dict[str, Any]]:
    """gold.coverage rows: every data distribution of the Eurostat inventory vs Landing and the tables."""
    from ingestion.common import coverage
    from ingestion.to_landing.eurostat.extract import CATALOGUE_KEY

    # the inventory as the last landing.inspect read it: every data distribution, its version, scope
    body = s3().get_object(Bucket=req("R2_LAKEHOUSE_BUCKET"), Key=CATALOGUE_KEY)["Body"].read()
    datasets = json.loads(body)["datasets"]
    names = {code: d.get("title") for code, d in datasets.items()}
    published = {code: d["token"] for code, d in datasets.items() if not d.get("excluded") and d.get("token")}
    excluded = {code: d["excluded"] for code, d in datasets.items() if d.get("excluded")}
    return coverage.rows(
        "eurostat",
        names,
        p["expected"],
        p["expected_silver"],
        p["bronze"],
        p["silver"],
        landed=p["landed"],
        published=published,
        rejected=set(p["rejected"]),
        excluded=excluded,
    )


if __name__ == "__main__":
    raise SystemExit(main())
