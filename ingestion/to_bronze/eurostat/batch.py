#!/usr/bin/env python3
"""Advance Eurostat data files from R2 Landing into the Iceberg Bronze table.

Reads the Landing layout of docs/landing.md
(``01_landing/eurostat/snapshot/data/<CODE>/<version>__<ingested>.tsv.gz`` + receipt) and
loads the latest version of every dataset. This is an R2-only, bounded worker. It never
calls Eurostat and never removes Landing. Iceberg raw_sha256 membership is the idempotent
checkpoint. A file the decoder rejects is recorded under
06_control/eurostat_bulk_bronze/failures/ and skipped on later runs.
"""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
from pathlib import PurePosixPath
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table import TableProperties
from pyiceberg.typedef import Record
import requests

from ingestion.to_bronze.eurostat.decode import EurostatBulkDecodeError, decode_landing_distribution

SOURCE_ID = "eurostat_bulk"
TABLE_ID = ("bronze", "eurostat_full_observations")
OUTPUT_PREFIX = "02_bronze/eurostat_full_observations/data/retained/"
DEFAULT_MAX_DISTRIBUTIONS = 64
LANDING_DATA_PREFIX = "01_landing/eurostat/snapshot/data/"
FAILURE_PREFIX = "06_control/eurostat_bulk_bronze/failures/"
SHA_RE = __import__("re").compile(r"^[0-9a-f]{64}$")
RETRY_DELAYS = (2, 5, 10, 20, 40)


def transient(exc: Exception) -> bool:
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
            return True
        if isinstance(exc, requests.HTTPError):
            return exc.response is not None and exc.response.status_code in {
                429, 500, 502, 503, 504
            }
        # PyIceberg wraps REST failures (including uncertain commit outcomes)
        # while preserving the original HTTPError as the exception cause.
        exc = exc.__cause__
    return False


def retry_read(operation, label: str):
    """Retry only reads; callers must separately reconcile ambiguous commits."""
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            return operation()
        except Exception as exc:
            if not transient(exc) or attempt == len(RETRY_DELAYS):
                raise
            delay = RETRY_DELAYS[attempt]
            print(json.dumps({"status": "transient_read_retry", "operation": label,
                              "attempt": attempt + 1, "delay_seconds": delay,
                              "error_type": type(exc).__name__}), flush=True)
            time.sleep(delay)


def req(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def s3():
    return boto3.client(
        "s3",
        endpoint_url=req("R2_S3_ENDPOINT").rstrip("/"),
        region_name="auto",
        aws_access_key_id=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4", retries={"max_attempts": 8, "mode": "standard"}),
    )


def catalog():
    return RestCatalog(
        name="zohelo",
        warehouse=req("R2_WAREHOUSE"),
        uri=req("R2_CATALOG_URI"),
        token=req("R2_DATA_CATALOG_TOKEN"),
        **{
            "s3.access-key-id": req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
            "s3.secret-access-key": req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
            "s3.endpoint": req("R2_S3_ENDPOINT"),
            "s3.region": "auto",
        },
    )


def sql_rows(query: str) -> list[dict[str, Any]]:
    url = (
        "https://api.sql.cloudflarestorage.com/api/v1/accounts/"
        f"{req('CLOUDFLARE_ACCOUNT_ID')}/r2-sql/query/{req('R2_LAKEHOUSE_BUCKET')}"
    )
    def read_query():
        response = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {req('R2_DATA_CATALOG_TOKEN')}",
                "Content-Type": "application/json",
            },
            json={"query": query},
            timeout=180,
        )
        response.raise_for_status()
        return response

    response = retry_read(read_query, "r2_sql_select")
    if response.status_code != 200:
        raise RuntimeError(f"R2 SQL {response.status_code}: {response.text[:500]}")
    payload = response.json()
    if payload.get("success") is False:
        raise RuntimeError(f"R2 SQL failed: {json.dumps(payload)[:800]}")
    candidates: list[Any] = []
    if isinstance(payload.get("result"), list):
        candidates.extend(payload["result"])
    elif isinstance(payload.get("result"), dict):
        candidates.append(payload["result"])
    candidates.append(payload)
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        value = candidate.get("results")
        if isinstance(value, list) and (not value or isinstance(value[0], dict)):
            return value
        if isinstance(value, dict):
            columns, rows = value.get("columns"), value.get("rows")
            if isinstance(columns, list) and isinstance(rows, list):
                return [dict(zip(columns, row)) for row in rows]
        rows = candidate.get("rows")
        if isinstance(rows, list) and (not rows or isinstance(rows[0], dict)):
            return rows
    raise RuntimeError(f"unsupported R2 SQL response: {json.dumps(payload)[:1000]}")


def existing_raw_hashes() -> set[str]:
    values = set()
    for row in sql_rows(
        "SELECT DISTINCT raw_sha256 FROM bronze.eurostat_full_observations "
        "WHERE raw_sha256 IS NOT NULL;"
    ):
        value = row.get("raw_sha256")
        if not isinstance(value, str) or SHA_RE.fullmatch(value) is None:
            raise RuntimeError(f"invalid Iceberg raw_sha256 value: {value!r}")
        values.add(value)
    return values


def output_identity(path: Path) -> tuple[str, int, int]:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    metadata = pq.read_metadata(path)
    return digest.hexdigest(), path.stat().st_size, int(metadata.num_rows)


def put_output(client, bucket: str, raw_sha: str, path: Path) -> tuple[str, int, int]:
    out_sha, size, rows = output_identity(path)
    key = f"{OUTPUT_PREFIX}{raw_sha}.parquet"
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        metadata = head.get("Metadata") or {}
        if (
            int(head.get("ContentLength", -1)) != size
            or metadata.get("sha256") != out_sha
            or metadata.get("raw-sha256") != raw_sha
        ):
            raise RuntimeError(f"existing retained Eurostat output conflicts: {key}")
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 404 and code not in {"404", "NoSuchKey", "NotFound"}:
            raise
        client.upload_file(
            str(path),
            bucket,
            key,
            ExtraArgs={
                "ContentType": "application/vnd.apache.parquet",
                "Metadata": {"sha256": out_sha, "raw-sha256": raw_sha},
            },
        )
        head = client.head_object(Bucket=bucket, Key=key)
        metadata = head.get("Metadata") or {}
        if (
            int(head.get("ContentLength", -1)) != size
            or metadata.get("sha256") != out_sha
            or metadata.get("raw-sha256") != raw_sha
        ):
            raise RuntimeError(f"retained Eurostat output upload verification failed: {key}")
    return f"s3://{bucket}/{key}", size, rows


def landing_candidates(client, landing_bucket: str) -> list[dict[str, Any]]:
    """The latest Landing version of every dataset (highest source version, then ingest)."""
    from concurrent.futures import ThreadPoolExecutor

    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=landing_bucket, Prefix=LANDING_DATA_PREFIX
    ):
        keys += [item["Key"] for item in page.get("Contents", [])
                 if item["Key"].endswith(".receipt.json")]

    def read(key: str) -> dict[str, Any]:
        body = retry_read(
            lambda: client.get_object(Bucket=landing_bucket, Key=key)["Body"].read(),
            "read_landing_receipt",
        )
        receipt = json.loads(body)
        sha = receipt.get("sha256")
        size = receipt.get("size_bytes")
        obj = receipt.get("object") or {}
        if (
            receipt.get("schema_version") != 2
            or receipt.get("kind") != "eurostat_landing"
            or receipt.get("area") != "data"
            or not isinstance(sha, str) or SHA_RE.fullmatch(sha) is None
            or type(size) is not int or size <= 0
            or obj.get("bucket") != landing_bucket
            or not str(obj.get("key", "")).startswith(LANDING_DATA_PREFIX)
        ):
            raise RuntimeError(f"invalid Eurostat Landing receipt: {key}")
        return {"source": "landing", "raw_sha256": sha, "size": size,
                "receipt_key": key, "receipt_bytes": body, "data_key": obj["key"],
                "dataset_id": receipt.get("dataset_code"),
                "order": (str(receipt.get("version_token") or ""), str(receipt.get("ingested_at") or ""))}

    with ThreadPoolExecutor(max_workers=16) as pool:
        receipts = list(pool.map(read, sorted(keys)))
    latest: dict[str, dict[str, Any]] = {}
    for item in receipts:
        held = latest.get(item["dataset_id"])
        if held is None or item["order"] > held["order"]:
            latest[item["dataset_id"]] = item
    return sorted(latest.values(), key=lambda item: item["dataset_id"])


def recorded_failures(client, bucket: str) -> set[str]:
    failed = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=FAILURE_PREFIX):
        for item in page.get("Contents", []):
            name = PurePosixPath(item["Key"]).name
            if name.endswith(".json"):
                failed.add(name[: -len(".json")])
    return failed


def record_failure(client, bucket: str, raw_sha: str, detail: dict[str, Any]) -> None:
    key = f"{FAILURE_PREFIX}{raw_sha}.json"
    try:
        client.head_object(Bucket=bucket, Key=key)
        return
    except ClientError:
        pass
    client.put_object(Bucket=bucket, Key=key, ContentType="application/json",
                      Body=json.dumps(detail, sort_keys=True, indent=2).encode())


def stream_landing_raw(client, bucket: str, key: str, expected_sha: str,
                       expected_size: int, destination: Path) -> None:
    if shutil.disk_usage(destination.parent).free < expected_size * 4 + 1024 * 1024 * 1024:
        raise RuntimeError("runner disk headroom is insufficient for Eurostat decode")
    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    digest = sha256()
    size = 0
    try:
        with destination.open("xb") as handle:
            for block in iter(lambda: body.read(8 * 1024 * 1024), b""):
                size += len(block)
                if size > expected_size:
                    raise RuntimeError("Eurostat Landing object exceeded its receipt size")
                digest.update(block)
                handle.write(block)
    finally:
        body.close()
    if size != expected_size or digest.hexdigest() != expected_sha:
        raise RuntimeError(f"Eurostat Landing bytes do not match their receipt: {key}")


def annotate(level: str, title: str, data: dict[str, Any]) -> None:
    if os.environ.get("GITHUB_ACTIONS") == "true":
        message = json.dumps(data, sort_keys=True, default=str, separators=(",", ":"))
        message = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::{level} title={title}::{message[:60000]}", flush=True)


def data_file(table, uri: str, rows: int, size: int) -> DataFile:
    return DataFile.from_args(
        _table_format_version=int(table.metadata.format_version),
        content=DataFileContent.DATA,
        file_path=uri,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=rows,
        file_size_in_bytes=size,
        spec_id=int(table.metadata.default_spec_id),
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-distributions", type=int, default=DEFAULT_MAX_DISTRIBUTIONS)
    parser.add_argument("--session-seconds", type=int, default=3000)
    args = parser.parse_args(argv)
    if not 1 <= args.max_distributions <= 64:
        parser.error("--max-distributions must be 1-64")
    if not 60 <= args.session_seconds <= 6600:
        parser.error("--session-seconds must be 60-6600")

    started = time.monotonic()
    client = s3()
    cat = retry_read(catalog, "catalog_configuration")
    table = retry_read(lambda: cat.load_table(TABLE_ID), "load_eurostat_table")
    bucket = req("R2_LAKEHOUSE_BUCKET")
    landing_bucket = req("R2_LANDING_BUCKET")
    existing = existing_raw_hashes()
    failed_before = recorded_failures(client, bucket)
    landing_files = landing_candidates(client, landing_bucket)
    candidates: list[dict[str, Any]] = []
    seen_raw: set[str] = set()
    for item in landing_files:
        sha_value = item["raw_sha256"]
        if sha_value in existing or sha_value in seen_raw or sha_value in failed_before:
            continue
        seen_raw.add(sha_value)
        candidates.append(item)
    candidates.sort(key=lambda item: (item["size"], item["raw_sha256"]))
    missing_data = len(candidates)
    selected = candidates[: args.max_distributions]

    print(json.dumps({
        "status": "eurostat_retained_candidates_selected",
        "landing_datasets_latest": len(landing_files),
        "existing_distinct_raw_sha256": len(existing),
        "missing_distinct_raw_sha256": missing_data,
        "recorded_failures_skipped": len(failed_before),
        "selected": len(selected),
        "selected_raw_bytes": sum(int(item["size"]) for item in selected),
    }, sort_keys=True), flush=True)

    if not selected:
        print(json.dumps({
            "result": "pass",
            "operation": "catch-up-eurostat-r2-bronze",
            "processed": 0,
            "existing_distinct_raw_sha256": len(existing),
            "missing_distinct_raw_sha256": missing_data,
            "recorded_failures_skipped": len(failed_before),
            "reason": "no_missing_landing_files",
            "landing_deleted": False,
        }, sort_keys=True))
        return 0

    prepared: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    current_names = [field.name for field in table.schema().fields]
    with tempfile.TemporaryDirectory(prefix="zohelo-eurostat-r2-catchup-") as temporary:
        root = Path(temporary)
        for index, item in enumerate(selected):
            if (prepared or failed) and time.monotonic() - started >= args.session_seconds:
                break
            raw_sha = item["raw_sha256"]
            work = root / f"{index:03d}-{raw_sha}"
            work.mkdir()
            raw_path = work / "distribution.tsv.gz"
            receipt_path = work / "receipt.json"
            output_path = work / "observations.parquet"
            try:
                receipt_path.write_bytes(item["receipt_bytes"])
                stream_landing_raw(client, landing_bucket, item["data_key"], raw_sha,
                                   int(item["size"]), raw_path)
                report = decode_landing_distribution(
                    raw_path,
                    receipt_path,
                    output_path,
                    receipt_uri=f"r2://{landing_bucket}/{item['receipt_key']}",
                    raw_uri=f"r2://{landing_bucket}/{item['data_key']}",
                )
            except EurostatBulkDecodeError as exc:
                # The file is intact but does not satisfy the Bronze contract: record it
                # durably and keep going, so one file cannot block every later batch.
                detail = {
                    "raw_sha256": raw_sha,
                    "layout": item["source"],
                    "dataset_id": item.get("dataset_id"),
                    "landing_key": item.get("data_key"),
                    "error": str(exc)[:1000],
                    "recorded_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "code_sha": os.environ.get("GITHUB_SHA", "unknown"),
                }
                record_failure(client, bucket, raw_sha, detail)
                failed.append(detail)
                print(json.dumps({"status": "eurostat_distribution_rejected", **detail},
                                 sort_keys=True), flush=True)
                shutil.rmtree(work, ignore_errors=True)
                continue
            output_names = list(pq.read_schema(output_path).names)
            if output_names != current_names:
                raise RuntimeError(
                    f"Eurostat decoder schema differs from Iceberg table: {output_names}"
                )
            uri, size, rows = put_output(client, bucket, raw_sha, output_path)
            if rows != int(report["observation_cells"]):
                raise RuntimeError("Eurostat decoder row count differs from Parquet footer")
            prepared.append({
                "raw_sha256": raw_sha,
                "uri": uri,
                "rows": rows,
                "bytes": size,
                "layout": item["source"],
                "dataset_id": report.get("dataset_id"),
                "distribution_id": report.get("distribution_id"),
            })
            print(json.dumps({
                "status": "eurostat_retained_distribution_prepared",
                **prepared[-1],
            }, sort_keys=True), flush=True)
            shutil.rmtree(work, ignore_errors=True)

    if not prepared:
        print(json.dumps({
            "result": "pass",
            "operation": "catch-up-eurostat-r2-bronze",
            "processed": len(failed),
            "appended_files": 0,
            "failed": len(failed),
            "failed_datasets": [f["dataset_id"] for f in failed],
            "existing_distinct_raw_sha256": len(existing),
            "missing_distinct_raw_sha256": missing_data - len(failed),
            "missing_distinct_raw_sha256_before": missing_data,
            "landing_deleted": False,
        }, sort_keys=True), flush=True)
        return 0

    # A lost commit response is ambiguous. Reload catalog membership before every
    # attempt; never resend an append based only on a connection error or SQL lag.
    before_paths = None
    for attempt in range(len(RETRY_DELAYS) + 1):
        table = retry_read(lambda: cat.load_table(TABLE_ID), "reload_eurostat_table")
        existing_files = retry_read(
            lambda: {str(task.file.file_path): task.file for task in table.scan().plan_files()},
            "read_eurostat_file_membership",
        )
        if before_paths is None:
            before_paths = set(existing_files)
        for item in prepared:
            present = existing_files.get(item["uri"])
            if present is not None and (
                int(present.record_count) != int(item["rows"])
                or int(present.file_size_in_bytes) != int(item["bytes"])
            ):
                raise RuntimeError("existing Eurostat Iceberg file identity differs")
        missing = [item for item in prepared if item["uri"] not in existing_files]
        if not missing:
            break
        try:
            with table.transaction() as tx:
                if tx.table_metadata.name_mapping() is None:
                    tx.set_properties(**{
                        TableProperties.DEFAULT_NAME_MAPPING:
                        tx.table_metadata.schema().name_mapping.model_dump_json()
                    })
                with tx.update_snapshot(snapshot_properties={
                    "zohelo.operation": "catch-up-eurostat-r2-bronze",
                    "zohelo.source": "eurostat_bulk",
                }).fast_append() as append:
                    for item in missing:
                        append.append_data_file(
                            data_file(table, item["uri"], int(item["rows"]), int(item["bytes"]))
                        )
            break
        except Exception as exc:
            if not transient(exc) or attempt == len(RETRY_DELAYS):
                raise
            delay = RETRY_DELAYS[attempt]
            print(json.dumps({"status": "ambiguous_commit_recheck",
                              "attempt": attempt + 1, "delay_seconds": delay,
                              "error_type": type(exc).__name__}), flush=True)
            time.sleep(delay)
    to_append = [item for item in prepared if item["uri"] not in before_paths]

    # SQL indexing may trail a successful catalog commit. Verification retries do
    # not perform another append.
    for attempt in range(len(RETRY_DELAYS) + 1):
        after = existing_raw_hashes()
        missing_after_commit = [
            item["raw_sha256"] for item in prepared if item["raw_sha256"] not in after
        ]
        if not missing_after_commit or attempt == len(RETRY_DELAYS):
            break
        time.sleep(RETRY_DELAYS[attempt])
    if missing_after_commit:
        raise RuntimeError(
            f"Eurostat Iceberg commit lacks prepared raw hashes: {missing_after_commit}"
        )

    print(json.dumps({
        "result": "pass",
        "operation": "catch-up-eurostat-r2-bronze",
        "processed": len(prepared) + len(failed),
        "appended_files": len(to_append),
        "failed": len(failed),
        "failed_datasets": [f["dataset_id"] for f in failed],
        "missing_distinct_raw_sha256": missing_data - len(prepared) - len(failed),
        "rows_added": sum(int(item["rows"]) for item in to_append),
        "bytes_added": sum(int(item["bytes"]) for item in to_append),
        "existing_distinct_raw_sha256_before": len(existing),
        "existing_distinct_raw_sha256_after": len(after),
        "missing_distinct_raw_sha256_before": missing_data,
        "landing_deleted": False,
        "elapsed_seconds": round(time.monotonic() - started, 1),
    }, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
