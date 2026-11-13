"""Shared R2 access for ingestion: S3 client, R2 SQL queries and Landing listings."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import PurePosixPath
import random
import re
import threading
import time
from typing import Any

import boto3
from botocore.config import Config
import requests

SHA_RE = re.compile(r"^[0-9a-f]{64}$")
CONTENT_ADDRESSED_RE = re.compile(r"^(raw|response)-([0-9a-f]{64})\.bin$")
SNAPSHOT = "snapshot/"
RESUMES = 20  # ranged re-reads of one broken object stream
# Landing folders of retired pipelines kept by the owner (21 Oct 2026): not this layout, never moved.
KEPT_OLD_FOLDERS = {
    "gus_dbw": ("native/", "_control/"),
    "opendata_org": ("ODO_SENZING_20260305.zip", "landing-snapshot.json"),  # Drive-era copy (Sept 2026)
}


def req(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


# ----------------------------------------------------------------------------- throttling
# R2 refuses requests while many jobs use one bucket: HTTP 429 "Too Many Requests", and 503 with
# ServiceUnavailable or SlowDown ("Reduce your rate of simultaneous reads on the same object").
# botocore's standard mode does not retry 429 at all, and stops retrying 503 once its retry quota
# is spent (about 100 retries per client, whatever max_attempts says), so one busy evening failed
# production jobs (#2985). Every storage client is made by storage_client() and shares THROTTLE.
THROTTLE_STATUSES = (429, 503)
THROTTLE_CODES = frozenset({"SlowDown", "ServiceUnavailable", "TooManyRequests", "RequestLimitExceeded"})
THROTTLE_ATTEMPTS = 10  # requests sent for one call at most: waits of 2, 4, ... 60 s, about 5 minutes
THROTTLE_BASE_SECONDS = 2.0
THROTTLE_CAP_SECONDS = 60.0
THROTTLE_BUDGET_SECONDS = 900.0  # time one process spends waiting; the shortest job timeout is 60 minutes


def throttle_code(response) -> str | None:
    """The error code of a throttle answer in botocore's (http response, parsed) pair, else None."""
    if not response:
        return None
    http, parsed = response
    error = (parsed or {}).get("Error") or {}
    code, status = str(error.get("Code") or ""), getattr(http, "status_code", None)
    if status in THROTTLE_STATUSES or code in THROTTLE_CODES or "Reduce your" in str(error.get("Message") or ""):
        return code or str(status)
    return None


class Throttle:
    """Bounded retries with exponential backoff and jitter for throttled storage requests.

    A handler of botocore's ``needs-retry`` event: it sees every attempt of every operation of a
    client (object reads and writes, listings, deletes, each part of a multipart transfer) and
    answers with the seconds to wait before the same request is sent again, or None to let the
    error through. A throttled request was refused, not applied, so sending it again writes the
    same bytes to the same key once. One call is sent at most ``attempts`` times; one process
    waits at most ``budget`` seconds in total (time during which at least one thread waits), after
    which a throttled call fails at once. Counts are kept for the activity annotation.
    """

    def __init__(
        self,
        attempts: int = THROTTLE_ATTEMPTS,
        base: float = THROTTLE_BASE_SECONDS,
        cap: float = THROTTLE_CAP_SECONDS,
        budget: float = THROTTLE_BUDGET_SECONDS,
        clock=time.monotonic,
        jitter=random.random,
    ) -> None:
        self.attempts, self.base, self.cap, self.budget = attempts, base, cap, budget
        self._clock, self._jitter, self._lock = clock, jitter, threading.Lock()
        self._waiting_until = 0.0
        self.waited = 0.0
        self.retries: dict[str, int] = {}
        self.gave_up: dict[str, int] = {}

    def __call__(self, response=None, attempts: int = 0, operation=None, **_) -> float | None:
        code = throttle_code(response)
        if code is None:
            return None  # not a throttle answer: botocore's own handler decides
        label = f"{getattr(operation, 'name', 'request')} {code}"
        delay = min(self.cap, self.base * 2 ** max(0, attempts - 1)) * (0.5 + self._jitter() / 2)
        asked = str(getattr(response[0], "headers", {}).get("Retry-After") or "")
        if asked.isdigit():
            delay = max(delay, min(self.cap, float(asked)))
        with self._lock:
            now = self._clock()
            added = max(0.0, now + delay - max(now, self._waiting_until))
            reason = "attempts" if attempts >= self.attempts else "budget" if self.waited + added > self.budget else ""
            if reason:
                self.gave_up[f"{label} ({reason})"] = self.gave_up.get(f"{label} ({reason})", 0) + 1
                return None
            self.waited += added
            self._waiting_until = max(self._waiting_until, now + delay)
            self.retries[label] = self.retries.get(label, 0) + 1
        return delay

    def report(self) -> dict[str, Any]:
        """Retries so far by operation and code; empty when nothing was throttled."""
        with self._lock:
            if not self.retries and not self.gave_up:
                return {}
            return {
                "retries": sum(self.retries.values()),
                "waited_seconds": round(self.waited, 1),
                "by_operation": dict(sorted(self.retries.items())),
                "gave_up": dict(sorted(self.gave_up.items())),
                "limits": {"attempts_per_call": self.attempts, "wait_budget_seconds": self.budget},
            }


THROTTLE = Throttle()


def storage_client(max_pool_connections: int = 10, throttle: Throttle = THROTTLE):
    """The S3 client of every activity: R2 endpoint, writer key, and THROTTLE on every operation.

    botocore's standard mode still retries broken connections, timeouts and 5xx answers."""
    client = boto3.client(
        "s3",
        endpoint_url=req("STORAGE_ENDPOINT_URL").rstrip("/"),
        region_name="auto",
        aws_access_key_id=req("STORAGE_WRITE_ACCESS_KEY"),
        aws_secret_access_key=req("STORAGE_WRITE_SECRET_KEY"),
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 8, "mode": "standard"},
            max_pool_connections=max_pool_connections,
        ),
    )
    client.meta.events.register_first("needs-retry.s3", throttle)
    return client


def s3():
    return storage_client()


def sql(query: str) -> list[dict[str, Any]]:
    """Run one R2 SQL query and return its rows as dicts."""
    url = (
        f"https://api.sql.cloudflarestorage.com/api/v1/accounts/"
        f"{req('PLATFORM_ACCOUNT_ID')}/r2-sql/query/{req('STORAGE_LAKEHOUSE_BUCKET_NAME')}"
    )
    response = requests.post(
        url,
        json={"query": query},
        timeout=180,
        headers={
            "Authorization": f"Bearer {req('STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN')}",
            "Content-Type": "application/json",
        },
    )
    if response.status_code != 200:
        raise RuntimeError(f"R2 SQL {response.status_code}: {response.text[:500]}")
    payload = response.json()
    if payload.get("success") is False:
        raise RuntimeError(f"R2 SQL failed: {json.dumps(payload)[:800]}")
    candidates = []
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
                return [dict(zip(columns, row, strict=False)) for row in rows]
        rows = candidate.get("rows")
        if isinstance(rows, list) and (not rows or isinstance(rows[0], dict)):
            return rows
    raise RuntimeError(f"unsupported R2 SQL response: {json.dumps(payload)[:1000]}")


def sha_column(query: str, column: str) -> set[str]:
    values = set()
    for row in sql(query):
        value = row.get(column)
        if value is None:
            continue
        if not isinstance(value, str) or not SHA_RE.fullmatch(value):
            raise RuntimeError(f"invalid {column} value from R2 SQL: {value!r}")
        values.add(value)
    return values


def head_sha256(client, bucket: str, keys: list[str]) -> dict[str, str]:
    """SHA-256 recorded in each object's metadata at write time."""

    def head(key: str) -> tuple[str, str]:
        digest = str((client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256", ""))
        if not SHA_RE.fullmatch(digest):
            raise RuntimeError(f"Landing object has no SHA-256 metadata: {key}")
        return key, digest

    with ThreadPoolExecutor(max_workers=16) as pool:
        return dict(pool.map(head, keys))


def get_with_backoff(client, bucket: str, key: str, attempts: int = 6) -> bytes:
    """GET an object, waiting out R2 throttling (ServiceUnavailable / SlowDown) with backoff."""
    import time

    from botocore.exceptions import ClientError

    for attempt in range(attempts):
        try:
            return client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code not in {"ServiceUnavailable", "SlowDown", "InternalError", "503"} or attempt == attempts - 1:
                raise
            time.sleep(2**attempt * 2)


def landing_objects(client, bucket: str, source: str) -> dict[str, dict[str, Any]]:
    """Landing files of one source by SHA-256, including files already moved to Archive.

    Agreed layout (docs/landing.md): 01_landing/<source>/snapshot/<area>/<entity>/<version>__<ingested>.<ext>.
    Every receipt counts, whether its data file is still in Landing (``present``) or was moved
    to Archive (the receipt stays as the pointer); a data file without a receipt is identified
    by its SHA-256 object metadata. Content-addressed raw-/response-<sha>.bin names of earlier
    layouts are included as they are.
    """
    prefix = f"01_landing/{source}/"
    objects, receipt_keys, result = {}, [], {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key, size = str(item["Key"]), int(item.get("Size", 0))
            if size <= 0 or key.endswith("/"):
                continue
            rest = key[len(prefix) :]
            if rest.startswith(KEPT_OLD_FOLDERS.get(source, ())):
                continue
            if rest.startswith(SNAPSHOT):
                if key.endswith(".receipt.json"):
                    receipt_keys.append(key)
                else:
                    objects[key] = size
                continue
            match = CONTENT_ADDRESSED_RE.fullmatch(PurePosixPath(key).name)
            if not match:
                raise RuntimeError(f"unexpected Landing object: {key}")
            kind, digest = match.groups()
            result.setdefault(
                digest,
                {
                    "key": key,
                    "size": size,
                    "kind": kind,
                    "present": True,
                    "layout": "content_addressed",
                    "sha256": digest,
                },
            )

    def read(key: str) -> tuple[str, str, int, str | None] | None:
        receipt = json.loads(get_with_backoff(client, bucket, key))
        digest, size = receipt.get("sha256"), receipt.get("size_bytes")
        data_key = (receipt.get("object") or {}).get("key")
        if not (isinstance(digest, str) and SHA_RE.fullmatch(digest) and type(size) is int and data_key):
            return None
        return data_key, digest, size, key

    with ThreadPoolExecutor(max_workers=8) as pool:  # R2 throttles bursts of reads
        receipted = {r[0]: r for r in pool.map(read, receipt_keys) if r}
    unreceipted = [k for k in objects if k not in receipted]
    for key, digest in head_sha256(client, bucket, unreceipted).items():
        receipted[key] = (key, digest, objects[key], None)
    for data_key, digest, size, receipt_key in receipted.values():
        if not data_key.startswith(prefix + SNAPSHOT):
            continue
        present = data_key in objects
        if present and objects[data_key] != size:
            raise RuntimeError(f"Landing object size differs from its receipt: {data_key}")
        area, entity, name = data_key[len(prefix) + len(SNAPSHOT) :].split("/", 2)
        token, _, rest = name.partition("__")
        ingested = rest.split(".", 1)[0].split("__", 1)[0]
        held = result.get(digest)
        if held is not None and int(held["size"]) != size:
            raise RuntimeError(f"same SHA-256 with different sizes in Landing: {digest}")
        # Identical bytes can sit under several entities or versions; keep every snapshot path.
        paths = [
            *(held or {}).get("snapshot_paths", []),
            {
                "key": data_key,
                "area": area,
                "entity": entity,
                "version": token,
                "ingested": ingested,
                "present": present,
                "receipt": receipt_key,
            },
        ]
        result[digest] = {
            "key": data_key if present or held is None else held["key"],
            "size": size,
            "kind": "raw" if area == "data" else "metadata",
            "layout": "snapshot",
            "area": area,
            "entity": entity,
            "version": token,
            "ingested": ingested,
            "sha256": digest,
            "snapshot_paths": paths,
            "present": present or bool(held and held.get("present")),
        }
    return result


def latest_snapshot(meta: dict[str, dict[str, Any]], area: str = "data") -> set[str]:
    """Digests of the latest version (then latest ingest) of every snapshot entity."""
    latest: dict[str, tuple[tuple[str, str], str]] = {}
    for digest, item in meta.items():
        for path in item.get("snapshot_paths", []):
            if path["area"] != area:
                continue
            order = (path["version"], path["ingested"])
            if path["entity"] not in latest or order > latest[path["entity"]][0]:
                latest[path["entity"]] = (order, digest)
    return {digest for _, digest in latest.values()}


# Catalog GETs/HEADs retried by urllib3 on dropped or refused connections and 429/5xx.
# A long scan leaves the pooled keep-alive connection idle; the catalog closes it and the
# next request fails with RemoteDisconnected (#851 bronze.verify, build properties 20 Oct).
# POST/DELETE (commits, drops) are retried only when the connection failed before the
# request was sent (other=0: TLS errors are not retried), so a commit is never sent twice.
CATALOG_RETRY = {"total": 5, "other": 0, "backoff_factor": 2, "status_forcelist": (429, 500, 502, 503, 504)}
CATALOG_OPEN_DELAYS = (5, 20)


def catalog():
    """The R2 Data Catalog (Apache Iceberg REST) holding bronze, silver and gold."""
    import time

    from pyiceberg.catalog.rest import RestCatalog
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    for attempt in range(len(CATALOG_OPEN_DELAYS) + 1):
        try:
            cat = RestCatalog(
                name="zohelo",
                warehouse=req("STORAGE_LAKEHOUSE_CATALOG_NAME"),
                uri=req("STORAGE_LAKEHOUSE_CATALOG_URL"),
                token=req("STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN"),
                **{
                    "s3.access-key-id": req("STORAGE_WRITE_ACCESS_KEY"),
                    "s3.secret-access-key": req("STORAGE_WRITE_SECRET_KEY"),
                    "s3.endpoint": req("STORAGE_ENDPOINT_URL"),
                    "s3.region": "auto",
                },
            )
            break
        except (requests.ConnectionError, requests.Timeout):
            if attempt == len(CATALOG_OPEN_DELAYS):
                raise
            time.sleep(CATALOG_OPEN_DELAYS[attempt])
    retry = Retry(allowed_methods=frozenset({"GET", "HEAD"}), raise_on_status=False, **CATALOG_RETRY)
    cat._session.mount("https://", HTTPAdapter(max_retries=retry))
    return cat


def sources() -> dict[str, dict[str, Any]]:
    """The source registry: ingestion/sources/<source>.yaml by source id."""
    from pathlib import Path

    import yaml

    folder = Path(__file__).resolve().parents[1] / "sources"
    return {path.stem: yaml.safe_load(path.read_text()) for path in sorted(folder.glob("*.yaml"))}


# ----------------------------------------------------------------------------- Archive
# Content-addressed raw history: 05_archive/<source>/raw/<sha256>.bin holds the exact
# source bytes of an already-compressed format; <sha256>.zst holds a zstd-compressed copy
# of an uncompressed format (XML, CSV, JSON …). Both restore to the original SHA-256.
COMPRESSED_FORMATS = (".gz", ".tgz", ".zip", ".7z", ".zst", ".bz2", ".xz", ".parquet")


def archive_key(source: str, digest: str, landing_key: str) -> str:
    native = landing_key.lower().endswith(COMPRESSED_FORMATS)
    return f"05_archive/{source}/raw/{digest}{'.bin' if native else '.zst'}"


def archived(client, bucket: str, source: str) -> dict[str, dict[str, Any]]:
    """Archive objects of a source by SHA-256: key, size, last modified."""
    result = {}
    prefix = f"05_archive/{source}/raw/"
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            name = item["Key"][len(prefix) :]
            digest = name.split(".", 1)[0]
            if SHA_RE.fullmatch(digest) and "/" not in name:
                result[digest] = {"key": item["Key"], "size": int(item["Size"]), "modified": item["LastModified"]}
    return result


def raw_chunks(client, bucket: str, key: str, chunk: int = 8 << 20):
    """Yield the original bytes of a Landing or Archive object (zstd archive copies decompressed).

    A plain object read that breaks (read timeout, dropped connection) resumes from the byte it
    reached with a ranged GET, up to RESUMES times: a single 21 GB stream (OpenData) cannot be
    expected to survive in one piece."""
    if key.endswith(".zst"):
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        try:
            import zstandard

            reader = zstandard.ZstdDecompressor().stream_reader(body)
            while block := reader.read(chunk):
                yield block
        finally:
            body.close()
        return
    import botocore.exceptions

    broken = (
        botocore.exceptions.IncompleteReadError,  # the server closed early, cleanly
        botocore.exceptions.ReadTimeoutError,
        botocore.exceptions.ConnectionError,
        botocore.exceptions.ResponseStreamingError,
        ConnectionError,
        TimeoutError,
    )
    offset, resumes, etag, size = 0, 0, None, None
    while True:
        extra = {"Range": f"bytes={offset}-", "IfMatch": etag} if offset else {}
        if size is not None and offset >= size:  # the break came after the last byte
            return
        response = client.get_object(Bucket=bucket, Key=key, **extra)
        if not offset:
            etag, size = response.get("ETag"), response.get("ContentLength")
        body = response["Body"]
        failed = False
        try:
            while block := body.read(chunk):
                offset += len(block)
                yield block
            return
        except broken:
            resumes += 1
            if resumes > RESUMES:
                raise
            failed = True
        finally:
            body.close()
        if failed:
            time.sleep(min(60, 5 * resumes))
