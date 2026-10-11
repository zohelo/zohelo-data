"""Manual Eurostat bulk TSV downloads into private R2 Landing (raw bytes only).

Owner-authorized 11 Oct 2026 as the only resumed ingestion path. It never parses
observations and never touches Bronze.

plan    Download the official data inventory, classify every non-Comext dataset
        (current / current_legacy / stale / missing) and write the classification
        to 06_control/eurostat_bulk_landing/coverage/. Downloads no dataset.
ingest  Download missing datasets first, then stale ones, to
        01_landing/eurostat/bulk/data/{CODE}/{version}.tsv.gz and write
        {version}.receipt.json next to it. The receipt marks completion; R2 is the
        only state, so a rerun resumes. Nothing is ever overwritten or deleted.

--limit applies per shard. Shards split datasets by a hash of the dataset code.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import fnmatch
import hashlib
import io
import json
import os
from pathlib import Path
import queue
import random
import re
import shutil
import sys
import tempfile
import threading
import time
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit
import zlib

from ingestion.full_source_adapters import (
    EUROSTAT_INVENTORY_URL,
    FullSourceAdapterError,
    distributions_from_inventory,
    parse_eurostat_async,
)
from ingestion.full_source_campaign import task_for


SOURCE_ID = "eurostat"
LEGACY_CAMPAIGN = "eurostat_bulk"
LANDING_PREFIX = "01_landing/eurostat/bulk/data/"
CONTROL_PREFIX = "06_control/eurostat_bulk_landing/"
OFFICIAL_HOST = "ec.europa.eu"
USER_AGENT = "zohelo-data-eurostat-bulk-landing/1 (manual, polite; github.com/zohelo/zohelo-data)"

CHUNK = 1024 * 1024
MAX_INVENTORY_BYTES = 128 * 1024 * 1024
MAX_SMALL_BODY = 64 * 1024
CONNECT_TIMEOUT = 30
READ_TIMEOUT = 300
MAX_ATTEMPTS = 4            # network / truncation / integrity failures per request
MAX_THROTTLES = 12          # 429/503 responses tolerated per request
DEFAULT_BACKOFF = 60        # seconds, when 429/503 carries no Retry-After
RETRY_BASE = 10             # seconds, transfer/integrity retry backoff base
MAX_RETRY_AFTER = 3600
ASYNC_POLL_SECONDS = 30
ASYNC_MAX_WAIT = 45 * 60
ASYNC_MAX_SUBMISSIONS = 3
MIN_FREE_DISK = 2 * 1024 ** 3
STATUSES = ("current", "current_legacy", "stale", "missing")


def transport_url(url: str) -> str:
    """Map a validated official URL to the address actually requested (identity)."""
    return url


# --------------------------------------------------------------------------- helpers

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def version_token(version: str | None) -> str:
    """Filesystem/object-key safe, deterministic token for an inventory version."""
    value = (version or "").strip()
    if not value:
        return "noversion"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        parsed = None
    if parsed is not None and parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe = re.sub(r"[^0-9A-Za-z.-]+", "-", value).strip("-")[:64] or "v"
    return f"{safe}-{hashlib.sha256(value.encode()).hexdigest()[:8]}"


def shard_of(code: str, shards: int) -> int:
    return int(hashlib.sha256(code.encode("utf-8")).hexdigest()[:16], 16) % shards


def request_url(url: str, params: dict) -> str:
    if not params:
        return url
    parts = urlsplit(url)
    query = "&".join(p for p in (parts.query, urlencode(params, doseq=True)) if p)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))


def require_official(url: str) -> str:
    parts = urlsplit(url)
    if (parts.scheme != "https" or (parts.hostname or "").lower() != OFFICIAL_HOST
            or parts.port not in (None, 443) or parts.username or parts.password):
        raise FullSourceAdapterError(f"refusing non-official URL: {url[:200]}")
    return url


def retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def landing_keys(code: str, token: str) -> tuple[str, str]:
    base = f"{LANDING_PREFIX}{code}/{token}"
    return base + ".tsv.gz", base + ".receipt.json"


# --------------------------------------------------------------------------- R2

class R2:
    def __init__(self) -> None:
        import boto3
        from boto3.s3.transfer import TransferConfig
        from botocore.config import Config

        def env(name: str) -> str:
            value = os.environ.get(name, "").strip()
            if not value:
                raise SystemExit(f"missing R2 configuration: {name}")
            return value

        self.landing = env("R2_LANDING_BUCKET")
        self.lakehouse = env("R2_LAKEHOUSE_BUCKET")
        self.client = boto3.client(
            "s3",
            endpoint_url=env("R2_S3_ENDPOINT").rstrip("/"),
            region_name="auto",
            aws_access_key_id=env("CLOUDFLARE_R2_ACCESS_KEY_ID"),
            aws_secret_access_key=env("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
            config=Config(signature_version="s3v4",
                          retries={"max_attempts": 10, "mode": "standard"},
                          max_pool_connections=32),
        )
        self.transfer = TransferConfig(multipart_threshold=64 * 1024 * 1024,
                                       multipart_chunksize=64 * 1024 * 1024,
                                       max_concurrency=4, use_threads=True)

    def head(self, bucket: str, key: str) -> dict | None:
        from botocore.exceptions import ClientError
        try:
            return self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    def put_new(self, bucket: str, key: str, body: bytes, content_type: str) -> None:
        """Create an object that must not exist yet (never overwrite)."""
        if self.head(bucket, key) is not None:
            raise RuntimeError(f"refusing to overwrite r2://{bucket}/{key}")
        self.client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type,
                               Metadata={"sha256": hashlib.sha256(body).hexdigest()})
        if self.head(bucket, key) is None:
            raise RuntimeError(f"write of r2://{bucket}/{key} could not be verified")

    def list_keys(self, bucket: str, prefix: str):
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                yield item["Key"]


# --------------------------------------------------------------------------- politeness

class Pacer:
    """One pause shared by all workers: a 429/503 stops every new request."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pause_until = 0.0
        self._consecutive = 0
        self.events: list[dict] = []

    def throttle(self, status: int, retry_after: float | None) -> float:
        with self._lock:
            self._consecutive += 1
            if retry_after is None:
                delay = min(900.0, DEFAULT_BACKOFF * 2 ** min(self._consecutive - 1, 4))
            else:
                delay = min(float(MAX_RETRY_AFTER), max(1.0, retry_after))
            until = time.monotonic() + delay
            if until > self._pause_until:
                self._pause_until = until
            self.events.append({"at": utc_now(), "status": status,
                                "retry_after": retry_after, "pause_seconds": round(delay, 1)})
            log({"event": "provider_pause", "status": status, "retry_after": retry_after,
                 "pause_seconds": round(delay, 1)})
            return delay

    def ok(self) -> None:
        with self._lock:
            self._consecutive = 0

    def wait(self, deadline: float) -> bool:
        """Sleep through a shared pause; False when it would cross ``deadline``."""
        while True:
            with self._lock:
                remaining = self._pause_until - time.monotonic()
            if remaining <= 0:
                return True
            if time.monotonic() + remaining > deadline:
                return False
            time.sleep(min(remaining, 5.0))


_print_lock = threading.Lock()


def log(value: dict) -> None:
    with _print_lock:
        print(json.dumps(value, sort_keys=True, default=str), flush=True)


# --------------------------------------------------------------------------- download

class Throttled(Exception):
    def __init__(self, status: int, retry_after: float | None) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status
        self.retry_after = retry_after


class Retryable(Exception):
    pass


class Failed(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail[:400]


class GzipCheck:
    """Streaming, constant-memory integrity check of a (multi-member) gzip stream."""

    def __init__(self) -> None:
        self._d = None
        self._padding = False
        self.members = 0
        self.decompressed_bytes = 0
        self.prefix = bytearray()

    def _take(self, out: bytes) -> None:
        self.decompressed_bytes += len(out)
        if len(self.prefix) < 4096:
            self.prefix += out[: 4096 - len(self.prefix)]

    def feed(self, data: bytes) -> None:
        while data:
            if self._d is None:
                if self.members:
                    stripped = data.lstrip(b"\x00")
                    if len(stripped) != len(data):
                        self._padding = True
                    data = stripped
                    if not data:
                        return
                    if self._padding:
                        raise zlib.error("data after gzip padding")
                self._d = zlib.decompressobj(wbits=31)
                self.members += 1
            self._take(self._d.decompress(data, CHUNK))
            while self._d.unconsumed_tail:
                self._take(self._d.decompress(self._d.unconsumed_tail, CHUNK))
            if self._d.eof:
                data = self._d.unused_data
                self._d = None
            else:
                data = b""

    def finish(self) -> None:
        if self._d is not None:
            self._take(self._d.flush())
            raise zlib.error("gzip stream ends inside a member (truncated)")
        if not self.members:
            raise zlib.error("empty gzip stream")


class Download:
    def __init__(self, status: int, final_url: str, headers: dict, path: Path,
                 sha256: str, size: int, gzip: GzipCheck | None, small: bytes) -> None:
        self.status = status
        self.final_url = final_url
        self.headers = headers
        self.path = path
        self.sha256 = sha256
        self.size = size
        self.gzip = gzip
        self.small = small


KEPT_HEADERS = ("content-type", "content-length", "content-encoding", "content-disposition",
                "last-modified", "etag", "date")


def fetch(session, url: str, dest: Path, hard_deadline: float, max_bytes: int | None = None,
          allow_plain: bool = False) -> Download:
    """One GET, body streamed to ``dest`` with SHA-256 and gzip verification."""
    current = require_official(url)
    for _ in range(6):
        if time.monotonic() > hard_deadline:
            raise Failed("aborted_time_budget", "hard deadline reached before request")
        try:
            response = session.get(transport_url(current), stream=True, allow_redirects=False,
                                   timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                                   headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"})
        except Exception as exc:  # connection errors, timeouts
            raise Retryable(f"{type(exc).__name__}: {exc}") from exc
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get("Location", "")
            response.close()
            current = require_official(urljoin(current, location))
            continue
        break
    else:
        raise Failed("too_many_redirects", current)

    with response:
        status = response.status_code
        headers = {k: response.headers[k] for k in KEPT_HEADERS if k in response.headers}
        if status in (429, 503):
            raise Throttled(status, retry_after_seconds(response.headers.get("Retry-After")))
        if status != 200:
            try:
                small = response.raw.read(MAX_SMALL_BODY, decode_content=False) or b""
            except Exception:
                small = b""
            return Download(status, current, headers, dest, "", len(small), None, small)

        expected = response.headers.get("Content-Length")
        expected = int(expected) if expected and expected.isdigit() else None
        digest = hashlib.sha256()
        size = 0
        check: GzipCheck | None = None
        head = b""
        decided = False
        try:
            with dest.open("wb") as handle:
                for chunk in response.raw.stream(CHUNK, decode_content=False):
                    if not chunk:
                        continue
                    if time.monotonic() > hard_deadline:
                        raise Failed("aborted_time_budget", "hard deadline reached mid-download")
                    handle.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
                    if not decided:
                        head += chunk
                        if len(head) < 2:
                            continue
                        decided = True
                        if head[:2] == b"\x1f\x8b":
                            check = GzipCheck()
                            check.feed(head)
                        head = head[:MAX_SMALL_BODY]
                    elif check is not None:
                        check.feed(chunk)
                    if check is None and not allow_plain and size > MAX_SMALL_BODY:
                        raise Failed("not_gzip", "non-gzip body larger than a control envelope")
                    if max_bytes is not None and size > max_bytes:
                        raise Failed("too_large", f"body exceeds {max_bytes} bytes")
                    if size % (64 * CHUNK) < len(chunk):
                        if shutil.disk_usage(dest.parent).free < MIN_FREE_DISK // 2:
                            raise Failed("runner_disk_low", "free disk fell below 1 GiB")
        except (Failed, zlib.error):
            raise
        except Exception as exc:  # ChunkedEncodingError, ProtocolError, read timeout
            raise Retryable(f"{type(exc).__name__}: {exc}") from exc
        if expected is not None and "content-encoding" not in headers and size != expected:
            raise Retryable(f"truncated body: {size} of {expected} bytes")
        if check is not None:
            check.finish()
        small = b"" if check is not None else head
        return Download(status, current, headers, dest, digest.hexdigest(), size, check, small)


def attempt(ctx: "Context", session, url: str, dest: Path, max_bytes: int | None = None,
            allow_plain: bool = False) -> Download:
    """Fetch with bounded retries; 429/503 pause every worker via the shared pacer."""
    failures = 0
    throttles = 0
    while True:
        if not ctx.pacer.wait(ctx.hard_deadline):
            raise Failed("aborted_time_budget", "provider pause extends past the time budget")
        try:
            result = fetch(session, url, dest, ctx.hard_deadline, max_bytes, allow_plain)
        except Throttled as exc:
            throttles += 1
            ctx.count("throttled_responses")
            if throttles > MAX_THROTTLES:
                raise Failed("throttled", f"{throttles} consecutive 429/503 responses")
            ctx.pacer.throttle(exc.status, exc.retry_after)
            continue
        except (Retryable, zlib.error) as exc:
            failures += 1
            ctx.count("retried_transfers")
            log({"event": "retry", "url": url, "attempt": failures, "detail": str(exc)[:300]})
            if failures >= MAX_ATTEMPTS:
                raise Failed("transfer_failed", str(exc))
            time.sleep(min(120.0, RETRY_BASE * 2 ** failures + random.uniform(0, RETRY_BASE / 2)))
            continue
        if 500 <= result.status < 600:
            failures += 1
            if failures >= MAX_ATTEMPTS:
                raise Failed(f"http_{result.status}", result.small[:300].decode("utf-8", "replace"))
            time.sleep(min(120.0, RETRY_BASE * 2 ** failures))
            continue
        ctx.pacer.ok()
        return result


def classify_body(result: Download) -> tuple[str, dict]:
    """Return ('data'|'envelope'|'needs_partition', info) or raise Failed."""
    if result.status == 413:
        return "needs_partition", {"http_status": 413}
    if result.status == 404:
        raise Failed("not_found", result.small[:300].decode("utf-8", "replace"))
    if result.status != 200:
        raise Failed(f"http_{result.status}", result.small[:300].decode("utf-8", "replace"))
    if result.gzip is not None:
        body = bytes(result.gzip.prefix)
        if body.lstrip()[:1] == b"<":
            if result.size > MAX_SMALL_BODY:
                raise Failed("unexpected_content", "large gzip body starting with XML")
            import gzip as _gzip
            body = _gzip.decompress(result.path.read_bytes())
            return "envelope", parse_envelope(body)
        if re.search(rb'"status"\s*:\s*413', body):
            return "needs_partition", {"provider_body": body[:200].decode("utf-8", "replace")}
        first_line = body.split(b"\n", 1)[0].lower()
        if b"\\time_period" not in first_line:
            raise Failed("unexpected_content", "gzip body is not a Eurostat TSV (no \\TIME_PERIOD header)")
        return "data", {}
    body = result.small
    if body.lstrip()[:1] == b"<":
        return "envelope", parse_envelope(body)
    if re.search(rb'"status"\s*:\s*413', body):
        return "needs_partition", {"provider_body": body[:200].decode("utf-8", "replace")}
    raise Failed("not_gzip", body[:200].decode("utf-8", "replace"))


def parse_envelope(body: bytes) -> dict:
    try:
        return parse_eurostat_async(body)
    except FullSourceAdapterError as exc:
        raise Failed("bad_async_envelope", str(exc)) from exc


# --------------------------------------------------------------------------- runtime

class Context:
    def __init__(self, r2: R2 | None, workdir: Path, deadline: float, hard_deadline: float,
                 run_id: str, code_sha: str, inventory_ref: dict) -> None:
        self.r2 = r2
        self.workdir = workdir
        self.deadline = deadline
        self.hard_deadline = hard_deadline
        self.run_id = run_id
        self.code_sha = code_sha
        self.inventory_ref = inventory_ref
        self.pacer = Pacer()
        self.async_lock = threading.Lock()
        self._lock = threading.Lock()
        self.counters: dict[str, int] = defaultdict(int)
        self.results: list[dict] = []

    def count(self, name: str, value: int = 1) -> None:
        with self._lock:
            self.counters[name] += value

    def record(self, result: dict) -> None:
        with self._lock:
            self.results.append(result)
        log({"event": "dataset", **result})


def follow_async(ctx: Context, session, item: dict, envelope: dict, dest: Path) -> tuple[Download, dict]:
    """Drive one asynchronous request to completion. Callers hold ctx.async_lock."""
    ctx.count("async_envelopes")
    started = time.monotonic()
    submissions = 1
    request_ids = []
    parsed = envelope
    while True:
        status = parsed.get("status")
        if parsed.get("request_id"):
            request_ids.append(parsed["request_id"])
        if status == "unsupported" and parsed.get("http_status") == 413:
            raise Failed("needs_partition", parsed.get("fault_message", "EXTRACTION_TOO_BIG"))
        if status == "no_results":
            raise Failed("no_results", parsed.get("fault_message", ""))
        if status == "available":
            result = attempt(ctx, session, parsed["ready"]["url"], dest)
        elif status == "async":
            if time.monotonic() - started > ASYNC_MAX_WAIT:
                raise Failed("async_timeout", f"not ready after {ASYNC_MAX_WAIT}s")
            if time.monotonic() + ASYNC_POLL_SECONDS > ctx.hard_deadline:
                raise Failed("aborted_time_budget", "async preparation crosses the hard deadline")
            time.sleep(ASYNC_POLL_SECONDS)
            poll = parsed.get("poll") or {}
            if not poll.get("url"):
                # DATA_NOT_YET_AVAILABLE faults carry no id: submit the request again.
                result = attempt(ctx, session, item["url"], dest)
            else:
                result = attempt(ctx, session, poll["url"], dest, max_bytes=MAX_SMALL_BODY)
        elif status in ("expired", "async_error"):
            submissions += 1
            if submissions > ASYNC_MAX_SUBMISSIONS:
                raise Failed("async_failed", f"{parsed.get('provider_status')} after {submissions - 1} submissions")
            result = attempt(ctx, session, item["url"], dest)
        else:
            raise Failed("async_unsupported", json.dumps(parsed, default=str)[:300])
        kind, info = classify_body(result)
        if kind == "data":
            return result, {"mode": "async", "request_ids": sorted(set(request_ids)),
                            "submissions": submissions}
        if kind == "needs_partition":
            raise Failed("needs_partition", json.dumps(info)[:300])
        parsed = info


def finalize(ctx: Context, item: dict, result: Download, via: dict, retrieved_at: str) -> dict:
    r2 = ctx.r2
    data_key, receipt_key = item["data_key"], item["receipt_key"]
    if r2.head(r2.landing, receipt_key) is not None:
        return {"outcome": "already_current"}
    existing = r2.head(r2.landing, data_key)
    if existing is not None:
        meta = existing.get("Metadata", {})
        if meta.get("sha256") != result.sha256 or int(existing["ContentLength"]) != result.size:
            raise Failed("orphan_conflict", "data object exists with different bytes and no receipt")
        reused = True
    else:
        r2.client.upload_file(
            str(result.path), r2.landing, data_key, Config=r2.transfer,
            ExtraArgs={"ContentType": "application/gzip",
                       "Metadata": {"sha256": result.sha256, "size": str(result.size),
                                    "retrieved-at": retrieved_at,
                                    "eurostat-version": item["version"] or "",
                                    "gzip-members": str(result.gzip.members),
                                    "decompressed-bytes": str(result.gzip.decompressed_bytes),
                                    "final-url": result.final_url[:512],
                                    "transfer-mode": via["mode"],
                                    "source-url": item["url"][:512]}})
        reused = False
    stored = r2.head(r2.landing, data_key)
    if (stored is None or int(stored["ContentLength"]) != result.size
            or stored.get("Metadata", {}).get("sha256") != result.sha256):
        raise RuntimeError(f"uploaded object r2://{r2.landing}/{data_key} failed verification")
    write_receipt(ctx, item, {
        "sha256": result.sha256,
        "size_bytes": result.size,
        "gzip": {"members": result.gzip.members,
                 "decompressed_bytes": result.gzip.decompressed_bytes,
                 "integrity": "verified"},
        "final_url": result.final_url,
        "retrieved_at": retrieved_at,
        "http_headers": result.headers,
        "transfer": via,
        "object": {"bucket": r2.landing, "key": data_key, "reused_existing_object": reused},
    })
    return {"outcome": "landed", "sha256": result.sha256, "size_bytes": result.size,
            "reused_existing_object": reused, "via": via["mode"]}


def write_receipt(ctx: Context, item: dict, observed: dict) -> None:
    receipt = {
        "schema_version": 1,
        "kind": "eurostat_bulk_landing",
        "source_id": SOURCE_ID,
        "dataset_code": item["code"],
        "eurostat_version": item["version"],
        "version_token": item["token"],
        "source_url": item["url"],
        **observed,
        "classification_at_start": item["status"],
        "inventory": ctx.inventory_ref,
        "run_id": ctx.run_id,
        "code_sha": ctx.code_sha,
    }
    ctx.r2.put_new(ctx.r2.landing, item["receipt_key"],
                   json.dumps(receipt, indent=2, sort_keys=True).encode(), "application/json")


def recover_orphan(ctx: Context, item: dict) -> dict | None:
    """Finish a data object that a previous run verified and uploaded but never receipted."""
    r2 = ctx.r2
    head = r2.head(r2.landing, item["data_key"])
    if head is None:
        return None
    meta = head.get("Metadata", {})
    required = ("sha256", "size", "retrieved-at", "gzip-members", "decompressed-bytes")
    if any(not meta.get(k) for k in required) or meta.get("size") != str(head["ContentLength"]):
        raise Failed("orphan_conflict", "data object without receipt lacks upload verification metadata")
    write_receipt(ctx, item, {
        "sha256": meta["sha256"],
        "size_bytes": int(head["ContentLength"]),
        "gzip": {"members": int(meta["gzip-members"]),
                 "decompressed_bytes": int(meta["decompressed-bytes"]),
                 "integrity": "verified_before_upload"},
        "final_url": meta.get("final-url"),
        "retrieved_at": meta["retrieved-at"],
        "http_headers": {},
        "transfer": {"mode": meta.get("transfer-mode", "unknown"),
                     "receipt_recovered_from_object_metadata": True},
        "object": {"bucket": r2.landing, "key": item["data_key"], "reused_existing_object": True},
    })
    return {"outcome": "landed", "sha256": meta["sha256"], "size_bytes": int(head["ContentLength"]),
            "reused_existing_object": True, "via": "receipt_recovery"}


def land(ctx: Context, session, item: dict) -> dict:
    base = {"dataset_code": item["code"], "eurostat_version": item["version"],
            "classification": item["status"]}
    dest = ctx.workdir / f"{threading.get_ident()}.download"
    started = time.monotonic()
    try:
        if ctx.r2.head(ctx.r2.landing, item["receipt_key"]) is not None:
            return {**base, "outcome": "already_current"}
        recovered = recover_orphan(ctx, item)
        if recovered is not None:
            return {**base, **recovered, "seconds": round(time.monotonic() - started, 1)}
        if shutil.disk_usage(ctx.workdir).free < MIN_FREE_DISK:
            raise Failed("runner_disk_low", "less than 2 GiB free before download")
        retrieved_at = utc_now()
        result = attempt(ctx, session, item["url"], dest)
        kind, info = classify_body(result)
        via = {"mode": "sync"}
        if kind == "needs_partition":
            raise Failed("needs_partition", json.dumps(info)[:300])
        if kind == "envelope" and info.get("http_status") == 413:
            raise Failed("needs_partition", info.get("fault_message", "EXTRACTION_TOO_BIG"))
        if kind == "envelope":
            log({"event": "async_envelope", "dataset_code": item["code"],
                 "status": info.get("provider_status") or info.get("status")})
            with ctx.async_lock:  # Eurostat asks for one asynchronous request at a time.
                result, via = follow_async(ctx, session, item, info, dest)
            retrieved_at = utc_now()
        outcome = finalize(ctx, item, result, via, retrieved_at)
        return {**base, **outcome, "seconds": round(time.monotonic() - started, 1)}
    except Failed as exc:
        return {**base, "outcome": "failed", "reason": exc.reason, "detail": exc.detail,
                "seconds": round(time.monotonic() - started, 1)}
    finally:
        dest.unlink(missing_ok=True)


def run_workers(ctx: Context, items: list[dict], workers: int) -> None:
    import requests

    work: queue.Queue = queue.Queue()
    for item in items:
        work.put(item)

    def worker() -> None:
        session = requests.Session()
        try:
            while True:
                if time.monotonic() >= ctx.deadline:
                    return
                try:
                    item = work.get_nowait()
                except queue.Empty:
                    return
                result = land(ctx, session, item)
                ctx.record(result)
                if result.get("reason") == "runner_disk_low":
                    return
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker) for _ in range(workers)]
        for future in futures:
            future.result()
    ctx.count("not_started_time_budget", work.qsize())


# --------------------------------------------------------------------------- planning

def fetch_inventory(ctx: Context) -> tuple[bytes, Download]:
    import requests

    url = request_url(EUROSTAT_INVENTORY_URL, {"type": "data", "lang": "en"})
    dest = ctx.workdir / "inventory.download"
    with requests.Session() as session:
        result = attempt(ctx, session, url, dest, max_bytes=MAX_INVENTORY_BYTES, allow_plain=True)
    if result.status != 200:
        raise SystemExit(f"Eurostat inventory returned HTTP {result.status}")
    body = dest.read_bytes()
    dest.unlink(missing_ok=True)
    if body[:2] == b"\x1f\x8b":
        import gzip as _gzip
        body = _gzip.decompress(body)
    return body, result


def load_landing_state(r2: R2) -> dict[str, dict[str, set]]:
    state: dict[str, dict[str, set]] = defaultdict(lambda: {"receipts": set(), "data": set()})
    for key in r2.list_keys(r2.landing, LANDING_PREFIX):
        rest = key[len(LANDING_PREFIX):]
        if "/" not in rest:
            continue
        code, name = rest.split("/", 1)
        if name.endswith(".receipt.json"):
            state[code]["receipts"].add(name[: -len(".receipt.json")])
        elif name.endswith(".tsv.gz"):
            state[code]["data"].add(name[: -len(".tsv.gz")])
    return state


def load_legacy() -> tuple[set[str], dict[str, dict], bool]:
    from ingestion.source_campaign_store import R2CampaignStore

    state = R2CampaignStore(LEGACY_CAMPAIGN, publication_only=True).load()
    completed = (state or {}).get("completed", {}) if isinstance(state, dict) else {}
    by_code: dict[str, dict] = defaultdict(lambda: {"entries": 0, "partition_leaves": 0})
    for entry in completed.values():
        if not isinstance(entry, dict):
            continue
        dataset_id = entry.get("dataset_id")
        original = entry.get("original_dataset_id")
        for code in {c for c in (dataset_id, original) if isinstance(c, str)}:
            by_code[code]["entries"] += 1
            if isinstance(dataset_id, str) and "::partition::" in dataset_id:
                by_code[code]["partition_leaves"] += 1
    return set(completed), by_code, state is not None


def plan_items(inventory_body: bytes, landing: dict, legacy_ids: set[str], legacy_by_code: dict,
               shards: int, patterns: list[str]) -> tuple[list[dict], dict]:
    distributions = distributions_from_inventory("data", inventory_body)
    stats = {"inventory_distributions": len(distributions), "excluded_comext": 0,
             "excluded_other_kinds": 0, "excluded_by_filter": 0}
    items = []
    for spec in distributions:
        if spec["kind"] != "eurostat_tsv_gzip":
            stats["excluded_other_kinds"] += 1
            continue
        code = spec["dataset_id"]
        if code.upper().startswith("DS-"):
            stats["excluded_comext"] += 1
            continue
        if patterns and not any(fnmatch.fnmatch(code.lower(), p) for p in patterns):
            stats["excluded_by_filter"] += 1
            continue
        version = spec.get("version") or ""
        token = version_token(version)
        held = landing.get(code, {"receipts": set(), "data": set()})
        legacy_id = task_for(spec)["id"]
        legacy = legacy_by_code.get(code, {"entries": 0, "partition_leaves": 0})
        if token in held["receipts"]:
            status = "current"
        elif legacy_id in legacy_ids:
            status = "current_legacy"
        elif held["receipts"] or legacy["entries"]:
            status = "stale"
        else:
            status = "missing"
        data_key, receipt_key = landing_keys(code, token)
        metadata = spec.get("catalogue_metadata") or {}
        items.append({
            "code": code,
            "title": metadata.get("title") or "",
            "version": version,
            "token": token,
            "status": status,
            "shard": shard_of(code, shards),
            "url": require_official(request_url(spec["url"], spec.get("params") or {})),
            "landing_versions": sorted(held["receipts"]),
            "orphan_data_object": token in held["data"] and token not in held["receipts"],
            "legacy_task_id": legacy_id,
            "legacy_entries": legacy["entries"],
            "legacy_partition_leaves": legacy["partition_leaves"],
            "data_key": data_key,
            "receipt_key": receipt_key,
        })
    items.sort(key=lambda row: row["code"])
    return items, stats


CSV_COLUMNS = ("dataset_code", "status", "eurostat_version", "version_token", "shard", "title",
               "landing_versions", "orphan_data_object", "legacy_entries",
               "legacy_partition_leaves", "legacy_task_id", "source_url", "landing_key")


def coverage_csv(items: list[dict]) -> bytes:
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(CSV_COLUMNS)
    for row in items:
        writer.writerow([row["code"], row["status"], row["version"], row["token"], row["shard"],
                         row["title"], ";".join(row["landing_versions"]),
                         str(row["orphan_data_object"]).lower(), row["legacy_entries"],
                         row["legacy_partition_leaves"], row["legacy_task_id"], row["url"],
                         row["data_key"]])
    return out.getvalue().encode("utf-8")


def counts(items: list[dict]) -> dict[str, int]:
    result = {status: 0 for status in STATUSES}
    for row in items:
        result[row["status"]] += 1
    return result


def step_summary(markdown: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(markdown + "\n")


# --------------------------------------------------------------------------- main

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("plan", "ingest"), required=True)
    parser.add_argument("--allow-production-write", action="store_true",
                        help="required for ingest (writes raw bytes to R2 Landing)")
    parser.add_argument("--datasets", default="",
                        help="comma/space separated dataset codes or glob patterns (case-insensitive)")
    parser.add_argument("--limit", type=int, default=0, help="max datasets per shard (0 = all)")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--budget-minutes", type=float, default=330.0,
                        help="stop starting new downloads after this many minutes")
    parser.add_argument("--grace-minutes", type=float, default=20.0,
                        help="abort in-flight transfers this long after the budget")
    args = parser.parse_args(argv)
    if not 1 <= args.shards <= 16 or not 0 <= args.shard < args.shards:
        parser.error("use 1-16 shards and 0 <= shard < shards")
    if not 1 <= args.workers <= 8:
        parser.error("workers must be 1-8")
    if args.limit < 0:
        parser.error("limit must be >= 0")
    if args.mode == "ingest" and not args.allow_production_write:
        parser.error("ingest writes to R2 Landing and requires --allow-production-write")
    patterns = [p.lower() for p in re.split(r"[\s,]+", args.datasets.strip()) if p]

    started = time.monotonic()
    deadline = started + args.budget_minutes * 60
    hard_deadline = deadline + args.grace_minutes * 60
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{stamp}-{os.urandom(3).hex()}"
    if os.environ.get("GITHUB_RUN_ID"):
        run_id += f"-run{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    code_sha = os.environ.get("GITHUB_SHA", "unknown")
    r2 = R2()

    with tempfile.TemporaryDirectory(prefix="eurostat-landing-") as tmp:
        ctx = Context(r2, Path(tmp), deadline, hard_deadline, run_id, code_sha, {})
        inventory_body, inventory_dl = fetch_inventory(ctx)
        inventory_sha = hashlib.sha256(inventory_body).hexdigest()
        inventory_key = f"{CONTROL_PREFIX}inventories/data/{inventory_sha}.tsv"
        if r2.head(r2.lakehouse, inventory_key) is None:
            r2.put_new(r2.lakehouse, inventory_key, inventory_body, "text/tab-separated-values")
        ctx.inventory_ref = {"sha256": inventory_sha, "bytes": len(inventory_body),
                             "retrieved_at": utc_now(), "source_url": inventory_dl.final_url,
                             "control_object": f"r2://{r2.lakehouse}/{inventory_key}"}

        landing = load_landing_state(r2)
        legacy_ids, legacy_by_code, legacy_found = load_legacy()
        items, stats = plan_items(inventory_body, landing, legacy_ids, legacy_by_code,
                                  args.shards, patterns)
        total = counts(items)
        summary = {
            "mode": args.mode, "run_id": run_id, "code_sha": code_sha,
            "inventory": ctx.inventory_ref, "dataset_filter": patterns,
            "legacy_campaign_state_found": legacy_found,
            "legacy_completed_entries": len(legacy_ids),
            "datasets_considered": len(items), "classification": total, **stats,
            "scope": "eurostat_tsv_gzip data distributions, Comext (DS-*) excluded",
        }

        if args.mode == "plan":
            base = f"{CONTROL_PREFIX}coverage/{run_id}"
            r2.put_new(r2.lakehouse, base + ".csv", coverage_csv(items), "text/csv")
            summary["coverage_csv"] = f"r2://{r2.lakehouse}/{base}.csv"
            summary["per_shard"] = {}
            for shards in (1, 2, 4, 8):
                summary["per_shard"][str(shards)] = [
                    counts([i for i in items if shard_of(i["code"], shards) == s])
                    for s in range(shards)
                ]
            summary["elapsed_seconds"] = round(time.monotonic() - started, 1)
            rendered = json.dumps(summary, indent=2, sort_keys=True)
            r2.put_new(r2.lakehouse, base + ".summary.json", rendered.encode(), "application/json")
            print(rendered, flush=True)
            table = "\n".join(f"| {s} | {total[s]:,} |" for s in STATUSES)
            step_summary(
                f"## Eurostat bulk → Landing: plan\n\n"
                f"Inventory `{inventory_sha[:12]}` · {len(items):,} datasets "
                f"(Comext excluded: {stats['excluded_comext']:,})\n\n"
                f"| status | datasets |\n|---|---:|\n{table}\n\n"
                f"Coverage CSV: `{summary['coverage_csv']}`\n")
            return 0

        selected = [i for i in items if i["shard"] == args.shard]
        queue_items = ([i for i in selected if i["status"] == "missing"]
                       + [i for i in selected if i["status"] == "stale"])
        if args.limit:
            queue_items = queue_items[: args.limit]
        log({"event": "ingest_start", "shard": args.shard, "shards": args.shards,
             "queued": len(queue_items), "shard_classification": counts(selected)})
        run_workers(ctx, queue_items, args.workers)

        landed = [r for r in ctx.results if r["outcome"] == "landed"]
        failed = [r for r in ctx.results if r["outcome"] == "failed"]
        by_reason: dict[str, int] = defaultdict(int)
        for row in failed:
            by_reason[row["reason"]] += 1
        summary.update({
            "shard": args.shard, "shards": args.shards, "workers": args.workers,
            "limit_per_shard": args.limit,
            "shard_classification_at_start": counts(selected),
            "queued": len(queue_items),
            "landed": len(landed),
            "landed_bytes": sum(r["size_bytes"] for r in landed),
            "already_current": sum(r["outcome"] == "already_current" for r in ctx.results),
            "failed": len(failed), "failed_by_reason": dict(by_reason),
            "not_started_time_budget": ctx.counters.get("not_started_time_budget", 0),
            "counters": dict(ctx.counters), "provider_pauses": ctx.pacer.events,
            "results": ctx.results,
            "elapsed_seconds": round(time.monotonic() - started, 1),
        })
        key = f"{CONTROL_PREFIX}runs/{run_id}/shard-{args.shard}-of-{args.shards}.json"
        rendered = json.dumps(summary, indent=2, sort_keys=True, default=str)
        r2.put_new(r2.lakehouse, key, rendered.encode(), "application/json")
        brief = {k: v for k, v in summary.items() if k != "results"}
        print(json.dumps(brief, indent=2, sort_keys=True, default=str), flush=True)
        reasons = "\n".join(f"| {k} | {v} |" for k, v in sorted(by_reason.items())) or "| – | 0 |"
        step_summary(
            f"## Eurostat bulk → Landing: ingest shard {args.shard + 1}/{args.shards}\n\n"
            f"Queued {len(queue_items):,} · landed **{len(landed):,}** "
            f"({summary['landed_bytes'] / 1e9:.2f} GB) · failed {len(failed):,} · "
            f"not started (time budget) {summary['not_started_time_budget']:,} · "
            f"provider pauses {len(ctx.pacer.events)}\n\n"
            f"| failure reason | datasets |\n|---|---:|\n{reasons}\n\n"
            f"Run summary: `r2://{r2.lakehouse}/{key}`\n")
        hard = [r for r in failed if r["reason"] not in {"needs_partition", "no_results", "not_found"}]
        return 1 if hard else 0


if __name__ == "__main__":
    sys.exit(main())
