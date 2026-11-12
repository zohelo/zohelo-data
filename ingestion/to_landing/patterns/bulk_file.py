"""bulk_file pattern: sources whose entities are complete files at fixed URLs.

Declared in ``ingestion/sources/<source>.yaml`` (docs/landing.md, "Common ingestion
framework")::

    extraction:
      kind: bulk_file
      bulk_file:
        files:
          - entity: WDI_CSV
            url: https://databank.worldbank.org/data/download/WDI_CSV.zip
            format: zip              # zip | gzip | 7z | raw
            version: last_modified   # last_modified (fallback: download time) | ingested
            url_date: Europe/Warsaw  # optional: {date} in url = today (YYYYMMDD) in this zone
            reduce: news_feed        # optional: land only the allowed part of the file (news: headline items)
          - entity: treasury_debt_series   # a file whose URL changes per release
            url_from:                # instead of url: find the link on a stable page
              page: https://www.gov.pl/web/finanse/szeregiczasowe
              pattern: /attachment/  # re.search on each <a href>, made absolute against the page
              pick: first            # first (default) | last matching link, in page order
          - entity: budget_execution       # one file per year, each on its own page
            years: {from: 2018, open: 2}  # entities budget_execution_2018 … _<current year, Warsaw>; {year} in
            url_from:                # url / url_from.page is that year; the current year's file is optional
              page: https://www.gov.pl/web/finanse/sprawozdania-miesieczne-{year}
              pattern: /attachment/

Each file lands as ``01_landing/<source>/snapshot/data/<entity>/<version>__<ingested>.<ext>``
with a receipt, only when its SHA-256 differs from the newest landed copy. The download
streams to disk, is checked by format, uploaded and verified by size and SHA-256; network
errors and 5xx/408/429 are retried, other 4xx fail at once. Modes: inspect (Landing vs the
server's Last-Modified; an unreachable URL is an error), load, verify (every receipt has
its file in Landing or the Archive).
"""

from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
import gzip
from hashlib import sha256
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any
from urllib.parse import urljoin
import zipfile

import requests
import yaml

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

ROOT = Path(__file__).resolve().parents[3]
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
EXTENSIONS = {"zip": "zip", "gzip": "gz", "7z": "7z", "raw": "bin"}
CONTENT_TYPES = {
    "zip": "application/zip",
    "gzip": "application/gzip",
    "7z": "application/x-7z-compressed",
    "raw": "application/octet-stream",
}
RETRIES = 4
RETRY_STATUSES = {408, 429}  # client errors worth retrying; other 4xx fail at once


class PermanentError(RuntimeError):
    """A download failure that retrying will not fix (a 4xx response)."""


def declaration(source: str, today: datetime | None = None) -> list[dict[str, Any]]:
    """The declared files of a bulk_file source; a file with ``years`` is one entity per year (expand_years)."""
    config = yaml.safe_load((ROOT / "ingestion" / "sources" / f"{source}.yaml").read_text())
    extraction = config.get("extraction") or {}
    if extraction.get("kind") != "bulk_file":
        raise ValueError(f"{source}: extraction.kind is not bulk_file")
    return [s for spec in extraction["bulk_file"]["files"] for s in expand_years(spec, today)]


def expand_years(spec: dict[str, Any], today: datetime | None = None) -> list[dict[str, Any]]:
    """A file declared with ``years: {from: <year>}`` as one spec per year up to the current year (Warsaw):
    entity ``<entity>_<year>``, ``{year}`` replaced in url and url_from.page. The current year's spec is
    ``optional`` (in January its page may not exist or list a file yet): load and inspect skip it with a
    notice instead of failing. With ``years: {from: <year>, open: <n>}`` only the newest n years are downloaded
    again once landed; an older year is ``closed`` and load skips it while Landing holds a copy (its file no
    longer changes; a missing copy is still downloaded). Other specs are returned unchanged."""
    if "years" not in spec:
        return [spec]
    from zoneinfo import ZoneInfo

    current = (today or datetime.now(UTC)).astimezone(ZoneInfo("Europe/Warsaw")).year
    base = {k: v for k, v in spec.items() if k != "years"}
    out = []
    open_years = int(spec["years"].get("open", 0))
    for year in range(int(spec["years"]["from"]), current + 1):
        one = {**base, "entity": f"{spec['entity']}_{year}", "year": year, "optional": year == current}
        if open_years and year <= current - open_years:
            one["closed"] = True
        if "url" in base:
            one["url"] = base["url"].replace("{year}", str(year))
        if "url_from" in base:
            one["url_from"] = {**base["url_from"], "page": base["url_from"]["page"].replace("{year}", str(year))}
        out.append(one)
    return out


def single_entity(source: str) -> str:
    """The entity of a bulk_file source that declares exactly one file (its Bronze loader reads that one)."""
    entities = [spec["entity"] for spec in declaration(source)]
    if len(entities) != 1:
        raise ValueError(f"{source}: expected exactly one declared file, found {entities}")
    return entities[0]


def newest(client: Any, bucket: str, source: str, entity: str) -> dict[str, Any]:
    """Receipt of the newest landed copy of an entity; raises when nothing has landed."""
    held = landed(client, bucket, prefix(source, entity))
    if not held:
        raise RuntimeError(f"{source}: no Landing receipts under {prefix(source, entity)}")
    return held[-1]


def prefix(source: str, entity: str, landing_root: str = "01_landing") -> str:
    """Landing folder of one entity."""
    return f"{landing_root}/{source}/snapshot/data/{entity}/"


def landed(client: Any, bucket: str, folder: str) -> list[dict[str, Any]]:
    """Receipts under a prefix, oldest landed first.

    Ordered by ``ingested_at``, not ``version_token``: the token mixes two time bases (the
    server's Last-Modified, or the download time when that header is missing), while a copy
    lands only when its bytes changed, so the last one landed is the current one.
    """
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=folder):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]
    rows = [{**json.loads(get_with_backoff(client, bucket, k)), "receipt_key": k} for k in keys]
    return sorted(rows, key=lambda r: r["ingested_at"])


def stamp(moment: datetime) -> str:
    """Layout timestamp YYYYMMDDTHHMMSSZ (UTC)."""
    return moment.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def server_version(headers: dict[str, str]) -> str | None:
    """The response's Last-Modified as a layout timestamp, if any."""
    try:
        return stamp(parsedate_to_datetime(headers["Last-Modified"]))
    except (KeyError, TypeError, ValueError):
        return None


def url_of(spec: dict[str, Any]) -> str:
    """The file's URL; with ``url_date``, ``{date}`` is today's date (YYYYMMDD) in that time zone."""
    if not spec.get("url_date"):
        return spec["url"]
    from zoneinfo import ZoneInfo

    return spec["url"].format(date=datetime.now(ZoneInfo(spec["url_date"])).strftime("%Y%m%d"))


class _Links(HTMLParser):
    """Collects the href of every <a> tag, in page order."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            self.hrefs += [value for name, value in attrs if name == "href" and value]


def links_matching(html: str, page_url: str, pattern: str) -> list[str]:
    """Absolute URLs of the page's <a href> links that match ``pattern`` (re.search), in page order, without repeats."""
    parser = _Links()
    parser.feed(html)
    parser.close()
    found: list[str] = []
    for href in parser.hrefs:
        url = urljoin(page_url, href.strip())
        if re.search(pattern, url) and url not in found:
            found.append(url)
    return found


def page_links(spec: dict[str, Any]) -> list[str]:
    """Every link on ``url_from.page`` matching ``url_from.pattern``, with retries like download()."""
    page, pattern = spec["url_from"]["page"], spec["url_from"]["pattern"]
    for attempt in range(RETRIES):
        try:
            response = requests.get(page, headers={"User-Agent": USER_AGENT}, timeout=30, allow_redirects=True)
            if 400 <= response.status_code < 500 and response.status_code not in RETRY_STATUSES:
                raise PermanentError(f"page {response.status_code}: {page}")
            if response.status_code != 200:
                raise RuntimeError(f"page {response.status_code}")
            return links_matching(response.text, response.url, pattern)
        except PermanentError:
            raise
        except (requests.RequestException, RuntimeError) as exc:
            if attempt == RETRIES - 1:
                raise
            print(json.dumps({"status": "retry", "url": page, "error": str(exc)[:200]}), flush=True)
            time.sleep(60 * (attempt + 1))
    raise RuntimeError("unreachable")


def resolve_url(spec: dict[str, Any], links: list[str] | None = None) -> str:
    """The file's URL: from ``url_from`` (the first or last matching link on a stable page), else url_of(spec)."""
    if not spec.get("url_from"):
        return url_of(spec)
    links = page_links(spec) if links is None else links
    if not links:
        raise PermanentError(f"no link matches {spec['url_from']['pattern']!r} on {spec['url_from']['page']}")
    return links[-1] if spec["url_from"].get("pick", "first") == "last" else links[0]


def check_format(path: Path, fmt: str) -> dict[str, Any]:
    """Raise when the file is not a valid instance of its declared format; return details."""
    if fmt == "zip":
        with zipfile.ZipFile(path) as archive:
            bad = archive.testzip()
            if bad:
                raise RuntimeError(f"ZIP member failed its CRC: {bad}")
            return {"members": [(i.filename, i.file_size) for i in archive.infolist()]}
    if fmt == "7z":  # the 7-Zip tool of the runner image tests every member's CRC
        import shutil
        import subprocess

        tool = shutil.which("7z") or shutil.which("7za")
        if not tool:
            raise RuntimeError("7z is not installed")
        try:
            tested = subprocess.run(
                [tool, "t", "-bd", str(path)], capture_output=True, text=True, errors="replace", timeout=1800
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("7z test took longer than 30 minutes") from exc
        if tested.returncode != 0:
            raise RuntimeError(f"7z archive failed its test: {(tested.stdout + tested.stderr)[-300:]}")
        return {}
    if fmt == "gzip":
        with gzip.open(path) as handle:
            while handle.read(8 << 20):
                pass
    return {}


def download(
    url: str, path: Path, fmt: str, retries: int = RETRIES, read_timeout: int = 300, wait: int = 60
) -> tuple[str, int, dict[str, str], str, dict[str, Any]]:
    """Stream to disk and check the format, with retries; returns (sha256, size, headers, URL, detail).

    A spec may shorten ``retries``, ``read_timeout`` and ``wait`` (seconds, times the attempt number) for an API
    that answers small files fast or refuses for long stretches (GDELT)."""
    for attempt in range(retries):
        digest, size = sha256(), 0
        try:
            with requests.get(
                url,
                headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"},  # exact bytes, true length
                stream=True,
                timeout=(30, read_timeout),
                allow_redirects=True,
            ) as response:
                if 400 <= response.status_code < 500 and response.status_code not in RETRY_STATUSES:
                    raise PermanentError(f"download {response.status_code}: {url}")
                if response.status_code != 200:
                    raise RuntimeError(f"download {response.status_code}")
                with path.open("wb") as handle:
                    for block in response.iter_content(8 << 20):
                        handle.write(block)
                        digest.update(block)
                        size += len(block)
                expected = int(response.headers.get("Content-Length") or 0)
                if expected and size != expected:
                    raise RuntimeError(f"download truncated: {size} of {expected} bytes")
                detail = check_format(path, fmt)  # a corrupt file is downloaded again
                return digest.hexdigest(), size, dict(response.headers), response.url, detail
        except PermanentError:
            raise
        except (
            requests.RequestException,  # includes mid-stream drops (ChunkedEncodingError)
            RuntimeError,
            zipfile.BadZipFile,
            gzip.BadGzipFile,
            EOFError,
        ) as exc:
            if attempt == retries - 1:
                raise
            print(json.dumps({"status": "retry", "url": url, "error": str(exc)[:200]}), flush=True)
            time.sleep(wait * (attempt + 1))
    raise RuntimeError("unreachable")


PROBES = {"head", "get"}  # a file's `probe:` (default head); the configuration check rejects other values


def probe(url: str, method: str = "head") -> tuple[requests.Response | None, str]:
    """HEAD the URL (GET headers only when HEAD is refused), with retries; returns (response, status or error).
    ``method="get"`` (a file's ``probe: get``) skips HEAD for servers that answer it wrongly (UNdata and the UIS
    Data API 404, zus.pl drops the connection; #2546): a streamed GET whose body is never read."""
    response, http = None, ""
    for attempt in range(3):
        try:
            if method == "get":
                response = requests.get(
                    url, headers={"User-Agent": USER_AGENT}, allow_redirects=True, timeout=60, stream=True
                )
                response.close()  # headers only; the body is not read
            else:
                response = requests.head(
                    url,
                    headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"},
                    allow_redirects=True,
                    timeout=60,
                )
            if method != "get" and response.status_code in (403, 405, 501):  # some servers refuse HEAD but serve GET
                response = requests.get(
                    url, headers={"User-Agent": USER_AGENT}, allow_redirects=True, timeout=60, stream=True
                )
                response.close()  # headers only; the body is not read
            http = str(response.status_code)
        except requests.RequestException as exc:
            response, http = None, type(exc).__name__
        permanent = response is not None and 400 <= response.status_code < 500
        if http == "200" or (permanent and response.status_code not in RETRY_STATUSES):
            break
        if attempt < 2:
            time.sleep(10 * (attempt + 1))
    return response, http


def reducer(kind: str) -> Any:
    """The function that keeps only the allowed part of a downloaded file (``reduce:`` in a file declaration)."""
    if kind == "news_feed":
        from ingestion.to_bronze.news_feeds.load import reduce_feed

        return reduce_feed
    raise ValueError(f"unknown reduce: {kind!r}")


def reduce_file(path: Path, kind: str) -> tuple[str, int]:
    """Rewrite the downloaded file in place with only its allowed part; returns the new (sha256, size)."""
    body = reducer(kind)(path.read_bytes())
    path.write_bytes(body)
    return sha256(body).hexdigest(), len(body)


def reduce_held(client: Any, bucket: str, source: str, spec: dict[str, Any], landing_root: str) -> int:
    """Rewrite copies landed before ``reduce:`` was declared (still in Landing) to their allowed part, under the same
    key, and update their receipts; a copy already reduced is left alone. Returns the number rewritten."""
    done = 0
    for receipt in landed(client, bucket, prefix(source, spec["entity"], landing_root)):
        if receipt.get("detail", {}).get("reduced"):
            continue
        key = receipt["object"]["key"]
        try:
            body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except client.exceptions.NoSuchKey:
            continue  # moved to the Archive already; the Archive is handled by its retention
        reduced = reducer(spec["reduce"])(body)
        digest = sha256(reduced).hexdigest()
        client.put_object(
            Bucket=bucket, Key=key, Body=reduced, ContentType=CONTENT_TYPES["raw"], Metadata={"sha256": digest}
        )
        receipt_key = receipt.pop("receipt_key")
        receipt.update({"sha256": digest, "size_bytes": len(reduced), "original_sha256": receipt["sha256"]})
        receipt["detail"] = {
            **receipt.get("detail", {}),
            "reduced": spec["reduce"],
            "reduced_at": stamp(datetime.now(UTC)),
        }
        client.put_object(
            Bucket=bucket,
            Key=receipt_key,
            ContentType="application/json",
            Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
        )
        done += 1
    return done


def load_file(
    client: Any, bucket: str, source: str, spec: dict[str, Any], landing_root: str, same: Any = None
) -> dict[str, Any]:
    """Download one declared file and land it with a receipt unless its bytes are unchanged.

    ``same(held_receipt, path)`` (optional, a source's own rule) returns True when the download differs from the newest
    landed copy only in parts the source does not load (an HTML page's per-request nonce): it is then not landed."""
    fmt = spec.get("format", "raw")
    folder = prefix(source, spec["entity"], landing_root)
    held = landed(client, bucket, folder)
    with tempfile.TemporaryDirectory(prefix=f"zohelo-{source}-") as temporary:
        path = Path(temporary) / f"file.{EXTENSIONS[fmt]}"
        started = time.monotonic()
        url = resolve_url(spec)
        limits = {k: spec[k] for k in ("retries", "read_timeout", "wait") if k in spec}
        digest, size, headers, final_url, detail = download(url, path, fmt, **limits)
        if spec.get("reduce"):
            digest, size = reduce_file(path, spec["reduce"])
            detail = {**detail, "reduced": spec["reduce"]}
        seconds = round(time.monotonic() - started)
        if held and (held[-1]["sha256"] == digest or (same is not None and same(held[-1], path))):
            return {"entity": spec["entity"], "outcome": "unchanged", "sha256": digest, "download_seconds": seconds}
        ingested = stamp(datetime.now(UTC))
        basis = spec.get("version", "last_modified")
        version = (server_version(headers) if basis == "last_modified" else None) or ingested
        key = f"{folder}{version}__{ingested}.{EXTENSIONS[fmt]}"
        client.upload_file(
            str(path), bucket, key, ExtraArgs={"ContentType": CONTENT_TYPES[fmt], "Metadata": {"sha256": digest}}
        )
        head = client.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] != size or (head.get("Metadata") or {}).get("sha256") != digest:
            raise RuntimeError(f"Landing write could not be verified: {key}")
    receipt = {
        "schema_version": 3,
        "kind": f"{source}_landing",
        "source_id": source,
        "entity": spec["entity"],
        "content": "snapshot",
        "area": "data",
        "version_token": version,
        "version_basis": "Last-Modified" if version != ingested else "download time",
        "ingested_at": ingested,
        "source_url": url,
        "final_url": final_url,
        "sha256": digest,
        "size_bytes": size,
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": held[-1]["sha256"] if held else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {
            "pattern": "bulk_file",
            "format": fmt,
            **detail,
            "http": {
                "last_modified": headers.get("Last-Modified"),
                "etag": headers.get("ETag"),
                "content_type": headers.get("Content-Type"),
            },
        },
    }
    client.put_object(
        Bucket=bucket,
        Key=key.rsplit(".", 1)[0] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return {
        "entity": spec["entity"],
        "outcome": "landed_new" if not held else "landed_changed",
        "sha256": digest,
        "size_mb": round(size / 1e6, 1),
        "version": version,
        "download_seconds": seconds,
    }


def main(
    source: str,
    mode: str,
    landing_root: str = "01_landing",
    specs: list[dict[str, Any]] | None = None,
    pause_seconds: float = 0.0,
    missing_ok: bool = False,
    same: Any = None,
) -> int:
    """Run one mode for every declared file of a bulk_file source; exit 1 on missing data.

    ``specs`` replaces the declared files (sources whose files are built from a seed, to_landing/<source>/);
    ``pause_seconds`` waits between downloads (a source's published rate limit); with ``missing_ok`` a file
    the server refuses with a 4xx (e.g. a renamed Wikipedia article) is reported as a warning, not a failure;
    ``same`` is passed to load_file (a source's rule for a download that differs only in parts it does not load).
    """
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    results, status, missing = [], 0, []
    stored = archived(client, bucket, source) if mode == "verify" else {}
    for n, spec in enumerate(declaration(source) if specs is None else specs):
        folder = prefix(source, spec["entity"], landing_root)
        if mode == "load":
            if spec.get("reduce"):
                rewritten = reduce_held(client, bucket, source, spec, landing_root)
                if rewritten:
                    print(json.dumps({"entity": spec["entity"], "reduced_held_copies": rewritten}), flush=True)
            if spec.get("closed") and landed(client, bucket, folder):
                results.append({"entity": spec["entity"], "outcome": "closed year, held"})
                continue
            if n and pause_seconds:
                time.sleep(pause_seconds)
            try:
                results.append(load_file(client, bucket, source, spec, landing_root, same))
            except PermanentError as exc:
                if spec.get("optional"):
                    results.append({"entity": spec["entity"], "outcome": "not published yet", "error": str(exc)[:200]})
                    continue
                if not missing_ok:
                    raise
                missing.append({"entity": spec["entity"], "error": str(exc)[:200]})
            continue
        held = landed(client, bucket, folder)
        row: dict[str, Any] = {
            "entity": spec["entity"],
            "landed_versions": len(held),
            "newest": held[-1]["version_token"] if held else None,
        }
        if mode == "inspect":
            try:
                links = page_links(spec) if spec.get("url_from") else None
                url = resolve_url(spec, links)
            except (PermanentError, requests.RequestException, RuntimeError) as exc:
                row.update({"state": "unreachable", "http": str(exc)[:200]})
                if spec.get("optional") and isinstance(exc, PermanentError):
                    row["state"] = "not published yet"  # the current year of a per-year file (expand_years)
                else:
                    status = 1
                results.append(row)
                continue
            if links is not None:
                row["page_links"] = links[:10]  # every match (first 10), to choose url_from.pick
            row["url"] = url
            head, http = probe(url, spec.get("probe", "head"))
            row["server_version"] = (server_version(dict(head.headers)) if head is not None else None) or "unknown"
            if http != "200":  # a moved or blocked URL is an error, not a missing validator
                row.update({"state": "unreachable", "http": http})
                status = 1
            elif not held:
                row["state"] = "missing"
            elif row["server_version"] == "unknown":  # no validator: load compares the SHA-256
                row["state"] = "unknown"
            else:
                row["state"] = "stale" if row["server_version"] > held[-1]["version_token"] else "current"
        else:
            present = set()
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=folder):
                present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
            missing = [
                r["object"]["key"]
                for r in held
                if r["object"]["key"] not in present
                and stored.get(r["sha256"], {}).get("key") != archive_key(source, r["sha256"], r["object"]["key"])
            ]
            row.update({"missing_data": len(missing), "missing_sample": missing[:5]})
            status = max(status, 1 if missing else 0)
        results.append(row)
    if missing:
        annotate("warning", f"{source} landing {mode}: files refused", {"missing": missing})
    summary = {"source": source, "mode": mode, "files": results}
    print(json.dumps(summary, sort_keys=True))
    annotate("notice" if status == 0 else "error", f"{source} landing {mode}", summary)
    step_summary(f"## {source} · landing {mode}\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return status
