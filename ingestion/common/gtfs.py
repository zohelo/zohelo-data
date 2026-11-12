"""City GTFS timetables (register PL-ENV-029 Gdańsk, -030 Poznań, -031 Kraków, -032 Wrocław): shared Landing and Bronze.

Each city is its own source (``ingestion/sources/gtfs_<city>.yaml``). Its feeds are the files of
``extraction.bulk_file`` (a fixed URL, or ``url_from`` a listing page); its packages ``to_landing/gtfs_<city>/``
and ``to_bronze/gtfs_<city>/`` only name this module. Static GTFS only; GTFS-Realtime is out of scope.

Landing. A feed is a rolling timetable (Gdańsk: the next 14 days) and the source keeps no history, so every download
whose bytes are new is kept: the entity is the feed and its validity window, ``<feed>_<first>_<last>`` (the first and
last date any service runs, from calendar.txt and calendar_dates.txt). Each window is then the newest version of its
own entity in Landing, Bronze and the Archive, so the latest-only rules hold unchanged; a corrected file for the same
window is a new version of that entity and replaces it; identical bytes land once. A ZIP whose window cannot be read
lands as ``<feed>_unknown`` and Bronze rejects it with the reason. One request at a time, ``PAUSE_SECONDS`` apart
(Gdańsk's robots.txt asks for a crawl delay of 10 s and disallows its CKAN API: only the fixed download URL is used).

Bronze. ``bronze.<source>_records``: one row per record of agency, routes, stops, trips, calendar, calendar_dates
and feed_info (the fields kept below, values as text, ``gtfs_file`` naming the file), plus one row per trip for
stop_times.txt with the number of its stop_times rows (``stop_times_rows``): stop_times are never stored row by row
(about 2.3 M rows a day for Gdańsk; the exact file stays in the Archive). shapes.txt and other files are not read.
A ZIP without routes, stops, trips, stop_times and a calendar file, a file without its required columns, or a closed
code outside GTFS (weekday flag, exception type, date, route type) rejects the file with the reason
(``bronze.<source>_rejected_files``). DuckDB reads every CSV (no per-cell Python).
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
import io
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any
import zipfile

PAUSE_SECONDS = 10
WINDOW = re.compile(r"^(?P<feed>.+)_(?P<first>\d{8})_(?P<last>\d{8})$")
UNKNOWN = "unknown"
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
# file → (required columns, optional columns kept); every other column of the file is not loaded
FILES: dict[str, tuple[list[str], list[str]]] = {
    "agency.txt": (["agency_name"], ["agency_id", "agency_timezone"]),
    "routes.txt": (["route_id", "route_type"], ["agency_id", "route_short_name", "route_long_name"]),
    "stops.txt": (["stop_id"], ["stop_code", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station"]),
    "trips.txt": (["route_id", "service_id", "trip_id"], ["direction_id"]),
    "calendar.txt": (["service_id", *WEEKDAYS, "start_date", "end_date"], []),
    "calendar_dates.txt": (["service_id", "date", "exception_type"], []),
    "feed_info.txt": (["feed_publisher_name"], ["feed_lang", "feed_start_date", "feed_end_date", "feed_version"]),
}
REQUIRED_FILES = ["routes.txt", "stops.txt", "trips.txt", "stop_times.txt"]
FIELDS = list(dict.fromkeys(c for required, optional in FILES.values() for c in [*required, *optional]))


def table_ids(source: str, namespace: str = "bronze") -> tuple[tuple[str, str], tuple[str, str]]:
    return (namespace, f"{source}_records"), (namespace, f"{source}_rejected_files")


def columns() -> list[tuple[str, str]]:
    from ingestion.common import bulk_bronze

    return [
        ("entity", "VARCHAR"),
        ("gtfs_file", "VARCHAR"),
        ("source_row_number", "BIGINT"),
        *[(name, "VARCHAR") for name in FIELDS],
        ("stop_times_rows", "BIGINT"),
        *bulk_bronze.PROVENANCE,
    ]


# --- Landing ---------------------------------------------------------------------------------------------------

_last_request = 0.0


def _pace() -> None:
    """One request at a time, PAUSE_SECONDS between the start of two requests of a run."""
    global _last_request
    wait = _last_request + PAUSE_SECONDS - time.monotonic()
    if _last_request and wait > 0:
        time.sleep(wait)
    _last_request = time.monotonic()


def _rows(archive: zipfile.ZipFile, name: str) -> list[dict[str, str]]:
    with archive.open(name) as handle:
        reader = csv.reader(io.TextIOWrapper(handle, encoding="utf-8-sig", newline=""))
        header = [h.strip() for h in next(reader, [])]
        return [dict(zip(header, (v.strip() for v in row), strict=False)) for row in reader if row]


def window(archive: zipfile.ZipFile) -> tuple[str, str]:
    """(first, last) date YYYYMMDD on which any service of the feed runs: calendar.txt rows with a weekday and
    added dates of calendar_dates.txt. Raises ValueError when neither file gives a date."""
    names = set(archive.namelist())
    starts, ends = [], []
    if "calendar.txt" in names:
        for row in _rows(archive, "calendar.txt"):
            if any(row.get(d) == "1" for d in WEEKDAYS):
                starts.append(row.get("start_date", ""))
                ends.append(row.get("end_date", ""))
    if "calendar_dates.txt" in names:
        added = [
            row.get("date", "") for row in _rows(archive, "calendar_dates.txt") if row.get("exception_type") == "1"
        ]
        starts += added
        ends += added
    dates = [d for d in starts + ends if re.fullmatch(r"\d{8}", d)]
    if not dates or len(dates) != len(starts) + len(ends):
        raise ValueError("no service dates in calendar.txt / calendar_dates.txt, or a date that is not YYYYMMDD")
    return min(starts), max(ends)


def held(client, bucket: str, source: str, feed: str, landing_root: str = "01_landing") -> list[dict[str, Any]]:
    """Receipts of every landed copy of one feed (all windows), oldest landed first."""
    from ingestion.to_landing.patterns import bulk_file

    receipts = bulk_file.landed(client, bucket, f"{landing_root}/{source}/snapshot/data/{feed}_")
    out = []
    for receipt in receipts:
        match = WINDOW.match(receipt["entity"])
        if (match and match["feed"] == feed) or receipt["entity"] == f"{feed}_{UNKNOWN}":
            out.append(receipt)
    return out


def land(client, bucket: str, source: str, spec: dict[str, Any], landing_root: str = "01_landing") -> dict[str, Any]:
    """Download one feed and land it under its window entity unless that entity's newest copy has the same bytes."""
    from ingestion.to_landing.patterns import bulk_file

    feed = spec["entity"]
    with tempfile.TemporaryDirectory(prefix=f"zohelo-{source}-") as temporary:
        path = Path(temporary) / "feed.zip"
        started = time.monotonic()
        if spec.get("url_from"):
            _pace()
        url = bulk_file.resolve_url(spec)
        _pace()
        digest, size, headers, final_url, detail = bulk_file.download(url, path, "zip")
        seconds = round(time.monotonic() - started)
        note = None
        try:
            with zipfile.ZipFile(path) as archive:
                first, last = window(archive)
            entity = f"{feed}_{first}_{last}"
        except (ValueError, KeyError, UnicodeDecodeError, csv.Error) as exc:
            entity, first, last, note = f"{feed}_{UNKNOWN}", None, None, f"{type(exc).__name__}: {exc}"[:300]
        folder = bulk_file.prefix(source, entity, landing_root)
        before = bulk_file.landed(client, bucket, folder)
        if before and before[-1]["sha256"] == digest:
            return {"feed": feed, "entity": entity, "outcome": "unchanged", "download_seconds": seconds}
        ingested = bulk_file.stamp(datetime.now(UTC))
        basis = spec.get("version", "last_modified")
        version = (bulk_file.server_version(headers) if basis == "last_modified" else None) or ingested
        key = f"{folder}{version}__{ingested}.zip"
        client.upload_file(
            str(path), bucket, key, ExtraArgs={"ContentType": "application/zip", "Metadata": {"sha256": digest}}
        )
        head = client.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] != size or (head.get("Metadata") or {}).get("sha256") != digest:
            raise RuntimeError(f"Landing write could not be verified: {key}")
    receipt = {
        "schema_version": 3,
        "kind": f"{source}_landing",
        "source_id": source,
        "entity": entity,
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
        "replaces_sha256": before[-1]["sha256"] if before else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {
            "pattern": "gtfs",
            "format": "zip",
            "feed": feed,
            "window": [first, last],
            "window_error": note,
            **detail,
            "http": {
                k.lower(): headers.get(k) for k in ("Last-Modified", "ETag", "Content-Type", "Content-Disposition")
            },
        },
    }
    client.put_object(
        Bucket=bucket,
        Key=key.rsplit(".", 1)[0] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    outcome = "landed_changed" if before else "landed_new"
    return {"feed": feed, "entity": entity, "outcome": outcome, "size_mb": round(size / 1e6, 1), "version": version}


def extract(source: str, landing_root: str = "01_landing") -> int:
    """landing.extract: every declared feed once; a feed that fails is reported and fails the run after the others."""
    import requests

    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate, step_summary
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    results, status = [], 0
    for spec in bulk_file.declaration(source):
        try:
            results.append(land(client, bucket, source, spec, landing_root))
        except (bulk_file.PermanentError, requests.RequestException, RuntimeError, zipfile.BadZipFile) as exc:
            results.append({"feed": spec["entity"], "outcome": "failed", "error": f"{type(exc).__name__}: {exc}"[:300]})
            status = 1
    summary = {"source": source, "mode": "load", "feeds": results}
    print(json.dumps(summary, sort_keys=True), flush=True)
    annotate("notice" if status == 0 else "error", f"{source} landing load", summary)
    step_summary(f"## {source} · landing load\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return status


def inspect(source: str, landing_root: str = "01_landing") -> int:
    """landing.inspect: per feed the windows landed and the server's Last-Modified (HEAD); no writes."""
    import requests

    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    results, status = [], 0
    for spec in bulk_file.declaration(source):
        copies = held(client, bucket, source, spec["entity"], landing_root)
        row: dict[str, Any] = {
            "feed": spec["entity"],
            "landed_copies": len(copies),
            "windows": len({c["entity"] for c in copies}),
            "newest": copies[-1]["entity"] if copies else None,
        }
        try:
            if spec.get("url_from"):
                _pace()
            url = bulk_file.resolve_url(spec)
        except (bulk_file.PermanentError, requests.RequestException, RuntimeError) as exc:
            results.append({**row, "state": "unreachable", "http": str(exc)[:200]})
            status = 1
            continue
        _pace()
        response, http = bulk_file.probe(url, spec.get("probe", "head"))
        server = (bulk_file.server_version(dict(response.headers)) if response is not None else None) or "unknown"
        row.update({"url": url, "server_version": server})
        if http != "200":
            row.update({"state": "unreachable", "http": http})
            status = 1
        elif not copies:
            row["state"] = "missing"
        elif server == "unknown":
            row["state"] = "unknown"  # no validator: extract compares the SHA-256
        else:
            row["state"] = "stale" if server > copies[-1]["version_token"] else "current"
        results.append(row)
    summary = {"source": source, "mode": "inspect", "feeds": results}
    print(json.dumps(summary, sort_keys=True), flush=True)
    annotate("notice" if status == 0 else "error", f"{source} landing inspect", summary)
    return status


def entities(source: str, landing_root: str = "01_landing") -> list[str]:
    """Every landed window entity of the source's feeds, sorted."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    found = set()
    for spec in bulk_file.declaration(source):
        found |= {r["entity"] for r in held(client, bucket, source, spec["entity"], landing_root)}
    return sorted(found)


def landing_activities(source: str) -> dict[str, Any]:
    """landing.inspect / landing.extract / landing.verify of a city feed source."""

    def _verify(p) -> int:
        from ingestion.to_landing.patterns import bulk_file

        return bulk_file.main(source, "verify", specs=[{"entity": e} for e in entities(source)])

    return {
        "landing.inspect": lambda p: inspect(source),
        "landing.extract": lambda p: extract(source),
        "landing.verify": _verify,
    }


# --- Bronze ----------------------------------------------------------------------------------------------------


def _header(path: Path) -> list[str]:
    with path.open("rb") as handle:
        first = handle.readline().decode("utf-8-sig").rstrip("\r\n")
    return [h.strip() for h in next(csv.reader([first]), [])]


DATE = r"\d{8}"  # GTFS dates are YYYYMMDD
ROUTE_TYPE = r"\d{1,4}"  # basic (0-12) and extended (100-1702) route types


def _matches(column: str, pattern: str) -> str:
    return f"regexp_full_match(coalesce({column}, ''), '{pattern}')"


def _check(con, sql: str, what: str) -> None:
    """Reject the file when the query finds rows (a value outside the GTFS code list)."""
    bad = con.execute(sql).fetchall()
    if bad:
        raise ValueError(f"{what}: {bad[:5]}")


def decode(body: bytes, receipt: dict[str, Any]):
    """One GTFS ZIP → the Arrow rows of ``bronze.<source>_records`` (module docstring); raises ValueError."""
    import duckdb

    from ingestion.common import bulk_bronze

    if not WINDOW.match(receipt["entity"]):
        reason = (receipt.get("detail") or {}).get("window_error")
        raise ValueError(f"validity window not readable at Landing: {reason}")
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not a ZIP: {exc}") from exc
    names = set(archive.namelist())
    missing = [f for f in REQUIRED_FILES if f not in names]
    if missing or not ({"calendar.txt", "calendar_dates.txt"} & names):
        raise ValueError(f"unknown GTFS layout: missing {missing or 'calendar.txt and calendar_dates.txt'}")
    prov = bulk_bronze.provenance(receipt)
    con = duckdb.connect()
    con.execute("set preserve_insertion_order = true")
    parts: list[str] = []
    with tempfile.TemporaryDirectory(prefix="zohelo-gtfs-") as temporary:
        paths = {}
        for name in [*FILES, "stop_times.txt"]:
            if name not in names:
                continue
            paths[name] = Path(temporary) / name
            with archive.open(name) as source, paths[name].open("wb") as target:
                while chunk := source.read(8 << 20):
                    target.write(chunk)
        try:
            for name, path in paths.items():
                header = _header(path)
                required, optional = FILES.get(name, (["trip_id"], []))
                absent = [c for c in required if c not in header]
                if absent:
                    raise ValueError(f"{name}: required columns missing {absent} (header {header[:20]})")
                csv_ = (
                    f"read_csv('{path}', header = true, all_varchar = true, quote = '\"', escape = '\"', delim = ',')"
                )
                con.execute(f"create view {name[:-4]} as select * from {csv_}")
                if name == "stop_times.txt":
                    parts.append(
                        "select 'stop_times.txt' as gtfs_file, cast(null as bigint) as source_row_number, "
                        + ", ".join(f"trip_id as {f}" if f == "trip_id" else f"null as {f}" for f in FIELDS)
                        + ", count(*) as stop_times_rows from stop_times group by trip_id"
                    )
                    continue
                kept = {c: h for h in header for c in [*required, *optional] if h == c}
                parts.append(
                    f"select '{name}' as gtfs_file, cast(row_number() over () + 1 as bigint) as source_row_number, "
                    + ", ".join(f'"{kept[f]}" as {f}' if f in kept else f"null as {f}" for f in FIELDS)
                    + f", cast(null as bigint) as stop_times_rows from {name[:-4]}"
                )
            if "calendar.txt" in paths:
                flags = " or ".join(f"coalesce({d}, '') not in ('0', '1')" for d in WEEKDAYS)
                _check(
                    con, f"select service_id from calendar where {flags} limit 5", "calendar.txt weekday flag not 0/1"
                )
                _check(
                    con,
                    "select service_id, start_date, end_date from calendar "
                    f"where not {_matches('start_date', DATE)} or not {_matches('end_date', DATE)} limit 5",
                    "calendar.txt date not YYYYMMDD",
                )
            if "calendar_dates.txt" in paths:
                _check(
                    con,
                    "select service_id, date, exception_type from calendar_dates "
                    f"where coalesce(exception_type, '') not in ('1', '2') or not {_matches('date', DATE)} limit 5",
                    "calendar_dates.txt exception type not 1/2 or date not YYYYMMDD",
                )
            _check(
                con,
                f"select route_id, route_type from routes where not {_matches('route_type', ROUTE_TYPE)} limit 5",
                "routes.txt route_type not a GTFS integer code",
            )
            table = con.sql(
                f"""
                select ? as entity, p.*, ? as raw_sha256, ? as source_version, cast(? as timestamptz) as ingested_at
                from ({" union all ".join(parts)}) as p
                """,
                params=[prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"]],
            ).to_arrow_table()
        except duckdb.Error as exc:
            raise ValueError(f"CSV not readable: {str(exc)[:500]}") from exc
        finally:
            con.close()
    from ingestion.common.bulk_bronze import arrow_schema

    return table.select([name for name, _ in columns()]).cast(arrow_schema(columns()))


def load(source: str, landing_root: str = "01_landing", namespace: str = "bronze", lake_root: str | None = None) -> int:
    """bronze.load: the newest copy of every landed window → bronze.<source>_records (one Parquet file each)."""
    from ingestion.common import bulk_bronze

    table_id, rejected_id = table_ids(source, namespace)
    return bulk_bronze.load(
        source,
        table_id,
        rejected_id,
        columns(),
        decode,
        "records",
        landing_root=landing_root,
        lake_root=lake_root or ("02_bronze" if namespace == "bronze" else namespace),
        specs=[{"entity": e} for e in entities(source, landing_root)],
    )


def bronze_activities(source: str) -> dict[str, Any]:
    """bronze.load / bronze.verify / landing.archive / archive.prune of a city feed source."""

    def _verify(p) -> int:
        from ingestion.common import lineage

        return lineage.main(["--source", source])

    def _archive(p) -> int:
        from ingestion.common import archive

        return archive.main(["--source", source])

    def _prune(p) -> int:
        from ingestion.common import retention

        return retention.main(["--source", source])

    return {
        "bronze.load": lambda p: load(source),
        "bronze.verify": _verify,
        "landing.archive": _archive,
        "archive.prune": _prune,
    }
