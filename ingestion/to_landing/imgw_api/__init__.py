"""IMGW public data API Landing: snapshots of the current-state endpoints through the shared bulk_file pattern.

The two files declared in ingestion/sources/imgw_api.yaml are landed as JSON (exact bytes, receipt after the file,
version = download time; a response with the same SHA-256 as the newest copy is skipped). ``landing.extract`` is
the pattern's load with one rule added: a file marked ``daily: true`` (the meteo endpoint, used for the station
dimension, about 520 KB) is fetched only when its newest copy is more than 20 hours old, so the frequent synop
snapshots do not bring a station list each time. The age comes from the receipt key names of one listing.
landing.inspect and landing.verify are the pattern's.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import re

SOURCE = "imgw_api"
LANDING_ROOT = "01_landing"  # a Dev trial points it at a zz_dev_ scratch prefix
DAILY_AGE = timedelta(hours=20)


def newest_ingest(client, bucket: str, entity: str) -> datetime | None:
    from ingestion.to_landing.patterns import bulk_file

    stamps = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=bulk_file.prefix(SOURCE, entity, LANDING_ROOT)
    ):
        for item in page.get("Contents", []):
            match = re.search(r"__([0-9]{8}T[0-9]{6}Z)\.receipt\.json$", item["Key"])
            if match:
                stamps.append(match.group(1))
    return datetime.strptime(max(stamps), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC) if stamps else None


def _extract(p) -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    now = datetime.now(UTC)
    specs = []
    for spec in bulk_file.declaration(SOURCE):
        if spec.get("daily"):
            held = newest_ingest(client, bucket, spec["entity"])
            if held is not None and now - held < DAILY_AGE:
                print(f"{spec['entity']}: newest copy {held:%Y-%m-%d %H:%M} UTC is under 20 hours old, not fetched")
                continue
        specs.append(spec)
    return bulk_file.main(SOURCE, "load", landing_root=LANDING_ROOT, specs=specs, pause_seconds=1.0)


ACTIVITIES = {"landing.extract": _extract}
