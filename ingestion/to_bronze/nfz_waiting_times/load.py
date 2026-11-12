#!/usr/bin/env python3
"""NFZ waiting times bronze.load: newest Landing copy of every entity → bronze.nfz_waiting_times_queues.

Each Landing file is one case and voivodeship of one snapshot month (ingestion/to_landing/nfz_waiting_times/): a
ZIP of the exact API answers, ``page_<nnnn>.json``. One Bronze row per queue record (provider x place x benefit x
case), values as published text (numbers as their JSON text, null as null). Every month is its own entity, so the
newest copy of each month stays (``bronze_latest_only``) and the months accumulate.

Kept: the queue id, case, benefit, provider code and name, the place, its locality, county and TERYT code, the
RPWDL registry number and the resort code parts VII and VIII, the place's flags, the published statistics
(month-end persons awaiting, persons removed, average waiting period and their month) and the forecast time to the
benefit (``pcus``) with its as-at date. Left out on purpose (still in the Landing and Archive bytes): REGON and NIP
of the provider, street address, coordinates, every phone, fax, e-mail and web address, public transport lines
(contact data, and for one-person practices data about a person; not needed for the measures).

A page that is not a queue answer, a page out of order, a record of another type or with an attribute the API
v1.4 does not describe, or an answer whose records differ from the entity's ``meta.count``, rejects the file into
bronze.nfz_waiting_times_rejected_files (never guessed).
"""

from __future__ import annotations

import io
import json
import re
from typing import Any
import zipfile

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.common.r2 import req, s3
from ingestion.to_landing.nfz_waiting_times import extract

SOURCE = "nfz_waiting_times"
TABLE = "nfz_waiting_times_queues"
REJECTED = "nfz_waiting_times_rejected_files"
# queue attributes of the API v1.4 (swagger schema queue-attributes) → Bronze column; None = not stored
ATTRIBUTES = {
    "case": "case_code",
    "benefit": "benefit",
    "anesthesia": "anesthesia",
    "many-places": "many_places",
    "provider": "provider_name",
    "provider-code": "provider_code",
    "regon-provider": None,
    "nip-provider": None,
    "teryt-provider": "teryt_provider",
    "place": "place_name",
    "address": None,
    "locality": "locality",
    "county": "county",
    "phone": None,
    "information-phone": None,
    "registration-phone": None,
    "additional-phone": None,
    "fax": None,
    "email": None,
    "website": None,
    "teryt-place": "teryt_place",
    "registry-number": "registry_number",
    "id-resort-part-VII": "resort_part_vii",
    "id-resort-part-VIII": "resort_part_viii",
    "benefits-for-children": "benefits_for_children",
    "age-range": "age_range",
    "covid-19": "covid_19",
    "toilet": "toilet",
    "ramp": "ramp",
    "car-park": "car_park",
    "elevator": "elevator",
    "type-building": "type_building",
    "ac": "air_conditioning",
    "automatic-door": "automatic_door",
    "public-transport-lines": None,
    "wheelchairs": "wheelchairs",
    "corridors": "corridors",
    "latitude": None,
    "longitude": None,
    "queue-in-cer": "queue_in_cer",
    "statistics": None,  # unpacked below
    "dates": None,  # unpacked below
    "benefits-provided": "benefits_provided",
}
PROVIDER_DATA = {
    "awaiting": "awaiting",
    "removed": "removed",
    "average-period": "average_period",
    "update": "statistics_month",
}
COMPUTED_DATA = {"average-period": "computed_average_period", "update": "computed_update"}
DATES = {"applicable": "dates_applicable", "pcus": "pcus", "date-situation-as-at": "date_situation_as_at"}
VALUE_COLUMNS = [
    "queue_id",
    *[c for c in ATTRIBUTES.values() if c],
    *PROVIDER_DATA.values(),
    *COMPUTED_DATA.values(),
    *DATES.values(),
]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(c, "VARCHAR") for c in VALUE_COLUMNS],
    *bulk_bronze.PROVENANCE,
    ("page_number", "BIGINT"),
    ("source_row_number", "BIGINT"),  # position of the record in its page, from 1
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
MEMBER = re.compile(r"page_(\d{4})\.json")


def text(value: Any) -> str | None:
    """A published value as text: strings unchanged, numbers and booleans as their JSON text, null as null."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def unpack(value: Any, names: dict[str, str], what: str, number: str) -> dict[str, str | None]:
    if value is None:
        return dict.fromkeys(names.values())
    if not isinstance(value, dict) or set(value) - set(names):
        raise ValueError(f"record {number}: {what} {str(value)[:200]}")
    return {column: text(value.get(key)) for key, column in names.items()}


def record_row(item: dict[str, Any], number: str) -> dict[str, Any]:
    if item.get("type") != "queue" or not isinstance(item.get("attributes"), dict):
        raise ValueError(f"record {number}: not a queue: {str(item)[:200]}")
    attributes = item["attributes"]
    unknown = set(attributes) - set(ATTRIBUTES)
    if unknown:
        raise ValueError(f"record {number}: attributes the API v1.4 does not describe: {sorted(unknown)}")
    row: dict[str, Any] = {"queue_id": text(item.get("id"))}
    row.update({column: text(attributes.get(key)) for key, column in ATTRIBUTES.items() if column})
    statistics = attributes.get("statistics")
    if statistics is not None and (
        not isinstance(statistics, dict) or set(statistics) - {"provider-data", "computed-data"}
    ):
        raise ValueError(f"record {number}: statistics {str(statistics)[:200]}")
    statistics = statistics or {}
    row.update(unpack(statistics.get("provider-data"), PROVIDER_DATA, "provider-data", number))
    row.update(unpack(statistics.get("computed-data"), COMPUTED_DATA, "computed-data", number))
    row.update(unpack(attributes.get("dates"), DATES, "dates", number))
    return row


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        names = archive.namelist()
        numbers = []
        for name in names:
            match = MEMBER.fullmatch(name)
            if not match:
                raise ValueError(f"unexpected member {name}")
            numbers.append(int(match.group(1)))
        if numbers != list(range(1, len(numbers) + 1)):
            raise ValueError(f"pages out of order or missing: {names[:5]}")
        rows: list[dict[str, Any]] = []
        count = None
        for page_number, name in enumerate(names, start=1):
            page = json.loads(archive.read(name))
            meta = page.get("meta") if isinstance(page, dict) else None
            if not isinstance(meta, dict) or meta.get("title") != "queue" or not isinstance(page.get("data"), list):
                raise ValueError(f"{name}: not a queue answer: {str(page)[:200]}")
            if count is None:
                count = meta.get("count")
            for n, item in enumerate(page["data"], start=1):
                row = record_row(item, f"{page_number}.{n}")
                rows.append({**row, "page_number": page_number, "source_row_number": n})
    if len(rows) != count:
        raise ValueError(f"{len(rows)} records, meta.count {count}")
    if not rows:
        raise ValueError("no queues")
    base = bulk_bronze.provenance(receipt)
    data = {n: [base[n]] * len(rows) for n in ("entity", "raw_sha256", "source_version", "ingested_at")}
    for column in (*VALUE_COLUMNS, "page_number", "source_row_number"):
        data[column] = [r[column] for r in rows]
    return pa.Table.from_pydict({n: data[n] for n, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: 32 files a month of about 150,000 records together."""
    held = extract.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), landing_root)
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "queues",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(held)],
    )
