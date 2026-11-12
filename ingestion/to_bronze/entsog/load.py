#!/usr/bin/env python3
"""ENTSOG bronze.load: newest Landing copy of every gas month → bronze.entsog_operational_data.

Each Landing file is one ``/api/v1/operationaldatas`` answer for one operator, indicator and gas month
(ingestion/to_landing/entsog/). One Bronze row per record, every field the API documents as text exactly as sent:
numbers as their JSON text, booleans as ``true``/``false``, JSON null as NULL, an empty string stays empty (the API
sends ``value: ""`` with ``isNA: 1`` for points without data; Silver keeps those missing, never zero). An answer that
is not an operationaldatas page, holds no record, holds a field the decoder does not know, or holds another
operator, indicator or period type than its receipt declares rejects the file (bronze.entsog_rejected_files).
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.common.r2 import req, s3
from ingestion.to_landing.entsog import extract

SOURCE = "entsog"
TABLE = "entsog_operational_data"
REJECTED = "entsog_rejected_files"
# The 35 fields of an operationaldatas record (meta.fields of the answer, Dev probe 43320099570) → column names.
FIELDS = {
    "id": "record_id",
    "dataSet": "data_set",
    "indicator": "indicator",
    "periodType": "period_type",
    "periodFrom": "period_from",
    "periodTo": "period_to",
    "operatorKey": "operator_key",
    "tsoEicCode": "tso_eic_code",
    "operatorLabel": "operator_label",
    "pointKey": "point_key",
    "pointLabel": "point_label",
    "tsoItemIdentifier": "tso_item_identifier",
    "directionKey": "direction_key",
    "unit": "unit",
    "itemRemarks": "item_remarks",
    "generalRemarks": "general_remarks",
    "value": "value_text",
    "lastUpdateDateTime": "last_update_date_time",
    "isUnlimited": "is_unlimited",
    "flowStatus": "flow_status",
    "interruptionType": "interruption_type",
    "restorationInformation": "restoration_information",
    "capacityType": "capacity_type",
    "capacityBookingStatus": "capacity_booking_status",
    "isCamRelevant": "is_cam_relevant",
    "isNA": "is_na",
    "originalPeriodFrom": "original_period_from",
    "isCmpRelevant": "is_cmp_relevant",
    "bookingPlatformKey": "booking_platform_key",
    "bookingPlatformLabel": "booking_platform_label",
    "bookingPlatformURL": "booking_platform_url",
    "interruptionCalculationRemark": "interruption_calculation_remark",
    "pointType": "point_type",
    "idPointType": "id_point_type",
    "isArchived": "is_archived",
}
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(c, "VARCHAR") for c in FIELDS.values()],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the record in the answer, from 1
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    page = json.loads(body, parse_float=str, parse_int=str)  # keep the number text exactly as published
    records = page.get("operationaldatas") if isinstance(page, dict) else None
    if not isinstance(records, list) or not records:
        raise ValueError(f"not an operationaldatas answer with records: {body[:200]!r}")
    detail = receipt.get("detail") or {}
    columns: dict[str, list[Any]] = {c: [] for c in FIELDS.values()}
    for number, record in enumerate(records, start=1):
        unknown = set(record) - set(FIELDS)
        if unknown:
            raise ValueError(f"record {number}: unknown fields {sorted(unknown)}")
        if record.get("operatorKey") != detail.get("operator") or record.get("indicator") != detail.get("indicator"):
            raise ValueError(
                f"record {number}: operator/indicator {record.get('operatorKey')}/{record.get('indicator')}"
            )
        if record.get("periodType") != "day":
            raise ValueError(f"record {number}: periodType {record.get('periodType')!r}")
        for field, column in FIELDS.items():
            columns[column].append(text(record.get(field)))
    count = len(records)
    base = bulk_bronze.provenance(receipt)
    data = {n: [base[n]] * count for n in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update(columns)
    data["source_row_number"] = list(range(1, count + 1))
    return pa.Table.from_pydict({n: data[n] for n, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 70 answers of 1.5 MB."""
    held = extract.landed_all(s3(), req("STORAGE_LANDING_BUCKET_NAME"), landing_root)
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "operational_data",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(held)],
    )
