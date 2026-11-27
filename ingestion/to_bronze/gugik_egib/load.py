#!/usr/bin/env python3
"""GUGiK KI EGiB bronze.load: every landed hits snapshot → bronze.gugik_egib_hits (one row per WFS request).

Each Landing file is one part of a round of one feature type (ingestion/to_landing/gugik_egib/; entity
``<feature type>_<round date>_<part>``, part PL or a voivodeship code): one line per resultType=hits request,
with the request's scope (PL, voivodeship, powiat), code, feature type, filtered property, URL, time and the
answer's exact text. A part has at most 39 lines of under 2 KB, so they are read with the json module (no table
decoding is needed); numberMatched, numberReturned and timeStamp of the answer's wfs:FeatureCollection are kept as
text with the request columns and provenance (the answer text itself is not copied: its bytes stay in Landing and
the Archive).

Rejected (bronze.gugik_egib_rejected_files): a line with other keys than LINE_KEYS, an answer whose UTF-8 bytes do not
have its body_sha256, an answer that is not a wfs:FeatureCollection with a numeric numberMatched, a scope outside
PL/voivodeship/powiat, a code that does not fit its scope or the file's part, a request repeated, or a part without
its own PL or voivodeship line.
"""

from __future__ import annotations

from hashlib import sha256
import json
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "gugik_egib"
TABLE_ID = ("bronze", "gugik_egib_hits")
REJECTED_ID = ("bronze", "gugik_egib_rejected_files")
LINE_KEYS = {"scope", "code", "type_name", "property", "url", "requested_at", "http_status", "body_sha256", "body"}
CODE_LENGTH = {"PL": 0, "voivodeship": 2, "powiat": 4}
TEXT = ["scope", "code", "type_name", "property", "requested_at", "number_matched", "number_returned", "time_stamp"]
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(n, "VARCHAR") for n in TEXT],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def _attribute(head: str, name: str) -> str | None:
    marker = f' {name}="'
    start = head.find(marker)
    return None if start < 0 else head[start + len(marker) : head.find('"', start + len(marker))]


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    entity = receipt["entity"]
    part = entity.rsplit("_", 1)[-1]
    rows: dict[str, list] = {n: [] for n in TEXT}
    seen = set()
    for n, raw in enumerate(body.decode("utf-8").splitlines(), start=1):
        line = json.loads(raw)
        if set(line) != LINE_KEYS:
            raise ValueError(f"{entity} line {n}: keys {sorted(set(line) ^ LINE_KEYS)} differ from the layout")
        answer = line["body"]
        if sha256(answer.encode("utf-8")).hexdigest() != line["body_sha256"]:
            raise ValueError(f"{entity} line {n}: answer bytes do not match body_sha256")
        head = answer[: answer.find(">", answer.find("FeatureCollection")) + 1]
        matched = _attribute(head, "numberMatched")
        if "wfs:FeatureCollection" not in head or not (matched or "").isdigit():
            raise ValueError(f"{entity} line {n}: not a FeatureCollection with numberMatched: {answer[:200]!r}")
        scope, code = line["scope"], line["code"]
        if scope not in CODE_LENGTH or len(code) != CODE_LENGTH[scope] or not (code.isdigit() or code == ""):
            raise ValueError(f"{entity} line {n}: scope {scope!r} with code {code!r}")
        if (part == "PL") != (scope == "PL") or not code.startswith("" if part == "PL" else part):
            raise ValueError(f"{entity} line {n}: {scope} {code!r} is not of part {part}")
        if (scope, code) in seen:
            raise ValueError(f"{entity} line {n}: {scope} {code} requested twice")
        seen.add((scope, code))
        values = {**line, "number_matched": matched, "number_returned": _attribute(head, "numberReturned")}
        values["time_stamp"] = _attribute(head, "timeStamp")
        for name in TEXT:
            rows[name].append(values[name])
    if ("PL", "") not in seen and ("voivodeship", part) not in seen:
        raise ValueError(f"{entity}: no answer for the part itself ({part})")
    count = len(rows["scope"])
    prov = bulk_bronze.provenance(receipt)
    data = {name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update(rows)
    data["source_row_number"] = list(range(1, count + 1))
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: each snapshot is about 400 lines."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing import gugik_egib as landing

    landing.LANDING_ROOT = landing_root
    specs = landing.entities(landing.held(s3(), req("STORAGE_LANDING_BUCKET_NAME")))
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "hits",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=specs,
    )
