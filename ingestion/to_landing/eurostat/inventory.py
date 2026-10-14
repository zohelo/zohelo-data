"""Eurostat Files API inventory and asynchronous-delivery parsing.

Turns the official inventory into request descriptors and parses SDMX asynchronous
envelopes and faults. Pure functions: no network, no storage.
"""

from __future__ import annotations

import csv
from copy import deepcopy
import io
import re
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlsplit
import xml.etree.ElementTree as ET

WDI_SOURCE_ID = "world_bank_wdi"


EUROSTAT_SOURCE_ID = "eurostat"


EUROSTAT_INVENTORY_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/files/inventory"
)


EUROSTAT_DATA_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{dataset}"
)


EUROSTAT_COMEXT_DATA_URL = (
    "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data/{dataset}"
)


EUROSTAT_ASYNC_STATUS_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/1.0/async/status/{request_id}"
)


EUROSTAT_ASYNC_DATA_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/1.0/async/data/{request_id}"
)


_DATASET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.$-]{0,127}$")


_SOURCE_ALIASES = {
    "wdi": WDI_SOURCE_ID,
    "world_bank": WDI_SOURCE_ID,
    WDI_SOURCE_ID: WDI_SOURCE_ID,
    EUROSTAT_SOURCE_ID: EUROSTAT_SOURCE_ID,
}


_EUROSTAT_PATH_RE = re.compile(
    r"^/eurostat/api/(?:comext/)?dissemination/sdmx/2\.1/data/([^/]+)/?$",
    re.IGNORECASE,
)


_EUROSTAT_ALLOWED_QUERY_KEYS = {"format", "lang", "compressed"}


_ASYNC_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{0,127}$")


class FullSourceAdapterError(ValueError):
    """A catalogue, distribution descriptor, or local payload is invalid."""


def distributions_from_inventory(
    inventory_kind: str, body: bytes | str
) -> list[dict[str, Any]]:
    """Plan every exact distribution listed by one official Eurostat inventory."""
    inventory_type = _inventory_type(inventory_kind)
    rows = parse_catalogue(EUROSTAT_SOURCE_ID, body, inventory_type)
    planned: list[dict[str, Any]] = []
    for row in rows:
        if inventory_type == "data":
            planned.append(
                {
                    "dataset_id": row["dataset_id"],
                    "url": row["tsv_url"],
                    "params": row["request_params"],
                    "version": row["last_data_change"],
                    "kind": "eurostat_tsv_gzip",
                    "catalogue_metadata": row,
                }
            )
            if row["structure_url"]:
                planned.append(
                    {
                        "dataset_id": f"{row['dataset_id']}::__structure__",
                        "url": row["structure_url"],
                        "params": {},
                        "version": row["last_structural_change"],
                        "kind": "eurostat_structure_sdmx",
                        "catalogue_metadata": row,
                    }
                )
        elif inventory_type == "codelist":
            planned.append(
                {
                    "dataset_id": row["dataset_id"],
                    "url": row["tsv_url"],
                    "params": {},
                    "version": row["version"],
                    "kind": "eurostat_codelist_tsv",
                    "catalogue_metadata": row,
                }
            )
        else:
            planned.append(
                {
                    "dataset_id": row["dataset_id"],
                    "url": row["package_url"],
                    "params": {},
                    "version": row["last_metadata_change"],
                    "kind": "eurostat_metadata_zip",
                    "catalogue_metadata": row,
                }
            )
    return planned


def parse_catalogue(
    source_id: str, body: bytes | str, inventory_kind: str = "data"
) -> list[dict[str, Any]]:
    """Parse a complete source catalogue into JSON-serializable metadata rows."""
    source = _source(source_id)
    if source != EUROSTAT_SOURCE_ID:
        raise FullSourceAdapterError(f"{source} has no separately parsed catalogue")

    inventory_type = _inventory_type(inventory_kind)
    text = _decode_text(body, f"Eurostat {inventory_type} inventory")
    reader = csv.DictReader(io.StringIO(text, newline=""), delimiter="\t")
    if reader.fieldnames is None:
        raise FullSourceAdapterError("Eurostat inventory has no header")
    headings = {_heading(name): name for name in reader.fieldnames if name is not None}
    code_heading = headings.get("code")
    if code_heading is None:
        raise FullSourceAdapterError("Eurostat inventory has no Code column")
    title_heading = headings.get("title") or headings.get("label")
    if inventory_type == "data":
        version_heading = headings.get("last data change")
        structural_heading = headings.get("last structural change")
        tsv_heading = headings.get("data download url (tsv)")
        structure_heading = headings.get("data structure download url")
        if tsv_heading is None:
            raise FullSourceAdapterError(
                "Eurostat data inventory has no Data download url (tsv) column"
            )
        if structure_heading is None:
            raise FullSourceAdapterError(
                "Eurostat data inventory has no Data structure download url column"
            )
    elif inventory_type == "codelist":
        version_heading = headings.get("version")
        tsv_heading = headings.get("specific tsv download url")
        if tsv_heading is None:
            raise FullSourceAdapterError(
                "Eurostat codelist inventory has no Specific tsv download url column"
            )
    else:
        version_heading = headings.get("last metadata change")
        package_heading = headings.get("esms package download url")
        if package_heading is None:
            raise FullSourceAdapterError(
                "Eurostat metadata inventory has no Esms package download url column"
            )

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line_number, raw in enumerate(reader, start=2):
        if None in raw:
            raise FullSourceAdapterError(
                f"Eurostat inventory row {line_number} has more fields than its header"
            )
        if not any((value or "").strip() for value in raw.values()):
            continue
        dataset_id = (raw.get(code_heading) or "").strip()
        _validate_dataset_id(dataset_id, line_number)
        identity = dataset_id.upper()
        if identity in seen:
            raise FullSourceAdapterError(
                f"Eurostat inventory repeats dataset code {dataset_id!r}"
            )
        seen.add(identity)

        inventory_fields = {
            str(key): value
            for key, value in raw.items()
            if key is not None and value not in (None, "")
        }
        common = {
            "dataset_id": dataset_id,
            "title": (raw.get(title_heading) or "").strip()
            if title_heading
            else None,
            "inventory_fields": inventory_fields,
        }
        if inventory_type == "data":
            direct_url = (raw.get(tsv_heading) or "").strip()
            if direct_url:
                tsv_url, query_keys = _validated_eurostat_data_url(
                    direct_url, dataset_id
                )
                route_source = "inventory"
                request_params = {}
                if "format" not in query_keys:
                    request_params["format"] = "TSV"
                if "compressed" not in query_keys:
                    request_params["compressed"] = "true"
            else:
                template = (
                    EUROSTAT_COMEXT_DATA_URL
                    if dataset_id.upper().startswith("DS-")
                    else EUROSTAT_DATA_URL
                )
                tsv_url = template.format(dataset=dataset_id)
                route_source = "official_template"
                request_params = {"format": "TSV", "compressed": "true"}
            structure_url = (raw.get(structure_heading) or "").strip()
            if not structure_url:
                raise FullSourceAdapterError(
                    f"Eurostat dataset {dataset_id!r} has no structure URL"
                )
            structure_url = _validated_eurostat_structure_url(
                structure_url, dataset_id
            )
            common.update(
                {
                    "last_data_change": (raw.get(version_heading) or "").strip()
                    if version_heading
                    else None,
                    "last_structural_change": (
                        raw.get(structural_heading) or ""
                    ).strip()
                    if structural_heading
                    else None,
                    "tsv_url": tsv_url,
                    "request_params": request_params,
                    "structure_url": structure_url or None,
                    "route_source": route_source,
                }
            )
        elif inventory_type == "codelist":
            direct_url = (raw.get(tsv_heading) or "").strip()
            if not direct_url:
                raise FullSourceAdapterError(
                    f"Eurostat codelist {dataset_id!r} has no versioned TSV URL"
                )
            common.update(
                {
                    "version": (raw.get(version_heading) or "").strip()
                    if version_heading
                    else None,
                    "tsv_url": _validated_eurostat_codelist_url(
                        direct_url, dataset_id
                    ),
                }
            )
        else:
            package_url = (raw.get(package_heading) or "").strip()
            if not package_url:
                raise FullSourceAdapterError(
                    f"Eurostat metadata item {dataset_id!r} has no package URL"
                )
            common.update(
                {
                    "last_metadata_change": (raw.get(version_heading) or "").strip()
                    if version_heading
                    else None,
                    "package_url": _validated_eurostat_metadata_url(
                        package_url, dataset_id
                    ),
                }
            )
        rows.append(common)

    if not rows:
        raise FullSourceAdapterError("Eurostat inventory contains no datasets")
    return rows


def parse_eurostat_async(
    body: bytes | str, initial_request: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Parse an official asynchronous envelope into safe follow-up descriptors.

    The original request descriptor is copied into the result so durable runners can
    retain the exact request that led to asynchronous processing.  Follow-up URLs are
    constructed locally from a strictly validated provider request identifier; response
    URLs are never followed.
    """
    text = _decode_text(body, "Eurostat asynchronous envelope")
    lowered = text.lower()
    if "<!doctype" in lowered or "<!entity" in lowered:
        raise FullSourceAdapterError(
            "Eurostat asynchronous XML cannot contain DTD/entity declarations"
        )
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        raise FullSourceAdapterError("invalid Eurostat asynchronous XML envelope") from exc
    root_name = _local_name(root.tag).lower()
    fault = root if root_name == "fault" else next(
        (
            element
            for element in root.iter()
            if _local_name(element.tag).lower() == "fault"
        ),
        None,
    )
    if fault is not None:
        result = _parse_eurostat_fault(fault)
        if initial_request is not None:
            if not isinstance(initial_request, Mapping):
                raise FullSourceAdapterError("initial_request must be a mapping")
            result["initial_request"] = deepcopy(dict(initial_request))
        return result
    if root_name != "envelope":
        raise FullSourceAdapterError(
            "Eurostat asynchronous response is not an Envelope or Fault"
        )

    request_ids = [
        (element.text or "").strip()
        for element in root.iter()
        if _local_name(element.tag).lower() in {"id", "key"}
        and len(element) == 0
        and (element.text or "").strip()
    ]
    statuses = [
        (element.text or "").strip().upper()
        for element in root.iter()
        if _local_name(element.tag).lower() == "status"
        and len(element) == 0
        and (element.text or "").strip()
    ]
    if len(request_ids) != 1 or not _ASYNC_ID_RE.fullmatch(request_ids[0]):
        raise FullSourceAdapterError(
            "Eurostat asynchronous envelope has no single safe request id"
        )
    allowed_statuses = {
        "SUBMITTED",
        "PROCESSING",
        "AVAILABLE",
        "EXPIRED",
        "UNKNOWN_REQUEST",
        "ERROR",
    }
    if len(statuses) != 1 or statuses[0] not in allowed_statuses:
        raise FullSourceAdapterError(
            "Eurostat asynchronous envelope has no single valid status"
        )
    request_id = request_ids[0]
    state = {
        "SUBMITTED": "async",
        "PROCESSING": "async",
        "AVAILABLE": "available",
        "EXPIRED": "expired",
        "UNKNOWN_REQUEST": "async_error",
        "ERROR": "async_error",
    }[statuses[0]]
    result: dict[str, Any] = {
        "status": state,
        "provider_status": statuses[0],
        "request_id": request_id,
        "poll": {
            "url": EUROSTAT_ASYNC_STATUS_URL.format(request_id=request_id),
            "params": {},
        },
        "ready": {
            "url": EUROSTAT_ASYNC_DATA_URL.format(request_id=request_id),
            "params": {},
        },
    }
    if initial_request is not None:
        if not isinstance(initial_request, Mapping):
            raise FullSourceAdapterError("initial_request must be a mapping")
        result["initial_request"] = deepcopy(dict(initial_request))
    return result


def _parse_eurostat_fault(fault: ET.Element) -> dict[str, Any]:
    codes = [
        (element.text or "").strip()
        for element in fault.iter()
        if _local_name(element.tag).lower() == "faultcode"
        and len(element) == 0
        and (element.text or "").strip()
    ]
    messages = [
        (element.text or "").strip()
        for element in fault.iter()
        if _local_name(element.tag).lower() == "faultstring"
        and len(element) == 0
        and (element.text or "").strip()
    ]
    if len(codes) != 1 or len(messages) != 1 or not codes[0].isdigit():
        raise FullSourceAdapterError(
            "Eurostat asynchronous fault has no single code and message"
        )
    message = messages[0]
    fault_type = message.split(":", 1)[0].strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", fault_type):
        raise FullSourceAdapterError("Eurostat asynchronous fault has an invalid type")
    common = {
        "fault_code": codes[0],
        "fault_type": fault_type,
        "fault_message": message,
    }
    if codes[0] == "413" and fault_type == "EXTRACTION_TOO_BIG":
        return {
            "status": "unsupported",
            "http_status": 413,
            "reason": "full dataset requires an adaptive partition plan",
            **common,
        }
    if codes[0] == "100" and fault_type == "NO_RESULTS":
        return {"status": "no_results", **common}
    if codes[0] == "100" and fault_type == "DATA_NOT_YET_AVAILABLE":
        return {
            "status": "async",
            "provider_status": "DATA_NOT_YET_AVAILABLE",
            **common,
        }
    if codes[0] == "100" and fault_type == "UNKNOWN_REQUEST":
        return {
            "status": "async_error",
            "provider_status": "UNKNOWN_REQUEST",
            **common,
        }
    return {"status": "unsupported", **common}


def _validated_eurostat_data_url(url: str, dataset_id: str) -> tuple[str, set[str]]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "ec.europa.eu"
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise FullSourceAdapterError(
            f"Eurostat inventory has an unsafe TSV URL for {dataset_id!r}"
        )
    match = _EUROSTAT_PATH_RE.fullmatch(parsed.path)
    if match is None or match.group(1).upper() != dataset_id.upper():
        raise FullSourceAdapterError(
            f"Eurostat inventory TSV URL does not match dataset {dataset_id!r}"
        )
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if len({key.lower() for key, _ in query}) != len(query):
        raise FullSourceAdapterError(
            f"Eurostat inventory has duplicate TSV URL parameters for {dataset_id!r}"
        )
    query_keys: set[str] = set()
    for key, value in query:
        normalized = key.lower()
        if normalized not in _EUROSTAT_ALLOWED_QUERY_KEYS:
            raise FullSourceAdapterError(
                f"Eurostat inventory has an unsafe TSV URL parameter {key!r}"
            )
        if normalized == "format" and value.upper() != "TSV":
            raise FullSourceAdapterError(
                f"Eurostat inventory TSV URL has non-TSV format for {dataset_id!r}"
            )
        query_keys.add(normalized)
        if normalized == "compressed" and value.lower() != "true":
            raise FullSourceAdapterError(
                f"Eurostat inventory TSV URL disables compression for {dataset_id!r}"
            )
    return url, query_keys


def _validated_eurostat_structure_url(url: str, dataset_id: str) -> str:
    parsed = _official_eurostat_url(url, f"structure URL for {dataset_id!r}")
    pattern = re.compile(
        r"^/eurostat/api/dissemination/sdmx/2\.1/dataflow/ESTAT/([^/]+)/?$",
        re.IGNORECASE,
    )
    match = pattern.fullmatch(parsed.path)
    if match is None or match.group(1).upper() != dataset_id.upper():
        raise FullSourceAdapterError(
            f"Eurostat inventory structure URL does not match dataset {dataset_id!r}"
        )
    query = parse_qsl(parsed.query, keep_blank_values=True)
    allowed = {"references", "format"}
    if any(key.lower() not in allowed for key, _ in query):
        raise FullSourceAdapterError(
            f"Eurostat inventory has an unsafe structure URL for {dataset_id!r}"
        )
    return url


def _validated_eurostat_codelist_url(url: str, dataset_id: str) -> str:
    parsed = _official_eurostat_url(url, f"codelist URL for {dataset_id!r}")
    pattern = re.compile(
        r"^/eurostat/api/dissemination/sdmx/3\.0/structure/codelist/ESTAT/"
        r"([^/]+)/([^/]+)$",
        re.IGNORECASE,
    )
    match = pattern.fullmatch(parsed.path)
    if match is None or match.group(1).upper() != dataset_id.upper():
        raise FullSourceAdapterError(
            f"Eurostat inventory codelist URL does not match code {dataset_id!r}"
        )
    query = parse_qsl(parsed.query, keep_blank_values=True)
    allowed = {"format", "formatversion"}
    if any(key.lower() not in allowed for key, _ in query):
        raise FullSourceAdapterError(
            f"Eurostat inventory has an unsafe codelist URL for {dataset_id!r}"
        )
    formats = [value.upper() for key, value in query if key.lower() == "format"]
    if formats != ["TSV"]:
        raise FullSourceAdapterError(
            f"Eurostat inventory codelist URL is not TSV for {dataset_id!r}"
        )
    return url


def _validated_eurostat_metadata_url(url: str, dataset_id: str) -> str:
    parsed = _official_eurostat_url(url, f"metadata URL for {dataset_id!r}")
    if parsed.path != "/eurostat/api/dissemination/files":
        raise FullSourceAdapterError(
            f"Eurostat inventory has an unsafe metadata URL for {dataset_id!r}"
        )
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if len(query) != 1 or query[0][0].lower() != "file":
        raise FullSourceAdapterError(
            f"Eurostat inventory has an unsafe metadata URL for {dataset_id!r}"
        )
    expected = f"metadata/{dataset_id}.sdmx.zip".lower()
    if query[0][1].lower() != expected:
        raise FullSourceAdapterError(
            f"Eurostat inventory metadata URL does not match code {dataset_id!r}"
        )
    return url


def _official_eurostat_url(url: str, label: str):
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "ec.europa.eu"
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise FullSourceAdapterError(f"Eurostat inventory has an unsafe {label}")
    return parsed


def _inventory_type(inventory_kind: str) -> str:
    if not isinstance(inventory_kind, str):
        raise FullSourceAdapterError("inventory kind must be a string")
    normalized = inventory_kind.strip().lower()
    aliases = {
        "data": "data",
        "eurostat_inventory": "data",
        "codelist": "codelist",
        "eurostat_inventory_codelist": "codelist",
        "metadata": "metadata",
        "eurostat_inventory_metadata": "metadata",
    }
    try:
        return aliases[normalized]
    except KeyError:
        raise FullSourceAdapterError(
            f"unsupported Eurostat inventory kind {inventory_kind!r}"
        ) from None


def _source(source_id: str) -> str:
    if not isinstance(source_id, str):
        raise FullSourceAdapterError("source_id must be a string")
    source = _SOURCE_ALIASES.get(source_id.strip().lower())
    if source is None:
        raise FullSourceAdapterError(f"unsupported full-distribution source {source_id!r}")
    return source


def _decode_text(body: bytes | str, label: str) -> str:
    if isinstance(body, str):
        return body.lstrip("\ufeff")
    if not isinstance(body, bytes):
        raise FullSourceAdapterError(f"{label} must be bytes or text")
    try:
        return body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise FullSourceAdapterError(f"{label} is not valid UTF-8") from exc


def _heading(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _validate_dataset_id(dataset_id: str, line_number: int) -> None:
    if not _DATASET_RE.fullmatch(dataset_id):
        raise FullSourceAdapterError(
            f"Eurostat inventory row {line_number} has an invalid dataset code"
        )


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].rsplit(":", 1)[-1]
