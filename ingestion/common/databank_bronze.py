"""Shared decoders for World Bank DataBank bulk CSV archives (WGI_CSV.zip, IDS_CSV.zip; WDI has its own loader).

A DataBank bulk ZIP holds one observation member (wide: one column per year) and metadata members
(country, series, country-series, series-time, footnotes). Members are identified by their headers,
not by name. Two decoders for ingestion/common/bulk_bronze.py, which writes one Parquet file per
Landing file and table:

``observations``  the observation member as one row per economy × series (× counterpart area) × year
                  with a value; empty cells are absent observations and are not stored. Codes, names
                  and values stay source text (IDS pads some codes with spaces: Silver trims).
``metadata``      every other member as field/value rows (member, source row number, field, value
                  text), so no column is lost when the World Bank adds one.

A header column the observation decoder does not know (other than a year) rejects the file.
"""

from __future__ import annotations

import csv
import io
from typing import Any
import zipfile

import pyarrow as pa

from ingestion.common import bulk_bronze

# Header names of the observation member → Bronze column (WGI: Country/Indicator; IDS: Series, Counterpart-Area).
KNOWN = {
    "Country Code": "country_code",
    "Country Name": "country_name",
    "Indicator Code": "series_code",
    "Series Code": "series_code",
    "Indicator Name": "series_name",
    "Series Name": "series_name",
    "Counterpart-Area Code": "counterpart_code",
    "Counterpart-Area Name": "counterpart_name",
}
OBS_COLUMNS = [
    ("entity", "VARCHAR"),
    ("country_code", "VARCHAR"),
    ("country_name", "VARCHAR"),
    ("counterpart_code", "VARCHAR"),  # IDS only (creditor area); null when the archive has none
    ("counterpart_name", "VARCHAR"),
    ("series_code", "VARCHAR"),
    ("series_name", "VARCHAR"),
    ("year", "VARCHAR"),
    ("value_text", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
META_COLUMNS = [
    ("entity", "VARCHAR"),
    ("member", "VARCHAR"),
    ("source_row_number", "BIGINT"),
    ("field", "VARCHAR"),
    ("value_text", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
]
OBS_ARROW = bulk_bronze.arrow_schema(OBS_COLUMNS)
META_ARROW = bulk_bronze.arrow_schema(META_COLUMNS)


def _members(body: bytes) -> list[tuple[str, list[list[str]]]]:
    """Every CSV member of the archive as (name, rows), header first."""
    out = []
    try:  # a broken archive or CSV is a verdict on the file: rejected with the reason, not a crashed load
        archive = zipfile.ZipFile(io.BytesIO(body))
        for info in archive.infolist():
            if info.filename.lower().endswith(".csv"):
                text = archive.read(info).decode("utf-8-sig")
                out.append((info.filename, list(csv.reader(io.StringIO(text, newline="")))))
    except (zipfile.BadZipFile, csv.Error) as exc:
        raise ValueError(f"{type(exc).__name__}: {exc}") from exc
    if not out:
        raise ValueError("archive holds no CSV member")
    return out


def _is_observation(header: list[str]) -> bool:
    return "Country Code" in header and any(h.strip().isdigit() and len(h.strip()) == 4 for h in header)


def _split(body: bytes) -> tuple[tuple[str, list[list[str]]], list[tuple[str, list[list[str]]]]]:
    members = [m for m in _members(body) if m[1]]
    observation = [m for m in members if _is_observation(m[1][0])]
    if len(observation) != 1:
        raise ValueError(f"expected one observation member, found {[m[0] for m in observation]}")
    return observation[0], [m for m in members if m is not observation[0]]


def observations(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    (name, rows), _ = _split(body)
    header = [h.strip() for h in rows[0]]
    years = [(i, h) for i, h in enumerate(header) if h.isdigit() and len(h) == 4]
    year_at = {i for i, _ in years}
    unknown = [h for i, h in enumerate(header) if i not in year_at and h not in KNOWN]
    if unknown:
        raise ValueError(f"{name}: unknown columns {unknown}")
    at = {KNOWN[h]: i for i, h in enumerate(header) if h in KNOWN}
    if not {"country_code", "series_code"} <= set(at):
        raise ValueError(f"{name}: no country or series code column in {header[:8]}")
    names = ("country_code", "country_name", "counterpart_code", "counterpart_name", "series_code", "series_name")
    columns: dict[str, list[Any]] = {n: [] for n in (*names, "year", "value_text", "source_row_number")}
    for number, row in enumerate(rows[1:], start=2):
        if not row:
            continue
        if len(row) != len(header):
            raise ValueError(f"{name} line {number}: {len(row)} fields, header has {len(header)}")
        keys = [row[at[n]] if n in at else None for n in names]
        for i, year in years:
            if row[i] != "":
                for n, v in zip(names, keys, strict=True):
                    columns[n].append(v)
                columns["year"].append(year)
                columns["value_text"].append(row[i])
                columns["source_row_number"].append(number)
    count = len(columns["year"])
    if not count:
        raise ValueError(f"{name}: no observations")
    base = bulk_bronze.provenance(receipt)
    data = {n: [base[n]] * count for n in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update(columns)
    return pa.Table.from_pydict({n: data[n] for n, _ in OBS_COLUMNS}, schema=OBS_ARROW)


def metadata(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    _, others = _split(body)
    columns: dict[str, list[Any]] = {n: [] for n in ("member", "source_row_number", "field", "value_text")}
    for name, rows in others:
        header = rows[0]
        for number, row in enumerate(rows[1:], start=2):
            if len(row) > len(header):
                raise ValueError(f"{name} line {number}: {len(row)} fields, header has {len(header)}")
            padded = row + [""] * (len(header) - len(row))  # a short row: its missing trailing fields are empty
            for field, value in zip(header, padded, strict=True):
                if field and value != "":
                    columns["member"].append(name)
                    columns["source_row_number"].append(number)
                    columns["field"].append(field)
                    columns["value_text"].append(value)
    count = len(columns["member"])
    if not count:
        raise ValueError("no metadata members")
    base = bulk_bronze.provenance(receipt)
    data = {n: [base[n]] * count for n in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update(columns)
    return pa.Table.from_pydict({n: data[n] for n, _ in META_COLUMNS}, schema=META_ARROW)


def load(source: str, prefix: str, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """Both tables of a DataBank source: ``<prefix>_observations``, ``<prefix>_metadata``; rejections in
    ``<prefix>_rejected_files``. Each table keeps its own checkpoint (its file list)."""
    lake_root = "02_bronze" if namespace == "bronze" else namespace
    rejected = (namespace, f"{prefix}_rejected_files")
    status = 0
    for table, columns, decode, folder in (
        ("observations", OBS_COLUMNS, observations, "observations"),
        ("metadata", META_COLUMNS, metadata, "metadata"),
    ):
        status |= bulk_bronze.load(
            source,
            (namespace, f"{prefix}_{table}"),
            rejected,
            columns,
            decode,
            folder,
            landing_root=landing_root,
            lake_root=lake_root,
        )
    return status
