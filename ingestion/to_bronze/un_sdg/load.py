#!/usr/bin/env python3
"""UN Global SDG Indicators Database bronze.load: newest Landing copy of the release ZIP → bronze.un_sdg_observations.

The ZIP (register GL-018, <release>_AllData_After_<date>_CSV.zip from the database archive page) holds one CSV: one row
per series record × indicator it is listed under, every area, year and disaggregation of the release. Its header is the
26 columns of CORE, then one column per attribute (ATTRIBUTES) and per disaggregation dimension; the dimension columns
follow the series of the release (2026.Q3.G.01 has 57, from Age to Type_of_household) and are not fixed, so Bronze keeps
them as one JSON text ``dimensions`` of name → code, keys sorted, without the CSV's empty mark ``NA``. Every other value
stays as published text (``NA`` included). Bronze keeps every row of every area; DuckDB reads the CSV (no per-cell
Python).

A file is rejected into bronze.un_sdg_rejected_files (never guessed) when: it is not a ZIP of exactly one CSV; the
header does not start with CORE; an attribute column is missing or repeated; a column name is empty or repeated; a row
has another width; it is not UTF-8; or it holds no row.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
import shutil
import tempfile
from typing import Any
import zipfile

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "un_sdg"
TABLE = "un_sdg_observations"
REJECTED = "un_sdg_rejected_files"
CORE = {
    "": "row_label", "ID": "id", "Goal": "goal", "Target": "target", "Indicator": "indicator",
    "ReleaseStatus": "release_status", "ReleaseName": "release_name", "SeriesID": "series_id",
    "SeriesCode": "series_code", "isDSDSeries": "is_dsd_series", "SeriesDescription": "series_description",
    "SeriesObservationCount": "series_observation_count", "GeoAreaCode": "geo_area_code",
    "GeoAreaName": "geo_area_name", "TimePeriod": "time_period", "Value": "value", "ValueType": "value_type",
    "Time_Detail": "time_detail", "TimeCoverage": "time_coverage", "UpperBound": "upper_bound",
    "LowerBound": "lower_bound", "BasePeriod": "base_period", "Source": "source", "GeoInfoUrl": "geo_info_url",
    "FootNote": "footnote", "ObservationID": "observation_id",
}  # fmt: skip
ATTRIBUTES = {
    "Nature": "nature",
    "Units": "units",
    "UnitMultiplier": "unit_multiplier",
    "Observation Status": "observation_status",
}
NO_VALUE = "NA"  # the CSV's empty mark
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(c, "VARCHAR") for c in CORE.values()],
    *[(c, "VARCHAR") for c in ATTRIBUTES.values()],
    ("dimensions", "VARCHAR"),  # JSON object: dimension name → code, sorted, NA left out
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),  # position of the row among the CSV's data rows, from 1
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def header_of(path: Path) -> list[str]:
    with path.open(encoding="utf-8", newline="") as handle:
        header = next(csv.reader(handle), None)
    if not header:
        raise ValueError("empty CSV")
    if header[: len(CORE)] != list(CORE):
        raise ValueError(f"not the SDG database header: {header[:8]}… ({len(header)} columns)")
    rest = header[len(CORE) :]
    if any(not name.strip() for name in rest) or len(set(header)) != len(header):
        raise ValueError(f"an empty or repeated column name in {rest}")
    missing = [a for a in ATTRIBUTES if a not in rest]
    if missing:
        raise ValueError(f"attribute columns missing: {missing}")
    return header


def quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-un-sdg-") as temporary:
        path = Path(temporary) / "data.csv"
        try:
            with zipfile.ZipFile(io.BytesIO(body)) as archive:
                members = archive.infolist()
                if len(members) != 1 or not members[0].filename.lower().endswith(".csv"):
                    raise ValueError(f"not a ZIP of one CSV: {[m.filename for m in members][:5]}")
                with archive.open(members[0]) as source, path.open("wb") as target:
                    shutil.copyfileobj(source, target, 16 << 20)
        except zipfile.BadZipFile as exc:
            raise ValueError(f"not a ZIP: {exc}") from exc
        header = header_of(path)
        names = [f"c{i}" for i in range(len(header))]
        by_name = dict(zip(header, names, strict=True))
        dims = [h for h in header[len(CORE) :] if h not in ATTRIBUTES]
        entries = ", ".join(f"struct_pack(k := {quoted(d)}, v := {by_name[d]})" for d in dims)
        dimensions = (
            f"cast(to_json(map_from_entries(list_sort(list_filter([{entries}], "
            f"lambda x: x.v is not null and x.v <> '{NO_VALUE}')))) as varchar)"
            if dims
            else "'{}'"
        )
        con = duckdb.connect()
        try:
            con.execute("set threads = 1")  # file order for source_row_number
            con.execute("set preserve_insertion_order = true")
            table = con.sql(
                f"""
                select
                    ? as entity,
                    {", ".join(f"{by_name[h]} as {c}" for h, c in CORE.items())},
                    {", ".join(f"{by_name[h]} as {c}" for h, c in ATTRIBUTES.items())},
                    {dimensions} as dimensions,
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at,
                    cast(row_number() over () as bigint) as source_row_number
                from read_csv(?, header = true, delim = ',', quote = '"', escape = '"', all_varchar = true,
                              strict_mode = true, null_padding = false, encoding = 'utf-8',
                              names = [{", ".join(quoted(n) for n in names)}])
                """,
                params=[prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"], str(path)],
            ).to_arrow_table()
        except duckdb.Error as exc:
            raise ValueError(f"not a readable CSV: {str(exc)[:300]}") from exc
        finally:
            con.close()
    if not table.num_rows:
        raise ValueError("no rows")
    return table.cast(ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one ZIP of about 70 MB (a CSV of about 1.8 GB)."""
    del budget_minutes
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        COLUMNS,
        decode,
        "observations",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
