#!/usr/bin/env python3
"""World Bank WDI bronze.load: newest Landing archive → Bronze.

From the WDI CSV ZIP (members identified by their headers, not by name):

  bronze.wdi_bulk_observations  the observation file (wide: one column per year) as one row
                                per country × indicator × year with a value; empty cells are
                                absent observations and are not stored. Values as text.
  bronze.wdi_bulk_metadata      every other member (country, series, country-series,
                                series-time, footnote) as field/value rows: member, source
                                row number, field, value text — so no column is lost when
                                the World Bank adds one.

One Parquet file per Landing file and table, partitioned by entity; the file list is the
checkpoint. Rejections: bronze.wdi_rejected_files.
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any
import zipfile

import duckdb

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import archive_key, catalog, raw_chunks, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file

OBS_ID = ("bronze", "wdi_bulk_observations")
META_ID = ("bronze", "wdi_bulk_metadata")
REJECTED_ID = ("bronze", "wdi_rejected_files")
PROVENANCE = [("raw_sha256", "VARCHAR"), ("source_version", "VARCHAR"), ("ingested_at", "TIMESTAMPTZ")]
OBS_COLUMNS = [
    ("entity", "VARCHAR"),
    ("country_name", "VARCHAR"),
    ("country_code", "VARCHAR"),
    ("indicator_name", "VARCHAR"),
    ("indicator_code", "VARCHAR"),
    ("year", "VARCHAR"),
    ("value_text", "VARCHAR"),
    *PROVENANCE,
    ("source_row_number", "BIGINT"),
]
META_COLUMNS = [
    ("entity", "VARCHAR"),
    ("member", "VARCHAR"),
    ("source_row_number", "BIGINT"),
    ("field", "VARCHAR"),
    ("value_text", "VARCHAR"),
    *PROVENANCE,
]
OBS_HEADER = ["Country Name", "Country Code", "Indicator Name", "Indicator Code"]


def header_of(path: Path) -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return next(csv.reader(handle))


def decode(zip_path: Path, receipt: dict[str, Any], workdir: Path) -> tuple[Path, Path, dict[str, int]]:
    members = [m for m in zipfile.ZipFile(zip_path).infolist() if m.filename.lower().endswith(".csv")]
    zipfile.ZipFile(zip_path).extractall(workdir / "members", members)
    paths = [workdir / "members" / m.filename for m in members]
    observation = [
        p for p in paths if header_of(p)[:4] == OBS_HEADER and header_of(p)[4:5] and header_of(p)[4].isdigit()
    ]
    if len(observation) != 1:
        raise ValueError(f"expected one observation member, found {[p.name for p in observation]}")
    ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    prov = [receipt["sha256"], receipt["version_token"], ingested]
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'")
    obs_out, meta_out = workdir / "obs.parquet", workdir / "meta.parquet"
    years = [c for c in header_of(observation[0])[4:] if c.isdigit()]
    con.execute(
        "CREATE TABLE raw AS SELECT * FROM read_csv(?, header=true, all_varchar=true, "
        "delim=',', quote='\"', escape='\"', nullstr='\x01', strict_mode=true)",
        [str(observation[0])],
    )
    con.execute("CREATE TABLE wide AS SELECT rowid + 2 AS line, * FROM raw")
    year_list = ", ".join(f'"{y}"' for y in years)
    target = str(obs_out).replace("'", "''")
    con.execute(
        f"""COPY (SELECT ? AS entity, "Country Name" AS country_name, "Country Code" AS country_code,
                       "Indicator Name" AS indicator_name, "Indicator Code" AS indicator_code, year, value_text,
                       ? AS raw_sha256, ? AS source_version, ?::TIMESTAMPTZ AS ingested_at, line AS source_row_number
                FROM (UNPIVOT wide ON {year_list} INTO NAME year VALUE value_text)
                WHERE value_text <> '') TO '{target}' (FORMAT parquet, COMPRESSION zstd)""",
        [receipt["entity"], *prov],
    )
    counts = {"observations": con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]}
    meta_rows = 0
    with tempfile.NamedTemporaryFile(
        "w", suffix=".csv", dir=workdir, delete=False, newline="", encoding="utf-8"
    ) as tmp:
        writer = csv.writer(tmp)
        writer.writerow(["member", "source_row_number", "field", "value_text"])
        for path in paths:
            if path in observation:
                continue
            with path.open(encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)
                header = next(reader)
                for number, row in enumerate(reader, start=2):
                    for field, value in zip(header, row, strict=False):
                        if field and value != "":
                            writer.writerow([path.name, number, field, value])
                            meta_rows += 1
        meta_csv = tmp.name
    target = str(meta_out).replace("'", "''")
    con.execute(
        f"""COPY (SELECT ? AS entity, member, source_row_number::BIGINT AS source_row_number, field, value_text,
                       ? AS raw_sha256, ? AS source_version, ?::TIMESTAMPTZ AS ingested_at
                FROM read_csv(?, header=true, all_varchar=true, quote='\"', escape='\"', nullstr='\x01'))
                TO '{target}' (FORMAT parquet, COMPRESSION zstd)""",
        [receipt["entity"], *prov, meta_csv],
    )
    counts["metadata_values"] = meta_rows
    counts["members"] = len(paths)
    con.close()
    return obs_out, meta_out, counts


def load(budget_minutes: float) -> int:
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    for table_id, columns in ((OBS_ID, OBS_COLUMNS), (META_ID, META_COLUMNS)):
        bf.retry(lambda t=table_id, c=columns: bf.ensure_table(cat, t, c, "entity"), "table")
    done = bf.loaded_hashes(cat.load_table(OBS_ID)) | bf.rejected_hashes(cat, REJECTED_ID)
    receipt = bulk_file.newest(client, landing_bucket, "world_bank_wdi", bulk_file.single_entity("world_bank_wdi"))
    totals: dict[str, Any] = {"candidates": 0, "appended_files": 0, "rejected": 0}
    if receipt["sha256"] not in done:
        totals["candidates"] = 1
        with tempfile.TemporaryDirectory(prefix="zohelo-wdi-bronze-") as temporary:
            work = Path(temporary)
            key = receipt["object"]["key"]
            try:
                client.head_object(Bucket=landing_bucket, Key=key)
            except client.exceptions.ClientError:
                key = archive_key("world_bank_wdi", receipt["sha256"], key)
            digest, zip_path = sha256(), work / "wdi.zip"
            with zip_path.open("wb") as handle:
                for block in raw_chunks(client, landing_bucket, key):
                    handle.write(block)
                    digest.update(block)
            if digest.hexdigest() != receipt["sha256"]:
                raise RuntimeError(f"WDI raw bytes do not match their receipt: {key}")
            try:
                obs, meta, counts = decode(zip_path, receipt, work)
            except (ValueError, duckdb.Error, zipfile.BadZipFile, csv.Error, UnicodeDecodeError) as exc:
                bf.record_rejections(
                    cat,
                    REJECTED_ID,
                    [
                        {
                            "entity": receipt["entity"],
                            "raw_sha256": receipt["sha256"],
                            "landing_key": receipt["object"]["key"],
                            "error": f"{type(exc).__name__}: {exc}"[:1000],
                            "code_sha": os.environ.get("GITHUB_SHA", ""),
                        }
                    ],
                )
                totals["rejected"] = 1
            else:
                for table_id, path, name in ((META_ID, meta, "metadata"), (OBS_ID, obs, "observations")):
                    uri = bf.upload(
                        client,
                        lake,
                        f"02_bronze/world_bank_wdi/{name}/{receipt['entity']}/{receipt['sha256']}.parquet",
                        path,
                        receipt["sha256"],
                    )
                    bf.commit(cat, table_id, [uri], ["entity", "raw_sha256"], "wdi-bronze-load")
                totals.update(counts)
                totals["appended_files"] = 1
    annotate("notice", "world_bank_wdi Bronze from Landing", {"result": "complete", **totals})
    step_summary(f"## world_bank_wdi · bronze load\n\n```json\n{json.dumps(totals, indent=2)}\n```\n")
    return 0
