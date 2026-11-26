#!/usr/bin/env python3
"""GUGiK orthophoto sheet index Landing → Bronze → Silver, one year's feature type at a time (design #875).

Entity = ``orto_<YYYY>`` (ingestion/to_landing/gugik_orto/); its newest complete Landing version (every page part
present) is the entity version. Bronze and Silver hold one file per entity, replaced whole (entity_tables):

  02_bronze/gugik_orto/sheets/<entity>/<version>.parquet          the features as published, values as text
  03_silver/gugik_orto/sheets/<entity>/<version>.<model>.parquet  warehouse/entity/gugik_orto_sheets.sql

``<model>`` is a digest of the Silver SQL: a changed model recomputes Silver from unchanged Bronze. Each Bronze row
keeps the SHA-256 of its page (``raw_sha256``), which is what proves the page loaded for the Archive. A page that is
not a FeatureCollection of the year's feature type, a feature property outside PROPERTIES, a feature id held twice,
or a Silver check that fails (keys null, a feature id twice, a year that is not the feature type's year) rejects the
entity into bronze.gugik_orto_rejected_files and the tables keep the previous version. Prints one JSON result line.
"""

from __future__ import annotations

import argparse
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.exceptions import NoSuchTableError

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing import gugik_orto as landing
from warehouse import entity

SOURCE = "gugik_orto"
ENTITY = "entity"
BRONZE = ("bronze", "gugik_orto_sheets")
SILVER = ("silver", "gugik_orto_sheets")
REJECTED = ("bronze", "gugik_orto_rejected_files")
MODEL = "gugik_orto_sheets"
BRONZE_ROOT, SILVER_ROOT = "02_bronze", "03_silver"  # a Dev trial points them at zz_dev_ scratch prefixes
GML, NS = "{http://www.opengis.net/gml/3.2}", "{http://www.gugik.gov.pl}"
# DescribeFeatureType of SkorowidzOrtofomapy<YYYY> (Dev probe 45810994708, 26 Nov 2026), in order; msGeometry apart.
PROPERTIES = (
    "godlo",
    "akt_rok",
    "piksel",
    "kolor",
    "zrodlo_danych",
    "uklad_xy",
    "modul_archiwizacji",
    "nr_zglosz",
    "akt_data",
    "czy_ark_wypelniony",
    "url_do_pobrania",
    "dt_pzgik",
    "wlk_pliku_mb",
)
COLUMNS = [
    (ENTITY, "VARCHAR"),
    ("feature_type", "VARCHAR"),
    ("gml_id", "VARCHAR"),
    *[(p, "VARCHAR") for p in PROPERTIES],
    ("geometry_type", "VARCHAR"),
    ("srs_name", "VARCHAR"),
    ("pos_list", "VARCHAR"),
    ("rings", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("part_index", "BIGINT"),
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
SILVER_KEY = ("feature_id",)
SILVER_NOT_NULL = ("feature_id", "sheet_id", "data_year", "raw_sha256")
STATS = [ENTITY, "raw_sha256"]


class IndexDecodeError(ValueError):
    """A page is not the year's feature collection of the known layout (the entity is rejected)."""


def model_digest() -> str:
    return sha256((Path(entity.__file__).parent / "entity" / f"{MODEL}.sql").read_bytes()).hexdigest()[:12]


def _feature(element: ET.Element, feature_type: str) -> dict[str, Any]:
    if element.tag != NS + feature_type:
        raise IndexDecodeError(f"member {element.tag} is not {feature_type}")
    row: dict[str, Any] = {"gml_id": element.get(GML + "id")}
    for child in element:
        name = child.tag.removeprefix(NS)
        if child.tag == GML + "boundedBy":
            continue
        if name == "msGeometry":
            shapes = list(child)
            rings = [r for r in child.iter(GML + "posList")]
            row["geometry_type"] = shapes[0].tag.removeprefix(GML) if shapes else None
            row["srs_name"] = shapes[0].get("srsName") if shapes else None
            row["pos_list"] = rings[0].text.strip() if rings and rings[0].text else None
            row["rings"] = str(len(rings))
        elif name in PROPERTIES:
            stamp = child.find(f"{GML}timePosition")
            row[name] = (stamp.text if stamp is not None else child.text or "").strip() or None
        else:
            raise IndexDecodeError(f"unknown feature property {child.tag}")
    return row


def decode(bodies: list[tuple[bytes, dict[str, Any]]], output: Path) -> int:
    """The pages of one entity version → one Bronze Parquet file; returns rows."""
    feature_type = "SkorowidzOrtofomapy" + bodies[0][1]["entity"].removeprefix("orto_")
    rows: list[dict[str, Any]] = []
    for body, receipt in bodies:
        prov = bulk_bronze.provenance(receipt)
        try:
            for _event, element in ET.iterparse(io.BytesIO(body), events=("end",)):
                if element.tag == "{http://www.opengis.net/wfs/2.0}member":
                    for member in element:
                        rows.append(
                            {
                                **_feature(member, feature_type),
                                **prov,
                                "feature_type": feature_type,
                                "part_index": receipt["part"]["index"],
                                "source_row_number": len(rows) + 1,
                            }
                        )
                    element.clear()
        except ET.ParseError as exc:
            raise IndexDecodeError(f"part {receipt['part']['index']}: not XML: {exc}") from exc
    ids = [r["gml_id"] for r in rows]
    if not rows or len(set(ids)) != len(ids) or len(rows) != bodies[0][1]["detail"]["matched"]:
        raise IndexDecodeError(f"{len(rows)} features, {len(set(ids))} ids, hits {bodies[0][1]['detail']['matched']}")
    table = pa.Table.from_pylist([{c: r.get(c) for c, _ in COLUMNS} for r in rows], schema=ARROW)
    pq.write_table(table, output, compression="zstd")
    return len(rows)


def newest(client, bucket: str) -> dict[str, list[dict[str, Any]]]:
    """Entity -> the receipts (part order) of its newest complete Landing version."""
    out = {}
    for name, held in landing.landing_state(client, bucket).items():
        out[name] = [json.loads(client.get_object(Bucket=bucket, Key=k)["Body"].read()) for k in held["keys"]]
    return out


def table_state(cat, table_id) -> dict[str, dict[str, Any]]:
    try:
        return et.state(bf.retry(lambda: cat.load_table(table_id), "load_table"), ENTITY)
    except NoSuchTableError:
        return {}


def silver_columns(path: Path) -> list[tuple[str, str]]:
    kinds = {
        "string": "VARCHAR",
        "large_string": "VARCHAR",
        "double": "DOUBLE",
        "date32[day]": "DATE",
        "int32": "INTEGER",
        "int64": "BIGINT",
        "bool": "BOOLEAN",
    }
    return [(f.name, kinds[str(f.type)]) for f in pq.read_schema(path)]


def run(minutes: float) -> dict[str, Any]:
    client, cat = s3(), bf.retry(catalog, "catalog")
    lake, landing_bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME"), req("STORAGE_LANDING_BUCKET_NAME")
    started, digest = time.monotonic(), model_digest()
    versions = newest(client, landing_bucket)
    b_state, s_state = table_state(cat, BRONZE), table_state(cat, SILVER)
    refused = bf.rejected_hashes(cat, REJECTED)
    out: dict[str, Any] = {
        "entities_landed": len(versions),
        "loaded": 0,
        "bronze_rows_added": 0,
        "silver_rows_written": 0,
        "skipped_rejected_before": 0,
    }
    rejected: list[dict[str, Any]] = []
    created: dict[str, Any] = {}
    try:
        with tempfile.TemporaryDirectory(prefix="zohelo-gugik-orto-") as temporary:
            work = Path(temporary)
            for name, receipts in sorted(versions.items(), reverse=True):  # newest years first
                version = receipts[0]["version_token"]
                if version in refused or f"{version}.{digest}" in refused:
                    out["skipped_rejected_before"] += 1
                    continue
                need_bronze = b_state.get(name, {}).get("recipes") != {version}
                if not need_bronze and s_state.get(name, {}).get("recipes") == {f"{version}.{digest}"}:
                    continue
                if time.monotonic() > started + max(minutes, 5) * 60:
                    out["stopped_at_budget"] = name
                    break
                rejection = {
                    "entity": name,
                    "raw_sha256": version,
                    "landing_key": receipts[0]["object"]["key"],
                    "code_sha": os.environ.get("GITHUB_SHA", ""),
                }
                local, silver = work / f"{name}.bronze.parquet", work / f"{name}.silver.parquet"
                if need_bronze:
                    bodies = [(bulk_bronze.read_raw(client, landing_bucket, SOURCE, r), r) for r in receipts]
                    try:
                        out["bronze_rows_added"] += decode(bodies, local)
                    except IndexDecodeError as exc:
                        rejected.append({**rejection, "error": f"decoder: {exc}"[:1000]})
                        continue
                    source = str(local)
                else:
                    source = b_state[name]["paths"][0]
                try:
                    report = entity.apply(
                        MODEL,
                        source,
                        silver,
                        not_null=SILVER_NOT_NULL,
                        unique_key=SILVER_KEY,
                        reject_if=f"data_year <> {int(name.removeprefix('orto_'))}",
                        memory_limit="4GB",
                        temp_directory=str(work),
                    )
                except entity.EntityModelError as exc:
                    rejected.append(
                        {**rejection, "raw_sha256": f"{version}.{digest}", "error": f"silver: {exc}"[:1000]}
                    )
                    continue
                if need_bronze:
                    uri = bf.upload(
                        client, lake, f"{BRONZE_ROOT}/{SOURCE}/sheets/{name}/{version}.parquet", local, version
                    )
                    if not created.get("bronze"):
                        created["bronze"] = bf.retry(lambda: bf.ensure_table(cat, BRONZE, COLUMNS, ENTITY), "table")
                        if not b_state:  # a new table: compaction off, one snapshot kept
                            et.configure(BRONZE)
                    et.replace(cat, BRONZE, ENTITY, {name: [uri]}, STATS, "gugik-orto-entities")
                    b_state[name] = {"recipes": {version}, "paths": [uri]}
                uri = bf.upload(
                    client, lake, f"{SILVER_ROOT}/{SOURCE}/sheets/{name}/{version}.{digest}.parquet", silver, version
                )
                if not created.get("silver"):
                    created["silver"] = bf.retry(
                        lambda path=silver: bf.ensure_table(cat, SILVER, silver_columns(path), ENTITY), "table"
                    )
                    if not s_state:
                        et.configure(SILVER)
                et.replace(cat, SILVER, ENTITY, {name: [uri]}, STATS, "gugik-orto-entities")
                s_state[name] = {"recipes": {f"{version}.{digest}"}, "paths": [uri]}
                out["loaded"] += 1
                out["silver_rows_written"] += report["rows"]
                local.unlink(missing_ok=True)
                silver.unlink(missing_ok=True)
    finally:
        bf.record_rejections(cat, REJECTED, rejected)
    out["rejected"] = len(rejected)
    out["rejected_sample"] = [r["error"][:300] for r in rejected[:3]]
    out["minutes"] = round((time.monotonic() - started) / 60, 1)
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=60)
    args = parser.parse_args(argv)
    try:
        out, failed = run(args.minutes), None
    except Exception as exc:  # not a verdict on the data: the next run tries again
        out, failed = {}, f"{type(exc).__name__}: {str(exc)[:300]}"
    out["failed"] = failed
    print(json.dumps(out, sort_keys=True), flush=True)
    annotate("error" if failed else "warning" if out.get("rejected") else "notice", f"{SOURCE} entities", out)
    step_summary(f"## {SOURCE} · Landing → Bronze → Silver\n\n```json\n{json.dumps(out, indent=2)}\n```\n")
    return 1 if failed else 0


def verify() -> int:
    """Bronze and Silver against Landing, per entity, from metadata (et.completeness)."""
    client, cat = s3(), bf.retry(catalog, "catalog")
    held = landing.landing_state(client, req("STORAGE_LANDING_BUCKET_NAME"))
    digest, refused = model_digest(), bf.rejected_hashes(cat, REJECTED)
    expected = {
        e: h["version"]
        for e, h in held.items()
        if h["version"] not in refused and f"{h['version']}.{digest}" not in refused
    }
    report = et.completeness(
        expected, table_state(cat, BRONZE), table_state(cat, SILVER), {e: f"{v}.{digest}" for e, v in expected.items()}
    )
    report["rejected_entities"] = len(held) - len(expected)
    ok = report["bronze_pct"] >= 99 and report["silver_pct"] >= 99
    report["result"] = "pass" if ok else "fail"
    annotate("notice" if ok else "error", f"{SOURCE} bronze.verify", report)
    step_summary(f"## {SOURCE} · bronze.verify\n\n```json\n{json.dumps(report, indent=2)}\n```\n")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
