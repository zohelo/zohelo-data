"""Dev trial for O37 (#2820): v1 and v2 written by main's code and by this branch's code from the same Gold,
under the scratch prefix zz_dev_o37/ (removed at the end). Read-only against production. Reports the v2 KPI
count before/after, file sizes against the limits, and the v1 difference (only rows of unlicensed sources)."""

from __future__ import annotations

from collections import Counter
import gzip
import io
import json
import os
from pathlib import Path
import sys
import traceback

import pyarrow.parquet as pq

from ingestion.common import report
from ingestion.common.r2 import catalog, req, s3

SCRATCH = "zz_dev_o37/"
real_annotate = report.annotate
report.annotate = lambda level, title, data: print(f"[{level}] {title}: {json.dumps(data, default=str)[:2000]}")


def objects(client, prefix: str) -> dict[str, bytes]:
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    found = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            found[key[len(prefix) :]] = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return found


def clean(client) -> int:
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=SCRATCH):
        keys += [str(i["Key"]) for i in page.get("Contents", [])]
    for i in range(0, len(keys), 500):
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i : i + 500]]})
    return len(keys)


def strip(obj):
    if isinstance(obj, dict):
        return {k: strip(v) for k, v in obj.items() if k != "generated_at"}
    if isinstance(obj, list):
        return [strip(v) for v in obj]
    return obj


def same(name: str, a: bytes, b: bytes) -> bool:
    if a == b:
        return True
    if name.endswith((".json", ".geojson")):
        return strip(json.loads(a)) == strip(json.loads(b))
    if name.endswith(".ics"):
        keep = lambda body: [x for x in body.decode().splitlines() if not x.startswith("DTSTAMP")]  # noqa: E731
        return keep(a) == keep(b)
    if name.endswith(".parquet"):
        return pq.read_table(io.BytesIO(a)).equals(pq.read_table(io.BytesIO(b)))
    return False


def v1_diff(old: dict[str, bytes], new: dict[str, bytes], licensed: set[str]) -> dict:
    only_old = sorted(set(old) - set(new))
    only_new = sorted(set(new) - set(old))
    differ = sorted(n for n in set(old) & set(new) if not same(n, old[n], new[n]))
    removed_rows = {}
    for name in only_old:  # every file that left must be a series/latest of a KPI whose rows are all unlicensed
        if name.endswith("series.parquet"):
            table = pq.read_table(io.BytesIO(old[name]))
            removed_rows[name] = dict(Counter(table.column("source_key").to_pylist()))
        elif name.endswith("latest.json"):
            removed_rows[name] = dict(Counter(v["source_key"] for v in json.loads(old[name])["values"]))
    unexpected_rows = {n: c for n, c in removed_rows.items() if set(c) & licensed}
    catalog_ok = None
    if "catalog/catalog.json" in differ:
        a, b = json.loads(old["catalog/catalog.json"]), json.loads(new["catalog/catalog.json"])
        expected = []
        for k in a["kpis"]:
            kept = [s for s in k["source_keys"] if s in licensed]
            if kept:
                expected.append({**k, "source_keys": kept})
        catalog_ok = strip({**a, "kpis": expected}) == strip(b)
        dropped = [k["kpi_key"] for k in a["kpis"] if k["kpi_key"] not in {x["kpi_key"] for x in b["kpis"]}]
    other = [n for n in differ if n != "catalog/catalog.json"]
    return {
        "files_old": len(old),
        "files_new": len(new),
        "only_old": only_old,
        "only_new": only_new,
        "differ": differ,
        "other_differences": other,
        "removed_rows_by_source": removed_rows,
        "removed_rows_from_licensed_sources": unexpected_rows,
        "catalog_equals_old_minus_unlicensed": catalog_ok,
        "catalog_kpis_dropped": dropped if catalog_ok is not None else [],
        "verdict": not only_new and not other and not unexpected_rows and catalog_ok in (True, None),
    }


def main() -> int:
    from dev import publish_old, publish_v2_old
    from warehouse import publish, publish_v2

    out = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
    out.mkdir(parents=True, exist_ok=True)
    cat, client = catalog(), s3()
    result: dict = {}
    try:
        clean(client)
        # v1, main's code and this branch's code, same Gold
        publish_old.main(prefix=SCRATCH + "old/v1/")
        new_v1_result = publish.main(prefix=SCRATCH + "new/v1/")
        result["v1_licence_gate"] = new_v1_result.get("licence_gate")
        old_v1 = objects(client, SCRATCH + "old/v1/")
        new_v1 = objects(client, SCRATCH + "new/v1/")
        licensed = publish.licensed_sources(publish.seed("source"), publish.seed("licence"))
        result["unlicensed_sources"] = sorted({s["source_key"] for s in publish.seed("source")} - licensed)
        result["v1"] = v1_diff(old_v1, new_v1, licensed)
        # v2: main's code (status gate) and this branch's code (Gold series gate), each on its own v1 catalog
        checked_at = publish.checked(publish.gold(cat, "gold", "coverage"))
        stamp = publish.iso(__import__("datetime").datetime.now(__import__("datetime").UTC))
        old_v2 = publish_v2_old.write(
            cat,
            client,
            {},
            checked_at,
            json.loads(old_v1["catalog/catalog.json"]),
            json.loads(old_v1["status.json"]),
            stamp,
            prefix=SCRATCH + "old/v2/",
        )
        breaches: list[str] = []
        real_sized = publish_v2._sized

        def recording(name: str, body: bytes) -> dict:
            try:
                return real_sized(name, body)
            except ValueError as error:
                breaches.append(str(error))
                return {"bytes": len(body), "gzip_bytes": len(gzip.compress(body))}

        publish_v2._sized = recording
        try:
            new_v2 = publish_v2.write(
                cat,
                client,
                {},
                checked_at,
                json.loads(new_v1["catalog/catalog.json"]),
                json.loads(new_v1["status.json"]),
                stamp,
                prefix=SCRATCH + "new/v2/",
            )
            files = objects(client, SCRATCH + "new/v2/")
            cat_doc = json.loads(files["catalog.json"])
            series = {n: len(gzip.compress(b)) for n, b in files.items() if n.endswith("series.json")}
            result["v2_new"] = {
                "kpis": len(new_v2["kpis"]),
                "by_status": dict(Counter(k["status"] for k in cat_doc["kpis"])),
                "draft_kpis": sorted(k["kpi_key"] for k in cat_doc["kpis"] if k["status"] == "draft"),
                "missing": new_v2["missing"],
                "empty": new_v2["empty"],
                "latest_values": new_v2["latest_values"],
                "briefs": len(new_v2["briefs"]),
                "removed": new_v2["removed"],
                "gzip_bytes": {n: len(gzip.compress(b)) for n, b in files.items() if not n.startswith("kpi/")},
                "raw_bytes": {n: len(b) for n, b in files.items() if not n.startswith("kpi/")},
                "series_gzip_max": sorted(series.items(), key=lambda x: -x[1])[:5],
                "series_gzip_total": sum(series.values()),
                "size_breaches": breaches,
                "read_back_differences": new_v2["differences"],
            }
        finally:
            publish_v2._sized = real_sized
        result["v2_old"] = {
            "kpis": len(old_v2["kpis"]),
            "kpi_keys": old_v2["kpis"],
            "gzip_bytes": {n: b for n, b in old_v2["bytes"].items() if not n.startswith("kpi/")},
        }
        result["v2_left"] = sorted(set(old_v2["kpis"]) - set(new_v2["kpis"]))
        status = 0 if result["v1"]["verdict"] and not breaches else 1
    except Exception as error:  # report, then clean up
        result["error"] = f"{type(error).__name__}: {str(error)[:2500]}"
        traceback.print_exc()
        status = 1
    finally:
        result["scratch_removed"] = clean(client)
    (out / "o37_trial.json").write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps(result, indent=2, default=str))
    compact = {
        "v2_kpis_before": result.get("v2_old", {}).get("kpis"),
        "v2_kpis_after": result.get("v2_new", {}).get("kpis"),
        "v2_by_status": result.get("v2_new", {}).get("by_status"),
        "v2_left": result.get("v2_left"),
        "v2_missing": result.get("v2_new", {}).get("missing"),
        "v2_empty": result.get("v2_new", {}).get("empty"),
        "v2_gzip": result.get("v2_new", {}).get("gzip_bytes"),
        "v2_series_gzip_max": result.get("v2_new", {}).get("series_gzip_max", [])[:3],
        "v2_size_breaches": result.get("v2_new", {}).get("size_breaches"),
        "v1_gate": result.get("v1_licence_gate"),
        "v1_only_old": result.get("v1", {}).get("only_old"),
        "v1_other_diffs": result.get("v1", {}).get("other_differences"),
        "v1_only_new": result.get("v1", {}).get("only_new"),
        "v1_catalog_ok": result.get("v1", {}).get("catalog_equals_old_minus_unlicensed"),
        "v1_removed_rows": result.get("v1", {}).get("removed_rows_by_source"),
        "v1_verdict": result.get("v1", {}).get("verdict"),
        "error": result.get("error"),
        "scratch_removed": result.get("scratch_removed"),
    }
    real_annotate("notice" if status == 0 else "error", "dev: O37 publish trial", compact)
    return status


if __name__ == "__main__":
    sys.exit(main())
