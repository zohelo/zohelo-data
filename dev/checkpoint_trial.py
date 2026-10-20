"""Trial of the per-dataset Eurostat loader (#875) on scratch tables in zz_dev_checkpoint.

1. Load the given datasets (Bronze registered from existing production files where they exist,
   decoded otherwise; Silver from the SQL model) into zz_dev_checkpoint.eurostat_observations
   (Bronze) and the Silver table of the same name.
2. Run again: nothing to do (no-op proof).
3. Remove one dataset from both tables with entity_tables.replace (empty file list) and run
   again: exactly that dataset is redone; the snapshots show whole-file operations only.
4. Compare per dataset: new Bronze rows = rows the old Bronze table holds for the same Landing
   file; Silver rows = Bronze rows.
Writes only to zz_dev_checkpoint (tables dropped without purge: Bronze may reference
production files, which stay) and the zz_dev_checkpoint/ prefix (deleted at the end).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import time

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.eurostat import entities

NS = "zz_dev_checkpoint"


def cleanup(cat, client) -> None:
    with contextlib.suppress(Exception):
        for tid in cat.list_tables(NS):
            cat.drop_table(tid)  # no purge: never deletes data files
    bucket = req("R2_LAKEHOUSE_BUCKET")
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{NS}/"):
        keys += [{"Key": o["Key"]} for o in page.get("Contents", [])]
    for i in range(0, len(keys), 1000):
        client.delete_objects(Bucket=bucket, Delete={"Objects": keys[i : i + 1000]})


def run(only: list[str], minutes: float) -> tuple[int, float]:
    started = time.monotonic()
    code = entities.main(["--trial", NS, "--only", *only, "--minutes", str(minutes)])
    return code, round((time.monotonic() - started) / 60, 1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("datasets", nargs="+")
    parser.add_argument("--minutes", type=float, default=300)
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    cat, client = catalog(), s3()
    cleanup(cat, client)
    out: dict = {"datasets": len(args.datasets)}
    out["run1"] = run(args.datasets, args.minutes)
    out["run2_noop"] = run(args.datasets, 30)
    bronze_id, silver_id = (NS, "bronze_eurostat_observations"), (NS, "silver_eurostat_observations")
    victim = sorted(args.datasets)[0]
    et.replace(cat, bronze_id, "dataset_id", {victim: []}, ["dataset_id"], "trial-remove")
    et.replace(cat, silver_id, "dataset_id", {victim: []}, ["dataset_id"], "trial-remove")
    out["run3_one_redone"] = run(args.datasets, 30)
    bronze = et.state(cat.load_table(bronze_id), "dataset_id")
    silver = et.state(cat.load_table(silver_id), "dataset_id")
    old = et.state(cat.load_table(("bronze", "eurostat_full_observations")), "dataset_id")
    compare = []
    for ds in sorted(args.datasets):
        b, s, o = bronze.get(ds, {}), silver.get(ds, {}), old.get(ds, {})
        same_file = b.get("recipes") and b["recipes"] <= o.get("recipes", set())
        compare.append(
            {
                "ds": ds,
                "bronze_rows": b.get("rows"),
                "silver_rows": s.get("rows"),
                "old_rows": o.get("rows") if same_file else None,
                "bronze_files": len(b.get("paths", [])),
                "silver_files": len(s.get("paths", [])),
                "ok": bool(b) and b.get("rows") == s.get("rows") and len(b["paths"]) == 1 == len(s["paths"]),
            }
        )
    out["all_ok"] = all(c["ok"] for c in compare)
    out["mismatch_vs_old"] = [c for c in compare if c["old_rows"] is not None and c["old_rows"] != c["bronze_rows"]]
    out["bronze_gb"] = round(sum(b["bytes"] for b in bronze.values()) / 1e9, 3)
    out["silver_gb"] = round(sum(s["bytes"] for s in silver.values()) / 1e9, 3)
    snaps = cat.load_table(bronze_id).metadata.snapshots[-3:]
    out["last_bronze_snapshots"] = [
        {k: v for k, v in s.summary.additional_properties.items() if "files" in k or "records" in k} for s in snaps
    ]
    annotate("notice" if out["all_ok"] else "error", "checkpoint trial", out)
    annotate("notice", "checkpoint trial per dataset", compare[:40])
    print(json.dumps(out, default=str))
    if not args.keep:
        cleanup(cat, client)
    return 0 if out["all_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
