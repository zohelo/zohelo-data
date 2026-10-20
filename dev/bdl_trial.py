"""Trial of the per-subgroup BDL loader (#875) on scratch tables in zz_dev_bdl.

Loads P1314, P1410, P3018 (the duplicate cases of the 20 Oct probe) plus a sample of other
subgroups, runs again (must be a no-op), then compares per subgroup: new Bronze rows = old
Bronze rows (bronze.bdl_subgroup_observations partition, metadata only); Silver rows and
rejections. Writes only to zz_dev_bdl (dropped at the end) and the zz_dev_bdl/ prefix.
"""

from __future__ import annotations

import argparse
import contextlib
from hashlib import sha256
import json

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gus_bdl import entities
from ingestion.to_bronze.gus_bdl.batch import candidates

NS = "zz_dev_bdl"


def cleanup(cat, client) -> None:
    with contextlib.suppress(Exception):
        for tid in cat.list_tables(NS):
            cat.drop_table(tid)
    bucket = req("R2_LAKEHOUSE_BUCKET")
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{NS}/"):
        keys += [{"Key": o["Key"]} for o in page.get("Contents", [])]
    for i in range(0, len(keys), 1000):
        client.delete_objects(Bucket=bucket, Delete={"Objects": keys[i : i + 1000]})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=60)
    args = parser.parse_args()
    cat, client = catalog(), s3()
    cleanup(cat, client)
    subgroups = sorted({p["subgroup_id"] for p in candidates(client, req("R2_LANDING_BUCKET"))})
    chosen = ["P1314", "P1410", "P3018"] + sorted(subgroups, key=lambda s: sha256(s.encode()).hexdigest())[
        : args.sample
    ]
    out: dict = {"subgroups": len(chosen)}
    out["run1"] = entities.main(["--trial", NS, "--only", *chosen, "--minutes", "200"])
    out["run2_noop"] = entities.main(["--trial", NS, "--only", *chosen, "--minutes", "30"])
    new_b = et.state(cat.load_table((NS, "bronze_bdl_observations")), "subgroup_id")
    new_s = et.state(cat.load_table((NS, "silver_bdl_observations")), "subgroup_id")
    old_b = et.state(cat.load_table(("bronze", "bdl_subgroup_observations")), "subgroup_id")
    rows = []
    for sg in chosen:
        rows.append(
            {
                "sg": sg,
                "new_bronze": new_b.get(sg, {}).get("rows"),
                "old_bronze": old_b.get(sg, {}).get("rows"),
                "silver": new_s.get(sg, {}).get("rows"),
                "files": [len(new_b.get(sg, {}).get("paths", [])), len(new_s.get(sg, {}).get("paths", []))],
            }
        )
    loaded = [r for r in rows if r["new_bronze"] is not None]
    out["loaded"] = len(loaded)
    out["bronze_rows_equal_old"] = sum(1 for r in loaded if r["new_bronze"] == r["old_bronze"])
    out["bronze_rows_differ"] = [r for r in loaded if r["new_bronze"] != r["old_bronze"]][:10]
    out["silver_rows_removed_as_overlap"] = sum((r["new_bronze"] or 0) - (r["silver"] or 0) for r in loaded)
    out["not_loaded"] = [r["sg"] for r in rows if r["new_bronze"] is None]
    out["one_file_each"] = all(r["files"] == [1, 1] for r in loaded)
    out["the_three"] = [r for r in rows if r["sg"] in ("P1314", "P1410", "P3018")]
    annotate("notice", "bdl trial", out)
    print(json.dumps(out, default=str))
    cleanup(cat, client)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
