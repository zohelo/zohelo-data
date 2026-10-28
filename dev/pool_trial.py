"""Dev trial for #2183: do datasets that died with BrokenProcessPool in run 40674991884 load in a fresh run?

Runs the Eurostat entity loader with --trial (scratch namespace and prefix only; production Bronze
files are only read) for some of the failed datasets, then removes every scratch table and object.
Args: [--start-method fork|forkserver|spawn] [--workers N]
"""

from __future__ import annotations

import argparse
import contextlib

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.eurostat import entities

SCRATCH = "zz_dev_pool2183"
DATASETS = [
    "ENPS_ENV_WASTRT",
    "ENPE_BOP_IIP6_SERV",
    "ISS_18SOJOBN",
    "ENPE_ROAD_IF",
    "GBV_ANY_COB",
    "GBV_IPV_ED",
    "ENPS_FISH_AQ2A",
    "ENPE_ROAD_GO",
    "ENPS_GOV_10DD",
    "AVIA_TF_ACC",
    "AVIA_TF_ACA",
    "MIGR_ASYRESCRA",
]


def cleanup() -> dict:
    cat = catalog()
    dropped = []
    with contextlib.suppress(Exception):
        for table_id in cat.list_tables(SCRATCH):
            cat.purge_table(table_id)
            dropped.append(".".join(table_id))
        cat.drop_namespace(SCRATCH)
    client, bucket, deleted = s3(), req("STORAGE_LAKEHOUSE_BUCKET_NAME"), 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            deleted += len(keys)
    left = sum(
        len(p.get("Contents", []))
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/")
    )
    return {"tables_dropped": dropped, "objects_deleted": deleted, "objects_left": left}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-method", default="fork")
    parser.add_argument("--workers", default="2")
    args = parser.parse_args()
    code = 1
    try:
        code = entities.main(
            [
                "--trial",
                SCRATCH,
                "--only",
                *DATASETS,
                "--workers",
                args.workers,
                "--minutes",
                "60",
                "--start-method",
                args.start_method,
            ]
        )
    finally:
        annotate("notice", "pool trial cleanup", cleanup())
    return code


if __name__ == "__main__":
    raise SystemExit(main())
