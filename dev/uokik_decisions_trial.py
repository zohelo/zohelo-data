"""Dev trial for #3318/#3319 (uokik_decisions; test branch only, never on main) on the PR's final code.

The decisions host refuses the runner, so this run shows what the source's own Landing code does about it: landing
inspect and landing extract (load) of ingestion/to_landing/uokik_decisions/extract.py into the scratch prefix
zz_dev_uokik/ of the Landing bucket, each twice; both must fail with a reported error and write nothing. Everything
under the scratch prefix is listed and removed in `finally`. Only decyzje.uokik.gov.pl is contacted.
"""

from __future__ import annotations

import json
import time

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.uokik_decisions import extract

SCRATCH = "zz_dev_uokik"


def objects(client, bucket: str) -> list[str]:
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def main() -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    result: dict = {"code": "feat/3318-uokik-decisions", "runs": []}
    try:
        for n in (1, 2):
            for mode in ("inspect", "load"):
                started = time.monotonic()
                code = extract.main(["--mode", mode, "--landing-root", SCRATCH, "--budget-minutes", "10"])
                result["runs"].append([n, mode, code, round(time.monotonic() - started), len(objects(client, bucket))])
        result["written"] = len(objects(client, bucket))
        result["expected"] = "exit 1 for every run (host refuses the runner), 0 objects written"
        ok = all(r[2] == 1 for r in result["runs"]) and result["written"] == 0
        result["verdict"] = "as expected" if ok else "UNEXPECTED"
    finally:
        left = objects(client, bucket)
        for key in left:
            client.delete_object(Bucket=bucket, Key=key)
        result["scratch_removed"] = len(left)
        result["scratch_left"] = len(objects(client, bucket))
        print("TRIAL", json.dumps(result), flush=True)
        annotate("notice", "uokik_decisions trial (final code)", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
