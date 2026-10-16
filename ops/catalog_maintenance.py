#!/usr/bin/env python3
"""Show, and with --apply set, the R2 Data Catalog maintenance settings.

Table maintenance (compaction, snapshot expiration) is a catalog setting, not a script.
Without arguments this only reads and annotates the settings. With --apply it sets
snapshot expiration to the owner's data policy (#822, #820): every table keeps only its
current snapshot; older snapshots are expired once they are older than one hour (the
grace for queries running during a commit). The catalog's maintenance job runs hourly,
so a replaced snapshot is gone within about two hours of the write. Files the expired
snapshots leave behind are removed by `ops/orphan_files --delete`.
"""

from __future__ import annotations

import argparse

import requests

from ingestion.common.r2 import req
from ingestion.common.report import annotate

API = "https://api.cloudflare.com/client/v4/accounts/{account}/r2-catalog/{bucket}"

# Owner data policy #822: no table history in the lakehouse (the API's minimum is 1).
SNAPSHOT_POLICY = {"state": "enabled", "min_snapshots_to_keep": 1, "max_snapshot_age": "1h"}


def call(method: str, url: str, headers: dict, body: dict | None = None) -> dict:
    """One API call as {status, result, errors}; network errors and non-JSON bodies are reported, not raised."""
    try:
        response = requests.request(method, url, headers=headers, json=body, timeout=30)
    except requests.RequestException as exc:
        return {"status": None, "result": None, "errors": [str(exc)[:300]]}
    try:
        payload = response.json()
    except ValueError:
        payload = {"errors": [response.text[:300]]}
    return {"status": response.status_code, "result": payload.get("result"), "errors": payload.get("errors")}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="set snapshot expiration to the #822 policy")
    args = parser.parse_args(argv)

    base = API.format(account=req("CLOUDFLARE_ACCOUNT_ID"), bucket=req("R2_LAKEHOUSE_BUCKET"))
    headers = {"Authorization": f"Bearer {req('R2_DATA_CATALOG_TOKEN')}"}
    out: dict = {}
    if args.apply:
        out["update"] = call("POST", f"{base}/maintenance-configs", headers, {"snapshot_expiration": SNAPSHOT_POLICY})
    out["catalog"] = call("GET", base, headers)
    out["maintenance"] = call("GET", f"{base}/maintenance-configs", headers)
    failed = any(part["status"] != 200 for part in out.values())
    if args.apply and not failed:
        config = (out["maintenance"]["result"] or {}).get("maintenance_config", {})
        current = config.get("snapshot_expiration", {})
        mismatch = {key: current.get(key) for key, value in SNAPSHOT_POLICY.items() if current.get(key) != value}
        if mismatch:
            out["policy_mismatch"] = mismatch
            failed = True
    annotate("error" if failed else "notice", "catalog maintenance", out)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
