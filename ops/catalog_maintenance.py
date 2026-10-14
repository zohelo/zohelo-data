#!/usr/bin/env python3
"""Show the R2 Data Catalog maintenance settings (compaction, snapshot expiration).

Table maintenance is a catalog setting the owner turns on in Cloudflare, not a script;
this confirms it is on and with which values (read-only).
"""
from __future__ import annotations

import requests

from ingestion.common.r2 import req
from ingestion.common.report import annotate

API = "https://api.cloudflare.com/client/v4/accounts/{account}/r2-catalog/{bucket}"


def main(argv: list[str] | None = None) -> int:
    base = API.format(account=req("CLOUDFLARE_ACCOUNT_ID"), bucket=req("R2_LAKEHOUSE_BUCKET"))
    headers = {"Authorization": f"Bearer {req('R2_DATA_CATALOG_TOKEN')}"}
    out, failed = {}, False
    for name, url in (("catalog", base), ("maintenance", f"{base}/maintenance-configs")):
        response = requests.get(url, headers=headers, timeout=30)
        body = response.json()
        out[name] = {"status": response.status_code, "result": body.get("result"), "errors": body.get("errors")}
        failed |= response.status_code != 200
    annotate("error" if failed else "notice", "catalog maintenance", out)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
