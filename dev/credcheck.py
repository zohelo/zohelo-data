"""One-off: the zohelo organisation credentials work for every production use of this repo.

Writes only scratch: object zz_dev_credcheck/probe.txt in both buckets and namespace
zz_dev_credcheck in the catalog, all removed again. Prints statuses, never a value.
"""

from __future__ import annotations

import pyarrow as pa
import requests

from ingestion.common.r2 import catalog, req, s3, sql
from ingestion.common.report import annotate

NS = "zz_dev_credcheck"
KEY = "zz_dev_credcheck/probe.txt"


def check(name: str, fn) -> None:
    try:
        annotate("notice", name, f"ok {fn() or ''}")
    except Exception as exc:  # report every check, then fail at the end
        annotate("error", name, f"{type(exc).__name__}: {str(exc)[:300]}")
        FAILED.append(name)


FAILED: list[str] = []


def storage(bucket: str) -> str:
    client = s3()
    client.put_object(Bucket=bucket, Key=KEY, Body=b"check")
    client.delete_object(Bucket=bucket, Key=KEY)
    return "put + delete"


def iceberg() -> str:
    cat = catalog()
    cat.create_namespace_if_not_exists(NS)
    ident = (NS, "probe")
    if cat.table_exists(ident):
        cat.drop_table(ident)
    data = pa.table({"id": pa.array([1], pa.int64())})
    table = cat.create_table(ident, schema=data.schema)
    table.append(data)
    rows = table.scan().to_arrow().num_rows
    cat.purge_table(ident)
    cat.drop_namespace(NS)
    return f"create, append, read {rows} row, purge"


def r2_sql() -> str:
    ns, name = catalog().list_tables("bronze")[0]
    rows = sql(f"SELECT * FROM {ns}.{name} LIMIT 1")
    return f"{len(rows)} row from {ns}.{name}"


def bdl_api() -> str:
    url = "https://bdl.stat.gov.pl/api/v1/subjects?format=json&page-size=1"
    keyed = requests.get(url, headers={"X-ClientId": req("GUS_BDL_API_KEY")}, timeout=60)
    anonymous = requests.get(url, timeout=60)

    def limits(r: requests.Response) -> str:
        return ";".join(f"{k}={v}" for k, v in r.headers.items() if k.lower().startswith("x-rate-limit"))

    keyed.raise_for_status()
    return f"keyed [{limits(keyed)}] anonymous [{limits(anonymous)}]"


def bdl_web() -> str:
    from ingestion.to_landing.gus_bdl.web_export import ensure_browser, login

    ensure_browser()
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(locale="pl-PL").new_page()
        login(page, req("GUS_BDL_WEB_EMAIL"), req("GUS_BDL_WEB_PASSWORD"))
        browser.close()
    return "signed in"


def main() -> None:
    check("storage write: landing bucket", lambda: storage(req("R2_LANDING_BUCKET")))
    check("storage write: lakehouse bucket", lambda: storage(req("R2_LAKEHOUSE_BUCKET")))
    check("catalog write token: Iceberg table", iceberg)
    check("catalog write token: R2 SQL", r2_sql)
    check("GUS BDL API key", bdl_api)
    check("GUS BDL website login", bdl_web)
    if FAILED:
        raise SystemExit(f"failed: {', '.join(FAILED)}")


if __name__ == "__main__":
    main()
