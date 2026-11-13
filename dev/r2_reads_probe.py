"""Read-only (#2873 item 3a): does the catalog token read Cloudflare's R2 analytics (GraphQL), and if so,
R2 operations by action, bucket, day and hour. No R2 object request is made; nothing is written."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
import os
import sys

import requests

from ingestion.common.r2 import req
from ingestion.common.report import annotate

URL = "https://api.cloudflare.com/client/v4/graphql"
CLASS_B = {
    "GetObject", "HeadObject", "HeadBucket", "UsageSummary", "GetBucketEncryption", "GetBucketLocation",
    "GetBucketCors", "GetBucketLifecycleConfiguration",
}  # fmt: skip


def gql(query: str, variables: dict | None = None) -> tuple[int, dict]:
    headers = {"Authorization": f"Bearer {req('STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN')}"}
    r = requests.post(URL, headers=headers, json={"query": query, "variables": variables or {}}, timeout=120)
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, {"text": r.text[:500]}


def groups(dims: str, where: dict, limit: int = 10000) -> list[dict] | str:
    query = (
        "query($a:String!,$f:AccountR2OperationsAdaptiveGroupsFilter_InputObject){viewer{accounts(filter:{accountTag:$a}){"
        f"r2OperationsAdaptiveGroups(limit:{limit},filter:$f){{sum{{requests}} dimensions{{{dims}}}}}}}}}}}"
    )
    status, body = gql(query, {"a": req("PLATFORM_ACCOUNT_ID"), "f": where})
    if status != 200 or body.get("errors") or not body.get("data"):
        return f"{status} {str(body.get('errors') or body)[:600]}"
    accounts = body["data"]["viewer"]["accounts"]
    return accounts[0]["r2OperationsAdaptiveGroups"] if accounts else "no account in the answer"


def main() -> None:
    now = datetime.now(UTC)
    status, body = gql('{__type(name:"AccountR2OperationsAdaptiveGroupsDimensions"){fields{name}}}')
    fields = [f["name"] for f in ((body.get("data") or {}).get("__type") or {}).get("fields") or []]
    annotate("notice", "1 analytics schema", {"status": status, "dimensions": fields, "errors": str(body.get("errors"))[:500]})

    start = (now - timedelta(days=31)).strftime("%Y-%m-%d")
    by_day = groups("date actionType bucketName", {"date_geq": start})
    if isinstance(by_day, str):
        annotate("error", "2 analytics refused", {"answer": by_day})
        sys.exit(1)
    total: dict[str, int] = defaultdict(int)
    per_bucket: dict[str, int] = defaultdict(int)
    days: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for g in by_day:
        d, n = g["dimensions"], g["sum"]["requests"]
        total[d["actionType"]] += n
        cls = "B" if d["actionType"] in CLASS_B else "A_or_free"
        per_bucket[f"{d['bucketName']} {cls}"] += n
        days[d["date"]][0 if cls == "B" else 1] += n
    annotate(
        "notice",
        "2 operations 31 days",
        {
            "from": start,
            "rows": len(by_day),
            "by_action": dict(sorted(total.items(), key=lambda kv: -kv[1])[:25]),
            "by_bucket_class": dict(sorted(per_bucket.items(), key=lambda kv: -kv[1])),
        },
    )
    annotate("notice", "3 per day [class B, other]", dict(sorted(days.items())))

    since = (now - timedelta(hours=96)).strftime("%Y-%m-%dT%H:00:00Z")
    hourly = groups("datetimeHour actionType bucketName", {"datetime_geq": since})
    if isinstance(hourly, str):
        annotate("warning", "4 hourly refused", {"answer": hourly})
        return
    hours: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for g in hourly:
        d = g["dimensions"]
        if d["actionType"] in ("GetObject", "HeadObject"):
            key = ("L" if "lake" in d["bucketName"] else "D") + ("g" if d["actionType"] == "GetObject" else "h")
            hours[d["datetimeHour"][5:13]][key] += g["sum"]["requests"]
    items = sorted(hours.items())
    half = (len(items) + 1) // 2
    legend = "Lg/Lh = lakehouse bucket GetObject/HeadObject, Dg/Dh = other bucket"
    annotate("notice", "4 hourly reads, older half", {"legend": legend, **{k: dict(v) for k, v in items[:half]}})
    annotate("notice", "5 hourly reads, newer half", {k: dict(v) for k, v in items[half:]})
    buckets = sorted({g["dimensions"]["bucketName"] for g in hourly})
    annotate("notice", "6 buckets", {"buckets": buckets, "lakehouse": os.environ.get("STORAGE_LAKEHOUSE_BUCKET_NAME", "") in buckets})


if __name__ == "__main__":
    main()
