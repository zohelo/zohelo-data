"""Read-only: what the BDL Landing receipts and files of the duplicate subgroups look like (#875).

For P1314, P1410, P3018 (and one clean subgroup for contrast): every receipt (without bytes),
then for the newest version the CSV header, row count, rows with empty year/value, and the
first duplicate keys within and across parts. Writes nothing.
"""

from __future__ import annotations

from collections import Counter
import csv
import io
import json
import zipfile

from ingestion.common.r2 import get_with_backoff, req, s3
from ingestion.common.report import annotate

DATA = "01_landing/gus_bdl/snapshot/data/"


def main() -> int:
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    for subgroup in ("P1314", "P1410", "P3018", "P1001"):
        receipts = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{DATA}{subgroup}/"):
            for o in page.get("Contents", []):
                if o["Key"].endswith(".receipt.json"):
                    r = json.loads(get_with_backoff(client, bucket, o["Key"]))
                    receipts.append(
                        {
                            k: r.get(k)
                            for k in (
                                "version_token",
                                "ingested_at",
                                "part",
                                "size_bytes",
                                "sha256",
                                "run_id",
                                "request",
                            )
                        }
                        | {"key": o["Key"][len(DATA) :]}
                    )
        annotate("notice", f"bdl receipts {subgroup}", receipts[:30])
        if not receipts:
            continue
        newest = max(r["version_token"] or "" for r in receipts)
        parts = [r for r in receipts if r["version_token"] == newest]
        keys: Counter = Counter()
        info = []
        for r in sorted(parts, key=lambda r: (r["ingested_at"], json.dumps(r["part"]))):
            data_key = r["key"].replace(".receipt.json", "")
            listing = client.list_objects_v2(Bucket=bucket, Prefix=DATA + data_key)
            found = [o["Key"] for o in listing.get("Contents", []) if not o["Key"].endswith(".receipt.json")]
            if not found:
                info.append({"part": r["part"], "ingested": r["ingested_at"], "data": "moved or missing"})
                continue
            body = get_with_backoff(client, bucket, found[0])
            z = zipfile.ZipFile(io.BytesIO(body))
            member = next(m for m in z.namelist() if m.lower().endswith(".csv"))
            rows = list(csv.reader(io.TextIOWrapper(z.open(member), encoding="utf-8-sig"), delimiter=";"))
            header, body_rows = rows[0], rows[1:]
            n = len(header) - (1 if header[-1] == "" else 0)
            year_i, value_i = n - 4, n - 3
            local = Counter(tuple(row[:year_i] + [row[year_i]]) for row in body_rows)
            info.append(
                {
                    "part": r["part"],
                    "ingested": r["ingested_at"],
                    "header": header[:12],
                    "rows": len(body_rows),
                    "row_lengths": dict(Counter(len(x) for x in body_rows).most_common(4)),
                    "empty_year": sum(1 for x in body_rows if len(x) > year_i and not x[year_i].strip()),
                    "empty_value": sum(1 for x in body_rows if len(x) > value_i and not x[value_i].strip()),
                    "dup_keys_in_part": sum(1 for v in local.values() if v > 1),
                    "dup_sample": [list(k) for k, v in local.items() if v > 1][:3],
                    "years": sorted({x[year_i] for x in body_rows if len(x) > year_i})[:3]
                    + ["…"]
                    + sorted({x[year_i] for x in body_rows if len(x) > year_i})[-3:],
                }
            )
            for k in local:
                keys[k] += 1
        annotate(
            "notice",
            f"bdl newest version {subgroup} ({newest})",
            {"parts": info, "keys_in_several_parts": sum(1 for v in keys.values() if v > 1)},
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
