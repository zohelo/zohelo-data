"""Dev trial for #2378 (World Bank IDS) (not for main): inspect, load and verify the world_bank_ids
declaration into the zz_dev_world_bank_ids/ Landing prefix, profile the landed ZIP, then delete
every object under the prefix."""

from __future__ import annotations

import csv
import io
import zipfile

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

SOURCE = "world_bank_ids"
ROOT = f"zz_dev_{SOURCE}"


def objects(client, bucket: str) -> list[dict]:
    found = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{ROOT}/"):
        found += page.get("Contents", [])
    return found


def profile(body: bytes) -> None:
    """Annotate each CSV member: size, columns, rows, distinct values of the first columns, year range."""
    book = zipfile.ZipFile(io.BytesIO(body))
    for info in book.infolist():
        with book.open(info) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
            header = next(reader, [])
            rows, distinct, empty = 0, [set() for _ in header[:4]], 0
            years = [h for h in header if h.strip()[:4].isdigit()]
            for row in reader:
                rows += 1
                for i, s in enumerate(distinct):
                    if i < len(row) and len(s) < 5000:
                        s.add(row[i])
                empty += sum(1 for v in row[len(header) - len(years):] if not v.strip()) if years else 0
        annotate("notice", f"{SOURCE} profile {info.filename}", {
            "bytes": info.file_size, "columns": len(header), "first_columns": header[:6],
            "year_columns": [years[0], years[-1], len(years)] if years else None, "rows": rows,
            "distinct_first4": [len(s) for s in distinct],
            "empty_year_cells_pct": round(100 * empty / (rows * len(years)), 1) if years and rows else None,
        })


def main() -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    codes, left = {}, -1
    try:
        for mode in ("inspect", "load", "verify"):
            codes[mode] = bulk_file.main(SOURCE, mode, landing_root=ROOT)
        landed = [{"key": o["Key"], "bytes": o["Size"]} for o in objects(client, bucket)
                  if not o["Key"].endswith(".receipt.json")]
        annotate("notice", f"{SOURCE} trial landed", {"codes": codes, "files": landed})
        for item in landed:
            profile(client.get_object(Bucket=bucket, Key=item["key"])["Body"].read())
    finally:
        keys = [o["Key"] for o in objects(client, bucket)]
        for start in range(0, len(keys), 1000):
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[start : start + 1000]]})
        left = len(objects(client, bucket))
        annotate("notice" if left == 0 else "error", f"{SOURCE} trial clean-up", {"deleted": len(keys), "left": left})
    return 0 if codes and all(c == 0 for c in codes.values()) and left == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
