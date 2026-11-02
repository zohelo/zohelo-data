"""Dev trial for #2345 (not for main): inspect, load and verify the mf_treasury_debt declaration into the
zz_dev_mf_treasury_debt/ Landing prefix, report the landed files, then delete every object under it."""

from __future__ import annotations

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file

ROOT = "zz_dev_mf_treasury_debt"


def objects(client, bucket: str) -> list[dict]:
    found = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{ROOT}/"):
        found += page.get("Contents", [])
    return found


def main() -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    codes = {}
    spec = bulk_file.declaration("mf_treasury_debt")[0]
    heads = []
    for link in bulk_file.page_links(spec):
        head, http = bulk_file.probe(link)
        h = dict(head.headers) if head is not None else {}
        heads.append(
            {
                "url": link,
                "http": http,
                "type": h.get("Content-Type"),
                "disposition": h.get("Content-Disposition"),
                "bytes": h.get("Content-Length"),
            }
        )
    annotate("notice", "mf_treasury_debt page links", {"links": heads})
    try:
        for mode in ("inspect", "load", "verify"):
            codes[mode] = bulk_file.main("mf_treasury_debt", mode, landing_root=ROOT)
        landed = [
            {"key": o["Key"], "bytes": o["Size"]}
            for o in objects(client, bucket)
            if not o["Key"].endswith(".receipt.json")
        ]
        annotate("notice", "mf_treasury_debt trial landed", {"codes": codes, "entities": len(landed), "files": landed})
    finally:
        keys = [o["Key"] for o in objects(client, bucket)]
        for start in range(0, len(keys), 1000):
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[start : start + 1000]]})
        left = len(objects(client, bucket))
        annotate(
            "notice" if left == 0 else "error", "mf_treasury_debt trial clean-up", {"deleted": len(keys), "left": left}
        )
    return 0 if codes and all(c == 0 for c in codes.values()) and left == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
