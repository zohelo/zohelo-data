"""Dev trial for #2125: catalog tables under zz_dev_published/v1/, sizes, then delete the prefix."""

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from warehouse.publish import catalog_tables

PREFIX = "zz_dev_published/"


def listing(client, bucket: str, prefix: str) -> dict[str, int]:
    return {
        o["Key"]: o["Size"]
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for o in p.get("Contents", [])
    }


def main() -> int:
    client, bucket = s3(), req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    try:
        result = catalog_tables(catalog(), client, PREFIX + "v1/", "gold")
        result["sizes"] = listing(client, bucket, PREFIX + "v1/catalog/")
        annotate("notice", "catalog_tables", result)
    finally:
        keys = list(listing(client, bucket, PREFIX))
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True})
        annotate("notice", f"deleted {len(keys)}", {})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
