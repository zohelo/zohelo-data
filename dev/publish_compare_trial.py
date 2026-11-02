"""Dev trial for #2224: gold.publish into zz_dev_published/v1/ with the read-back comparison, then delete
everything under zz_dev_published/."""

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from warehouse import publish

SCRATCH = "zz_dev_published/"


def main() -> int:
    """Publish to the scratch prefix, then remove it again (also after a failure)."""
    try:
        result = publish.main(prefix=SCRATCH + "v1/")
        annotate("notice", "publish compare trial", {"differences": result["differences"], "written": result["written"]})
    finally:
        client = s3()
        bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
        deleted = 0
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=SCRATCH):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                deleted += len(keys)
        annotate("notice", f"deleted {deleted}", {"prefix": SCRATCH})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
