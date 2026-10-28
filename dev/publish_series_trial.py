"""Dev trial for #2124: gold.publish main() under zz_dev_published/v1/, read fx_rate's series, delete the prefix."""

import io

import pyarrow.parquet as pq

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate
from warehouse.publish import main as publish

PREFIX = "zz_dev_published/"


def main() -> int:
    client, bucket = s3(), req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    try:
        publish(prefix=PREFIX + "v1/")
        key = PREFIX + "v1/kpi/fx_rate/series.parquet"
        table = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket, Key=key)["Body"].read()))
        sizes = {
            o["Key"].removeprefix(PREFIX + "v1/"): o["Size"]
            for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX)
            for o in p.get("Contents", [])
        }
        annotate("notice", "fx_rate series", {"columns": table.column_names, "rows": table.num_rows, "sizes": sizes})
    finally:
        keys = [
            o["Key"]
            for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX)
            for o in p.get("Contents", [])
        ]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True})
        annotate("notice", f"deleted {len(keys)}", {})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
