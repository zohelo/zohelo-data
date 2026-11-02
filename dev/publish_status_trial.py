"""Dev trial for #2324: build status.json as gold.publish main() does and put it to a scratch prefix."""

from datetime import UTC, datetime
import json

import run
from ingestion.common import scheduler
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from warehouse import publish

PREFIX = "zz_dev_published/v1/"


def main() -> None:
    client = s3()
    key = PREFIX + "status.json"
    try:
        generated_at = publish.iso(datetime.now(UTC))
        checked_at = publish.checked(publish.gold(catalog(), "gold", "coverage"))
        next_at = scheduler.next_runs(run.load_config(), datetime.now(UTC))
        status = publish.schema_checked(
            publish.status_json(publish.seed("source"), checked_at, next_at, generated_at), "status"
        )
        body = json.dumps(status, ensure_ascii=False, separators=(",", ":")).encode()
        if len(body) > publish.STATUS_LIMIT:
            raise ValueError(f"status.json is {len(body)} bytes")
        publish.put(client, key, body)
        back = json.loads(publish.get(client, key))
        annotate("notice", "status.json", {"bytes": len(body), "sources": len(back["sources"]), "rows": back["sources"]})
    finally:
        client.delete_object(Bucket=req("STORAGE_LAKEHOUSE_BUCKET_NAME"), Key=key)
        annotate("notice", "deleted 1", {"key": key})


if __name__ == "__main__":
    main()
