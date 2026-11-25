"""Shared pieces of an own Landing extractor: the identifying User-Agent and a never-overwrite write.

docs/landing.md: Landing keeps the exact source bytes and is never overwritten. ``put_new`` writes one object and
refuses a key that already holds other bytes (the same bytes are accepted, so a repeated run is a no-op), then
checks the write. Same behaviour as the copy in ingestion/to_landing/world_bank_icp/extract.py, which world_bank_icp
and world_bank_gem still import (moving them is their own change).
"""

from __future__ import annotations

from hashlib import sha256

USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def put_new(client, bucket: str, key: str, body: bytes, content_type: str) -> None:
    """Never overwrite Landing: a key that exists is an error unless the bytes are equal."""
    digest = sha256(body).hexdigest()
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        if (head.get("Metadata") or {}).get("sha256") == digest:
            return
        raise RuntimeError(f"Landing key exists with different bytes: {key}")
    except client.exceptions.ClientError as exc:  # only "not found" means free to write
        if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
            raise
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, Metadata={"sha256": digest})
    if (client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")
