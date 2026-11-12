"""Companies House Landing activities: the shared bulk_file pattern, with one request saved.

The monthly file is 0.5 GB and its name carries the snapshot date, so a file that Landing already holds is
recognised without downloading it: the listing page links the same file name, and the file's HEAD answer has the
same Last-Modified and Content-Length as the newest receipt. Then ``landing.extract`` writes nothing and downloads
nothing (a rerun in the month the file was landed). In every other case the shared pattern downloads the file and
lands it unless its SHA-256 is unchanged.

landing.inspect  bulk_file inspect (missing / stale / current); declared by extraction.kind, no code here
landing.extract  held file check (listing page + HEAD), else bulk_file load
landing.verify   bulk_file verify; declared by extraction.kind, no code here
"""

from __future__ import annotations

from typing import Any

SOURCE = "companies_house"


def held_file(spec: dict[str, Any], receipts: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The newest receipt's file when the listing page still links it unchanged (name, Last-Modified, size);
    None when it must be downloaded (nothing landed, another name, another validator or an unreadable answer)."""
    from ingestion.to_landing.patterns import bulk_file

    if not receipts or receipts[-1].get("version_basis") != "Last-Modified":
        return None
    newest = receipts[-1]
    url = bulk_file.resolve_url(spec)
    if url != newest.get("source_url"):
        return None
    head, http = bulk_file.probe(url, spec.get("probe", "head"))
    if head is None or http != "200":
        return None
    headers = dict(head.headers)
    if bulk_file.server_version(headers) != newest["version_token"]:
        return None
    if str(headers.get("Content-Length") or "") != str(newest["size_bytes"]):
        return None
    return {"url": url, "version": newest["version_token"], "sha256": newest["sha256"]}


def extract(landing_root: str = "01_landing") -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    (spec,) = bulk_file.declaration(SOURCE)
    receipts = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"], landing_root))
    held = held_file(spec, receipts)
    if held is None:
        return bulk_file.main(SOURCE, "load", landing_root=landing_root)
    summary = {"source": SOURCE, "mode": "load", "files": [{"entity": spec["entity"], "outcome": "held", **held}]}
    annotate("notice", f"{SOURCE} landing load", summary)
    return 0


def _extract(p) -> int:
    return extract()


ACTIVITIES = {"landing.extract": _extract}
