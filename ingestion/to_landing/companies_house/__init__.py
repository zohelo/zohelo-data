"""Companies House Landing activities: the shared bulk_file pattern, reduced before Landing, one request saved.

Personal data (register CD-014: no natural persons' data; precedent O33): the monthly file has 55 columns, among
them the registered office's care-of line, PO box, street lines, county and postcode and up to ten previous names.
A registered office can be a director's home, a care-of line or a former name a person's. ``reduce_snapshot``
(``reduce: companies_house`` in the declaration) keeps the 13 columns the platform uses and drops the rest before
the copy is landed, so those columns are never stored: not in Landing, the Archive or Bronze. A file without one
of the kept columns, or with a row of another width, is not landed (the run fails and says why).

The file is 0.5 GB and its name carries the snapshot date, so a file that Landing already holds is recognised
without downloading it: the listing page links the same file name, and the file's HEAD answer has the same
Last-Modified and ETag as the newest receipt. Then ``landing.extract`` writes nothing and downloads nothing (a
rerun in the month the file was landed). In every other case the shared pattern downloads the file, reduces it and
lands it unless the reduced bytes (SHA-256) are unchanged.

landing.inspect  bulk_file inspect (missing / stale / current); declared by extraction.kind, no code here
landing.extract  held file check (listing page + HEAD), else bulk_file load (download, reduce, land)
landing.verify   bulk_file verify; declared by extraction.kind, no code here
"""

from __future__ import annotations

import csv
import io
from typing import Any
import zipfile

SOURCE = "companies_house"
COLUMNS = (  # the columns landed, in the file's order; every other column is dropped before Landing
    "CompanyName",
    "CompanyNumber",
    "RegAddress.PostTown",
    "RegAddress.Country",
    "CompanyCategory",
    "CompanyStatus",
    "CountryOfOrigin",
    "DissolutionDate",
    "IncorporationDate",
    "SICCode.SicText_1",
    "SICCode.SicText_2",
    "SICCode.SicText_3",
    "SICCode.SicText_4",
)


def reduce_snapshot(body: bytes) -> bytes:
    """The snapshot ZIP with only ``COLUMNS`` of its one CSV (header names stripped: some follow a space), as a
    ZIP with one CSV of the same name. The same input gives the same bytes (fixed member date, fixed compression).
    Raises when the ZIP does not hold exactly one CSV, a kept column is missing or repeated, or a row has another
    number of fields than the header: such a file cannot be reduced safely and is not landed."""
    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        members = [m for m in archive.infolist() if m.filename.lower().endswith(".csv")]
        if len(members) != 1 or len(archive.infolist()) != 1:
            raise RuntimeError(f"{SOURCE}: expected one CSV in the ZIP, found {archive.namelist()[:5]}")
        info = zipfile.ZipInfo(members[0].filename.rsplit("/", 1)[-1], date_time=(1980, 1, 1, 0, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        with (
            archive.open(members[0]) as raw,
            zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as reduced,
            reduced.open(info, "w", force_zip64=True) as target,
        ):
            rows = csv.reader(io.TextIOWrapper(raw, encoding="utf-8", newline=""))
            text = io.TextIOWrapper(target, encoding="utf-8", newline="", write_through=False)
            writer = csv.writer(text, lineterminator="\n")
            header = [name.strip() for name in next(rows, [])]
            wrong = [c for c in COLUMNS if header.count(c) != 1]
            if wrong:
                raise RuntimeError(f"{SOURCE}: the CSV header lacks or repeats {wrong}; nothing is landed")
            keep, width = [header.index(c) for c in COLUMNS], len(header)
            writer.writerow(COLUMNS)
            for number, row in enumerate(rows, start=2):
                if len(row) != width:
                    raise RuntimeError(
                        f"{SOURCE}: CSV line {number} has {len(row)} fields, the header {width}; nothing is landed"
                    )
                writer.writerow([row[i] for i in keep])
            text.flush()
            text.detach()
    return out.getvalue()


def held_file(spec: dict[str, Any], receipts: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The newest receipt's file when the listing page still links it unchanged (name, Last-Modified, ETag);
    None when it must be downloaded (nothing landed, another name, another or no validator, an unreadable
    answer, or a copy landed without the reduction)."""
    from ingestion.to_landing.patterns import bulk_file

    if not receipts or receipts[-1].get("version_basis") != "Last-Modified":
        return None
    newest = receipts[-1]
    detail = newest.get("detail") or {}
    etag = (detail.get("http") or {}).get("etag")
    if detail.get("reduced") != spec.get("reduce") or not etag:
        return None
    url = bulk_file.resolve_url(spec)
    if url != newest.get("source_url"):
        return None
    head, http = bulk_file.probe(url, spec.get("probe", "head"))
    if head is None or http != "200":
        return None
    if bulk_file.server_version(dict(head.headers)) != newest["version_token"]:
        return None
    if head.headers.get("ETag") != etag:
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
