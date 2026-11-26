"""GIS sanitary report: Landing activities (register PLS-018).

Every annual report on https://www.gov.pl/web/gis/raport---stan-sanitarny-kraju is one bulk_file entity of the
seed list in ingestion/sources/gis_sanitary.yaml (``ssk_<year>`` / ``pis_<year>``: the PDF attachment address read
on that page). Landing, receipts and versions are the shared bulk_file pattern's.

landing.inspect  seed list vs Landing (bulk_file inspect), plus the page check below; no writes
landing.extract  the files that are missing or whose Last-Modified (read case-insensitively from the headers of a
                 streamed GET whose body is not read) is newer than the newest landed version, 5 s apart: the
                 reports are 4 to 38 MB and almost never replaced; the bulk_file load then skips unchanged bytes;
                 plus the page check. The landed version is the download time when the shared pattern finds no
                 Last-Modified (the 26 Nov trial), which a replaced file's Last-Modified still exceeds
landing.verify   every receipt has its file (bulk_file verify)

Page check: a new edition gets a new attachment address on the same page, so the page's attachment links are
compared with the seed list and ``extraction.excluded`` (the editions left out on purpose). A link in neither is a
warning annotation "attachment on the report page not in the seed list" with the link text; the run does not fail
on it (the landed editions stay correct) and the warning repeats until a seed row or an exclusion is added.
"""

from __future__ import annotations

import html
import re
import time

SOURCE = "gis_sanitary"
PAGE = "https://www.gov.pl/web/gis/raport---stan-sanitarny-kraju"
LINK = re.compile(r'(?is)<a\b[^>]*href="(?:https://www\.gov\.pl)?(/attachment/[0-9a-f-]{36})"[^>]*>(.*?)</a>')
PAUSE_SECONDS = 5.0


def attachments(page_html: str) -> dict[str, str]:
    """Attachment address -> link text (tags and whitespace removed), in page order."""
    out: dict[str, str] = {}
    for href, text in LINK.findall(page_html):
        label = " ".join(html.unescape(re.sub(r"(?s)<[^>]+>", " ", text)).replace("​", "").split())
        out.setdefault("https://www.gov.pl" + href, label)
    return out


def unknown_attachments(page_html: str, seeded: set[str], excluded: set[str]) -> dict[str, str]:
    return {url: text for url, text in attachments(page_html).items() if url not in seeded | excluded}


def check_page() -> dict[str, str]:
    """Annotate a warning for attachments on the page that are neither seeded nor excluded; returns them."""
    import requests
    import yaml

    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    response = requests.get(PAGE, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(30, 60))
    response.raise_for_status()
    config = yaml.safe_load((bulk_file.ROOT / "ingestion" / "sources" / f"{SOURCE}.yaml").read_text())
    excluded = {str(item["id"]) for item in config["extraction"].get("excluded") or []}
    seeded = {spec["url"] for spec in bulk_file.declaration(SOURCE)}
    unknown = unknown_attachments(response.text, seeded, excluded)
    if unknown:
        annotate(
            "warning",
            f"{SOURCE}: attachment on the report page not in the seed list",
            {
                "attachments": unknown,
                "action": "add a seed row (entity ssk_<year> or pis_<year>) to ingestion/sources/gis_sanitary.yaml, "
                "or an extraction.excluded entry with the reason",
                "page": PAGE,
            },
        )
    return unknown


def changed_specs(landing_root: str = "01_landing") -> list[dict]:
    """Seed files missing from Landing, or whose server Last-Modified is newer than the newest landed version."""
    from ingestion.common.r2 import req, s3
    from ingestion.to_landing.patterns import bulk_file

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out = []
    for n, spec in enumerate(bulk_file.declaration(SOURCE)):
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"], landing_root))
        if not held:
            out.append(spec)
            continue
        if n:
            time.sleep(PAUSE_SECONDS)
        head, http = bulk_file.probe(spec["url"], "get")  # headers only; HEAD gave no usable Last-Modified
        modified = head.headers.get("Last-Modified") if head is not None and http == "200" else None
        version = bulk_file.server_version({"Last-Modified": modified}) if modified else None
        if version is None or version > held[-1]["version_token"]:
            out.append(spec)  # no validator or a newer file: the load compares the SHA-256
    return out


def extract(landing_root: str = "01_landing") -> int:
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "load", landing_root, specs=changed_specs(landing_root), pause_seconds=PAUSE_SECONDS)


def _run(mode: str) -> int:
    from ingestion.to_landing.patterns import bulk_file

    status = extract() if mode == "load" else bulk_file.main(SOURCE, mode)
    if mode != "verify":
        check_page()
    return status


ACTIVITIES = {
    "landing.inspect": lambda p: _run("inspect"),
    "landing.extract": lambda p: _run("load"),
    "landing.verify": lambda p: _run("verify"),
}
