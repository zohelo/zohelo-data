#!/usr/bin/env python3
"""News feeds bronze.load: every landed copy of every feed → bronze.news_items (headlines and links only).

One row per RSS ``<item>`` or Atom ``<entry>``. Kept: title, link, guid, the outlet (from the feed's
declaration in ingestion/sources/news_feeds.yaml), the feed's language and the publication time, as source
text and parsed to UTC. Never kept: descriptions, article text, categories, authors, enclosures or images
(docs/topics.md, "Links, not content"). A feed that is neither RSS 2.0 nor Atom, or an item without a title
or link, rejects the file (bronze.news_feeds_rejected_files). Every copy not yet held is loaded
(``all_versions``): items roll over between copies and Silver deduplicates. Shared code:
ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing.patterns import bulk_file

SOURCE = "news_feeds"
TABLE_ID = ("bronze", "news_items")
REJECTED_ID = ("bronze", "news_feeds_rejected_files")
ATOM = "{http://www.w3.org/2005/Atom}"
DC_DATE = "{http://purl.org/dc/elements/1.1/}date"
WARSAW = ZoneInfo("Europe/Warsaw")  # feeds that give a local time without an offset (Puls Biznesu)
COLUMNS = [
    ("entity", "VARCHAR"),
    ("outlet", "VARCHAR"),
    ("feed_format", "VARCHAR"),  # rss | atom
    ("feed_lang", "VARCHAR"),  # channel <language> or xml:lang, as given
    ("title", "VARCHAR"),
    ("link", "VARCHAR"),
    ("guid", "VARCHAR"),
    ("published_text", "VARCHAR"),  # pubDate / updated / published / dc:date as given
    ("published_at", "TIMESTAMPTZ"),  # parsed, UTC; null when the text cannot be read
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def parse_time(text: str | None) -> datetime | None:
    """RFC 822 (RSS), ISO 8601 (Atom) or 'YYYY-MM-DD HH:MM:SS' local Warsaw time, as UTC; None if unreadable."""
    if not text:
        return None
    text = text.strip()
    try:
        moment = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError):
        try:
            moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=WARSAW)
    return moment.astimezone(UTC)


def clean(text: str | None) -> str | None:
    return " ".join(text.split()) if text else None


def outlets() -> dict[str, str]:
    """Entity → outlet key, from the feed declarations."""
    return {spec["entity"]: spec["outlet"] for spec in bulk_file.declaration(SOURCE)}


def items(root: ET.Element) -> tuple[str, str | None, list[dict[str, Any]]]:
    """(format, language, items with title/link/guid/published_text) of an RSS 2.0 or Atom document."""
    if root.tag == "rss":
        channel = root.find("channel")
        if channel is None:
            raise ValueError("RSS without <channel>")
        found = [
            {
                "title": clean(item.findtext("title")),
                "link": clean(item.findtext("link")),
                "guid": clean(item.findtext("guid")),
                "published_text": clean(item.findtext("pubDate") or item.findtext(DC_DATE)),
            }
            for item in channel.findall("item")
        ]
        return "rss", clean(channel.findtext("language")), found
    if root.tag == f"{ATOM}feed":
        found = []
        for entry in root.findall(f"{ATOM}entry"):
            links = entry.findall(f"{ATOM}link")
            link = next((x.get("href") for x in links if x.get("rel", "alternate") == "alternate"), None)
            found.append(
                {
                    "title": clean("".join(entry.find(f"{ATOM}title").itertext()))
                    if entry.find(f"{ATOM}title") is not None
                    else None,
                    "link": clean(link),
                    "guid": clean(entry.findtext(f"{ATOM}id")),
                    "published_text": clean(entry.findtext(f"{ATOM}published") or entry.findtext(f"{ATOM}updated")),
                }
            )
        return "atom", root.get("{http://www.w3.org/XML/1998/namespace}lang"), found
    raise ValueError(f"not RSS 2.0 or Atom: root element {root.tag!r}")


def decode(body: bytes, receipt: dict[str, Any], outlet_of: dict[str, str] | None = None) -> pa.Table:
    outlet = (outlet_of or outlets())[receipt["entity"]]
    fmt, lang, found = items(ET.fromstring(body))
    base = bulk_bronze.provenance(receipt)
    rows = []
    for n, item in enumerate(found, start=1):
        if not item["title"] or not item["link"]:
            raise ValueError(f"item {n} without title or link")
        rows.append(
            {
                **base,
                "outlet": outlet,
                "feed_format": fmt,
                "feed_lang": lang,
                **item,
                "published_at": parse_time(item["published_text"]),
                "source_row_number": n,
            }
        )
    if not rows:
        raise ValueError("feed without items")
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: a few feeds of under 150 kB per run."""
    outlet_of = outlets()
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        lambda body, receipt: decode(body, receipt, outlet_of),
        "items",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        all_versions=True,
    )
