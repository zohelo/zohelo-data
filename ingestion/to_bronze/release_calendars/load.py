#!/usr/bin/env python3
"""Release calendars bronze.load: newest copy of every calendar file → bronze.release_calendar_items.

One row per announced release, decoded per publisher (entity prefix):
  eurostat_euro_indicators  JSON events: start (UTC), title, reference period, dataset codes, theme, record id
  gus_day_<YYYYMMDD>        the "Publikacje GUS" list of a kalendarium day page: title and link; the date is the
                            page's (GUS publishes signal releases at 09:30 Warsaw; the page gives no time)
  ecb_statistical           "DD/MM/YYYY HH:MM [Tentative] <title> (Dataset: X) Reference period: …" entries
  ecb_meetings              "DD/MM/YYYY <meeting>" entries (Governing Council and General Council)
Text as published; a page whose list cannot be found rejects the file (a changed layout is never loaded
silently); a GUS day without GUS publications loads as no rows. Newest copy per entity (latest-only Bronze).
"""

from __future__ import annotations

from html import unescape
from html.parser import HTMLParser
import json
import re
from typing import Any

import pyarrow as pa

from ingestion.common import bulk_bronze
from ingestion.to_landing import release_calendars as landing

SOURCE = "release_calendars"
TABLE_ID = ("bronze", "release_calendar_items")
REJECTED_ID = ("bronze", "release_calendars_rejected_files")
COLUMNS = [
    ("entity", "VARCHAR"),
    ("publisher", "VARCHAR"),  # eurostat | gus | ecb
    ("release_date", "VARCHAR"),  # YYYY-MM-DD
    ("release_time", "VARCHAR"),  # HH:MM local to the publisher, or a UTC instant (Eurostat), when given
    ("title", "VARCHAR"),
    ("url", "VARCHAR"),
    ("reference_period", "VARCHAR"),
    ("dataset_codes", "VARCHAR"),  # Eurostat dataset codes / ECB dataset, comma separated
    ("tentative", "VARCHAR"),  # true | false
    ("item_id", "VARCHAR"),  # Eurostat record id
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)
GUS_BASE = "https://stat.gov.pl"
EUROSTAT_PAGE = "https://ec.europa.eu/eurostat/news/euro-indicators/release-calendar"


def text_of(html: str) -> str:
    """Visible text with tags as single spaces."""
    html = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", html)
    return " ".join(unescape(re.sub(r"<[^>]+>", " ", html)).split())


def eurostat(body: bytes) -> list[dict[str, Any]]:
    events = json.loads(body)
    if not isinstance(events, list):
        raise ValueError("Eurostat calendar: not a JSON list")
    return [
        {
            "release_date": e["start"][:10],
            "release_time": e["start"],
            "title": e["title"],
            "url": EUROSTAT_PAGE,
            "reference_period": e.get("period"),
            "dataset_codes": e.get("datasetCodes") or None,
            "tentative": str(bool(e.get("preliminary"))).lower(),
            "item_id": str(e.get("recordid")),
        }
        for e in events
    ]


class _GusList(HTMLParser):
    """Titles and links of <h3 class="title"><a href> inside the "Publikacje GUS" tab of a day page."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.section, self.in_h2, self.h2, self.in_title, self.found = None, False, "", False, False
        self.items: list[dict[str, str]] = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "h2":
            self.in_h2, self.h2 = True, ""
        elif tag == "h3" and "title" in (a.get("class") or ""):
            self.in_title = True
        elif tag == "a" and self.in_title and self.section == "gus":
            self.items.append({"url": a.get("href") or "", "title": ""})

    def handle_endtag(self, tag):
        if tag == "h2":
            self.in_h2 = False
            heading = " ".join(self.h2.split())
            self.section = "gus" if heading == "Publikacje GUS" else "other"
            self.found = self.found or self.section == "gus"
        elif tag == "h3":
            self.in_title = False

    def handle_data(self, data):
        if self.in_h2:
            self.h2 += data
        elif self.in_title and self.section == "gus" and self.items:
            self.items[-1]["title"] += data


def gus(body: bytes, entity: str) -> list[dict[str, Any]]:
    parser = _GusList()
    parser.feed(body.decode("utf-8", errors="replace"))
    if not parser.found:
        raise ValueError("GUS day page without a 'Publikacje GUS' section")
    day = entity.rsplit("_", 1)[-1]
    date = f"{day[:4]}-{day[4:6]}-{day[6:]}"
    return [
        {
            "release_date": date,
            "title": " ".join(i["title"].split()),
            "url": i["url"] if i["url"].startswith("http") else GUS_BASE + i["url"],
        }
        for i in parser.items
        if i["title"].strip()
    ]


ECB_STAT = re.compile(
    r"(\d{2})/(\d{2})/(\d{4}) (\d{2}:\d{2}) (Tentative )?(.+?) Reference period: ((?:Q\d |H\d |Week \d+ )?\S+)"
)
ECB_MEETING = re.compile(r"(\d{2})/(\d{2})/(\d{4}) ((?:Governing|General) Council.+?)(?= \d{2}/\d{2}/\d{4} |$)")


def ecb(body: bytes, entity: str) -> list[dict[str, Any]]:
    text = text_of(body.decode("utf-8", errors="replace"))
    rows = []
    if entity == "ecb_statistical":
        for m in ECB_STAT.finditer(text):
            title = m.group(6)
            dataset = re.search(r"\(Dataset: ([^)]+)\)", title)
            rows.append(
                {
                    "release_date": f"{m.group(3)}-{m.group(2)}-{m.group(1)}",
                    "release_time": m.group(4),
                    "title": title,
                    "url": landing.ECB[entity],
                    "reference_period": m.group(7).strip(),
                    "dataset_codes": dataset.group(1) if dataset else None,
                    "tentative": "true" if m.group(5) else "false",
                }
            )
    else:
        for m in ECB_MEETING.finditer(text):
            rows.append(
                {
                    "release_date": f"{m.group(3)}-{m.group(2)}-{m.group(1)}",
                    "title": m.group(4).strip(),
                    "url": landing.ECB[entity],
                }
            )
    if not rows:
        raise ValueError(f"{entity}: no calendar entries found")
    return rows


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    entity = receipt["entity"]
    if entity == "eurostat_euro_indicators":
        publisher, found = "eurostat", eurostat(body)
    elif entity.startswith("gus_day_"):
        publisher, found = "gus", gus(body, entity)
    elif entity in landing.ECB:
        publisher, found = "ecb", ecb(body, entity)
    else:
        raise ValueError(f"unknown calendar entity {entity!r}")
    base = bulk_bronze.provenance(receipt)
    rows = [
        {
            **{c: None for c, _ in COLUMNS},
            **base,
            **item,
            "publisher": publisher,
            "source_row_number": n,
        }
        for n, item in enumerate(found, start=1)
    ]
    return pa.Table.from_pylist(rows, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: about 55 small pages."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "items",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=landing.landed_specs(landing_root),
    )
