"""Lasy Państwowe BDL WFS: Landing reduction (register PL-ENV-018, #3301).

Landing, receipts and versions are the shared bulk_file pattern's (ingestion/sources/lasy_bdl.yaml), with
``reduce: lasy_bdl`` (``reduce_landing`` below). GeoServer's GeoJSON answer ends with the member
``"timeStamp":"<time of the request>"``, so two reads of an unchanged layer differ (Dev trial 45898596886: the second
read of each layer had the same size and another SHA-256) and a copy would land on every run. What lands is the
answer byte for byte without that one member; every feature, attribute, coordinate and the other top-level members
(totalFeatures, numberMatched, numberReturned, crs) stay as sent. An answer without exactly one such member near its
end fails the download (a changed answer format is not guessed). No activities of its own: landing.inspect, extract
and verify are the bulk_file pattern's.
"""

from __future__ import annotations

import re

SOURCE = "lasy_bdl"
TAIL = 4096  # the member is among the last top-level members, after the features
STAMP = re.compile(rb',"timeStamp":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z"')


def reduce_landing(body: bytes) -> bytes:
    """The GeoJSON answer without its request time stamp, exact bytes otherwise."""
    start = max(0, len(body) - TAIL)
    found = list(STAMP.finditer(body, start))
    if len(found) != 1:
        raise ValueError(f"{SOURCE}: expected one timeStamp member at the end of the answer, found {len(found)}")
    match = found[0]
    return body[: match.start()] + body[match.end() :]


ACTIVITIES: dict = {}
