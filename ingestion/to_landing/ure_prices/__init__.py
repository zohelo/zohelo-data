"""ure_prices Landing: the shared bulk_file pattern (declared by extraction.kind), paced, with one reduction.

ure.gov.pl ends every page with two HTML comments written at request time, the render time and a buffer digest
(``</html><!-- 0.029747 / WebAdministrator (26.11.2026 10:16) //--><!-- GZIP Buffer 648afb…//-->``), so two
downloads of an unchanged page never have the same bytes (Dev probe 45790234209: two GETs 4 s apart differed only
there). ``reduce_landing`` (``reduce: ure_prices`` in the declaration) removes exactly that trailer and nothing
else; every byte of the page up to ``</html>`` is landed as served, and a page without the trailer is landed
unchanged. Without it every run would land a new copy of every page and Bronze would load it again.

landing.extract   bulk_file load of the declared pages, PAUSE_SECONDS apart (one request at a time)
landing.inspect   bulk_file inspect; declared by extraction.kind, no code here
landing.verify    bulk_file verify; declared by extraction.kind, no code here
"""

from __future__ import annotations

import re

SOURCE = "ure_prices"
PAUSE_SECONDS = 5.0

TRAILER = re.compile(
    rb"(</html>)(?:<!-- [0-9.]+ / WebAdministrator \([0-9.: ]+\) //-->)?(?:<!-- GZIP Buffer [0-9a-f]+//-->)?\s*\Z",
    re.IGNORECASE,
)


def reduce_landing(body: bytes) -> bytes:
    """The page without the request-time comments after ``</html>``; raises when the page has no closing
    ``</html>`` at its end (a cut-off or non-HTML answer is not landed)."""
    match = TRAILER.search(body)
    if match is None:
        raise RuntimeError("ure_prices: the page does not end with </html> (cut off or not HTML), not landed")
    return body[: match.end(1)]


def extract(landing_root: str = "01_landing") -> int:
    """landing.extract: the bulk_file load of the declared pages, one at a time, PAUSE_SECONDS apart."""
    from ingestion.to_landing.patterns import bulk_file

    return bulk_file.main(SOURCE, "load", landing_root=landing_root, pause_seconds=PAUSE_SECONDS)


ACTIVITIES = {"landing.extract": lambda p: extract()}  # landing.inspect and landing.verify: the bulk_file pattern
