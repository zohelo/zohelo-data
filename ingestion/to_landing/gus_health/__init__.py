"""GUS Zdrowie i ochrona zdrowia: Landing activities (register PLS-004, health family).

Shared logic in ingestion/common/gus_publications.py: the health subject page links the newest edition, each edition
page links the other editions and its tables file; entity = one edition (``zdrowie_<year>``), from FIRST_YEAR.

landing.inspect  editions the pages list vs Landing; no writes
landing.extract  editions not landed yet, plus the newest edition again (unchanged bytes skipped)
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

from ingestion.common import gus_publications

SERIES = gus_publications.Series(
    source="gus_health",
    series_page="https://stat.gov.pl/obszary-tematyczne/zdrowie/zdrowie/",
    edition_text=r"^Zdrowie i ochrona zdrowia w (\d{4})(?: r\.| roku)?$",
    tables_text=r"Tablice w formacie XLSX|^Zdrowie i ochrona zdrowia w \d{4} r\.$",
    prefix="zdrowie",
    first_year=2012,
)

ACTIVITIES = gus_publications.activities(SERIES)
