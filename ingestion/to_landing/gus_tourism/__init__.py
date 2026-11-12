"""GUS Turystyka (tourism), annual tables: Landing activities (register PLS-035).

Shared logic in ingestion/common/gus_publications.py: the series page links the newest edition, each edition page
links the other editions and its tables file; entity = one edition (``turystyka_<year>``), from 2019.

landing.inspect  editions the pages list vs Landing; no writes
landing.extract  editions not landed yet, plus the newest edition again (unchanged bytes skipped)
landing.verify   every receipt has its file (bulk_file pattern)
"""

from __future__ import annotations

from ingestion.common import gus_publications

SERIES = gus_publications.Series(
    source="gus_tourism",
    series_page="https://stat.gov.pl/obszary-tematyczne/kultura-turystyka-sport/turystyka/",
    edition_text=r"^Turystyka w (\d{4})(?: r\.| roku)?$",
    tables_text=r"Tablice w formacie XLSX",
    prefix="turystyka",
    first_year=2019,
)

ACTIVITIES = gus_publications.activities(SERIES)
