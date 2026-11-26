"""GUS Zdrowie i ochrona zdrowia: Landing activities (register PLS-004, health family; #2452).

Shared logic in ingestion/common/gus_publications.py: the health subject page links the newest edition, each edition
page links the other editions and its tables file; entity = one edition (``zdrowie_<year>``), from 2012. The tables
link is named "... tablice" (2013-2016), "Tablice w formacie XLSX" or "Aneks tabelaryczny w formacie XLSX" (2017-),
and in 2012 only by the edition title (the ZIP beside the PDF of the same name); the ZIP of the 2010 edition linked
from every page ("... w 2010 roku 4.31 MB", no XLSX inside) matches none of these.

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
    tables_text=r"(?i)tablic|aneks tabelaryczny|^Zdrowie i ochrona zdrowia w \d{4} r\.$",
    prefix="zdrowie",
    first_year=2012,
)

ACTIVITIES = gus_publications.activities(SERIES)
