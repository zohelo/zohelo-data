"""GPW EBI (CD-007) Landing activities: the EBI rows of GPW's public report list, https://www.gpw.pl/espi-ebi-reports.

landing.inspect  the newest EBI rows against Landing (one request); no writes
landing.extract  the list with only "EBI" ticked, newest first, through the page's own "Show more" request (the
                 form posted to ajaxindex.php on the same host, 15 rows a request), from the top down to the newest
                 landed day or 3 days back, whichever is older; 365 days on the first run. One request at a time,
                 5 seconds apart, the platform's User-Agent; list rows only, never a report page or attachment, and
                 never https://www.gpw.pl/ebi, which refuses automated clients (decision D36). ESPI rows are not
                 asked for (they are CD-006, knf_espi) and are dropped if one is answered. Each day with reports
                 lands as one reduced JSON document (time, category, number, issuer, ISIN, title, id and link;
                 O32, O33; the prices shown beside each row are not read), written only when it changed.
landing.verify   every receipt has its file (Landing or Archive)
Shared code: ingestion/common/report_lists.py.
"""

from __future__ import annotations

SOURCE = "gpw_ebi"


def _today(reader, day):
    from ingestion.common import report_lists as lists

    return None, lists.read_gpw_since(reader, day)[0].get(day, [])


def _inspect(p) -> int:
    from ingestion.common import report_lists as lists

    return lists.inspect(SOURCE, _today)


def _extract(p) -> int:
    from ingestion.common import report_lists as lists

    return lists.extract_newest_first(SOURCE, lists.read_gpw_since, lists.GPW_PAGE)


def _verify(p) -> int:
    from ingestion.common import report_lists as lists

    return lists.verify(SOURCE)


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
