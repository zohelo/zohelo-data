"""KNF ESPI (CD-006) Landing activities: the public OAM list of issuer reports, https://moam.knf.gov.pl/moam.nsf.

landing.inspect  today's list against Landing (today's pages are the only requests); no writes
landing.extract  the list's view by day of receipt (``mOAM?readForm&RestrictToCategory=<day>``, its own pager,
                 20 reports a page): today and the 3 days before on every run, working days a past run failed on,
                 and on the first runs the 45 days before today, newest first. One request at a time, 5 seconds
                 apart, the platform's User-Agent; list pages only, never a report page or file (decision D36).
                 Each day with reports lands as one reduced JSON document (issuer, date, title, document id and
                 link; O32, O33), written only when it changed.
landing.verify   every receipt has its file (Landing or Archive)
Shared code: ingestion/common/report_lists.py.
"""

from __future__ import annotations

SOURCE = "knf_espi"


def _inspect(p) -> int:
    from ingestion.common import report_lists as lists

    return lists.inspect(SOURCE, lambda reader, day: lists.read_knf_day(reader, day)[:2])


def _extract(p) -> int:
    from ingestion.common import report_lists as lists

    return lists.extract_by_day(SOURCE, lists.read_knf_day, budget_minutes=min(p.budget_minutes, 45.0))


def _verify(p) -> int:
    from ingestion.common import report_lists as lists

    return lists.verify(SOURCE)


ACTIVITIES = {"landing.inspect": _inspect, "landing.extract": _extract, "landing.verify": _verify}
