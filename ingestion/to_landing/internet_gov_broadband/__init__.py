"""internet.gov.pl broadband report: Landing activities (register PL-ENV-034).

The national PDF of every edition is one bulk_file entity of the seed list in
ingestion/sources/internet_gov_broadband.yaml (one row per reference quarter: the attachment address read on the
edition's gov.pl release page). Landing, receipts and versions are the shared bulk_file pattern's.

landing.inspect  seed list vs Landing (bulk_file inspect), plus the edition check below; no writes
landing.extract  every seed file, 3 s apart; unchanged bytes are skipped (bulk_file load), plus the edition check
landing.verify   every receipt has its file (bulk_file verify)

Edition check: the release pages have no stable index, so a new edition only enters through a new seed row. Every
quarter that ended more than ``GRACE_DAYS`` ago (editions so far came 14 to 17 days after the quarter) must have a
row; a missing one is a warning annotation "edition expected but not in the seed list" naming the quarter, the
date it was expected from and where to look (the ministry's news list). The run does not fail on it: the loaded
editions stay correct, and the warning repeats on every run until the row is added.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

SOURCE = "internet_gov_broadband"
FIRST_QUARTER = (2024, 2)  # the first edition: data as of 30 June 2024, published 15 July 2024
GRACE_DAYS = 20
NEWS = "https://www.gov.pl/web/cyfryzacja/wiadomosci"


def quarter_end(year: int, quarter: int) -> date:
    """The last day of a quarter (the report's reference date)."""
    first_of_next = date(year + quarter // 4, quarter % 4 * 3 + 1, 1)
    return first_of_next - timedelta(days=1)


def expected(today: date) -> list[str]:
    """Quarters (``YYYY-Qn``) whose edition is due by ``today``: ended at least GRACE_DAYS days ago."""
    out, (year, quarter) = [], FIRST_QUARTER
    while quarter_end(year, quarter) + timedelta(days=GRACE_DAYS) <= today:
        out.append(f"{year}-Q{quarter}")
        year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
    return out


def missing_editions(specs: list[dict], today: date) -> list[dict]:
    """Due quarters without a seed row, each with the date its edition was expected from."""
    seeded = {str(spec.get("quarter")) for spec in specs}
    rows = []
    for label in expected(today):
        if label not in seeded:
            year, quarter = int(label[:4]), int(label[-1])
            rows.append({"quarter": label, "expected_from": str(quarter_end(year, quarter) + timedelta(GRACE_DAYS))})
    return rows


def check_editions(today: date | None = None) -> list[dict]:
    """Annotate a warning when a due edition has no seed row; returns the missing quarters."""
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    missing = missing_editions(bulk_file.declaration(SOURCE), today or datetime.now(UTC).date())
    if missing:
        annotate(
            "warning",
            f"{SOURCE}: edition expected but not in the seed list",
            {
                "missing": missing,
                "action": "find the edition's release page (news list below), read the national PDF's attachment "
                "address on it and add a seed row to ingestion/sources/internet_gov_broadband.yaml",
                "news_list": NEWS,
            },
        )
    return missing


def _run(mode: str) -> int:
    from ingestion.to_landing.patterns import bulk_file

    status = bulk_file.main(SOURCE, mode, pause_seconds=3.0 if mode == "load" else 0.0)
    if mode != "verify":
        check_editions()
    return status


ACTIVITIES = {
    "landing.inspect": lambda p: _run("inspect"),
    "landing.extract": lambda p: _run("load"),
    "landing.verify": lambda p: _run("verify"),
}
