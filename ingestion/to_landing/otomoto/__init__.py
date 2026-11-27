"""OTOMOTO Insights market report: Landing activities (register CD-034, #3264).

Each monthly edition is a public Infogram page whose HTML holds the report as JSON in one script statement,
``window.infographicData = {...};``; every edition is one bulk_file entity of the seed list in
ingestion/sources/otomoto.yaml (one row per edition month, the page address). Landing, receipts and versions are the
shared bulk_file pattern's, with ``reduce: otomoto`` (``reduce_landing`` below): what lands is the JSON object of that
statement, byte for byte as the page has it. The rest of the page changes on every request (a per-visitor cookie id
in a script and a time stamp on the preview image address; two reads 5 s apart in Dev probe 45813576806 differed only
there, the JSON was identical), so a whole-page copy would land on every run; the JSON changes only when OTOMOTO edits
the edition. A copy lands only when the SHA-256 of the JSON changed.

landing.inspect  seed list vs Landing (bulk_file inspect), plus the edition check below; no writes
landing.extract  every seed page, 5 s apart; unchanged bytes are skipped (bulk_file load), plus the edition check
landing.verify   every receipt has its file (bulk_file verify)

Edition check: the address of a new edition is linked only from https://www.otomoto.pl/news/raport-rynkowy-otomoto,
and www.otomoto.pl refuses the runner (HTTP 403 on robots.txt, Dev probe 43291903826), so that page is never read and
a new edition enters only through a new seed row (decision D41). Every month that ended more than ``GRACE_DAYS``
ago must have a row; a missing one is a warning annotation "edition expected but not in the seed list" naming the
month. The run does not fail on it: the loaded editions stay correct, and each edition repeats about 20 months, so a
late row loses nothing; the warning repeats on every run until the row is added.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
import re

SOURCE = "otomoto"
GRACE_DAYS = 45  # days after the end of month M by which the edition of M is expected (August 2026: created 9 Sep)
REPORT_PAGE = "https://www.otomoto.pl/news/raport-rynkowy-otomoto"
STATEMENT = re.compile(rb"window\.infographicData\s*=\s*(\{.*?\});\s*</script>", re.S)


def reduce_landing(body: bytes) -> bytes:
    """The JSON object of the page's one ``window.infographicData`` statement, exact bytes; any other page fails."""
    found = STATEMENT.findall(body)
    if len(found) != 1:
        raise ValueError(f"{SOURCE}: expected one infographicData statement in the page, found {len(found)}")
    return found[0]


def expected(first: str, today: date) -> list[str]:
    """Edition months (``YYYY-MM``) from ``first`` whose month ended at least GRACE_DAYS days before ``today``."""
    year, month = int(first[:4]), int(first[5:7])
    out = []
    while True:
        end = date(year + month // 12, month % 12 + 1, 1)  # the day after the month
        if (today - end).days + 1 < GRACE_DAYS:
            return out
        out.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)


def missing_editions(specs: list[dict], today: date) -> list[str]:
    """Due edition months without a seed row (counted from the first seeded month)."""
    seeded = sorted(str(spec.get("edition")) for spec in specs)
    if not seeded:
        return []
    return [m for m in expected(seeded[0], today) if m not in seeded]


def check_editions(today: date | None = None) -> list[str]:
    """Annotate a warning when a due edition has no seed row; returns the missing months."""
    from ingestion.common.report import annotate
    from ingestion.to_landing.patterns import bulk_file

    missing = missing_editions(bulk_file.declaration(SOURCE), today or datetime.now(UTC).date())
    if missing:
        annotate(
            "warning",
            f"{SOURCE}: edition expected but not in the seed list",
            {
                "missing": missing,
                "action": "read the edition's Infogram address on the report page (a desk reading, not from a runner) "
                "and add a seed row to ingestion/sources/otomoto.yaml",
                "report_page": REPORT_PAGE,
            },
        )
    return missing


def _run(mode: str) -> int:
    from ingestion.to_landing.patterns import bulk_file

    status = bulk_file.main(SOURCE, mode, pause_seconds=5.0 if mode == "load" else 0.0)
    if mode != "verify":
        check_editions()
    return status


ACTIVITIES = {
    "landing.inspect": lambda p: _run("inspect"),
    "landing.extract": lambda p: _run("load"),
    "landing.verify": lambda p: _run("verify"),
}
