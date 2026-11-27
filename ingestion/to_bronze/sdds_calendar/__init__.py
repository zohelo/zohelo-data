"""SDDS advance release calendar: Landing → Bronze activities (register TP-012, TP-013).

bronze.load       newest copy of every landed calendar (entity calendar_<year>) → bronze.sdds_calendar_cells, one row
                  per non-empty cell (ingestion/common/xlsx_cells.py), through ingestion/common/bulk_bronze.py, after
                  the layout guard below; a workbook that fails it → bronze.sdds_calendar_rejected_files with the reason
bronze.verify     ingestion/common/lineage.py --source sdds_calendar
landing.archive   ingestion/common/archive.py --source sdds_calendar (move to Archive)
archive.prune     ingestion/common/retention.py --source sdds_calendar (archive_policy.retention)

Layout (Dev probe 45963679231, the 2026 workbook of 25 Nov 2026): sheet "Spis tablic" with the title "SDDS Kalendarz
Upowszechniania Danych w <year> r." in B1, then one sheet per table named after its sector. In each table the header
row starts with "Kategorie" in column A, "Ostatnia aktualizacja" in B and one "MM YYYY" month per column from C on; a
category row has its name in A and the last update in B (an Excel date, or "dziennie"); a cell under a month is "-",
"dziennie", or one or more "DD.MM.YYYY (period)" releases, the period "MM YYYY", "Qn YYYY" or "n kw. YYYY" (one cell of
the 2026 file lacks its closing bracket at the end: "15.09.2026 (08 2026"). Anything else (an unknown sheet, a moved
header, text left over after the releases, a release under no month) rejects the workbook: a changed layout is never
loaded or guessed. Empty cells are not rows, so nothing can become a date or a count of 0 in Silver.
"""

from __future__ import annotations

import re
from typing import Any

SOURCE = "sdds_calendar"
TABLE = "sdds_calendar_cells"
REJECTED = "sdds_calendar_rejected_files"
INDEX = "Spis tablic"
SECTORS = {"Sektor podstawowy", "Sektor podatkowo-budżetowy", "Sektor finansowy", "Sektor zagraniczny", "Ludność"}
TITLE = re.compile(r"SDDS Kalendarz Upowszechniania Danych w (20\d\d) r\.")
MONTH = re.compile(r"\d{1,2} 20\d\d")
# Silver (warehouse/models/silver/sdds_calendar/sdds_calendar_releases.sql) uses the same two patterns.
RELEASE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})\s*\(([^()]*)(?:\)|$)")
PERIOD = re.compile(r"(?:\d{2}|Q[1-4]|[1-4] kw\.) 20\d\d")
NO_RELEASE = {"-", "dziennie"}


def check_layout(entity: str, cells: list[dict[str, Any]]) -> None:
    """Raise ValueError when the workbook is not the calendar layout above."""
    sheets = {c["sheet_name"] for c in cells}
    if sheets != SECTORS | {INDEX}:
        raise ValueError(f"sheets {sorted(sheets)} are not the calendar's {sorted(SECTORS | {INDEX})}")
    at = {(c["sheet_name"], c["row_number"], c["column_number"]): c for c in cells}
    title = TITLE.fullmatch((at.get((INDEX, 1, 2)) or {}).get("value_text", "").strip())
    if not title or entity != f"calendar_{title.group(1)}":
        raise ValueError(f"title cell B1 does not name the calendar year of {entity!r}")
    for sheet in sorted(SECTORS):
        own = [c for c in cells if c["sheet_name"] == sheet]
        heads = [c["row_number"] for c in own if c["column_number"] == 1 and c["value_text"] == "Kategorie"]
        if len(heads) != 1:
            raise ValueError(f"{sheet}: {len(heads)} header rows 'Kategorie'")
        head = heads[0]
        months = {c["column_number"] for c in own if c["row_number"] == head and c["column_number"] >= 3}
        if not months or any(not MONTH.fullmatch(at[(sheet, head, n)]["value_text"].strip()) for n in months):
            raise ValueError(f"{sheet}: header row {head} is not one 'MM YYYY' month per column from C")
        rows = {c["row_number"] for c in own if c["row_number"] > head and c["column_number"] >= 2}
        for row in sorted(rows):
            if (sheet, row, 1) not in at or (sheet, row, 2) not in at:
                raise ValueError(f"{sheet}: row {row} has cells but no category name or last-update cell")
            update = at[(sheet, row, 2)]
            if update["cell_type"] != "n" and update["value_text"].strip() not in NO_RELEASE:
                raise ValueError(f"{sheet}!{update['cell_ref']}: last update {update['value_text']!r}")
        for c in own:
            if c["row_number"] not in rows or c["column_number"] < 3:
                continue
            text = c["value_text"].strip()
            if c["column_number"] not in months:
                raise ValueError(f"{sheet}!{c['cell_ref']}: a cell under no month")
            if text in NO_RELEASE:
                continue
            found = RELEASE.findall(text)
            if not found or RELEASE.sub("", text).strip() or any(not PERIOD.fullmatch(p.strip()) for *_, p in found):
                raise ValueError(f"{sheet}!{c['cell_ref']}: not '-', 'dziennie' or 'DD.MM.YYYY (period)': {text!r}")


def decode(body: bytes, receipt: dict[str, Any]):
    """xlsx_cells.decode, then the calendar's layout guard."""
    from ingestion.common import xlsx_cells

    table = xlsx_cells.decode(body, receipt)
    check_layout(receipt["entity"], table.to_pylist())
    return table


def load(entities: list[str], landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    from ingestion.common import bulk_bronze, xlsx_cells

    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE),
        (namespace, REJECTED),
        xlsx_cells.COLUMNS,
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(entities)],
    )


def _load(p) -> int:
    from ingestion.to_landing import sdds_calendar as landing

    return load(sorted(landing.held()))


def _verify(p) -> int:
    from ingestion.common import lineage

    return lineage.main(["--source", SOURCE])


def _archive(p) -> int:
    from ingestion.common import archive

    return archive.main(["--source", SOURCE])


def _prune(p) -> int:
    from ingestion.common import retention

    return retention.main(["--source", SOURCE])


ACTIVITIES = {"bronze.load": _load, "bronze.verify": _verify, "landing.archive": _archive, "archive.prune": _prune}
