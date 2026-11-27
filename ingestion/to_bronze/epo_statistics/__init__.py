"""EPO patent statistics: Landing → Bronze activities (register EX-004).

bronze.load       newest copy of every landed workbook (entity <code>_<edition>, ingestion/to_landing/epo_statistics/)
                  → bronze.epo_statistics_cells, one row per non-empty cell (ingestion/common/xlsx_cells.py), through
                  ingestion/common/bulk_bronze.py; a file whose sheets are not its table's layout is rejected →
                  bronze.epo_statistics_rejected_files
bronze.verify     ingestion/common/lineage.py --source epo_statistics
landing.archive   ingestion/common/archive.py --source epo_statistics (move to Archive)
archive.prune     ingestion/common/retention.py --source epo_statistics (archive_policy.retention)

Empty cells are not rows and text cells keep their text, so neither can become 0 in Silver.
"""

from __future__ import annotations

import re
from typing import Any

SOURCE = "epo_statistics"
TABLE = "epo_statistics_cells"
REJECTED = "epo_statistics_rejected_files"
# Sheets per selected workbook (Dev probe 45924424161, 2025 edition): the country and field tables are one sheet
# "EP Applications <from>-<to>" / "EP Grants <from>-<to>", the field x country tables one sheet per year, the Poland
# workbook three sheets of which Silver reads "Provinces". Anything else is an unknown layout.
SHEETS = {
    "applications_by_country": r"EP Applications \d{4}-\d{4}",
    "applications_by_field": r"EP Applications \d{4}-\d{4}",
    "grants_by_country": r"EP Grants \d{4}-\d{4}",
    "grants_by_field": r"EP Grants \d{4}-\d{4}",
    "applications_by_field_country": r"\d{4}",
    "grants_by_field_country": r"\d{4}",
}


def check_sheets(entity: str, sheets: set[str]) -> None:
    """Raise ValueError when a workbook's sheets are not the layout of its table (entity <code>_<edition>)."""
    code, _, edition = entity.rpartition("_")
    if not re.fullmatch(r"20\d\d", edition):
        raise ValueError(f"entity {entity!r} has no edition")
    if code == "poland":
        if "Provinces" not in sheets:
            raise ValueError(f"no sheet 'Provinces' in {sorted(sheets)}")
        return
    pattern = SHEETS.get(code)
    if pattern is None:
        raise ValueError(f"undeclared table {code!r}")
    odd = sorted(s for s in sheets if not re.fullmatch(pattern, s))
    if odd or (not code.endswith("_field_country") and len(sheets) != 1):
        raise ValueError(f"sheets {sorted(sheets)} are not the layout {pattern!r} of {code}")


def decode(body: bytes, receipt: dict[str, Any]):
    """xlsx_cells.decode, then the sheet guard of the workbook's table."""
    from ingestion.common import xlsx_cells

    table = xlsx_cells.decode(body, receipt)
    check_sheets(receipt["entity"], set(table.column("sheet_name").to_pylist()))
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


def _entities() -> list[str]:
    from ingestion.to_landing import epo_statistics as landing

    return sorted(landing.held())


def _load(p) -> int:
    return load(_entities())


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
