"""GDDKiA rest areas (MOP table): Landing → Bronze activities (register PL-ENV-023).

bronze.load       newest copy of every landed edition → bronze.gddkia_mop_cells: one row per non-empty cell, in the
                  shared workbook-cell shape (ingestion/common/gus_publications.py bronze_columns, member empty),
                  read by ingestion/common/xlsx_cells.py. A file that is not an XLSX workbook, or has no header row
                  (column 1 "Lp.", with "Województwo" and "Obecna kategoria" in the same row), is rejected →
                  bronze.gddkia_mop_rejected_files (a changed layout is never guessed)
bronze.verify     ingestion/common/lineage.py --source gddkia_mop
landing.archive   ingestion/common/archive.py --source gddkia_mop (move to Archive)
archive.prune     ingestion/common/retention.py --source gddkia_mop (archive_policy.retention)
"""

from __future__ import annotations

from typing import Any
import zipfile

from ingestion.common import gus_publications

SOURCE = "gddkia_mop"


def _norm(text: str) -> str:
    return " ".join(str(text).split()).lower()


def header_row(cells: list[dict[str, Any]]) -> tuple[str, int] | None:
    """(sheet, row) of the table header, None when no row has "Lp." in column 1 and the two named columns."""
    rows: dict[tuple[str, int], list[str]] = {}
    for c in cells:
        rows.setdefault((c["sheet_name"], c["row_number"]), []).append(_norm(c["value_text"]))
    first = {
        (c["sheet_name"], c["row_number"])
        for c in cells
        if c["column_number"] == 1 and _norm(c["value_text"]).startswith("lp")
    }
    for key in sorted(first):
        texts = rows[key]
        if "województwo" in texts and any(t.startswith("obecna kategoria") for t in texts):
            return key
    return None


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: every non-empty cell of the workbook, after the header check."""
    from ingestion.common import bulk_bronze, xlsx_cells

    try:
        cells = list(xlsx_cells.cells(body))
    except (zipfile.BadZipFile, KeyError) as exc:
        raise ValueError(f"not an XLSX workbook: {exc}") from exc
    if header_row(cells) is None:
        raise ValueError("no header row with Lp., Województwo and Obecna kategoria (layout changed?)")
    base = bulk_bronze.provenance(receipt)
    rows = [{**base, "member": "", **c, "source_row_number": i} for i, c in enumerate(cells, 1)]
    return bulk_bronze.pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(gus_publications.bronze_columns()))


def bronze_load(entities: list[str], landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    from ingestion.common import bulk_bronze

    return bulk_bronze.load(
        SOURCE,
        (namespace, f"{SOURCE}_cells"),
        (namespace, f"{SOURCE}_rejected_files"),
        gus_publications.bronze_columns(),
        decode,
        "cells",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=[{"entity": e} for e in sorted(entities)],
    )


def _entities(landing_root: str = "01_landing") -> list[str]:
    from ingestion.to_landing.gddkia_mop import held

    return sorted(held(landing_root))


ACTIVITIES = {
    **gus_publications.bronze_activities(SOURCE, _entities),
    "bronze.load": lambda p: bronze_load(_entities()),
}
