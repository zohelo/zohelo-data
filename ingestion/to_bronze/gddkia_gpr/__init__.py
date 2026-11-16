"""GDDKiA Generalny Pomiar Ruchu: Landing → Bronze activities (register PL-ENV-022).

bronze.load       newest copy of every landed file → bronze.gddkia_gpr_cells: one row per non-empty cell, in the
                  shared workbook-cell shape (ingestion/common/gus_publications.py bronze_columns, member empty);
                  .xlsx through ingestion/common/xlsx_cells.py, the 2020/21 .xls files through xlrd
                  (requirements.txt). A file that is neither workbook is rejected →
                  bronze.gddkia_gpr_rejected_files
bronze.verify     ingestion/common/lineage.py --source gddkia_gpr
landing.archive   ingestion/common/archive.py --source gddkia_gpr (move to Archive)
archive.prune     ingestion/common/retention.py --source gddkia_gpr (archive_policy.retention)
"""

from __future__ import annotations

from typing import Any

from ingestion.common import gus_publications

SOURCE = "gddkia_gpr"
KINDS = {1: "s", 2: "n", 3: "n", 4: "b", 5: "e"}  # xlrd cell types: text, number, date (serial), boolean, error


def xls_cells(body: bytes):
    """Every non-empty cell of an .xls workbook as (sheet index, sheet name, row, column, cell ref, kind, text).
    Numbers keep their stored float (integers without '.0'); error cells keep their Excel text (#N/A), never a
    number. ``ignore_workbook_corruption``: the 2020/21 national-roads workbook has an inconsistent OLE sector
    table that xlrd refuses by default; the cell records themselves read in full (Dev probe, see the source YAML)."""
    import xlrd

    try:
        book = xlrd.open_workbook(file_contents=body, on_demand=True, ignore_workbook_corruption=True)
    except (xlrd.XLRDError, AssertionError) as exc:
        raise ValueError(f"not a readable .xls workbook: {type(exc).__name__}: {exc}") from exc
    for index, name in enumerate(book.sheet_names(), start=1):
        sheet = book.sheet_by_name(name)
        for r in range(sheet.nrows):
            for c in range(sheet.ncols):
                cell = sheet.cell(r, c)
                if cell.ctype not in KINDS or cell.value in ("", None):
                    continue
                value = cell.value
                if cell.ctype == 5:
                    text = xlrd.error_text_from_code.get(value, "#ERR")
                elif isinstance(value, float):
                    text = str(int(value)) if value.is_integer() else repr(value)
                else:
                    text = str(value)
                if text.strip():
                    yield index, name, r + 1, c + 1, xlrd.formula.cellname(r, c), KINDS[cell.ctype], text
        book.unload_sheet(name)


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: an XLSX workbook through the shared reader, else an .xls workbook through xlrd."""
    from ingestion.common import bulk_bronze

    if body[:2] == b"PK":
        return gus_publications.decode(body, receipt)
    base, rows = bulk_bronze.provenance(receipt), []
    for n, (index, name, row, col, ref, kind, text) in enumerate(xls_cells(body), start=1):
        rows.append(
            {
                **base,
                "member": "",
                "sheet_index": index,
                "sheet_name": name,
                "row_number": row,
                "column_number": col,
                "cell_ref": ref,
                "cell_type": kind,
                "value_text": text,
                "number_format": None,
                "source_row_number": n,
            }
        )
    if not rows:
        raise ValueError("workbook without cells")
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


def _entities() -> list[str]:
    from ingestion.to_landing import gddkia_gpr as landing

    return sorted(landing.held())


ACTIVITIES = {
    **gus_publications.bronze_activities(SOURCE, _entities),
    "bronze.load": lambda p: bronze_load(_entities()),
}
