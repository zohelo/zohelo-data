"""MRPiPS social assistance statistics: Landing → Bronze activities (register PLS-031).

bronze.load       newest copy of every landed report → bronze.mrpips_social_assistance_cells
                  (one row per cell of the .xls workbook, read with xlrd, or of an .xlsx workbook);
                  a file that is not an .xls workbook is rejected → bronze.mrpips_social_assistance_rejected_files
bronze.verify     ingestion/common/lineage.py --source mrpips_social_assistance
landing.archive   ingestion/common/archive.py --source mrpips_social_assistance (move to Archive)
archive.prune     ingestion/common/retention.py --source mrpips_social_assistance (archive_policy.retention)
"""

from __future__ import annotations

from typing import Any

from ingestion.common import gus_publications

SOURCE = "mrpips_social_assistance"
KINDS = {1: "s", 2: "n", 3: "n", 4: "b", 5: "e"}  # xlrd cell types: text, number, date (serial), boolean, error


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: every non-empty cell of the .xls workbook in the shared cell shape (member empty).
    Numbers keep their stored float (integers without '.0'); dates stay Excel serial numbers, as in xlsx_cells.
    Error cells keep their Excel text (#N/A, #DIV/0!, ...), which Silver's status test rejects.
    A year published as an XLSX workbook (2021) is read with the shared XLSX decoder."""
    import xlrd

    from ingestion.common import bulk_bronze

    if body[:2] == b"PK":
        return gus_publications.decode(body, receipt)
    try:
        book = xlrd.open_workbook(file_contents=body, on_demand=True)
    except xlrd.XLRDError as exc:
        raise ValueError(f"not an .xls workbook: {exc}") from exc
    rows, base, n = [], bulk_bronze.provenance(receipt), 0
    for index, name in enumerate(book.sheet_names(), start=1):
        sheet = book.sheet_by_name(name)
        for r in range(sheet.nrows):
            for c in range(sheet.ncols):
                cell = sheet.cell(r, c)
                if cell.ctype not in KINDS or cell.value in ("", None):
                    continue
                value = cell.value
                if cell.ctype == 5:  # an Excel error cell holds an integer code: keep the error text, never a number
                    text = xlrd.error_text_from_code.get(value, "#ERR")
                elif isinstance(value, float):
                    text = str(int(value)) if value.is_integer() else repr(value)
                else:
                    text = str(value)
                if not text.strip():
                    continue
                n += 1
                rows.append(
                    {
                        **base,
                        "member": "",
                        "sheet_index": index,
                        "sheet_name": name,
                        "row_number": r + 1,
                        "column_number": c + 1,
                        "cell_ref": xlrd.formula.cellname(r, c),
                        "cell_type": KINDS[cell.ctype],
                        "value_text": text,
                        "number_format": None,
                        "source_row_number": n,
                    }
                )
        book.unload_sheet(name)
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
    from ingestion.to_landing import mrpips_social_assistance as landing

    return sorted(landing.held())


ACTIVITIES = {
    **gus_publications.bronze_activities(SOURCE, _entities),
    "bronze.load": lambda p: bronze_load(_entities()),
}
