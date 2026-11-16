"""KG PSP fire-service intervention statistics: Landing → Bronze activities (register EX-003).

bronze.load       newest copy of every landed standard-tables ZIP → bronze.kgpsp_interventions_cells
                  (one row per non-empty cell of every workbook member: .xlsx read by ingestion/common/xlsx_cells.py,
                  old .xls read with xlrd as in mrpips_social_assistance; a ZIP inside the ZIP is opened too, the
                  member path joined with '!'); a file without any workbook member is rejected
                  → bronze.kgpsp_interventions_rejected_files
bronze.verify     ingestion/common/lineage.py --source kgpsp_interventions
landing.archive   ingestion/common/archive.py --source kgpsp_interventions (move to Archive)
archive.prune     ingestion/common/retention.py --source kgpsp_interventions (archive_policy.retention)
"""

from __future__ import annotations

import io
from typing import Any
import zipfile

from ingestion.common import gus_publications

SOURCE = "kgpsp_interventions"
XLS_KINDS = {1: "s", 2: "n", 3: "n", 4: "b", 5: "e"}  # xlrd cell types: text, number, date (serial), boolean, error


def _xls_cells(data: bytes):
    """Every non-empty cell of an .xls workbook in the xlsx_cells shape (number_format empty)."""
    import xlrd

    try:
        book = xlrd.open_workbook(file_contents=data, on_demand=True)
    except xlrd.XLRDError as exc:
        raise ValueError(f"not an .xls workbook: {exc}") from exc
    for index, name in enumerate(book.sheet_names(), start=1):
        sheet = book.sheet_by_name(name)
        for r in range(sheet.nrows):
            for c in range(sheet.ncols):
                cell = sheet.cell(r, c)
                if cell.ctype not in XLS_KINDS or cell.value in ("", None):
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
                yield {
                    "sheet_index": index,
                    "sheet_name": name,
                    "row_number": r + 1,
                    "column_number": c + 1,
                    "cell_ref": xlrd.formula.cellname(r, c),
                    "cell_type": XLS_KINDS[cell.ctype],
                    "value_text": text,
                    "number_format": None,
                }
        book.unload_sheet(name)


def workbooks(body: bytes, prefix: str = "") -> list[tuple[str, bytes]]:
    """(member path, bytes) of every .xlsx / .xls member of the ZIP, inner ZIPs opened, in name order."""
    out: list[tuple[str, bytes]] = []
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        for info in sorted(archive.infolist(), key=lambda i: i.filename):
            low = info.filename.lower()
            if info.is_dir():
                continue
            if low.endswith(".zip"):
                out += workbooks(archive.read(info.filename), prefix + info.filename + "!")
            elif low.endswith((".xlsx", ".xls")):
                out.append((prefix + info.filename, archive.read(info.filename)))
    return out


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: every non-empty cell of every workbook member, in the shared cell shape."""
    from ingestion.common import bulk_bronze, xlsx_cells

    try:
        members = workbooks(body)
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not a ZIP: {exc}") from exc
    if not members:
        raise ValueError("ZIP without an .xlsx or .xls workbook")
    rows, base = [], bulk_bronze.provenance(receipt)
    for member, data in members:
        try:
            cells = list(xlsx_cells.cells(data)) if data[:2] == b"PK" else list(_xls_cells(data))
        except (zipfile.BadZipFile, KeyError) as exc:
            raise ValueError(f"member {member!r} is not a workbook: {exc}") from exc
        rows += [{**base, "member": member, **c} for c in cells]
    if not rows:
        raise ValueError("workbooks without cells")
    rows = [{**r, "source_row_number": i} for i, r in enumerate(rows, 1)]
    return bulk_bronze.pa.Table.from_pylist(rows, schema=bulk_bronze.arrow_schema(gus_publications.bronze_columns()))


def bronze_load(entities: list[str], landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """bronze.load: the newest copy of every landed year → bronze.kgpsp_interventions_cells."""
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
    from ingestion.to_landing.kgpsp_interventions import held

    return sorted(held(landing_root))


def _load(p) -> int:
    return bronze_load(_entities())


ACTIVITIES = {**gus_publications.bronze_activities(SOURCE, _entities), "bronze.load": _load}
