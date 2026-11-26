"""KG PSP fire-service intervention statistics: Landing → Bronze activities (register EX-003).

bronze.load       newest copy of every landed standard-tables ZIP → bronze.kgpsp_interventions_cells: one row per
                  non-empty cell (ingestion/common/xlsx_cells.py, the shared workbook-cell shape with the ZIP member)
                  of the two tables modelled so far, found by the member's file name (MEMBER): table 1 "Zdarzenia wg
                  rodzaju i wielkości" (incidents by type and size) and table 10 "Wypadki z ludźmi podczas pożarów i
                  miejscowych zagrożeń" (casualties: counts of deaths and injured, no persons), each the
                  voivodeship-level workbook (2016-2022) or the one workbook per table (gmina rows, 2023 on); the
                  powiat and gmina copies of 2016-2022 (`1_1`, `1_2`, ...) and the other tables are not read. A ZIP
                  without exactly one workbook of each table, or with one that is not an .xlsx, is rejected →
                  bronze.kgpsp_interventions_rejected_files (a renamed or added member is never guessed)
bronze.verify     ingestion/common/lineage.py --source kgpsp_interventions
landing.archive   ingestion/common/archive.py --source kgpsp_interventions (move to Archive)
archive.prune     ingestion/common/retention.py --source kgpsp_interventions (archive_policy.retention)
"""

from __future__ import annotations

import io
import re
from typing import Any
import zipfile

from ingestion.common import gus_publications

SOURCE = "kgpsp_interventions"
TABLES = ("1", "10")
# Member file names seen 2016-2025 (Dev probes 45745935044, 45746735646): "2016/1_2016.xlsx", "2018/1. Zdarzenia wg
# rodzaju ... województwo.xlsx", "2019/1_Zdarzenia ...", "1_2025.xlsx", "tabele_stale_wrzucone/1.xlsx"; not
# "1_1_2016.xlsx", "1_2. Zdarzenia ...", "1a_2016.xlsx", "11_2016.xlsx".
MEMBER = re.compile(r"^(1|10)(?:\.|\s|_(?![12][_.]))")


def members(body: bytes) -> dict[str, tuple[str, bytes]]:
    """{table: (member path, bytes)} of the modelled tables; ValueError unless each is exactly one .xlsx member."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not a ZIP: {exc}") from exc
    found: dict[str, list[str]] = {t: [] for t in TABLES}
    for info in archive.infolist():
        match = MEMBER.match(info.filename.rsplit("/", 1)[-1])
        if match and not info.is_dir():
            found[match.group(1)].append(info.filename)
    wrong = {t: names for t, names in found.items() if len(names) != 1 or not names[0].lower().endswith(".xlsx")}
    if wrong:
        raise ValueError(f"expected one .xlsx member per table, found {wrong}")
    return {t: (names[0], archive.read(names[0])) for t, names in found.items()}


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: every non-empty cell of the table 1 and table 10 workbooks, in the shared cell shape."""
    from ingestion.common import bulk_bronze, xlsx_cells

    rows, base = [], bulk_bronze.provenance(receipt)
    for _table, (member, data) in sorted(members(body).items()):
        try:
            cells = list(xlsx_cells.cells(data))
        except (zipfile.BadZipFile, KeyError) as exc:
            raise ValueError(f"member {member!r} is not an XLSX workbook: {exc}") from exc
        if not cells:
            raise ValueError(f"member {member!r} without cells")
        rows += [{**base, "member": member, **c} for c in cells]
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


ACTIVITIES = {
    **gus_publications.bronze_activities(SOURCE, _entities),
    "bronze.load": lambda p: bronze_load(_entities()),
}
