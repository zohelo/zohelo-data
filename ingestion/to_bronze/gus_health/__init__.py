"""GUS Zdrowie i ochrona zdrowia: Landing → Bronze activities (register PLS-004, health family; #2451).

bronze.load       newest copy of every landed edition → bronze.gus_health_cells
                  (one row per workbook cell, the shared shape of ingestion/common/gus_publications.py);
                  rejections → bronze.gus_health_rejected_files
bronze.verify     ingestion/common/lineage.py --source gus_health
landing.archive   ingestion/common/archive.py --source gus_health (move to Archive)
archive.prune     ingestion/common/retention.py --source gus_health (archive_policy.retention)

The decoder is the shared one, except for one thing seen in these files: the ZIP of the 2014 edition holds an Excel
owner file next to its workbooks ("aneks tabelaryczny/~$4. Ratownictwo medyczne,...xlsx", the lock file Excel writes
while a workbook is open; a few bytes, not a workbook). A member whose file name starts with "~$" and that is not a
ZIP container is left out; one that is a ZIP container rejects the file (an unknown layout, never guessed).
"""

from __future__ import annotations

import io
import posixpath
from typing import Any
import zipfile

from ingestion.common import gus_publications

SOURCE = "gus_health"


def decode(body: bytes, receipt: dict[str, Any]):
    """bulk_bronze decoder: the shared one on the file without its Excel owner files ("~$..." members)."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile:
        return gus_publications.decode(body, receipt)
    owners = [i for i in archive.infolist() if posixpath.basename(i.filename).startswith("~$")]
    if not owners or "xl/workbook.xml" in archive.namelist():
        return gus_publications.decode(body, receipt)
    for info in owners:
        if archive.read(info)[:2] == b"PK":
            raise ValueError(f"member {info.filename!r} is named like an Excel owner file but is a ZIP container")
    kept = io.BytesIO()
    with zipfile.ZipFile(kept, "w") as out:
        for info in archive.infolist():
            if info not in owners:
                out.writestr(info, archive.read(info))
    return gus_publications.decode(kept.getvalue(), receipt)


def bronze_load(entities: list[str], landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """bronze.load: the newest copy of every landed edition → bronze.gus_health_cells."""
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
    from ingestion.to_landing.gus_health import SERIES

    return sorted(gus_publications.held(SERIES))


ACTIVITIES = {
    **gus_publications.bronze_activities(SOURCE, _entities),
    "bronze.load": lambda p: bronze_load(_entities()),
}
