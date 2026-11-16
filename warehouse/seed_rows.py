"""Code lists kept as rows per source: ``warehouse/seed_rows/<name>/*.csv`` (docs/source-registration.md, D36).

A list every source adds rows to (source, licence, kpi, kpi_measure, kpi_v2_map, unit, unit_map, unit_decimals)
is a folder of CSV files with one header: ``_shared.csv`` holds the rows from before the split and takes no new
ones; a source's rows are in ``<source_system>.csv``. Two pull requests that add rows therefore never edit the
same file. The dbt model ``models/gold/seed_rows/<name>.sql`` reads the folder into ``gold.<name>``; Python reads
it through ``rows()``, which also reads a plain dbt seed (``seeds/<name>.csv``), so a reader need not know which
kind a list is. Reads files only: no R2, no dbt.
"""

from __future__ import annotations

import csv
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
FOLDER = PROJECT / "seed_rows"
SEEDS = PROJECT / "seeds"
SHARED = "_shared"  # file stem of the rows from before the split
META = "seed_rows"  # config meta of the dbt model that reads a folder: its name


def names() -> list[str]:
    """The lists kept per source, in name order."""
    return sorted(path.name for path in FOLDER.iterdir() if path.is_dir())


def files(name: str) -> list[Path]:
    """The CSV files of one list, in file-name order (``_shared.csv`` first); none when it is not kept per source."""
    return sorted((FOLDER / name).glob("*.csv"))


def rows(name: str) -> list[dict[str, str]]:
    """Every row of a code list, as text: the files of its folder in file-name order, else the dbt seed."""
    out: list[dict[str, str]] = []
    for path in files(name) or [SEEDS / f"{name}.csv"]:
        with path.open(encoding="utf-8", newline="") as handle:
            out += list(csv.DictReader(handle))
    return out


def exists(name: str) -> bool:
    """True when the repository holds the list, as a folder of rows or as a dbt seed."""
    return bool(files(name)) or (SEEDS / f"{name}.csv").exists()
