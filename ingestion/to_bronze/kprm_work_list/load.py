#!/usr/bin/env python3
"""KPRM work list bronze.load: newest Landing copy of the work-list CSV → bronze.kprm_work_list_drafts.

The file is "Pobierz dane do pliku csv" of https://www.gov.pl/web/premier/wplip-rm (Wykaz prac legislacyjnych i
programowych Rady Ministrów): UTF-8 (a BOM is accepted, the file of 12 Nov 2026 has none), semicolon-separated,
CRLF rows, values in double quotes that hold line breaks and semicolons, one row per draft, the 19 columns of
HEADER. DuckDB reads it (no per-cell Python) with quotes honoured and strict mode on, so a row that is not exactly
19 fields wide fails the file; a header other than HEADER (names and order) rejects the file with the header it
found (bronze.kprm_work_list_rejected_files).

Kept, as source text: every column of KEPT, under ASCII names. Left out of Bronze and every later layer (Landing
keeps the exact bytes): `Osoba odpowiedzialna` (names of officials), the two long text columns (aims and substance
of the draft, up to 91,000 characters) and the three regulatory-impact columns (empty in every row on 12 Nov 2026,
long text when filled). Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "kprm_work_list"
TABLE_ID = ("bronze", "kprm_work_list_drafts")
REJECTED_ID = ("bronze", "kprm_work_list_rejected_files")
# The published header, in order (Dev probe 43320804384, 12 Nov 2026).
HEADER = [
    "Numer projektu",
    "Tytuł",
    "Rodzaj dokumentu",
    "Typ dokumentu",
    "Informacja dodatkowa",
    "Cele projektu oraz informacja o przyczynach i potrzebie rozwiązań planowanych w projekcie",
    "Istota rozwiązań planowanych w projekcie, w tym proponowane środki realizacji",
    "Oddziaływanie na życie społeczne nowych regulacji prawnych",
    "Spodziewane skutki i następstwa projektowanych regulacji prawnych",
    "Sposoby mierzenia efektów nowych regulacji prawnych",
    "Organ odpowiedzialny za opracowanie projektu",
    "Organ współpracujący przy opracowaniu projektu",
    "Osoba odpowiedzialna",
    "Organ odpowiedzialny",
    "Planowane przyjęcie przez RM",
    "Informacja o rezygnacji z prac nad projektem",
    "Status realizacji",
    "Data publikacji",
    "Podgląd",
]
# Source column → Bronze column; the other HEADER columns are not loaded (module docstring).
KEPT = {
    "Numer projektu": "numer_projektu",
    "Tytuł": "tytul",
    "Rodzaj dokumentu": "rodzaj_dokumentu",
    "Typ dokumentu": "typ_dokumentu",
    "Informacja dodatkowa": "informacja_dodatkowa",
    "Organ odpowiedzialny za opracowanie projektu": "organ_opracowujacy",
    "Organ współpracujący przy opracowaniu projektu": "organ_wspolpracujacy",
    "Organ odpowiedzialny": "organ_odpowiedzialny",
    "Planowane przyjęcie przez RM": "planowane_przyjecie",
    "Informacja o rezygnacji z prac nad projektem": "informacja_o_rezygnacji",
    "Status realizacji": "status_realizacji",
    "Data publikacji": "data_publikacji",
    "Podgląd": "podglad",
}
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in KEPT.values()],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    text = body.decode("utf-8-sig")  # UnicodeDecodeError rejects the file; a BOM is dropped
    first_line = text.split("\r\n", 1)[0]
    names = [name.strip().strip('"') for name in first_line.split(";")]
    if names != HEADER:
        raise ValueError(f"unknown work-list header ({len(names)} columns): {names}")
    prov = bulk_bronze.provenance(receipt)
    with tempfile.TemporaryDirectory(prefix="zohelo-kprm-") as temporary:
        path = Path(temporary) / "data.csv"
        path.write_text(text, encoding="utf-8", newline="")
        con = duckdb.connect()
        con.execute("set preserve_insertion_order = true")
        columns = ", ".join(f"'c{i}': 'VARCHAR'" for i in range(len(HEADER)))
        selected = ", ".join(f"c{HEADER.index(source)} as {target}" for source, target in KEPT.items())
        try:  # columns by position, named by HEADER, which the header check above has matched
            table = con.sql(
                f"""
                select
                    ? as entity,
                    {selected},
                    ? as raw_sha256,
                    ? as source_version,
                    cast(? as timestamptz) as ingested_at,
                    cast(row_number() over () as bigint) as source_row_number
                from read_csv(?, header = true, auto_detect = false, columns = {{{columns}}}, delim = ';',
                              quote = '"', escape = '"', strict_mode = true, null_padding = false,
                              max_line_size = 4000000, parallel = false)
                """,
                params=[prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"], str(path)],
            ).to_arrow_table()
        except duckdb.Error as exc:  # a row not 19 fields wide, an unclosed quote: the file is rejected with this
            reason = str(exc).split("Original Line", 1)[0].strip()  # the line itself may name officials: not kept
            raise ValueError(f"work-list CSV does not parse as 19 quoted fields per row: {reason[:600]}") from exc
        finally:
            con.close()
    if not table.num_rows:
        raise ValueError("no drafts")
    return table.cast(ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: one CSV file of about 11 MB."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "drafts",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
