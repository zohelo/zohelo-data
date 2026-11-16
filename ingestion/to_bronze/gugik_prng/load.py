#!/usr/bin/env python3
"""GUGiK PRNG bronze.load: newest Landing copy of each declared package → bronze.prng_names.

Each package (ingestion/sources/gugik_prng.yaml) is a ZIP of one data workbook ``PRNG_<PART>_XLSX.xlsx`` with one
sheet ``PRNG_<PART>_XLS`` (row 1 the attribute names, then one row per register object), a legend workbook
``PRNG_legenda_nazwy kolumn_XLS SHP GML.XLSX`` and an empty member ``Export danych PRNG z dnia <YYYY-MM-DD>[.txt]``
whose name is the export date. The sheet is read with DuckDB's excel extension (``read_xlsx``, every cell as text:
strings as stored, numbers as DuckDB prints the stored double, e.g. Excel serial dates ``45782.4299537037``). One row
per sheet row, the attributes under ASCII names (ATTRIBUTES; the physiographic sheet has no ``przymiotnik`` and no
parent-locality columns, which stay null), the export date, the sheet row number and provenance.

Rejected (bronze.prng_rejected_files): a ZIP without exactly one data workbook, a sheet name or header row that is
not the declared layout of its entity, no export-date member, or fewer rows read than the sheet's XML holds (no
silent truncation: read_xlsx stops at the first empty row). Code shared with other small bulk sources:
ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import io
from pathlib import Path
import re
import tempfile
from typing import Any
import xml.etree.ElementTree as ET
import zipfile

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "gugik_prng"
TABLE_ID = ("bronze", "prng_names")
REJECTED_ID = ("bronze", "prng_rejected_files")
# Header text of the sheets (12 Nov 2026) → Bronze column. Order of the localities sheet.
ATTRIBUTES = {
    "identyfikator IIP": "identyfikator_iip",
    "identyfikator PRNG": "identyfikator_prng",
    "nazwa główna": "nazwa_glowna",
    "element rozróżniający": "element_rozrozniajacy",
    "element rodzajowy": "element_rodzajowy",
    "dopełniacz": "dopelniacz",
    "przymiotnik": "przymiotnik",
    "status nazwy": "status_nazwy",
    "kategoria obiektu": "kategoria_obiektu",
    "rodzaj obiektu": "rodzaj_obiektu",
    "nazwa miejscowości nadrzędnej": "nazwa_miejscowosci_nadrzednej",
    "identyfikator miejscowości nadrzędnej": "identyfikator_miejscowosci_nadrzednej",
    "informacje dodatkowe": "informacje_dodatkowe",
    "system zewnętrzny": "system_zewnetrzny",
    "identyfikator zewnętrzny": "identyfikator_zewnetrzny",
    "tytuł dokumentu źródłowego": "tytul_dokumentu_zrodlowego",
    "data dokumentu źródłowego": "data_dokumentu_zrodlowego",
    "wydawca dokumentu źródłowego": "wydawca_dokumentu_zrodlowego",
    "rodzaj reprezentacji": "rodzaj_reprezentacji",
    "współrzędne geograficzne": "wspolrzedne_geograficzne",
    "współrzędne prostokątne XY": "wspolrzedne_prostokatne_xy",
    "nazwa historyczna": "nazwa_historyczna",
    "nazwa oboczna": "nazwa_oboczna",
    "nazwa dodatkowa": "nazwa_dodatkowa",
    "język nazwy dodatkowej": "jezyk_nazwy_dodatkowej",
    "latynizacja nazwy dodatkowej": "latynizacja_nazwy_dodatkowej",
    "endonim": "endonim",
    "język endonimu": "jezyk_endonimu",
    "latynizacja endonimu": "latynizacja_endonimu",
    "egzonim": "egzonim",
    "język egzonimu": "jezyk_egzonimu",
    "latynizacja egzonimu": "latynizacja_egzonimu",
    "województwo": "wojewodztwo",
    "powiat": "powiat",
    "gmina": "gmina",
    "identyfikator gminy": "identyfikator_gminy",
    "początek wersji obiektu": "poczatek_wersji_obiektu",
    "wersja obiektu": "wersja_obiektu",
    "ważna od": "wazna_od",
}
LOCALITY_ONLY = ("przymiotnik", "nazwa miejscowości nadrzędnej", "identyfikator miejscowości nadrzędnej")
# entity → (workbook member, sheet, header row in order)
LAYOUTS = {
    "miejscowosci": ("PRNG_MIEJSCOWOSCI_XLSX.xlsx", "PRNG_MIEJSCOWOSCI_XLS", list(ATTRIBUTES)),
    "obiekty_fizjograficzne": (
        "PRNG_OBIEKTY_FIZJOGRAFICZNE_XLSX.xlsx",
        "PRNG_OBIEKTY_FIZJOGRAFICZNE_XLS",
        [a for a in ATTRIBUTES if a not in LOCALITY_ONLY],
    ),
}
EXPORT = re.compile(r"^Export danych PRNG z dnia (\d{4}-\d{2}-\d{2})(\.txt)?$")
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in ATTRIBUTES.values()],
    ("export_date", "VARCHAR"),
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def _sheet_rows(z: zipfile.ZipFile, member: str, sheet: str) -> int:
    """Data rows of the sheet: <row> elements holding at least one cell, counted in the sheet's XML (streamed), minus
    the header row. The check that read_xlsx did not stop early (it stops at the first empty row)."""
    from ingestion.common import xlsx_cells

    with zipfile.ZipFile(io.BytesIO(z.read(member))) as workbook:
        paths = dict(xlsx_cells.sheets(workbook))
        if sheet not in paths:
            raise ValueError(f"{member}: no sheet {sheet!r}, found {sorted(paths)}")
        rows = 0
        with workbook.open(paths[sheet]) as handle:
            for _event, element in ET.iterparse(handle, events=("end",)):
                if element.tag == xlsx_cells.MAIN + "row":
                    rows += 1 if len(element) else 0
                    element.clear()
    return rows - 1


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    entity = receipt["entity"]
    if entity not in LAYOUTS:
        raise ValueError(f"no declared layout for entity {entity!r}")
    member, sheet, header = LAYOUTS[entity]
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        names = z.namelist()
        books = [n for n in names if n.lower().endswith(".xlsx") and "legenda" not in n.lower()]
        if books != [member]:
            raise ValueError(f"expected one data workbook {member!r}, found {books}")
        exports = [m.group(1) for n in names if (m := EXPORT.match(n.rsplit("/", 1)[-1]))]
        if len(exports) != 1:
            raise ValueError(f"expected one 'Export danych PRNG z dnia <date>' member, found {names}")
        try:
            declared = _sheet_rows(z, member, sheet)
        except zipfile.BadZipFile as exc:
            raise ValueError(f"{member} is not a valid workbook: {exc}") from exc
        with tempfile.TemporaryDirectory(prefix="zohelo-prng-") as temporary:
            path = Path(temporary) / "data.xlsx"
            path.write_bytes(z.read(member))
            con = duckdb.connect()
            con.execute("SET memory_limit = '4GB'; SET preserve_insertion_order = true")
            con.execute("INSTALL excel; LOAD excel;")
            rel = f"read_xlsx('{path}', sheet = '{sheet}', header = true, all_varchar = true)"
            select = ", ".join(
                f'"{a}" AS {c}' if a in header else f"NULL::VARCHAR AS {c}" for a, c in ATTRIBUTES.items()
            )
            try:
                found = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()]
                if found != header:
                    raise ValueError(f"{entity}: header is not the declared layout: {found}")
                rows = con.sql(f"SELECT {select} FROM {rel}").to_arrow_table()
            except duckdb.Error as exc:  # an unreadable workbook is a rejected file, not a failed run
                raise ValueError(f"{entity}: read_xlsx failed: {str(exc)[:300]}") from exc
            finally:
                con.close()
    count = rows.num_rows
    if count != declared:
        raise ValueError(f"{entity}: read {count} rows, the sheet declares {declared}")
    if count == 0:
        raise ValueError(f"{entity}: no rows")
    prov = bulk_bronze.provenance(receipt)
    data = {name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update({name: rows.column(name).cast(pa.string()) for name in ATTRIBUTES.values()})
    data["export_date"] = [exports[0]] * count
    data["source_row_number"] = list(range(2, count + 2))  # sheet row (row 1 is the header), scan order kept
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: two packages of about 23 MB, decoded in a few seconds each."""
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "names",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
    )
