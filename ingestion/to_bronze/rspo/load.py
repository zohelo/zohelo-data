#!/usr/bin/env python3
"""RSPO bronze.load: newest Landing copy of each yearly school list → bronze.rspo_institutions, allowlisted columns.

Each list (ingestion/to_landing/rspo/) is one XLSX workbook with one sheet ``Arkusz1``: row 1 the 58 column names
of HEADER, then one row per register entry (school, institution, complex or branch) with its RSPO number. The sheet
is read with DuckDB's excel extension (``read_xlsx``, every cell as text: strings as stored, numbers as DuckDB prints
the stored double). One Bronze row per sheet row, values as source text (an empty cell is NULL or '' as stored),
the sheet row number and provenance.

Privacy allowlist (register PLS-021: contact and person-related fields are minimised): only the columns of KEPT are
read. Never read: the institution's name, street, house and flat number, postcode, post office, locality and web
address, its REGON, the governing body's id and name (7,742 of 55,741 entries of 2025 are run by a natural person)
and the registering body's name; the area names are left out because the TERYT codes are kept.

Rejected (bronze.rspo_rejected_files): a workbook without exactly one sheet ``Arkusz1``, a header that is not HEADER,
a school year that is not the entity's, or fewer rows read than the sheet's XML holds (read_xlsx stops at the first
empty row). Code shared with other small bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import io
from pathlib import Path
import tempfile
from typing import Any
import xml.etree.ElementTree as ET
import zipfile

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "rspo"
TABLE_ID = ("bronze", "rspo_institutions")
REJECTED_ID = ("bronze", "rspo_rejected_files")
SHEET = "Arkusz1"
# The header of the lists 2020-2025 (Dev probe 45748207131: the same 58 names in the same order in all six).
# fmt: off
HEADER = [
    "Rok szkolny", "idTerytWojewodztwo", "Wojewodztwo", "idTerytPowiat", "Powiat", "idTerytGmina", "Gmina",
    "Typ gminy", "Miejscowość", "Czy szkoła", "idPodmiotGlowny", "idPodmiotNadrzedny", "idTypPodmiotuNadrzednego",
    "Typ podmiotu nadrzędnego", "RSPO", "idTypPodmiotu", "Typ podmiotu", "idRodzajPlacowki", "Rodzaj szkoły/placówki",
    "Nazwa placówki", "Ulica", "Numer domu", "Numer lokalu", "Kod pocztowy", "Poczta", "Adres www", "regon",
    "idPublicznosc", "Publiczność", "idKategoriaUczniow", "Kategoria uczniów", "idSpecyfikaSzkoly", "Specyfika szkoły",
    "idZwiazanieOrganizacyjneSzkoly", "Związanie organizacyjne", "idTypOrgProw", "Typ organu prowadzącego",
    "typ_org_prow", "idOrganProwadzacy", "Organ prowadzący", "idTerytWojewOrgRej", "Organ rejestrującey - województwo",
    "idTerytPowiatOrgRej", "Organ rejestrujący - powiat", "idTerytGminaOrgRej", "Organ rejestrujący - gmina",
    "idTypOrgRej", "Typ organu rejestrującego", "Organ rejestrujący", "ucz_ogolem", "w tym_ucz_dziewczeta",
    "w tym_w oddz_przedszk", "lb_oddz", "etatUmowa", "w tym_etat_niepełnozatr", "lb_naucz_osoby",
    "w tym_lb_naucz_pelnozatr", "w tym_lb_naucz_niepelnozatr",
]
# fmt: on
# Source column → Bronze column; every other column is never read.
KEPT = {
    "Rok szkolny": "school_year",
    "idTerytWojewodztwo": "voivodeship_teryt",
    "idTerytPowiat": "powiat_teryt",
    "idTerytGmina": "gmina_teryt",
    "Typ gminy": "gmina_type",
    "Czy szkoła": "is_school",
    "idPodmiotGlowny": "main_entity_id",
    "idPodmiotNadrzedny": "parent_entity_id",
    "idTypPodmiotuNadrzednego": "parent_type_id",
    "Typ podmiotu nadrzędnego": "parent_type_name",
    "RSPO": "rspo_number",
    "idTypPodmiotu": "institution_type_id",
    "Typ podmiotu": "institution_type_name",
    "idRodzajPlacowki": "unit_kind_id",
    "Rodzaj szkoły/placówki": "unit_kind_name",
    "idPublicznosc": "public_status_id",
    "Publiczność": "public_status_name",
    "idKategoriaUczniow": "pupil_category_id",
    "Kategoria uczniów": "pupil_category_name",
    "idSpecyfikaSzkoly": "school_specificity_id",
    "Specyfika szkoły": "school_specificity_name",
    "idZwiazanieOrganizacyjneSzkoly": "organisational_link_id",
    "Związanie organizacyjne": "organisational_link_name",
    "idTypOrgProw": "governing_body_type_id",
    "Typ organu prowadzącego": "governing_body_type_name",
    "typ_org_prow": "governing_body_class",
    "idTerytWojewOrgRej": "registering_voivodeship_teryt",
    "idTerytPowiatOrgRej": "registering_powiat_teryt",
    "idTerytGminaOrgRej": "registering_gmina_teryt",
    "idTypOrgRej": "registering_body_type_id",
    "Typ organu rejestrującego": "registering_body_type_name",
    "ucz_ogolem": "pupils",
    "w tym_ucz_dziewczeta": "pupils_girls",
    "w tym_w oddz_przedszk": "pupils_preschool_classes",
    "lb_oddz": "classes",
    "etatUmowa": "teacher_fte",
    "w tym_etat_niepełnozatr": "teacher_fte_part_time",
    "lb_naucz_osoby": "teachers",
    "w tym_lb_naucz_pelnozatr": "teachers_full_time",
    "w tym_lb_naucz_niepelnozatr": "teachers_part_time",
}
COLUMNS = [
    ("entity", "VARCHAR"),
    *[(name, "VARCHAR") for name in KEPT.values()],
    *bulk_bronze.PROVENANCE,
    ("source_row_number", "BIGINT"),
]
ARROW = bulk_bronze.arrow_schema(COLUMNS)


def sheet_rows(body: bytes) -> int:
    """Data rows of the only sheet: <row> elements holding at least one cell, counted in the sheet's XML (streamed),
    minus the header row. Also checks the workbook has exactly one sheet, named SHEET."""
    from ingestion.common import xlsx_cells

    try:
        with zipfile.ZipFile(io.BytesIO(body)) as workbook:
            sheets = xlsx_cells.sheets(workbook)
            if [name for name, _ in sheets] != [SHEET]:
                raise ValueError(f"expected one sheet {SHEET!r}, found {[name for name, _ in sheets]}")
            rows = 0
            with workbook.open(sheets[0][1]) as handle:
                for _event, element in ET.iterparse(handle, events=("end",)):
                    if element.tag == xlsx_cells.MAIN + "row":
                        rows += 1 if len(element) else 0
                        element.clear()
    except zipfile.BadZipFile as exc:
        raise ValueError(f"not a valid workbook: {exc}") from exc
    return rows - 1


def school_year(entity: str) -> str:
    """The school year a list's entity stands for: school_list_2025 → '2025/2026'."""
    year = int(entity.rsplit("_", 1)[-1])
    return f"{year}/{year + 1}"


def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
    entity = receipt["entity"]
    if not entity.startswith("school_list_"):
        raise ValueError(f"no declared layout for entity {entity!r}")
    declared = sheet_rows(body)
    with tempfile.TemporaryDirectory(prefix="zohelo-rspo-") as temporary:
        path = Path(temporary) / "list.xlsx"
        path.write_bytes(body)
        con = duckdb.connect()
        con.execute("SET memory_limit = '4GB'; SET preserve_insertion_order = true")
        con.execute("INSTALL excel; LOAD excel;")
        rel = f"read_xlsx('{path}', sheet = '{SHEET}', header = true, all_varchar = true)"
        try:
            found = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()]
            if [f.strip() for f in found] != HEADER:
                raise ValueError(f"{entity}: header is not the declared layout: {found}")
            by_name = {f.strip(): f for f in found}
            select = ", ".join(f'"{by_name[src]}" AS {dst}' for src, dst in KEPT.items())
            rows = con.sql(f"SELECT {select} FROM {rel}").to_arrow_table()
            years = [r[0] for r in con.execute(f'SELECT DISTINCT "{by_name["Rok szkolny"]}" FROM {rel}').fetchall()]
        except duckdb.Error as exc:  # an unreadable workbook is a rejected file, not a failed run
            raise ValueError(f"{entity}: read_xlsx failed: {str(exc)[:300]}") from exc
        finally:
            con.close()
    count = rows.num_rows
    if count != declared:
        raise ValueError(f"{entity}: read {count} rows, the sheet declares {declared}")
    if count == 0:
        raise ValueError(f"{entity}: no rows")
    if years != [school_year(entity)]:
        raise ValueError(f"{entity}: school years {years}, expected only {school_year(entity)!r}")
    prov = bulk_bronze.provenance(receipt)
    data = {name: [prov[name]] * count for name in ("entity", "raw_sha256", "source_version", "ingested_at")}
    data.update({name: rows.column(name).cast(pa.string()) for name in KEPT.values()})
    data["source_row_number"] = list(range(2, count + 2))  # sheet row (row 1 is the header), scan order kept
    return pa.Table.from_pydict({name: data[name] for name, _ in COLUMNS}, schema=ARROW)


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze", specs=None) -> int:
    """budget_minutes is unused: six workbooks of about 18 MB, decoded in well under a minute each."""
    from ingestion.to_landing import rspo as landing

    if specs is None:  # the landed entities (the yearly lists are found on dane.gov.pl, not declared)
        specs = [{"entity": e} for e in sorted(landing.held(landing_root))]
    return bulk_bronze.load(
        SOURCE,
        (namespace, TABLE_ID[1]),
        (namespace, REJECTED_ID[1]),
        COLUMNS,
        decode,
        "institutions",
        landing_root=landing_root,
        lake_root="02_bronze" if namespace == "bronze" else namespace,
        specs=specs,
    )
