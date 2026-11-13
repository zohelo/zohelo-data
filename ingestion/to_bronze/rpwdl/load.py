#!/usr/bin/env python3
"""RPWDL bronze.load: newest Landing copy of each register ZIP → five Bronze tables, allowlisted columns only.

Healthcare-provider ZIPs (healthcare_provider_active / _inactive) feed bronze.rpwdl_providers (podmioty.csv),
rpwdl_establishments (zaklady.csv), rpwdl_units (jednostki.csv) and rpwdl_cells (komorki.csv); the 16 practice ZIPs
feed bronze.rpwdl_practices (praktyki.csv). One Bronze row per CSV row, values as source text (the text NULL
included), provenance columns and the CSV row number. Privacy allowlist (register PLS-020): only the columns in
the maps below are kept; names, NIP, decision numbers, street addresses, postcodes, phone, e-mail and web
addresses are never read into Bronze, and the members wspolnicy.csv (practitioners' names, licence numbers) and
adresy_swiadczen.csv (places of practice) are not read at all; swiadczenia, kontrole and akredyt_* are left out.
Every needed member's header must equal the expected header exactly, else the ZIP is rejected
(bronze.rpwdl_rejected_files) for all its tables. DuckDB reads the CSVs (no per-cell Python). Code shared with
other bulk sources: ingestion/common/bulk_bronze.py.
"""

from __future__ import annotations

import io
from pathlib import Path
import tempfile
from typing import Any
import zipfile

import duckdb
import pyarrow as pa

from ingestion.common import bulk_bronze

SOURCE = "rpwdl"
REJECTED = "rpwdl_rejected_files"
PRACTICE_TYPES = [
    "physician_practice",
    "group_physician_practice",
    "nursing_practice",
    "group_nursing_practice",
    "physiotherapy_practice",
    "group_physiotherapy_practice",
    "diagnostic_practice",
    "group_diagnostic_practice",
]
PROVIDER_ENTITIES = ["healthcare_provider_active", "healthcare_provider_inactive"]
PRACTICE_ENTITIES = [f"{t}_{state}" for t in PRACTICE_TYPES for state in ("active", "inactive")]

_DATES_SUSPENSION = ["Początek okresu zawieszenia", "Koniec okresu zawieszenia"]
_ADDRESS = [
    "Ulica",
    "Budynek",
    "Lokal",
    "Kod pocztowy",
    "Miejscowość",
    "Telefon",
    "Email",
    "Strona WWW",
    "Kod SIMC",
    "Kod ulicy",
]
HEADERS = {
    "podmioty.csv": [
        "ID Księgi",
        "NIP",
        "REGON",
        "Numer księgi rejestrowej",
        "Data wykreślenia z rejestru",
        "Data wydania decyzji o wykreśleniu z rejestru",
        "Data zmiany wpisu do rejestru",
        "Numer decyzji",
        "Typ organu",
        "Kod organu",
        "Forma organizacyjno-prawna",
        "Forma organizacyjno-prawna opis",
        "Nazwa",
        "Nazwa podmiotu",
        "Teryt",
        "Teryt opis",
        *_ADDRESS,
        "Kod podmiotu tworzącego",
        "Nazwa podmiotu tworzącego",
        "Data rozpoczęcia działalności",
        "Data rozp. dział. art. 104",
        "Data zakończenia działalności",
        *_DATES_SUSPENSION,
    ],
    "zaklady.csv": [
        "ID Księgi",
        "ID ZOZ",
        "Regon",
        "Rodzaj działalności",
        "Nazwa",
        "Teryt",
        "Teryt opis",
        *_ADDRESS,
        "Data rozpoczęcia działalności",
        "Data zakończenia działalności",
    ],
    "jednostki.csv": [
        "ID Księgi",
        "ID ZOZ",
        "ID Jednostki",
        "Regon",
        "Kod jednostki",
        "Nazwa",
        "Teryt",
        "Teryt opis",
        "Ulica",
        "Budynek",
        "Lokal",
        "Kodpocztowy",
        *_ADDRESS[4:],
        "Data rozpoczęcia działalności JO",
        "Data zakończenia działalności JO",
        *_DATES_SUSPENSION,
    ],
    "komorki.csv": [
        "ID Księgi",
        "ID ZOZ",
        "ID Jednostki",
        "ID Komórki",
        "Regon",
        "Kod jednostki",
        "Kod komórki",
        "Specjalność komórki",
        "kodResortVIII",
        "Nazwa komórki",
        "Teryt",
        "Teryt opis",
        *_ADDRESS,
        "Liczba łóżek ogółem",
        "Liczba łóżek intensywnej opieki medycznej",
        "Liczba łóżek intensywnego nadzoru kardiologicznego",
        "Liczba łóżek dla noworodków",
        "Liczba inkubatorów",
        "Liczba łóżek intensywnej terapii",
        "Liczba łóżek intensywnej opieki oparzeń",
        "Liczba łóżek intensywnej opieki toksykologicznej",
        "Liczba łóżek intensywnej terapii noworodka",
        "Liczba łóżek opieki ciągłej dla noworodków po sztucznej wentylacji",
        "Liczba łóżek opieki pośredniej dla noworodków niewymagających wsparcia oddechowego",
        "Liczba łóżek nieinwazyjnej wentylacji mechanicznej",
        "Liczba stanowisk dializacyjnych",
        "Liczba miejsc pobytu dziennego",
        "Data rozpoczęcia działalności komórki",
        "Data zakończenia działalności komórki",
        *_DATES_SUSPENSION,
    ],
    "praktyki.csv": [
        "Id rejestru",
        "Typ praktyki",
        "Numer uchwały wykr.",
        "Data uchwały wykr.",
        "Numer ksiegi",
        "Nazwa",
        "Izba kod",
        "Izba opis",
        "Numer wpisu do rejestru przedsiebiorcow",
        "Forma organizacyjno - prawna kod",
        "Forma organizacyjno - prawna opis",
        *[
            f"Adres korespondencyjny - {c}"
            for c in (
                "Teryt",
                "Teryt opis",
                "Ulica",
                "Budynek",
                "Lokal",
                "Kod pocztowy",
                "Poczta",
                "Miejscowość",
                "Kod SIMC",
                "Kod ulicy",
            )
        ],
        "Data rozp. dzialalnosci",
        "Data rozp. dzialalnosci art. 104",
        "Data zmiany wpisu do rejestru",
        "Data zak. dzialalnosci",
    ],
}
# Known variants: the physiotherapy registers add "Adres korespondencyjny - Województwo" after the Teryt (Dev probe
# 43336929057); the kept columns are the same in both.
VARIANTS = {member: [header] for member, header in HEADERS.items()}
VARIANTS["praktyki.csv"].append(
    [*HEADERS["praktyki.csv"][:12], "Adres korespondencyjny - Województwo", *HEADERS["praktyki.csv"][12:]]
)
# Bronze table → (member, entities, {source column: Bronze column}); everything not listed is never read.
TABLES = {
    "rpwdl_providers": (
        "podmioty.csv",
        PROVIDER_ENTITIES,
        {
            "ID Księgi": "book_id",
            "Numer księgi rejestrowej": "book_number",
            "REGON": "regon",
            "Data wykreślenia z rejestru": "removal_date",
            "Data zmiany wpisu do rejestru": "entry_change_date",
            "Typ organu": "authority_type",
            "Kod organu": "authority_code",
            "Forma organizacyjno-prawna": "legal_form_code",
            "Forma organizacyjno-prawna opis": "legal_form_name",
            "Teryt": "teryt",
            "Kod podmiotu tworzącego": "founder_code",
            "Nazwa podmiotu tworzącego": "founder_name",
            "Data rozpoczęcia działalności": "start_date",
            "Data zakończenia działalności": "end_date",
            "Początek okresu zawieszenia": "suspension_start",
            "Koniec okresu zawieszenia": "suspension_end",
        },
    ),
    "rpwdl_establishments": (
        "zaklady.csv",
        PROVIDER_ENTITIES,
        {
            "ID Księgi": "book_id",
            "ID ZOZ": "establishment_id",
            "Regon": "regon",
            "Rodzaj działalności": "activity_kind",
            "Teryt": "teryt",
            "Data rozpoczęcia działalności": "start_date",
            "Data zakończenia działalności": "end_date",
        },
    ),
    "rpwdl_units": (
        "jednostki.csv",
        PROVIDER_ENTITIES,
        {
            "ID Księgi": "book_id",
            "ID ZOZ": "establishment_id",
            "ID Jednostki": "unit_id",
            "Regon": "regon",
            "Kod jednostki": "unit_code",
            "Teryt": "teryt",
            "Data rozpoczęcia działalności JO": "start_date",
            "Data zakończenia działalności JO": "end_date",
            "Początek okresu zawieszenia": "suspension_start",
            "Koniec okresu zawieszenia": "suspension_end",
        },
    ),
    "rpwdl_cells": (
        "komorki.csv",
        PROVIDER_ENTITIES,
        {
            "ID Księgi": "book_id",
            "ID ZOZ": "establishment_id",
            "ID Jednostki": "unit_id",
            "ID Komórki": "cell_id",
            "Regon": "regon",
            "Kod jednostki": "unit_code",
            "Kod komórki": "cell_code",
            "Specjalność komórki": "specialty_name",
            "kodResortVIII": "specialty_code",
            "Teryt": "teryt",
            "Liczba łóżek ogółem": "beds_total",
            "Liczba łóżek intensywnej terapii": "beds_intensive_care",
            "Liczba stanowisk dializacyjnych": "dialysis_stations",
            "Liczba miejsc pobytu dziennego": "day_places",
            "Data rozpoczęcia działalności komórki": "start_date",
            "Data zakończenia działalności komórki": "end_date",
            "Początek okresu zawieszenia": "suspension_start",
            "Koniec okresu zawieszenia": "suspension_end",
        },
    ),
    "rpwdl_practices": (
        "praktyki.csv",
        PRACTICE_ENTITIES,
        {
            "Id rejestru": "practice_id",
            "Typ praktyki": "practice_kind",
            "Data uchwały wykr.": "removal_date",
            "Numer ksiegi": "book_number",
            "Izba kod": "chamber_code",
            "Izba opis": "chamber_name",
            "Forma organizacyjno - prawna kod": "legal_form_code",
            "Forma organizacyjno - prawna opis": "legal_form_name",
            "Adres korespondencyjny - Teryt": "teryt",
            "Data rozp. dzialalnosci": "start_date",
            "Data zmiany wpisu do rejestru": "entry_change_date",
            "Data zak. dzialalnosci": "end_date",
        },
    ),
}
NEEDED = {"provider": ["podmioty.csv", "zaklady.csv", "jednostki.csv", "komorki.csv"], "practice": ["praktyki.csv"]}


def columns(table: str) -> list[tuple[str, str]]:
    return [
        ("entity", "VARCHAR"),
        *[(c, "VARCHAR") for c in TABLES[table][2].values()],
        *bulk_bronze.PROVENANCE,
        ("source_row_number", "BIGINT"),
    ]


def header_of(archive: zipfile.ZipFile, member: str) -> list[str]:
    with archive.open(member) as handle:
        first = handle.readline().decode("utf-8-sig").rstrip("\r\n")
    return [h.strip().strip('"') for h in first.split(";")]


def check_layout(archive: zipfile.ZipFile, entity: str) -> None:
    """Every member a table reads from this ZIP exists with exactly the expected header (ValueError otherwise)."""
    kind = "provider" if entity in PROVIDER_ENTITIES else "practice"
    names = set(archive.namelist())
    for member in NEEDED[kind]:
        if member not in names:
            raise ValueError(f"{entity}: member {member} missing (members: {sorted(names)})")
        found = header_of(archive, member)
        if found not in VARIANTS[member]:
            raise ValueError(f"{entity}: unknown {member} header: {found}")


def decoder(table: str):
    member, _, kept = TABLES[table]
    arrow = bulk_bronze.arrow_schema(columns(table))

    def decode(body: bytes, receipt: dict[str, Any]) -> pa.Table:
        try:
            archive = zipfile.ZipFile(io.BytesIO(body))
        except zipfile.BadZipFile as exc:
            raise ValueError(f"not a ZIP: {exc}") from exc
        check_layout(archive, receipt["entity"])
        prov = bulk_bronze.provenance(receipt)
        with tempfile.TemporaryDirectory(prefix="zohelo-rpwdl-") as temporary:
            path = Path(temporary) / "data.csv"
            with archive.open(member) as source, path.open("wb") as target:
                while chunk := source.read(8 << 20):
                    target.write(chunk)
            con = duckdb.connect()
            con.execute("set preserve_insertion_order = true")
            try:
                table_rows = con.sql(
                    f"""
                    select
                        ? as entity,
                        {", ".join(f'"{src}" as {dst}' for src, dst in kept.items())},
                        ? as raw_sha256,
                        ? as source_version,
                        cast(? as timestamptz) as ingested_at,
                        cast(row_number() over () + 1 as bigint) as source_row_number
                    from read_csv(?, header = true, all_varchar = true, delim = ';', quote = '"', escape = '"',
                                  strict_mode = true, null_padding = false, sample_size = -1)
                    """,
                    params=[prov["entity"], prov["raw_sha256"], prov["source_version"], prov["ingested_at"], str(path)],
                ).to_arrow_table()
            except duckdb.Error as exc:
                raise ValueError(f"{member} not readable: {exc}") from exc
            finally:
                con.close()
        return table_rows.cast(arrow)  # an empty register file (no rows) is loaded as zero rows

    return decode


def load(budget_minutes: float, landing_root: str = "01_landing", namespace: str = "bronze") -> int:
    """budget_minutes is unused: 18 ZIPs, about 104 MB, decoded one member at a time."""
    status = 0
    for table, (_, entities, _) in TABLES.items():
        status |= bulk_bronze.load(
            SOURCE,
            (namespace, table),
            (namespace, REJECTED),
            columns(table),
            decoder(table),
            table.removeprefix("rpwdl_"),
            landing_root=landing_root,
            lake_root="02_bronze" if namespace == "bronze" else namespace,
            specs=[{"entity": e} for e in entities],
        )
    return status
