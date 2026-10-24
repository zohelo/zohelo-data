"""Decode one Landing GUS DBW bulk ZIP into source-shaped Bronze Parquet.

The ZIP holds two semicolon CSVs (seen on every file, 24 Oct 2026):

- ``<entity>.csv``, the data, with a fixed header (rowNumber, id_zmienna, id_przekroj, nine
  dimension/position id pairs, id_okres, id_sposob_prezentacji_miara, id_daty (the year),
  id_brak_wartosci, id_tajnosci, id_flaga, wartosc, precyzja, and a trailing empty column);
- ``<entity without _pl>_Slowniki.csv``, the labels of every id the data uses
  (nazwa_kolumny;nazwa_slownika;id_elementu;opis), loaded as bronze.dbw_dictionary_items.

Bronze keeps it as published: every column as text (``wartosc`` keeps its decimal comma),
plus provenance (entity, version, ingest time, Landing SHA-256, source row number). Casting
and labels are Silver and Gold rules.
"""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from pathlib import Path
import zipfile

import duckdb

SOURCE_COLUMNS = (
    ["rowNumber", "id_zmienna", "id_przekroj"]
    + [f"id_{kind}_{i}" for i in range(1, 10) for kind in ("wymiar", "pozycja")]
    + ["id_okres", "id_sposob_prezentacji_miara", "id_daty", "id_brak_wartosci", "id_tajnosci", "id_flaga"]
    + ["wartosc", "precyzja"]
)
BRONZE_COLUMNS = (
    ("entity_id", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("raw_sha256", "VARCHAR"),
    ("source_row_number", "BIGINT"),
    *((name.lower(), "VARCHAR") for name in SOURCE_COLUMNS),
)


DICTIONARY_SOURCE = ["nazwa_kolumny", "nazwa_slownika", "id_elementu", "opis"]
DICTIONARY_COLUMNS = (
    ("entity_id", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
    ("raw_sha256", "VARCHAR"),
    ("source_row_number", "BIGINT"),
    *((name, "VARCHAR") for name in DICTIONARY_SOURCE),
)


class DbwDecodeError(ValueError):
    """A Landing file cannot satisfy the Bronze contract (recorded, never dropped silently)."""


def _sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _extract(archive: zipfile.ZipFile, member: zipfile.ZipInfo, path: Path) -> None:
    with archive.open(member) as src, path.open("wb") as dst:
        for chunk in iter(lambda: src.read(8 << 20), b""):
            dst.write(chunk)


def decode(source: Path, receipt: dict, output: Path, dictionary: Path, memory_limit: str = "4GB") -> dict:
    """Decode one Landing ZIP (identity from its receipt): data → ``output``, labels →
    ``dictionary``; returns {"rows", "dictionary_rows"}."""
    source, output, dictionary = Path(source), Path(output), Path(dictionary)
    if _sha256(source) != receipt["sha256"]:
        raise DbwDecodeError("DBW file SHA-256 does not match its receipt")
    csv_path, dict_path = output.parent / "data.csv", output.parent / "dictionary.csv"
    try:
        with zipfile.ZipFile(source) as archive:
            names = [m.filename for m in archive.infolist()]
            data = [m for m in archive.infolist() if m.filename == f"{receipt['entity']}.csv"]
            labels = [m for m in archive.infolist() if m.filename.endswith("_Slowniki.csv")]
            if len(data) != 1 or len(labels) != 1 or len(names) != 2:
                raise DbwDecodeError(f"expected <entity>.csv and one *_Slowniki.csv, found {names}")
            _extract(archive, data[0], csv_path)
            _extract(archive, labels[0], dict_path)
    except zipfile.BadZipFile as exc:
        raise DbwDecodeError(f"not a readable ZIP: {exc}") from exc
    try:
        dictionary_rows = _decode_dictionary(dict_path, receipt, dictionary, memory_limit)
    finally:
        dict_path.unlink(missing_ok=True)
    with csv_path.open("r", encoding="utf-8-sig", errors="strict") as handle:
        header = handle.readline().rstrip("\r\n").split(";")
    if header and header[-1] == "":
        header = header[:-1]
    if header != SOURCE_COLUMNS:
        raise DbwDecodeError(f"unexpected DBW CSV header: {header[:6]}…")
    names = {f"c{i}": "VARCHAR" for i in range(len(SOURCE_COLUMNS) + 1)}  # + the trailing empty column
    select = ", ".join(f"c{i} AS {name.lower()}" for i, name in enumerate(SOURCE_COLUMNS))
    ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").isoformat() + "+00:00"
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit = '{memory_limit}'")
        con.execute(f"SET temp_directory = '{output.parent}'")
        con.execute(
            "CREATE TABLE src AS SELECT * FROM read_csv(?, delim=';', header=false, skip=1, columns=?, "
            "quote='\"', null_padding=true, all_varchar=true)",
            [str(csv_path), names],
        )
        last = len(SOURCE_COLUMNS) - 1  # precyzja: the last named column, always filled
        rows, extra, short = con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE coalesce(c{last + 1}, '') <> ''), "
            f"count(*) FILTER (WHERE c{last} IS NULL) FROM src"
        ).fetchone()
        if extra:
            raise DbwDecodeError(f"{extra} rows have more fields than the header names")
        if short:
            raise DbwDecodeError(f"{short} rows have fewer fields than the header names")
        if not rows:
            raise DbwDecodeError("the CSV has no rows")
        target = str(output).replace("'", "''")
        con.execute(
            f"COPY (SELECT ? AS entity_id, ? AS source_version, CAST(? AS TIMESTAMPTZ) AS ingested_at, "
            f"? AS raw_sha256, CAST(row_number() OVER () AS BIGINT) AS source_row_number, {select} FROM src) "
            f"TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)",
            [receipt["entity"], receipt["version_token"], ingested, receipt["sha256"]],
        )
    except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:  # the file itself
        raise DbwDecodeError(f"CSV cannot be read: {str(exc)[:400]}") from exc
    except duckdb.Error as exc:
        raise RuntimeError(f"DBW decode could not run: {str(exc)[:400]}") from exc  # retried, not rejected
    finally:
        con.close()
        csv_path.unlink(missing_ok=True)
    return {"rows": int(rows), "dictionary_rows": dictionary_rows}


def _decode_dictionary(path: Path, receipt: dict, output: Path, memory_limit: str) -> int:
    """The labels CSV → Parquet (text as published); returns its row count."""
    with path.open("r", encoding="utf-8-sig", errors="strict") as handle:
        header = handle.readline().rstrip("\r\n").split(";")
    if header and header[-1] == "":
        header = header[:-1]
    if header != DICTIONARY_SOURCE:
        raise DbwDecodeError(f"unexpected DBW dictionary header: {header}")
    names = {f"c{i}": "VARCHAR" for i in range(len(DICTIONARY_SOURCE) + 1)}
    select = ", ".join(f"c{i} AS {name}" for i, name in enumerate(DICTIONARY_SOURCE))
    ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").isoformat() + "+00:00"
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit = '{memory_limit}'")
        try:
            con.execute(
                "CREATE TABLE src AS SELECT * FROM read_csv(?, delim=';', header=false, skip=1, columns=?, "
                "quote='\"', escape='\"', null_padding=true, all_varchar=true, strict_mode=true)",
                [str(path), names],
            )
        except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:
            raise DbwDecodeError(f"dictionary CSV cannot be read: {str(exc)[:300]}") from exc
        rows, extra, empty = con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE coalesce(c{len(DICTIONARY_SOURCE)}, '') <> ''), "
            "count(*) FILTER (WHERE c0 IS NULL OR c2 IS NULL) FROM src"
        ).fetchone()
        if extra or empty or not rows:
            raise DbwDecodeError(f"dictionary CSV: {extra} rows with extra fields, {empty} without column or id")
        target = str(output).replace("'", "''")
        con.execute(
            f"COPY (SELECT ? AS entity_id, ? AS source_version, CAST(? AS TIMESTAMPTZ) AS ingested_at, "
            f"? AS raw_sha256, CAST(row_number() OVER () AS BIGINT) AS source_row_number, {select} FROM src) "
            f"TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)",
            [receipt["entity"], receipt["version_token"], ingested, receipt["sha256"]],
        )
    except duckdb.Error as exc:
        raise RuntimeError(f"DBW dictionary decode could not run: {str(exc)[:300]}") from exc
    finally:
        con.close()
    return int(rows)
