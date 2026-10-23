"""Decode one Landing GUS DBW bulk ZIP into source-shaped Bronze Parquet.

The ZIP holds one semicolon CSV with a fixed header (rowNumber, id_zmienna, id_przekroj,
nine dimension/position id pairs, id_okres, id_sposob_prezentacji_miara, id_daty,
id_brak_wartosci, id_tajnosci, id_flaga, wartosc, precyzja, and a trailing empty column).
Bronze keeps it as published: every column as text (``wartosc`` keeps its decimal comma),
plus provenance (entity, version, ingest time, Landing SHA-256, source row number). Casting
and labels are Silver and Gold rules.
"""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
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


class DbwDecodeError(ValueError):
    """A Landing file cannot satisfy the Bronze contract (recorded, never dropped silently)."""


def _sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def decode(source: Path, receipt: dict, output: Path, memory_limit: str = "4GB") -> dict:
    """Decode one Landing ZIP (identity from its receipt) into ``output``; returns {"rows"}."""
    source, output = Path(source), Path(output)
    if _sha256(source) != receipt["sha256"]:
        raise DbwDecodeError("DBW file SHA-256 does not match its receipt")
    try:
        with zipfile.ZipFile(source) as archive:
            members = [m for m in archive.infolist() if m.filename.lower().endswith(".csv")]
            if len(members) != 1:
                raise DbwDecodeError(f"expected one CSV in the ZIP, found {len(members)}")
            csv_path = output.parent / "data.csv"
            with archive.open(members[0]) as src, csv_path.open("wb") as dst:
                for chunk in iter(lambda: src.read(8 << 20), b""):
                    dst.write(chunk)
    except zipfile.BadZipFile as exc:
        raise DbwDecodeError(f"not a readable ZIP: {exc}") from exc
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
    return {"rows": int(rows), "receipt": json.dumps({"entity": receipt["entity"]})}
