"""Decode one Landing MF white-list flat file (7z) into source-shaped Bronze Parquet.

The 7z holds ``<date>.json`` and ``<date>.json.sha512sum``. The JSON has a header
(``naglowek``: generation date, number of hash transformations, scheme) and three lists:
``skrotyPodatnikowCzynnych`` (SHA-512 hashes of date+NIP+account of active VAT taxpayers),
``skrotyPodatnikowZwolnionych`` (exempt taxpayers) and ``maski`` (virtual-account masks).
Bronze keeps one row per list item (``record_kind`` active / exempt / mask, ``value`` as
published, ``position`` in its list) plus the header values on every row; nothing is cast.
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256, sha512
from pathlib import Path
import shutil
import subprocess

import duckdb

LISTS = {"skrotyPodatnikowCzynnych": "active", "skrotyPodatnikowZwolnionych": "exempt", "maski": "mask"}
BRONZE_COLUMNS = [
    ("entity", "VARCHAR"),
    ("record_kind", "VARCHAR"),
    ("value", "VARCHAR"),
    ("position", "BIGINT"),
    ("generation_date", "VARCHAR"),
    ("transformations", "VARCHAR"),
    ("raw_sha256", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("ingested_at", "TIMESTAMPTZ"),
]


class WhiteListDecodeError(ValueError):
    """A Landing file cannot satisfy the Bronze contract (recorded, never dropped silently)."""


def _digest(path: Path, algorithm) -> str:
    h = algorithm()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(16 << 20), b""):
            h.update(block)
    return h.hexdigest()


def decode(raw: Path, receipt: dict, output: Path, memory_limit: str = "8GB") -> dict:
    """7z → Parquet; returns {"rows", "active", "exempt", "mask", "generation_date"}."""
    if _digest(raw, sha256) != receipt["sha256"]:
        raise WhiteListDecodeError("file SHA-256 does not match its receipt")
    tool = shutil.which("7z") or shutil.which("7za")
    if not tool:
        raise RuntimeError("7z is not installed")
    folder = output.parent / "unpacked"
    folder.mkdir()
    done = subprocess.run([tool, "x", "-bd", "-y", f"-o{folder}", str(raw)], capture_output=True, text=True)
    if done.returncode != 0:
        raise WhiteListDecodeError(f"7z cannot unpack the file: {(done.stdout + done.stderr)[-300:]}")
    raw.unlink()
    jsons = sorted(folder.glob("*.json"))
    sums = sorted(folder.glob("*.json.sha512sum"))
    if len(jsons) != 1 or len(sums) != 1:
        raise WhiteListDecodeError(f"expected one JSON and its sha512sum, found {[p.name for p in folder.iterdir()]}")
    stated = sums[0].read_text().split()[0].lower() if sums[0].read_text().split() else ""
    if _digest(jsons[0], sha512) != stated:
        raise WhiteListDecodeError("JSON does not match the SHA-512 published with it")
    try:
        return decode_json(jsons[0], receipt, output, memory_limit)
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def decode_json(path: Path, receipt: dict, output: Path, memory_limit: str = "8GB") -> dict:
    """The unpacked, checked JSON → Parquet."""
    source = str(path).replace("'", "''")
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit = '{memory_limit}'")
        con.execute(f"SET temp_directory = '{output.parent}'")
        try:
            con.execute(
                f"CREATE TABLE doc AS SELECT * FROM read_json('{source}', format='unstructured', "
                f"maximum_object_size={path.stat().st_size + (1 << 20)}, records=true)"
            )
            keys = [r[0] for r in con.execute("DESCRIBE doc").fetchall()]
        except (duckdb.InvalidInputException, duckdb.ConversionException) as exc:
            raise WhiteListDecodeError(f"JSON cannot be read: {str(exc)[:300]}") from exc
        if set(keys) != {"naglowek", *LISTS}:
            raise WhiteListDecodeError(f"unexpected JSON keys: {keys}")
        header = con.execute(
            "SELECT naglowek.dataGenerowaniaDanych::VARCHAR, naglowek.liczbaTransformacji::VARCHAR FROM doc"
        ).fetchone()
        if not header or not header[0] or not header[1]:
            raise WhiteListDecodeError("the header has no generation date or transformation count")
        ingested = datetime.strptime(receipt["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        parts = " UNION ALL ".join(
            f"SELECT '{kind}' AS record_kind, u.value::VARCHAR AS value, u.position::BIGINT AS position "
            f"FROM doc, unnest({key}) WITH ORDINALITY AS u(value, position)"
            for key, kind in LISTS.items()
        )
        target = str(output).replace("'", "''")
        con.execute(
            f"COPY (SELECT ? AS entity, record_kind, value, position, ? AS generation_date, ? AS transformations, "
            f"? AS raw_sha256, ? AS source_version, ?::TIMESTAMPTZ AS ingested_at FROM ({parts})) "
            f"TO '{target}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 500000)",
            [receipt["entity"], header[0], header[1], receipt["sha256"], receipt["version_token"], ingested],
        )
        counts = dict(con.execute(f"SELECT record_kind, count(*) FROM read_parquet('{target}') GROUP BY 1").fetchall())
    finally:
        con.close()
    if not counts.get("active"):
        raise WhiteListDecodeError("the file lists no active taxpayers")
    return {"rows": sum(counts.values()), **counts, "generation_date": header[0]}
