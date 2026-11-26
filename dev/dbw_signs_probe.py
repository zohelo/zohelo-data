"""Dev probe (read only, writes nothing): which no-value, confidentiality and flag ids GUS DBW's Silver
holds, how often with 0, a number or no value, and their labels in DBW's own dictionary files. Also the
file sizes of Silver and the dictionary (the R2 read estimate of a full load). Deleted before merge.

python -m dev.dbw_signs_probe
"""

from __future__ import annotations

import json
import math
import os
import time

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import CATALOG_CALLS, catalog, req

COLUMNS = {"missing_reason_id": "id_brak_wartosci", "confidentiality_id": "id_tajnosci", "flag_id": "id_flaga"}
CHUNK = 8 * 1024 * 1024  # boto3 download_file part size


def annotate(level: str, title: str, data) -> None:
    message = json.dumps(data, default=str, separators=(",", ":"), ensure_ascii=False)
    if len(message) > 3900:
        message = message[:3888] + "…[truncated]"
    if os.environ.get("GITHUB_ACTIONS") == "true":
        message = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::{level} title={title}::{message}", flush=True)
    else:
        print(f"[{level}] {title}: {message}", flush=True)


def connect():
    con = duckdb.connect()
    con.execute("SET memory_limit = '9GB'; SET preserve_insertion_order = false")
    con.execute("INSTALL httpfs; LOAD httpfs")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
    con.execute(
        "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    return con


def files(paths: list[str]) -> str:
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"


def sizes(table) -> dict:
    listed = table.inspect.files().select(["file_path", "file_size_in_bytes", "record_count"]).to_pylist()
    b = sorted(r["file_size_in_bytes"] for r in listed)
    return {
        "files": len(b),
        "mb": round(sum(b) / 1e6),
        "rows": sum(r["record_count"] for r in listed),
        "bytes_min_median_p99_max": [b[0], b[len(b) // 2], b[int(len(b) * 0.99)], b[-1]] if b else [],
        "files_over_8mib": sum(1 for x in b if x > CHUNK),
        # boto3 download_file: one HEAD, then one GET per 8 MiB part
        "class_b_if_downloaded_once": sum(1 + max(1, math.ceil(x / CHUNK)) for x in b),
    }


def http_log(con) -> dict:
    try:
        rows = con.execute(
            "select request.type, count(*) from duckdb_logs_parsed('HTTP') group by 1 order by 2 desc"
        ).fetchall()
        return {str(k): n for k, n in rows}
    except duckdb.Error as exc:
        return {"unavailable": str(exc)[:120]}


def main() -> None:
    cat = catalog()
    silver_table = cat.load_table(("silver", "dbw_observations"))
    dictionary_table = cat.load_table(("bronze", "dbw_dictionary_items"))
    silver = et.state(silver_table, "entity_id")
    dictionary = et.state(dictionary_table, "entity_id")
    annotate("notice", "dbw probe files", {"silver": sizes(silver_table), "dictionary": sizes(dictionary_table)})

    paths = sorted(p for e, held in silver.items() if e is not None for p in held["paths"])
    con = connect()
    try:
        con.execute("CALL enable_logging('HTTP')")
        logging = True
    except duckdb.Error:
        logging = False
    started = time.monotonic()
    combos = con.execute(
        f"""
        select missing_reason_id, confidentiality_id, flag_id,
            count(*) filter (where value is null) as empty,
            count(*) filter (where value = 0) as zero,
            count(*) filter (where value <> 0) as number,
            min(entity_id) as example
        from {files(paths)}
        group by all
        order by zero + number desc
        """
    ).fetchall()
    seconds = round(time.monotonic() - started)
    requests = http_log(con) if logging else {"logging": "off"}
    annotate("notice", "dbw probe scan", {
        "silver_files": len(paths), "seconds": seconds, "http_requests": requests,
        "requests_per_file": round(sum(v for v in requests.values() if isinstance(v, int)) / len(paths), 2)
        if requests and all(isinstance(v, int) for v in requests.values()) else None,
        "combinations": len(combos),
    })
    # the combinations, compact: [missing, confidentiality, flag, empty, zero, number]
    annotate("notice", "dbw probe combinations", [list(r[:6]) for r in combos[:120]])

    # each id once per column: rows, zeros, numbers; an example entity for its label
    per_id: dict[str, dict] = {c: {} for c in COLUMNS}
    for missing, confidential, flag, empty, zero, number, example in combos:
        for column, value in zip(COLUMNS, (missing, confidential, flag), strict=True):
            held = per_id[column].setdefault(str(value), {"empty": 0, "zero": 0, "number": 0, "examples": set()})
            held["empty"] += empty
            held["zero"] += zero
            held["number"] += number
            if len(held["examples"]) < 2:
                held["examples"].add(example)
    entities = sorted({e for ids in per_id.values() for v in ids.values() for e in v["examples"]})
    dict_paths = sorted(p for e in entities for p in dictionary.get(e, {}).get("paths", []))
    labels: dict[tuple[str, str], set[str]] = {}
    if dict_paths:
        names = ", ".join(f"'{n}'" for n in COLUMNS.values())
        for name, element, text in con.execute(
            f"""select distinct lower(nazwa_kolumny), cast(id_elementu as varchar), opis
                from {files(dict_paths)} where lower(nazwa_kolumny) in ({names})"""
        ).fetchall():
            labels.setdefault((name, element), set()).add(text)
    for column, dictionary_column in COLUMNS.items():
        annotate("notice", f"dbw probe {dictionary_column}", {
            value: {
                "label": sorted(labels.get((dictionary_column, value), set())) or None,
                "empty": held["empty"], "zero": held["zero"], "number": held["number"],
            }
            for value, held in sorted(per_id[column].items(), key=lambda kv: -(kv[1]["zero"] + kv[1]["number"]))
        })
    zero_conf = sum(z for m, c, f, e, z, n, x in combos if c is not None and c != 43)
    annotate("notice", "dbw probe questions", {
        "confidential_zero_rows": zero_conf,
        "confidential_number_rows": sum(n for m, c, f, e, z, n, x in combos if c is not None and c != 43),
        "sign38_number_rows": sum(n for m, c, f, e, z, n, x in combos if m == 38),
        "sign38_zero_rows": sum(z for m, c, f, e, z, n, x in combos if m == 38),
        "flag0_rows": sum(z + n for m, c, f, e, z, n, x in combos if f == 0),
        "flag_null_rows": sum(z + n for m, c, f, e, z, n, x in combos if f is None),
        "dictionary_files_read": len(dict_paths),
        "catalog_calls": dict(CATALOG_CALLS),
        "http_requests_total": http_log(con) if logging else None,
    })


if __name__ == "__main__":
    main()
