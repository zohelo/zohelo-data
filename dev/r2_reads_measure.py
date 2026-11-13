"""Read-only (#2873 item 3a): how many R2 requests DuckDB makes for typical reads of the model build.
(1) data files, rows and bytes per catalog table, from snapshot summaries (catalog REST only, no R2 object request);
(2) DuckDB HTTP log counts for a few production scans (GET and HEAD by kind of file, data files touched).
Writes nothing."""

from __future__ import annotations

from pathlib import Path
import re
import time

import duckdb

from ingestion.common import r2
from ingestion.common.report import annotate

BDL_COLUMNS = (
    "subgroup_id, variable_id, unit_code, unit_name, year, value, measure_unit, attribute, channel, "
    "source_version, raw_sha256, value_conflict"
)


def inventory() -> None:
    cat = r2.catalog()
    rows = []
    for namespace in ("bronze", "silver", "gold"):
        for identifier in cat.list_tables(namespace):
            try:
                snapshot = cat.load_table(identifier).current_snapshot()
            except Exception:  # a table swapped by a running build
                continue
            summary = (snapshot.summary if snapshot else None) or {}
            rows.append(
                (
                    ".".join(identifier),
                    int(summary.get("total-data-files") or 0),
                    int(summary.get("total-records") or 0),
                    round(int(summary.get("total-files-size") or 0) / 1e6),
                )
            )
    by_layer: dict[str, list[int]] = {}
    for name, files, records, mb in rows:
        item = by_layer.setdefault(name.split(".")[0], [0, 0, 0, 0])
        item[0] += 1
        item[1] += files
        item[2] += records
        item[3] += mb
    annotate(
        "notice",
        "1 catalog tables [tables, data files, rows, MB]",
        {"by_layer": by_layer, "most_files [table, files, rows, MB]": sorted(rows, key=lambda r: -r[1])[:28]},
    )
    annotate("notice", "1b largest tables [table, files, rows, MB]", sorted(rows, key=lambda r: -r[3])[:30])


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg")
    con.execute("SET memory_limit = '10GB'")
    con.execute("CREATE SECRET r2_catalog (TYPE iceberg, TOKEN ?)", [r2.req("STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN")])
    endpoint = r2.req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
    con.execute(
        "CREATE SECRET r2_files (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [r2.req("STORAGE_WRITE_ACCESS_KEY"), r2.req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    name, url = r2.req("STORAGE_LAKEHOUSE_CATALOG_NAME"), r2.req("STORAGE_LAKEHOUSE_CATALOG_URL")
    con.execute(f"ATTACH '{name}' AS r2 (TYPE iceberg, SECRET r2_catalog, ENDPOINT '{url}')")
    con.execute("CALL enable_logging('HTTP')")
    return con


def measured(con, sql: str) -> dict:
    con.execute("CALL truncate_duckdb_logs()")
    start = time.time()
    try:
        result = [str(x)[:24] for x in con.execute(sql).fetchall()[0]]
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {str(exc)[:300]}"}
    seconds = round(time.time() - start, 1)
    kinds = con.execute(
        """
        with log as (
            select request.type as method, regexp_replace(request.url, '[?].*$', '') as url, response.status as status
            from duckdb_logs_parsed('HTTP')
        )
        select method,
            case when url like '%.parquet' then 'data' when url like '%.avro' then 'manifest'
                when url like '%.json' then 'metadata' when url like '%r2.cloudflarestorage.com%' then 'other_r2'
                else 'catalog_api' end as kind,
            count(*) as requests, count(distinct url) as files,
            count(*) filter (where status not like '%2__%' and status not like '%OK%' and status not like '%Partial%') as not_ok
        from log group by 1, 2 order by 3 desc
        """
    ).fetchall()
    statuses = con.execute("select response.status, count(*) from duckdb_logs_parsed('HTTP') group by 1 order by 2 desc limit 4").fetchall()
    return {"seconds": seconds, "result": result, "requests [method, kind, requests, files, not ok]": kinds, "statuses": statuses}


def main() -> None:
    try:
        inventory()
    except Exception as exc:
        annotate("warning", "1 inventory failed", {"error": f"{type(exc).__name__}: {str(exc)[:400]}"})
    macro = Path("warehouse/macros/poc_blocks.sql").read_text(encoding="utf-8")
    body = re.search(r"macro poc_bdl_blocks\(\).*?endmacro", macro, re.S).group(0)
    blocks = sorted(set(re.findall(r"\('(P\d+)'", body)))
    values = ", ".join(f"('{b}')" for b in blocks)
    literal = ", ".join(f"'{b}'" for b in blocks)
    con = connect()
    hashed = f"count(*), bit_xor(hash({BDL_COLUMNS}))"
    queries = {
        "a gold.build_bdl, whole table, all columns": "select count(*), bit_xor(hash(b)) from r2.gold.build_bdl as b",
        "b gold.fact_annual, whole table, all columns": "select count(*), bit_xor(hash(f)) from r2.gold.fact_annual as f",
        "c silver.bdl_observations, one subgroup by filter, model columns": (
            f"select {hashed} from r2.silver.bdl_observations where subgroup_id = 'P2137' and value is not null and year >= 2010"
        ),
        f"d silver.bdl_observations joined to the {len(blocks)} blocks as the model does, 3 columns": (
            f"select count(*), sum(o.value) from r2.silver.bdl_observations as o "
            f"inner join (select * from (values {values}) as t(subgroup_id)) as b on o.subgroup_id = b.subgroup_id "
            "where o.value is not null and o.year >= 2010"
        ),
        f"e silver.bdl_observations, the {len(blocks)} blocks as a literal list, 3 columns": (
            f"select count(*), sum(o.value) from r2.silver.bdl_observations as o "
            f"where o.subgroup_id in ({literal}) and o.value is not null and o.year >= 2010"
        ),
    }
    for title, sql in queries.items():
        annotate("notice", f"2 {title}", measured(con, sql))


if __name__ == "__main__":
    main()
