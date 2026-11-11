"""Read-only probe (deleted before merge): which series duplicate (series_key, date_key) across the Gold facts. No writes."""

from __future__ import annotations

import json
import time

import duckdb

from ingestion.common.r2 import req
from ingestion.common.report import annotate


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET preserve_insertion_order=false;")
    for ext in ("httpfs", "iceberg"):
        con.execute(f"INSTALL {ext}; LOAD {ext};")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").strip("/")
    con.execute(f"""CREATE SECRET r2_catalog (TYPE iceberg, TOKEN '{req("STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN")}')""")
    con.execute(
        f"""CREATE SECRET r2_files (TYPE s3, KEY_ID '{req("STORAGE_WRITE_ACCESS_KEY")}',
        SECRET '{req("STORAGE_WRITE_SECRET_KEY")}', ENDPOINT '{endpoint}', REGION 'auto', URL_STYLE 'path')"""
    )
    con.execute(
        f"""ATTACH '{req("STORAGE_LAKEHOUSE_CATALOG_NAME")}' AS r2 (TYPE iceberg, SECRET r2_catalog,
        ENDPOINT '{req("STORAGE_LAKEHOUSE_CATALOG_URL")}')"""
    )
    return con


def show(con, title: str, query: str, limit: int = 80) -> None:
    started = time.monotonic()
    try:
        cur = con.execute(query)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchmany(limit)
        out = {"s": round(time.monotonic() - started, 1), "cols": cols, "rows": [[str(v)[:160] for v in r] for r in rows]}
        level = "notice"
    except Exception as exc:  # report and continue
        out, level = {"error": f"{type(exc).__name__}: {str(exc)[:600]}"}, "warning"
    text = json.dumps(out, ensure_ascii=False)
    print(title, text)
    for i in range(0, len(text), 3500):
        annotate(level, f"{title} [{i // 3500 + 1}]", text[i : i + 3500])


FACTS = " union all ".join(
    f"select '{t}' as fact, series_key, date_key, source_system, block_id, frequency, period_label, value from r2.gold.{t}"
    for t in ("fact_annual", "fact_quarterly", "fact_monthly", "fact_daily")
)


def main() -> int:
    con = connect()
    con.execute(f"create temp table f as {FACTS}")
    con.execute("""create temp table d as select series_key, date_key, count(*) n,
        list(distinct fact order by fact) facts, list(distinct source_system || ':' || block_id order by 1) blocks,
        list(period_label order by period_label) periods, list(distinct value) vals
        from f group by all having count(*) > 1""")
    show(con, "total", "select count(*) keys, sum(n) rows from d")
    show(con, "by blocks x facts", """select blocks, facts, count(*) keys, sum(n) rows,
        count(*) filter (where len(vals) = 1) same_value, any_value(series_key) sk, any_value(periods) p
        from d group by blocks, facts order by keys desc""")
    show(con, "samples", """select series_key, date_key, n, facts, blocks, periods, vals from (select *,
        row_number() over (partition by blocks, facts order by series_key) r from d) where r <= 2 order by blocks""")
    show(con, "grain_check", "select * from r2.gold.grain_check")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
