"""Dev trial (scratch only): per-block Gold of the Ministry of Finance public debt, Treasury debt and state budget.

Runs warehouse/observations.py for each source into the scratch namespace and prefix zz_dev_goldmf: the
first-load guard, the load, a second run with nothing to do, then checks of the scratch tables against production
Silver (rows with a value, rows not loaded by reason), the grain (series_key x date_key), null keys, unmatched
geography codes, statuses, units, Poland's newest values, and every row of the curated build stage
(gold.build_<source>) looked up in the new tables by area, frequency, date and value. Reads production Silver,
gold.map_geography and the curated build tables; writes nothing outside the scratch, which it removes. At most
10 annotations. Deleted before merge.

python -m dev.goldmf_trial
"""

from __future__ import annotations

import contextlib
import os
import subprocess
import sys
import tempfile
import time
import traceback

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from warehouse import observations as obs

NS = "zz_dev_goldmf"
SOURCES = tuple(sys.argv[1:]) or ("mf_public_debt", "mf_treasury_debt", "mf_state_budget")


def connect():
    con = duckdb.connect()
    con.execute("SET memory_limit = '8GB'; SET preserve_insertion_order = false")
    con.execute("INSTALL httpfs; LOAD httpfs")
    endpoint = req("STORAGE_ENDPOINT_URL").replace("https://", "").rstrip("/")
    con.execute(
        "CREATE SECRET r2 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [req("STORAGE_WRITE_ACCESS_KEY"), req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    return con


def files(paths: list[str]) -> str:
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"


def run(source: str, *extra: str) -> list:
    started = time.monotonic()
    out = obs.run(["--source", source, "--trial", NS, "--minutes", "60", *extra])
    brief = [out["result"], out.get("built", 0), out.get("rows", 0), out.get("rejected", 0), out.get("failed", 0)]
    if out.get("rejected_blocks") or out.get("failed_blocks"):
        brief.append({"rejected": out.get("rejected_blocks"), "failed": out.get("failed_blocks")})
    if "coverage" in out:
        c = out["coverage"]
        brief.append([c["blocks_in_gold"], c["blocks_in_silver"], c["series_in_gold"], c["coverage_pct"]])
    brief.append(round(time.monotonic() - started))
    return brief


def cleanup(cat, client) -> dict:
    gone = {"tables": 0, "objects": 0}
    names = [f"{s}_{t}" for s in SOURCES for t in obs.TABLES] + [obs.REJECTED[1]]
    errors = []
    for _ in range(3):
        listed = [t[1] for t in cat.list_tables(NS)] if (NS,) in cat.list_namespaces() else []
        if not listed:
            break
        for name in listed:
            try:
                cat.drop_table((NS, name))
                gone["tables"] += 1
            except Exception as exc:
                errors.append(f"{name}: {type(exc).__name__}")
        time.sleep(5)
    with contextlib.suppress(Exception):
        cat.drop_namespace(NS)
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=NS + "/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            gone["objects"] += len(keys)
    left = sum(
        len(p.get("Contents", []))
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=NS + "/")
    )
    production = {t[1] for t in cat.list_tables("gold")}
    return {
        **gone,
        "objects_left": left,
        "namespace_left": (NS,) in cat.list_namespaces(),
        "drop_errors": errors[:5],
        "production_gold_tables_of_these_loads": sorted(production & set(names) - {obs.REJECTED[1]}),
    }


def views(con, cat, source: str) -> dict:
    state = {name: et.state(cat.load_table(t), obs.ENTITY) for name, t in obs.table_ids(source, NS).items()}
    for name, short in (("observations", "o"), ("series", "s"), ("measures", "m"), ("geography", "g")):
        paths = [p for h in state[name].values() for p in h["paths"]]
        con.execute(f"create or replace view {short} as select * from {files(paths)}")
    silver = obs.silver_blocks(cat, source)
    ((block, item),) = silver.items()
    con.execute(f"create or replace view silver as select * from {files(item['paths'])}")
    return {
        "silver": [block, len(item["paths"]), item["rows"], round(item["bytes"] / 1e6, 1)],
        "gold_mb": {n: round(sum(h["bytes"] for h in s.values()) / 1e6, 2) for n, s in state.items()},
    }


def one(con, sql: str):
    return con.execute(sql).fetchall()[0]


def common(con) -> dict:
    rows, dup = one(
        con,
        "select coalesce(sum(n), 0), count(*) filter (where n > 1) "
        "from (select count(*) as n from o group by series_key, date_key)",
    )
    return {
        "rows": rows,
        "dup_series_x_date": dup,
        "null_keys": one(
            con,
            "select count(*) from o where series_key is null or measure_key is null or geography_key is null "
            "or date_key is null or frequency is null or value is null or observation_status_key is null",
        )[0],
        "series_measures": one(con, "select (select count(*) from s), (select count(*) from m)"),
        "freq": con.execute(
            "select frequency, count(*), min(date_key)::varchar, max(date_key)::varchar from o group by 1 order by 1"
        ).fetchall(),
        "status": con.execute("select observation_status_key, count(*) from o group by 1 order by 2 desc").fetchall(),
        "zero_nan_inf": one(
            con,
            "select count(*) filter (where value = 0), count(*) filter (where isnan(value)), "
            "count(*) filter (where isinf(value)) from o",
        ),
        "geo_codes_unmatched_pts": one(
            con,
            "select count(*), count(*) filter (where geography_key = '_X'), "
            "coalesce(sum(points) filter (where geography_key = '_X'), 0) from g",
        ),
        "unmatched": [
            r[0]
            for r in con.execute(
                "select geography_source_code from g where geography_key = '_X' order by points desc"
            ).fetchall()
        ][:30],
        "units": con.execute("select unit_key, count(*), sum(points) from m group by 1 order by 3 desc").fetchall()[
            :12
        ],
    }


def curated(con, cat, source: str) -> list:
    """Every row of the curated build_public_finance of the source, looked up in the new tables by period and value
    (PC_GDP / 100: the curated stage rescales; an annual curated point is the December or fourth-quarter point)."""
    table = cat.load_table(("gold", "build_public_finance"))
    arrow = table.scan(
        row_filter=f"source_system == '{source}'",
        selected_fields=("measure_key", "unit_code", "frequency", "date_key", "value"),
    ).to_arrow()
    con.register("cur", arrow)
    con.execute(
        """create or replace temp table cur_found as
           select c.*, exists (
               select 1 from o as n
               where abs(n.value - c.value / case when c.unit_code = 'PC_GDP' then 100 else 1 end)
                     <= 1e-9 * greatest(1, abs(c.value))
               and ((n.frequency = c.frequency and n.date_key = c.date_key)
                    or (c.frequency = 'A' and year(n.date_key) = year(c.date_key)
                        and ((n.frequency = 'Q' and month(n.date_key) = 10)
                             or (n.frequency = 'M' and month(n.date_key) = 12))))) as found
           from cur as c"""
    )
    return [
        one(con, "select count(*), count(*) filter (where found) from cur_found"),
        con.execute(
            "select measure_key, frequency, count(*) from cur_found where not found group by all order by 3 desc"
        ).fetchall()[:8],
    ]


def check_debt(con) -> dict:
    return {
        "silver": one(
            con,
            "select count(*), count(*) filter (where value is null or isnan(value) or isinf(value)), "
            "count(*) filter (where period_end > try_strptime(left(source_version, 8), '%Y%m%d')), "
            "count(*) filter (where try_strptime(left(source_version, 8), '%Y%m%d') is null), "
            "min(period_end)::varchar, max(period_end)::varchar, min(source_version), max(source_version) from silver",
        ),
        "silver_sheets": con.execute(
            "select sheet_name, unit, count(distinct header_row), count(*) from silver group by all order by 1, 2"
        ).fetchall()[:25],
        "measures_by_freq_unit": con.execute(
            "select frequency, unit_key, count(*), sum(points) from m group by all order by 1, 2"
        ).fetchall(),
        # an M measure whose dates are never one month apart may be a misread quarterly block
        "m_measures_min_gap_months": con.execute(
            "with d as (select measure_key, date_key, lag(date_key) over (partition by measure_key order by date_key)"
            " as prev from o where frequency = 'M') select min_gap, count(*) from (select measure_key, "
            "min(datediff('month', prev, date_key)) as min_gap from d group by 1) group by 1 order by 1"
        ).fetchall(),
        "q_a_measure_points": con.execute(
            "select frequency, min(points), max(points), count(*) from m where frequency <> 'M' group by 1"
        ).fetchall(),
        "measure_key_max_len": one(con, "select max(length(measure_key)) from m")[0],
        "keys_with_pipe": one(con, "select count(*) from m where contains(measure_key, '|')")[0],
        "poland_newest_sample": con.execute(
            "select measure_key, date_key::varchar, value from o qualify row_number() over (partition by measure_key "
            "order by date_key desc) = 1 order by date_key desc, measure_key limit 6"
        ).fetchall(),
    }


def check_budget(con) -> dict:
    return {
        "silver": one(
            con,
            "select count(*), count(*) filter (where measure = 'plan'), min(budget_year), max(budget_year), "
            "count(distinct budget_year || item_label) from silver",
        ),
        "measures_by_freq": con.execute(
            "select frequency, count(*), sum(points), min(first_date)::varchar, max(last_date)::varchar from m "
            "group by 1 order by 1"
        ).fetchall(),
        "main_lines_printed_twice": con.execute(
            "select item_key, budget_year, measure, count(*) from silver where item_key is not null "
            "group by all having count(*) > count(distinct coalesce(month_end, 0)) order by 2, 1 limit 8"
        ).fetchall(),
        "revenue_expenditure_newest": con.execute(
            "select measure_key, date_key::varchar, value from o where measure_key ilike '%DOCHODY@%' "
            "or measure_key ilike '%WYDATKI@%' qualify row_number() over (partition by measure_key "
            "order by date_key desc) = 1 order by date_key desc limit 6"
        ).fetchall(),
    }


CHECKS = {"mf_public_debt": check_debt, "mf_treasury_debt": check_debt, "mf_state_budget": check_budget}


def main() -> None:
    tmp = "/mnt/duckdb_tmp"
    try:
        subprocess.run(["sudo", "mkdir", "-p", tmp], check=True)
        subprocess.run(["sudo", "chown", str(os.getuid()), tmp], check=True)
    except Exception:
        tmp = tempfile.mkdtemp(prefix="duckdb_tmp")
    os.environ["DUCKDB_TEMP_DIRECTORY"] = tmp
    cat, client = catalog(), s3()
    before = cleanup(cat, client)
    obs.annotate = lambda *a, **k: None  # the trial writes its own compact annotations
    first = {"cleanup_before": before, "model_digests": {s: obs.model_digest(s) for s in SOURCES}}
    try:
        for source in SOURCES:
            try:
                runs = {
                    "guard": run(source, "--continue-only"),
                    "tables_after_guard": (
                        sorted(t[1] for t in cat.list_tables(NS) if t[1].startswith(source + "_"))
                        if (NS,) in cat.list_namespaces()
                        else []
                    ),
                    "load": run(source),
                    "again": run(source),
                    "again_continue_only": run(source, "--continue-only"),
                }
                con = connect()
                head = {**first, "runs": runs, **views(con, cat, source), **common(con)}
                first = {}
                checks = CHECKS[source](con)
                checks["curated_rows_found_misses"] = curated(con, cat, source)
                annotate("notice", f"goldmf trial: {source} gold", head)
                annotate("notice", f"goldmf trial: {source} checks", checks)
                con.close()
            except Exception:
                annotate("error", f"goldmf trial: {source} failed", {"traceback": traceback.format_exc()[-3000:]})
    finally:
        annotate("notice", "goldmf trial: cleanup", cleanup(cat, client))


if __name__ == "__main__":
    main()
