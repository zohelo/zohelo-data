"""Dev trial (scratch only): per-block Gold of UNESCO UIS, World Bank EdStats and GEM, and OWID (#3314).

Runs warehouse/observations.py for the four sources into the scratch namespace and prefix zz_dev_gold3314: the
first-load guard, the load, a second run with nothing to do, then checks of the scratch tables against production
Silver (rows with a value, rows not loaded by reason), the grain (series_key x date_key), null keys, unmatched
geography codes, statuses, units, Poland's newest values, and every row of the curated build stage
(gold.build_<source>) looked up in the new tables by area, frequency, date and value. Reads production Silver,
gold.map_geography and the curated build tables; writes nothing outside the scratch, which it removes. At most
10 annotations. Deleted before merge.

python -m dev.gold3314_trial
"""

from __future__ import annotations

import contextlib
import os
import subprocess
import tempfile
import time
import traceback

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from warehouse import observations as obs

NS = "zz_dev_gold3314"
SOURCES = ("unesco_uis", "world_bank_edstats", "world_bank_gem", "owid")


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
    return con.execute(sql).fetchone()


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
    """Every row of the curated build stage, looked up in the new tables by area code, frequency, date and value;
    the misses grouped by measure and status."""
    table = cat.load_table(("gold", f"build_{source}"))
    con.register(
        "cur",
        table.scan(
            selected_fields=(
                "measure_key",
                "geography_source_code",
                "frequency",
                "date_key",
                "value",
                "observation_status_key",
            )
        ).to_arrow(),
    )
    con.execute(
        """create or replace temp table cur_found as
           with new as (select s.geography_source_code as area, o.frequency, o.date_key, o.value
                        from o inner join s using (series_key))
           select c.*, exists (
               select 1 from new as n where n.area is not distinct from c.geography_source_code
               and n.frequency = c.frequency and n.date_key = c.date_key and n.value = c.value) as found
           from cur as c"""
    )
    return [
        one(con, "select count(*), count(*) filter (where found) from cur_found"),
        con.execute(
            "select measure_key, observation_status_key, count(*), count(*) filter (where value = 0) "
            "from cur_found where not found group by all order by 3 desc"
        ).fetchall()[:8],
        one(con, "select count(*) filter (where value = 0) from cur_found")[0],
    ]


def newest_pl(con, where: str = "true", limit: int = 10) -> list:
    return con.execute(
        "select o.measure_key, o.series_key, o.date_key::varchar, o.value, o.observation_status_key "
        "from o inner join s using (series_key) "
        f"where o.geography_key = 'PL' and {where} "
        "qualify row_number() over (partition by o.series_key order by o.date_key desc) = 1 order by 1, 2"
    ).fetchall()[:limit]


def check_unesco_uis(con) -> dict:
    return {
        "expected_gold_rows": one(
            con,
            "select count(*) from silver where value is not null and not isnan(value) and coalesce(magnitude, '') <> 'NA'",
        )[0],
        "silver_rows_na_nil": one(
            con,
            "select count(*), count(*) filter (where magnitude = 'NA'), count(*) filter (where magnitude = 'NIL') from silver",
        ),
        "measures": con.execute("select measure_key, unit_key, points from m order by 1").fetchall(),
        "status_by_measure": con.execute(
            "select measure_key, observation_status_key, count(*) from o group by all order by 1, 2"
        ).fetchall(),
        "series_sex_poland": con.execute(
            "select s.breakdown_json, s.is_total, s.sex_code, count(*) from o inner join s using (series_key) "
            "where o.geography_key = 'PL' group by all order by 1"
        ).fetchall(),
        "poland_newest": newest_pl(con),
    }


def check_world_bank_edstats(con) -> dict:
    return {
        "expected_gold_rows": one(con, "select count(*) from silver where value is not null and not isnan(value)")[0],
        "measures": con.execute("select measure_key, unit_key, points from m order by 1").fetchall(),
        "areas_matched": one(con, "select count(*) filter (where geography_key <> '_X'), count(*) from g"),
        "poland_2018": con.execute(
            "select o.measure_key, s.sex_code, o.value from o inner join s using (series_key) "
            "where o.geography_key = 'PL' and o.date_key = '2018-01-01' order by 1, 2"
        ).fetchall(),
    }


def check_world_bank_gem(con) -> dict:
    return {
        "expected_gold_rows": one(
            con,
            "select count(*) from silver where value is not null and not isnan(value) and "
            "case frequency when 'A' then period_start + interval 1 year when 'Q' then period_start + interval 3 month "
            "else period_start + interval 1 month end <= strptime(left(source_version, 8), '%Y%m%d') + interval 1 day",
        )[0],
        "silver_not_ended": con.execute(
            "select frequency, time_code, count(*) from silver where "
            "case frequency when 'A' then period_start + interval 1 year when 'Q' then period_start + interval 3 month "
            "else period_start + interval 1 month end > strptime(left(source_version, 8), '%Y%m%d') + interval 1 day "
            "group by all order by 1, 2"
        ).fetchall(),
        "measures": con.execute("select measure_key, unit_key, points from m order by 1").fetchall(),
        "poland_newest": newest_pl(con, limit=20),
    }


def check_owid(con) -> dict:
    return {
        "expected_gold_rows": one(
            con,
            "select count(*) from silver where value is not null and not isnan(value) and year between 1800 and 2200 "
            "and not (chart = 'consumption_co2_per_capita' and value = 0)",
        )[0],
        "silver_not_loaded": one(
            con,
            "select count(*) filter (where year < 1800), "
            "count(*) filter (where chart = 'consumption_co2_per_capita' and value = 0) from silver",
        ),
        "measures": con.execute("select measure_key, unit_key, points from m order by 1").fetchall(),
        "source_versions": con.execute("select measure_key, min(source_version), max(source_version) from s group by 1").fetchall(),
        "poland_newest": newest_pl(con),
        "matched_areas": one(con, "select count(distinct geography_key) filter (where geography_key <> '_X') from g")[0],
    }


CHECKS = {
    "unesco_uis": check_unesco_uis,
    "world_bank_edstats": check_world_bank_edstats,
    "world_bank_gem": check_world_bank_gem,
    "owid": check_owid,
}


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
                annotate("notice", f"gold3314 trial: {source} gold", head)
                annotate("notice", f"gold3314 trial: {source} checks", checks)
                con.close()
            except Exception:
                annotate("error", f"gold3314 trial: {source} failed", {"traceback": traceback.format_exc()[-3000:]})
    finally:
        annotate("notice", "gold3314 trial: cleanup", cleanup(cat, client))


if __name__ == "__main__":
    main()
