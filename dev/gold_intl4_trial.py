"""Dev trial (scratch only): per-block Gold of FAOSTAT, World Bank IDS, EEA and ECB (gold lane, #2873).

Runs warehouse/observations.py for the four sources into the scratch namespace and prefix zz_dev_gold_intl4:
the first-load guard, the load, a second run with nothing to do, then checks of the scratch tables against
production Silver (rows with a value, rows not loaded by reason), the grain (series_key x date_key), null
keys, unmatched geography codes, statuses, units, Poland's newest values, and every row of the curated build
stage (gold.build_<source>) looked up in the new tables by area, frequency, date and value. Reads production
Silver, gold.map_geography, gold.dim_geography and the curated build tables; writes nothing outside the
scratch, which it removes. At most 10 annotations. Deleted before merge.

python -m dev.gold_intl4_trial
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

NS = "zz_dev_gold_intl4"
SOURCES = ("faostat", "world_bank_ids", "eea", "ecb")
VERSION_DATE = (
    "try_cast(regexp_replace(regexp_extract(source_version, '\\d{4}-?\\d{2}-?\\d{2}'), "
    "'^(\\d{4})-?(\\d{2})-?(\\d{2})$', '\\1-\\2-\\3') as date)"
)


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
            except Exception as exc:  # noqa: BLE001 - reported in the annotation
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
    (block, item), = silver.items()
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
        "unmatched": [r[0] for r in con.execute(
            "select geography_source_code from g where geography_key = '_X' order by points desc"
        ).fetchall()][:40],
        "units": con.execute("select unit_key, count(*) from m group by 1 order by 2 desc").fetchall()[:8],
    }


CURATED = {"world_bank_ids": "build_ids"}


def curated(con, cat, source: str) -> list:
    """Every row of the curated build stage, looked up in the new tables by area code, frequency, date and value."""
    table = cat.load_table(("gold", CURATED.get(source, f"build_{source}")))
    con.register(
        "cur",
        table.scan(
            selected_fields=("geography_source_code", "frequency", "date_key", "value", "observation_status_key")
        ).to_arrow(),
    )
    return list(
        one(
            con,
            """with new as (select s.geography_source_code as area, o.frequency, o.date_key, o.value
                            from o inner join s using (series_key))
               select count(*), count(*) filter (where c.observation_status_key = 'forecast'),
                      count(*) filter (where exists (
                   select 1 from new as n where n.area is not distinct from c.geography_source_code and n.frequency = c.frequency
                   and n.date_key = c.date_key and n.value = c.value)) from cur as c""",
        )
    )


def poland(con, key: str = "PL") -> dict:
    return {
        "measures_rows": one(
            con, f"select count(distinct s.measure_key), count(*) from o inner join s using (series_key) "
            f"where o.geography_key = '{key}'"
        ),
        "newest": con.execute(
            "select o.measure_key as k, "
            "o.date_key::varchar, o.value, o.observation_status_key from o inner join s using (series_key) "
            f"where o.geography_key = '{key}' and s.is_total "
            "qualify row_number() over (partition by o.series_key order by o.date_key desc) = 1 order by 1"
        ).fetchall()[:8],
    }


def check_faostat(con) -> dict:
    return {
        "silver_rows_value": one(con, "select count(*), count(value) from silver"),
        "not_loaded": one(
            con,
            "select count(*) filter (where value is not null and flag = 'M'), "
            "count(*) filter (where value is not null and is_projection), "
            "count(*) filter (where value is not null and not (year between 1800 and 2200)) from silver",
        ),
        "flags": con.execute(
            "select flag, count(*), count(*) filter (where value = 0) from silver where value is not null "
            "group by 1 order by 2 desc"
        ).fetchall(),
        "areas_silver": one(con, "select count(distinct area_code) from silver")[0],
        "poland": poland(con),
    }


def check_world_bank_ids(con) -> dict:
    return {
        "silver_rows_value": one(con, "select count(*), count(value), count(distinct series_code) from silver"),
        "last_actual_year": one(
            con, "select max(year) filter (where series_code = 'DT.DOD.DECT.CD' and value is not null), "
            "max(year), min(source_version) from silver"
        ),
        "not_loaded_after_last_actual": con.execute(
            "select year, count(*) from silver where value is not null and year > (select max(year) from silver "
            "where series_code = 'DT.DOD.DECT.CD' and value is not null) group by 1 order by 1"
        ).fetchall(),
        "nan": one(con, "select count(*) from silver where isnan(value)")[0],
        "counterparts": one(con, "select count(distinct counterpart_code) from silver where value is not null")[0],
        "dup_if_counterpart_left_out": one(
            con,
            "select count(*) from (select 1 from silver where value is not null group by economy_code, "
            "series_code, year having count(*) > 1)",
        )[0],
        "turkey": poland(con, "TR"),
    }


def check_eea(con) -> dict:
    return {
        "silver_rows_value": one(con, "select count(*), count(value), count(distinct entity) from silver"),
        "notation": con.execute(
            "select notation, count(*) filter (where value is null), count(*) filter (where value = 0), "
            "count(*) filter (where value <> 0) from silver where notation is not null group by 1 order by 2 desc"
        ).fetchall()[:12],
        "units_formats": con.execute(
            "select unit, format_name, count(*) from silver where value is not null group by all order by 3 desc"
        ).fetchall()[:8],
        "poland": poland(con),
    }


def check_ecb(con) -> dict:
    return {
        "silver_rows_value": one(con, "select count(*), count(obs_value), count(distinct series_key) from silver"),
        "not_loaded": one(
            con,
            "select count(*) filter (where obs_value is not null and obs_status = 'F'), "
            "count(*) filter (where obs_value is not null and (period_start is null "
            "or freq not in ('A', 'Q', 'M', 'D', 'B'))), count(*) filter (where isnan(obs_value)) from silver",
        ),
        "freq_flow": con.execute(
            "select dataflow, freq, count(obs_value) from silver group by all order by 1, 2"
        ).fetchall(),
        "obs_status": con.execute(
            "select obs_status, count(*) from silver where obs_value is not null group by 1 order by 2 desc"
        ).fetchall(),
        "unit_mult": con.execute(
            "select unit, unit_mult, count(*) from silver where obs_value is not null group by all order by 3 desc"
        ).fetchall()[:10],
        "poland": poland(con),
    }


CHECKS = {
    "faostat": check_faostat,
    "world_bank_ids": check_world_bank_ids,
    "eea": check_eea,
    "ecb": check_ecb,
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
    digests = {s: obs.model_digest(s) for s in sorted(obs.SOURCES)}
    first = {"cleanup_before": before, "model_digests": digests}
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
                checks["curated_found"] = curated(con, cat, source)
                annotate("notice", f"intl4 trial: {source} gold", head)
                annotate("notice", f"intl4 trial: {source} checks", checks)
                con.close()
            except Exception:
                annotate("error", f"intl4 trial: {source} failed", {"traceback": traceback.format_exc()[-3000:]})
    finally:
        annotate("notice", "intl4 trial: cleanup", cleanup(cat, client))


if __name__ == "__main__":
    main()
