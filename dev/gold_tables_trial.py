"""Dev trial (scratch only): the per-block Gold load of sources whose Silver is dbt tables (#2998).

Runs warehouse/observations.py for NBP, OECD and World Bank WDI into the scratch namespace and prefix
zz_dev_gold_tables: the first-load guard, the load, a second run with nothing to do, then checks of the
scratch tables against production Silver (rows with a value, rows not loaded by reason), the grain
(series_key x date_key), null keys, unmatched geography codes, statuses, and every row of the curated
build stage (gold.build_nbp, build_oecd, build_wdi) looked up in the new tables. Reads production Silver,
gold.map_geography and the three curated build tables; writes nothing outside the scratch, which it
removes. Deleted before merge.

python -m dev.gold_tables_trial
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

NS = "zz_dev_gold_tables"
SOURCES = ("nbp", "oecd", "world_bank_wdi")


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


def run(source: str, *extra: str) -> dict:
    started = time.monotonic()
    out = obs.run(["--source", source, "--trial", NS, "--minutes", "60", *extra])
    keep = ("result", "blocks_in_silver", "todo", "built", "rows", "series", "measures", "bytes", "silver_rows",
            "remaining", "removed", "rejected", "rejected_blocks", "failed", "failed_blocks", "slowest", "reason")
    brief = {k: out[k] for k in keep if k in out and out[k] not in (0, [], {}, None)}
    brief["result"] = out["result"]
    brief["seconds"] = round(time.monotonic() - started)
    if "coverage" in out:
        c = out["coverage"]
        brief["coverage"] = [c["blocks_in_gold"], c["blocks_in_silver"], c["series_in_gold"], c["coverage_pct"]]
    return brief


def cleanup(cat, client) -> dict:
    gone = {"tables": 0, "objects": 0}
    names = [f"{s}_{t}" for s in SOURCES for t in obs.TABLES] + [obs.REJECTED[1]]
    for name in names:
        with contextlib.suppress(Exception):
            cat.drop_table((NS, name))
            gone["tables"] += 1
    with contextlib.suppress(Exception):
        cat.drop_namespace(NS)
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=NS + "/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            gone["objects"] += len(keys)
    left = sum(len(p.get("Contents", [])) for p in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=NS + "/"))
    production = {t[1] for t in cat.list_tables("gold")}
    return {**gone, "objects_left": left, "namespace_left": (NS,) in cat.list_namespaces(),
            "production_gold_tables_of_these_loads": sorted(production & set(names) - {obs.REJECTED[1]})}


def scratch(cat, source: str) -> dict[str, dict]:
    return {name: et.state(cat.load_table(table_id), obs.ENTITY) for name, table_id in obs.table_ids(source, NS).items()}


def views(con, cat, source: str) -> dict:
    """Views o, s, m, g over the scratch files and silver_<block> over production Silver; sizes."""
    state = scratch(cat, source)
    for name, short in (("observations", "o"), ("series", "s"), ("measures", "m"), ("geography", "g")):
        con.execute(f"create or replace view {short} as select * from {files([p for h in state[name].values() for p in h['paths']])}")
    silver = obs.silver_blocks(cat, source)
    for block, item in silver.items():
        con.execute(f"create or replace view silver_{block} as select * from {files(item['paths'])}")
    return {
        "blocks": {name: sorted(s) for name, s in state.items()}["observations"],
        "files_per_block_and_table": sorted({len(h["paths"]) for s in state.values() for h in s.values()}),
        "mb": {n: round(sum(h["bytes"] for h in s.values()) / 1e6, 2) for n, s in state.items()},
        "silver": {b: {"files": len(i["paths"]), "rows": i["rows"], "mb": round(i["bytes"] / 1e6, 1)} for b, i in silver.items()},
    }


def common(con) -> dict:
    rows, duplicates = con.execute(
        "select coalesce(sum(n), 0), count(*) filter (where n > 1) from (select count(*) as n from o group by series_key, date_key)"
    ).fetchone()
    return {
        "rows": rows,
        "duplicate_series_key_x_date_key": duplicates,
        "null_keys": con.execute(
            "select count(*) from o where series_key is null or measure_key is null or geography_key is null "
            "or date_key is null or frequency is null or value is null or observation_status_key is null"
        ).fetchone()[0],
        "series_measures": con.execute("select (select count(*) from s), (select count(*) from m)").fetchone(),
        "series_points": con.execute("select sum(points) from s").fetchone()[0],
        "by_block_frequency": con.execute(
            "select block_id, frequency, count(*), min(date_key)::varchar, max(date_key)::varchar from o group by 1, 2 order by 1, 2"
        ).fetchall(),
        "status": con.execute("select observation_status_key, count(*) from o group by 1 order by 2 desc").fetchall(),
        "zeros_nan_inf": con.execute(
            "select count(*) filter (where value = 0), count(*) filter (where isnan(value)), count(*) filter (where isinf(value)) from o"
        ).fetchone(),
        "geography": con.execute(
            "select count(*), count(*) filter (where geography_key = '_X'), coalesce(sum(points) filter (where geography_key = '_X'), 0), "
            "count(*) filter (where geography_key = '_Z') from g"
        ).fetchone(),
        "unmatched_codes": con.execute(
            "select geography_source_code, points from g where geography_key = '_X' order by 2 desc"
        ).fetchall()[:45],
    }


def arrow(cat, name: str, columns: tuple[str, ...]):
    return cat.load_table(("gold", name)).scan(selected_fields=columns).to_arrow()


def check_nbp(con, cat) -> dict:
    out = {
        "silver_fx": con.execute(
            "select count(*), count(mid_rate) + count(bid_rate) + count(ask_rate), "
            "count(*) filter (where published_variants > 1), count(*) filter (where effective_date is null), "
            "count(distinct table_code || effective_date::varchar || currency_code) from silver_nbp_fx_rates"
        ).fetchone(),
        "silver_fx_by_table": con.execute(
            "select table_code, count(*), count(mid_rate), count(bid_rate), count(ask_rate), count(distinct currency_code), "
            "min(effective_date)::varchar, max(effective_date)::varchar from silver_nbp_fx_rates group by 1 order by 1"
        ).fetchall(),
        "silver_gold": con.execute(
            "select count(*), count(price_pln_per_gram), count(distinct effective_date) from silver_nbp_gold_fixings"
        ).fetchone(),
        "gold_by_measure": con.execute("select measure_key, series, points from m order by 1").fetchall(),
    }
    con.register("curated", arrow(cat, "build_nbp", ("measure_key", "breakdown_json", "date_key", "value")))
    out["curated_build_nbp_found"] = con.execute(
        """with new as (
               select replace(replace(split_part(measure_key, '@', 1), 'nbp:nbp_fx_rates:PLN:', 'nbp:fx:'),
                      'nbp:nbp_gold_fixings:PLN/g:price', 'nbp:gold:price') as k,
                      split_part(series_key, '|', 3) as b, date_key, value from o)
           select count(*), count(n.k), count(*) filter (where n.value = c.value)
           from curated as c left join new as n on c.measure_key = n.k and c.breakdown_json = n.b and c.date_key = n.date_key"""
    ).fetchone()
    out["newest"] = con.execute(
        "select series_key, date_key::varchar, value from o where (series_key like '%A:mid@D%' and "
        "(series_key like '%\"EUR\"%' or series_key like '%\"USD\"%')) or series_key like '%gold_fixings%' "
        "qualify row_number() over (partition by series_key order by date_key desc) = 1 order by 1"
    ).fetchall()
    out["labels"] = con.execute("select breakdown_label_json from s where breakdown_json like '%EUR%' limit 2").fetchall()
    return out


def check_oecd(con, cat) -> dict:
    placed = """(freq = 'A' and regexp_matches(time_period, '^\\d{4}$')) or (freq = 'Q' and regexp_matches(time_period, '^\\d{4}-Q[1-4]$'))
                or (freq = 'M' and regexp_matches(time_period, '^\\d{4}-(0[1-9]|1[0-2])$'))"""
    out = {
        "silver": con.execute(
            f"select count(*), count(obs_value), count(*) filter (where obs_value is not null and not ({placed})), "
            "count(distinct series_key), count(distinct dataflow) from silver_oecd_cli"
        ).fetchone(),
        "silver_freq": con.execute("select freq, count(*), count(obs_value) from silver_oecd_cli group by 1").fetchall(),
        "silver_status": con.execute(
            "select obs_status, count(*), count(obs_value) from silver_oecd_cli group by 1 order by 2 desc"
        ).fetchall(),
        "null_dimensions": con.execute(
            "select count(*) filter (where measure is null), count(*) filter (where unit_measure is null), "
            "count(*) filter (where adjustment is null), count(*) filter (where transformation is null), "
            "count(*) filter (where methodology is null), count(*) filter (where ref_area is null) from silver_oecd_cli"
        ).fetchone(),
        "measures": con.execute("select measure_key, series, points from m order by 1").fetchall()[:40],
    }
    con.register("curated", arrow(cat, "build_oecd", ("measure_codes", "geography_source_code", "frequency", "date_key", "value")))
    out["curated_build_oecd_found"] = con.execute(
        """with new as (
               select split_part(o.measure_key, ':', 4) || ':' || split_part(o.measure_key, ':', 5) as k,
                      s.geography_source_code as area, o.frequency, o.date_key, o.value
               from o inner join s using (series_key)),
           hits as (
               select c.measure_codes, c.geography_source_code, c.date_key, count(n.k) as found,
                      count(*) filter (where n.value = c.value) as equal
               from curated as c left join new as n
                   on c.measure_codes = n.k and c.geography_source_code = n.area and c.frequency = n.frequency
                   and c.date_key = n.date_key
               group by all)
           select count(*), count(*) filter (where found > 0), count(*) filter (where equal > 0) from hits"""
    ).fetchone()
    out["poland_newest"] = con.execute(
        "select o.measure_key, o.date_key::varchar, o.value, o.observation_status_key from o inner join s using (series_key) "
        "where s.geography_source_code = 'POL' qualify row_number() over (partition by o.series_key order by o.date_key desc) = 1 "
        "order by 1"
    ).fetchall()[:12]
    return out


def check_wdi(con, cat) -> dict:
    out = {
        "silver": con.execute(
            "select count(*), count(value), count(*) filter (where value is null and nullif(trim(value_text), '') is not null), "
            "count(*) filter (where value is not null and year not between 1800 and 2200), "
            "count(distinct indicator_code), count(distinct economy_code), min(year), max(year), "
            "count(distinct indicator_code || '|' || economy_code) filter (where value is not null), "
            "any_value(source_version), count(distinct source_version) from silver_wdi_observations"
        ).fetchone(),
        "silver_duplicates": con.execute(
            "select count(*) from (select 1 from silver_wdi_observations where value is not null "
            "group by indicator_code, economy_code, year having count(*) > 1)"
        ).fetchone()[0],
        "unit_codes": con.execute("select unit_key, count(*) from m group by 1 order by 2 desc").fetchall()[:15],
        "measures_named_by_code": con.execute("select count(*) from m where measure_name = measure_codes").fetchone()[0],
        "forecast": con.execute(
            "select count(*), min(date_key)::varchar, max(date_key)::varchar from o where observation_status_key = 'forecast'"
        ).fetchone(),
        "areas": con.execute(
            "select case when geography_key in ('_X', '_Z') then geography_key when geography_key like '%:%' "
            "then split_part(geography_key, ':', 1) || ':' else 'country' end, count(*), sum(points) from g group by 1 order by 1"
        ).fetchall(),
    }
    con.register("curated", arrow(cat, "build_wdi", ("block_id", "geography_source_code", "geography_key", "date_key", "value")))
    out["curated_build_wdi_found"] = con.execute(
        """with new as (
               select split_part(split_part(o.measure_key, ':', 4), '@', 1) as k, s.geography_source_code as area,
                      o.geography_key, o.date_key, o.value
               from o inner join s using (series_key))
           select count(*), count(n.k), count(*) filter (where n.value = c.value),
                  count(*) filter (where n.geography_key = c.geography_key), count(distinct c.block_id)
           from curated as c left join new as n on c.block_id = n.k and c.geography_source_code = n.area and c.date_key = n.date_key"""
    ).fetchone()
    out["poland_newest"] = con.execute(
        "select split_part(split_part(measure_key, ':', 4), '@', 1), date_key::varchar, value from o "
        "where geography_key = 'PL' and split_part(split_part(measure_key, ':', 4), '@', 1) in "
        "('NY.GDP.MKTP.CD', 'SP.POP.TOTL', 'SL.UEM.TOTL.ZS', 'FP.CPI.TOTL.ZG', 'SP.DYN.LE00.IN', 'NY.GDP.PCAP.PP.CD') "
        "qualify row_number() over (partition by measure_key order by date_key desc) = 1 order by 1"
    ).fetchall()
    out["poland"] = con.execute(
        "select count(distinct measure_key), count(*) from o where geography_key = 'PL'"
    ).fetchone()
    return out


CHECKS = {"nbp": check_nbp, "oecd": check_oecd, "world_bank_wdi": check_wdi}


def main() -> None:
    tmp = "/mnt/duckdb_tmp"
    try:
        subprocess.run(["sudo", "mkdir", "-p", tmp], check=True)
        subprocess.run(["sudo", "chown", str(os.getuid()), tmp], check=True)
    except Exception:
        tmp = tempfile.mkdtemp(prefix="duckdb_tmp")
    os.environ["DUCKDB_TEMP_DIRECTORY"] = tmp
    cat, client = catalog(), s3()
    before = cleanup(cat, client)  # leftovers of a killed earlier run
    obs.annotate = lambda *a, **k: None  # the trial writes its own compact annotations
    digests = {s: obs.model_digest(s) for s in sorted(obs.SOURCES)}
    start = {"cleanup_before": before, "model_digests": digests}
    try:
        for source in SOURCES:
            try:
                runs = {**start, "no filter, empty Gold": run(source, "--continue-only")}
                start = {}
                runs["tables after the guard"] = (
                    sorted(t[1] for t in cat.list_tables(NS) if t[1].startswith(source + "_"))
                    if (NS,) in cat.list_namespaces() else []
                )
                runs["load"] = run(source)
                runs["again"] = run(source)
                runs["again, continue only"] = run(source, "--continue-only")
                con = connect()
                runs["tables"] = views(con, cat, source)
                annotate("notice", f"tables trial: {source} runs", runs)
                checks = common(con)
                annotate("notice", f"tables trial: {source} gold", checks)
                annotate("notice", f"tables trial: {source} against silver and the curated build", CHECKS[source](con, cat))
                con.close()
            except Exception:
                annotate("error", f"tables trial: {source} failed", {"traceback": traceback.format_exc()[-3000:]})
    finally:
        annotate("notice", "tables trial: cleanup", cleanup(cat, client))


if __name__ == "__main__":
    main()
