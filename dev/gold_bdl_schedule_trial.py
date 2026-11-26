"""Dev trial (scratch only): GUS BDL per-block Gold before gold.observations is authorized (gold lane, #1178).

1. Zeros: are the exact zeros of silver.bdl_observations published zeros or filled empty cells? The BDL
   API's attribute dictionary; zeros, empty cells and their attributes and value texts by channel on a
   sample of subgroups (both channels); and one web-export subgroup's zero cells compared, cell by cell,
   with the BDL API's answer for the same variable, level and year (value and attribute id).
2. Build: warehouse/observations.py for gus_bdl into the scratch namespace and prefix zz_dev_gold_bdl2 on a
   sample of subgroups (the curated KPI subgroups, the largest, monthly and quarterly ones): the first-load
   guard, the load, a second run with nothing to do (same filter); grain (series_key x date_key), null keys, Silver rows
   = Gold rows + rows not loaded, unmatched geography, statuses, Poland's newest values, and the curated
   gold.build_bdl rows of these subgroups looked up under the same key.

Reads production Silver, gold.map_geography, gold.build_bdl and the BDL API (registered key, a few dozen
requests); writes nothing outside the scratch, which it removes. At most 10 annotations. Deleted before merge.

python -m dev.gold_bdl_schedule_trial
"""

from __future__ import annotations

import contextlib
import os
import random
import subprocess
import tempfile
import time
import traceback

import duckdb

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.gus_bdl.api import Bdl
from warehouse import observations as obs

NS = "zz_dev_gold_bdl2"
SOURCE = "gus_bdl"
CURATED = ["P2137", "P2392", "P1873", "P2497", "P3824", "P1410", "P2425", "P2961", "P2496", "P2730", "P1869"]
TERYT7 = (
    "case when length(c) = 7 then c when c = '000000000000' then '0000000' "
    "when substr(c, 10, 3) <> '000' then substr(c, 3, 2) || substr(c, 8, 2) || substr(c, 10, 3) "
    "when substr(c, 8, 2) <> '00' then substr(c, 3, 2) || substr(c, 8, 2) || '000' "
    "when substr(c, 5, 8) = '00000000' then substr(c, 3, 2) || '00000' end"
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
    con.execute(f"create or replace macro teryt7(c) as ({TERYT7})")
    return con


def files(paths: list[str]) -> str:
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"


def one(con, sql: str):
    return con.execute(sql).fetchone()


def channels(cat) -> dict[str, str]:
    """Subgroup -> channel of its Silver file, from the manifest's column bounds (metadata only)."""
    table = cat.load_table(obs.SOURCES[SOURCE]["silver"])
    found = table.inspect.files()
    out = {}
    for partition, metrics in zip(  # noqa: B905 - columns of one arrow table
        found.column("partition").to_pylist(), found.column("readable_metrics").to_pylist()
    ):
        bounds = (metrics or {}).get("channel") or {}
        low, high = bounds.get("lower_bound"), bounds.get("upper_bound")
        sub = (partition or {}).get(table.spec().fields[0].name)
        out[sub] = low if low == high else f"{low}..{high}"
    return out


def zeros(con, silver: dict, chan: dict) -> dict:
    """Zero and empty cells by channel, attribute and value text on a sample of both channels."""
    rng = random.Random(1178)
    api = sorted(b for b, c in chan.items() if c == "api" and b in silver)
    web = sorted(b for b, c in chan.items() if c == "web_export" and b in silver)
    by_rows = sorted(web, key=lambda b: silver[b]["rows"], reverse=True)
    sample = sorted(set(rng.sample(api, min(6, len(api))) + by_rows[:3] + rng.sample(web, min(9, len(web)))))
    paths = [p for b in sample for p in silver[b]["paths"]]
    con.execute(
        f"create or replace table z as select subgroup_id, channel, attribute, value_text, value, variable_id, "
        f"unit_code, year from {files(paths)}"
    )
    return {
        "channels_all_subgroups": {
            c: sum(1 for v in chan.values() if v == c) for c in sorted(set(chan.values()), key=str)
        },
        "sample": sample,
        "by_channel": con.execute(
            "select channel, count(*), count(value), count(*) filter (where value = 0), "
            "count(*) filter (where value = 0 and attribute is null) from z group by 1 order by 1"
        ).fetchall(),
        "zero_attr": con.execute(
            "select channel, attribute, value_text, count(*) from z where value = 0 "
            "group by all order by 4 desc limit 12"
        ).fetchall(),
        "empty_text_attr": con.execute(
            "select channel, value_text, attribute, count(*) from z where value is null group by all "
            "order by 4 desc limit 10"
        ).fetchall(),
        "attr_all": con.execute(
            "select channel, attribute, count(*), count(*) filter (where value = 0) from z where attribute is not null "
            "group by all order by 3 desc limit 12"
        ).fetchall(),
        "zero_share_by_subgroup": con.execute(
            "select subgroup_id, channel, count(value), round(100.0 * count(*) filter (where value = 0) / "
            "nullif(count(value), 0), 1) from z group by all order by 1"
        ).fetchall(),
    }


def api_compare(con, bdl: Bdl) -> dict:
    """The web-export subgroup with the most zero cells at gmina level: its top variable x year, every
    gmina cell compared with the API's answer (val, attrId) for the same variable and year."""
    pick = one(
        con,
        "select subgroup_id, variable_id, year, count(*) from z where channel = 'web_export' and value = 0 "
        "and variable_id is not null and length(teryt7(unit_code)) = 7 and right(teryt7(unit_code), 3) <> '000' "
        "group by all order by 4 desc limit 1",
    )
    if not pick:
        return {"pick": None}
    sub, var, year, _ = pick
    results, page = [], 0
    while page < 40:
        body = bdl.get(
            f"/data/by-variable/{var}", **{"unit-level": 6, "year": year, "page-size": 100, "page": page, "lang": "pl"}
        )
        results += body.get("results") or []
        if not body.get("links", {}).get("next"):
            break
        page += 1
    rows = [
        (u.get("id"), u.get("name"), v.get("val"), v.get("attrId"))
        for u in results
        for v in (u.get("values") or [])
        if str(v.get("year")) == str(year)
    ]
    con.execute("create or replace table a (unit_id varchar, name varchar, val double, attr integer)")
    con.executemany("insert into a values (?, ?, ?, ?)", rows)
    con.execute(
        f"create or replace table w as select teryt7(unit_code) as t7, value, value_text, attribute from z "
        f"where subgroup_id = '{sub}' and variable_id = {int(var)} and year = {int(year)} "
        f"and right(teryt7(unit_code), 3) <> '000' and length(teryt7(unit_code)) = 7"
    )
    return {
        "pick": [sub, var, year],
        "api_pages": page + 1,
        "api_units": len(results),
        "api_cells": len(rows),
        "api_by_val_attr": con.execute(
            "select val = 0 as zero, val is null as empty, attr, count(*) from a group by all order by 4 desc limit 8"
        ).fetchall(),
        "web_cells": one(con, "select count(*), count(value), count(*) filter (where value = 0) from w"),
        "compare": con.execute(
            "select case when w.value is null then 'web empty' when w.value = 0 then 'web 0' else 'web number' end, "
            "case when a.unit_id is null then 'api no cell' when a.val is null then 'api empty' "
            "when a.val = w.value then 'api same' else 'api other' end, a.attr, count(*) "
            "from w left join a on w.t7 = teryt7(a.unit_id) group by all order by 4 desc limit 12"
        ).fetchall(),
        "api_cells_not_in_web": one(
            con, "select count(*) from a where teryt7(unit_id) not in (select t7 from w where t7 is not null)"
        )[0],
    }


def run(*extra: str) -> list:
    started = time.monotonic()
    out = obs.run(["--source", SOURCE, "--trial", NS, "--minutes", "120", *extra])
    brief = {
        k: out.get(k)
        for k in (
            "result",
            "todo",
            "built",
            "rows",
            "silver_rows",
            "rejected",
            "failed",
            "not_loaded",
            "built_by_group",
            "minutes",
            "bytes",
        )
    }
    if out.get("rejected_blocks") or out.get("failed_blocks"):
        brief["bad"] = {"rejected": out.get("rejected_blocks"), "failed": out.get("failed_blocks")}
    brief["s"] = round(time.monotonic() - started)
    return brief


def cleanup(cat, client) -> dict:
    gone = {"tables": 0, "objects": 0}
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
        "production_gus_bdl_per_block_tables": sorted(n for n in production if n.startswith("gus_bdl_")),
    }


def build_sample(silver: dict) -> list[str]:
    curated = [b for b in CURATED if b in silver]
    rest = sorted((b for b in silver if b not in curated), key=lambda b: silver[b]["rows"], reverse=True)
    rng = random.Random(26)
    return sorted(set(curated + rest[:2] + rng.sample(rest[2:], 8)))


def gold_checks(con, cat, blocks: list[str], silver: dict) -> dict:
    state = {name: et.state(cat.load_table(t), obs.ENTITY) for name, t in obs.table_ids(SOURCE, NS).items()}
    for name, short in (("observations", "o"), ("series", "s"), ("measures", "m"), ("geography", "g")):
        paths = [p for h in state[name].values() for p in h["paths"]]
        con.execute(f"create or replace view {short} as select * from {files(paths)}")
    rows, dup = one(
        con,
        "select coalesce(sum(n), 0), count(*) filter (where n > 1) "
        "from (select count(*) as n from o group by series_key, date_key)",
    )
    return {
        "blocks_in_gold": sorted(state["observations"]),
        "silver_rows": sum(silver[b]["rows"] for b in blocks),
        "gold_mb": {n: round(sum(h["bytes"] for h in s.values()) / 1e6, 1) for n, s in state.items()},
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
        "zero_rows": one(con, "select count(*) filter (where value = 0), count(*) from o")[0],
        "geo": one(
            con,
            "select count(*), count(*) filter (where geography_key = '_X'), "
            "coalesce(sum(points) filter (where geography_key = '_X'), 0), coalesce(sum(points), 0) from g",
        ),
        "unmatched_top": con.execute(
            "select geography_source_code, points from g where geography_key = '_X' order by points desc limit 12"
        ).fetchall(),
        "units_top": con.execute("select unit_key, count(*) from m group by 1 order by 2 desc limit 8").fetchall(),
    }


def poland_and_curated(con, cat, blocks: list[str]) -> dict:
    newest = con.execute(
        "select split_part(o.measure_key, ':', 2) as b, o.measure_key, o.date_key::varchar, o.value, "
        "o.observation_status_key from o inner join s using (series_key) where o.geography_key = 'PL' "
        "and s.is_total and split_part(o.measure_key, ':', 2) in ('P2137', 'P2392', 'P2497', 'P1410', 'P2961') "
        "qualify row_number() over (partition by o.series_key order by o.date_key desc) = 1 "
        "order by 1, 2 limit 12"
    ).fetchall()
    table = cat.load_table(("gold", "build_bdl"))
    cur = table.scan(
        selected_fields=("block_id", "measure_key", "geography_key", "date_key", "breakdown_json", "value")
    ).to_arrow()
    con.register("cur_all", cur)
    listed = ", ".join(f"'{b}'" for b in blocks)
    con.execute(f"create or replace table cur as select * from cur_all where block_id in ({listed})")
    found = con.execute(
        """with new as (select o.measure_key, o.geography_key, o.date_key, s.breakdown_json, o.value
                        from o inner join s using (series_key))
           select c.block_id, count(*) as curated,
                  count(n.value) filter (where n.value = c.value) as same,
                  count(n.value) filter (where n.value <> c.value) as differ,
                  count(*) filter (where n.value is null) as not_under_same_key,
                  count(*) filter (where n.value is null and exists (
                      select 1 from o as x where x.geography_key = c.geography_key and x.date_key = c.date_key
                      and x.value = c.value and split_part(x.measure_key, ':', 2) = c.block_id)) as same_value_other_key
           from cur as c left join new as n
             on n.measure_key = c.measure_key and n.geography_key = c.geography_key and n.date_key = c.date_key
            and coalesce(n.breakdown_json, '') = coalesce(c.breakdown_json, '')
           group by 1 order by 1"""
    ).fetchall()
    return {"poland_newest": newest, "curated_by_block": found}


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
    try:
        silver = obs.silver_blocks(cat, SOURCE)
        con = connect()
        try:
            bdl = Bdl.shared()
            attrs = bdl.get("/attributes", lang="pl", **{"page-size": 100})
            annotate(
                "notice",
                "bdl trial: API attributes",
                {
                    "silver_blocks": len(silver),
                    "silver_rows": sum(v["rows"] for v in silver.values()),
                    "attributes": [
                        [a.get("id"), a.get("symbol"), (a.get("description") or "")[:60]]
                        for a in attrs.get("results") or []
                    ],
                },
            )
            chan = channels(cat)
            annotate("notice", "bdl trial: zeros by channel", zeros(con, silver, chan))
            annotate(
                "notice", "bdl trial: zeros against the API", {**api_compare(con, bdl), "api_requests": bdl.requests}
            )
        except Exception:
            annotate("error", "bdl trial: zero check failed", {"traceback": traceback.format_exc()[-3000:]})
        blocks = build_sample(silver)
        try:
            runs = {"guard": run("--continue-only")}
            runs["tables_after_guard"] = (
                sorted(t[1] for t in cat.list_tables(NS)) if (NS,) in cat.list_namespaces() else []
            )
            runs["load"] = run("--only", *blocks)
            runs["again"] = run("--only", *blocks)
            annotate(
                "notice",
                "bdl trial: gold runs",
                {"before": before, "digest": obs.model_digest(SOURCE), "blocks": blocks, **runs},
            )
            annotate("notice", "bdl trial: gold checks", gold_checks(con, cat, blocks, silver))
            annotate("notice", "bdl trial: poland and curated", poland_and_curated(con, cat, blocks))
        except Exception:
            annotate("error", "bdl trial: gold failed", {"traceback": traceback.format_exc()[-3000:]})
        con.close()
    finally:
        annotate("notice", "bdl trial: cleanup", cleanup(cat, client))


if __name__ == "__main__":
    main()
