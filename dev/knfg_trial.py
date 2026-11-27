"""Dev trial (scratch only): per-block Gold of the KNF monthly banking-sector data (knf_banking).

silver.knf_banking_series is not in production yet (first Bronze load 26 Nov 2026; Silver comes with the weekly
model build), so the trial builds it from production bronze.knf_banking_cells (read-only) with the branch's Silver
SQL (Jinja-rendered with warehouse/macros/xlsx_cells.sql) in local DuckDB, writes it to the scratch table
zz_dev_knfg.knf_banking_series, and points warehouse/observations.py at it. Then: the first-load guard, the load, a
second run with nothing to do, a continue-only run; checks of the scratch Gold tables against that Silver (rows with a
value, rows not loaded by reason), the grain (series_key x date_key), null keys, units and unit_map, frequencies per
measure, keys with '|', Poland's newest values, and every row of the curated build stage (poc_build_knf_banking,
rendered on the same Silver) looked up in the new tables. Writes nothing outside the scratch, which it removes.
At most 10 annotations. Deleted before merge.

python -m dev.knfg_trial
"""

from __future__ import annotations

import contextlib
import csv
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import traceback

import duckdb
import jinja2
import yaml

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from warehouse import observations as obs

ROOT = Path(__file__).resolve().parents[1]
NS = "zz_dev_knfg"
SOURCE = "knf_banking"
TABLE = "knf_banking_series"
MACROS = (ROOT / "warehouse/macros/xlsx_cells.sql").read_text()

# the curated stage's seven items (poc_build_knf_banking.sql `selected`): measure_code -> (sheet, item_path)
CURATED = {
    "total_assets": ("Bilans", "Aktywa, razem"),
    "net_profit_ytd": ("RZiS", "Zysk (strata) roku bieżącego"),
    "nonfin_receivables_not_impaired": ("Należności_1", "SEKTOR NIEFINANSOWY > Należności bez utraty wartości, w tym:"),
    "nonfin_receivables_impaired": (
        "Należności_1",
        "SEKTOR NIEFINANSOWY > Należności ze stwierdzoną utratą wartości - Faza 3",
    ),
    "nonfin_deposits": (
        "Zobowiązania",
        "Zobowiązania wobec sektora niefinansowego > DEPOZYTY (bez depozytów zablokowanych) > "
        "Depozyty sektora niefinansowego",
    ),
    "employees": ("Struktura", "Zatrudnienie > Liczba zatrudnionych"),
    "total_capital_ratio": ("Adekwatność", "Łączny współczynnik kapitałowy"),
}
GROUPS = {
    "{}": "banking_sector",
    '{"bank_group":"commercial_banks"}': "commercial_banks",
    '{"bank_group":"credit_institution_branches"}': "credit_institution_branches",
    '{"bank_group":"cooperative_banks"}': "cooperative_banks",
}


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def one(con, sql: str):
    return con.execute(sql).fetchall()[0]


def files(paths: list[str]) -> str:
    return "read_parquet([" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "])"


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
        "production_gold_tables_of_this_load": sorted(p for p in production if p.startswith(SOURCE + "_")),
    }


def silver_tests(con, table: str) -> dict:
    spec = yaml.safe_load((ROOT / f"warehouse/models/silver/{SOURCE}/_{SOURCE}.yml").read_text())["models"][0]
    out = {}
    for test in spec.get("tests", []):
        if "unique" in test:
            col = test["unique"]["column_name"]
            out["unique"] = one(con, f"select count(*) from (select {col} k from {table} group by k having count(*) > 1)")[0]
    for column in spec["columns"]:
        for test in column.get("tests", []):
            if test == "not_null":
                out[f"nn {column['name']}"] = one(con, f"select count(*) from {table} where {column['name']} is null")[0]
            elif isinstance(test, dict) and "accepted_values" in test:
                values = [str(v) for v in test["accepted_values"]["arguments"]["values"]]
                con.execute("create or replace temp table allowed as select unnest(?) as v", [values])
                out[f"av {column['name']}"] = one(
                    con, f"select count(*) from {table} where cast({column['name']} as varchar) not in (select v from allowed)"
                )[0]
    return {k: v for k, v in out.items() if v} or "none failed"


def build_silver(cat, con) -> dict:
    cells = cat.load_table(("bronze", f"{SOURCE}_cells")).scan().to_arrow()
    con.register(f"{SOURCE}_cells", cells)
    tables = {f"{SOURCE}_cells": f"{SOURCE}_cells", TABLE: "silver", "poc_build_knf_banking": "curated"}
    sql = render(ROOT / f"warehouse/models/silver/{SOURCE}/{TABLE}.sql", tables)
    con.execute("create or replace table silver as " + sql)
    con.execute("create or replace table curated as " + render(ROOT / "warehouse/models/gold/poc_build_knf_banking.sql", tables))
    arrow = con.sql("select * from silver").to_arrow_table()
    if (NS,) not in cat.list_namespaces():
        cat.create_namespace(NS)
    table = cat.create_table((NS, TABLE), schema=arrow.schema)
    table.append(arrow)
    return {
        "bronze_cells": cells.num_rows,
        "silver": one(
            con,
            "select count(*), count(*) filter (where value is null or isnan(value) or isinf(value)), "
            "min(period_end)::varchar, max(period_end)::varchar, min(source_version), max(source_version), "
            "count(distinct raw_sha256) from silver",
        ),
        "silver_tests": silver_tests(con, "silver"),
        "units": con.execute(
            "select coalesce(unit_label, '<null>'), contains(coalesce(number_format, ''), '%'), count(*) "
            "from silver group by all order by 3 desc"
        ).fetchall(),
        "items_with_two_units": one(
            con,
            "select count(*) from (select sheet_name, item_path, frequency from silver group by all "
            "having count(distinct contains(coalesce(number_format, ''), '%')) > 1)",
        )[0],
        "sheet_group_freq": con.execute(
            "select sheet_name, group_label, frequency, count(distinct item_path), count(*) from silver "
            "group by all order by 1, 2, 3"
        ).fetchall(),
        "item_paths_with_pipe": one(con, "select count(*) from silver where contains(item_path, '|')")[0],
        "curated_rows": one(con, "select count(*) from curated")[0],
    }


def run(*extra: str) -> list:
    started = time.monotonic()
    out = obs.run(["--source", SOURCE, "--trial", NS, "--minutes", "60", *extra])
    brief = [out["result"], out.get("built", 0), out.get("rows", 0), out.get("rejected", 0), out.get("failed", 0)]
    if out.get("rejected_blocks") or out.get("failed_blocks"):
        brief.append({"rejected": out.get("rejected_blocks"), "failed": out.get("failed_blocks")})
    if "coverage" in out:
        c = out["coverage"]
        brief.append([c["blocks_in_gold"], c["blocks_in_silver"], c["series_in_gold"], c["coverage_pct"]])
    brief.append(round(time.monotonic() - started))
    return brief


def views(rcon, cat) -> dict:
    state = {name: et.state(cat.load_table(t), obs.ENTITY) for name, t in obs.table_ids(SOURCE, NS).items()}
    for name, short in (("observations", "o"), ("series", "s"), ("measures", "m"), ("geography", "g")):
        paths = [p for h in state[name].values() for p in h["paths"]]
        rcon.execute(f"create or replace view {short} as select * from {files(paths)}")
    return {"gold_mb": {n: round(sum(h["bytes"] for h in s.values()) / 1e6, 2) for n, s in state.items()}}


def checks(rcon) -> dict:
    unit_map = {r["unit_code"] for r in csv.DictReader(open(ROOT / f"warehouse/seed_rows/unit_map/{SOURCE}.csv"))}
    rows, dup = one(
        rcon,
        "select coalesce(sum(n), 0), count(*) filter (where n > 1) "
        "from (select count(*) as n from o group by series_key, date_key)",
    )
    units = rcon.execute("select unit_key, count(*), sum(points) from m group by 1 order by 3 desc").fetchall()
    return {
        "rows": rows,
        "dup_series_x_date": dup,
        "null_keys": one(
            rcon,
            "select count(*) from o where series_key is null or measure_key is null or geography_key is null "
            "or date_key is null or frequency is null or value is null or observation_status_key is null",
        )[0],
        "series_measures": one(rcon, "select (select count(*) from s), (select count(*) from m)"),
        "freq": rcon.execute(
            "select frequency, count(*), min(date_key)::varchar, max(date_key)::varchar from o group by 1 order by 1"
        ).fetchall(),
        "freq_per_measure_max": one(rcon, "select max(n) from (select count(distinct frequency) n from o group by measure_key)")[0],
        "status": rcon.execute("select observation_status_key, count(*) from o group by 1").fetchall(),
        "zero_nan_inf": one(
            rcon,
            "select count(*) filter (where value = 0), count(*) filter (where isnan(value)), "
            "count(*) filter (where isinf(value)) from o",
        ),
        "geography": rcon.execute("select geography_source_code, geography_key, points from g").fetchall(),
        "units": units,
        "units_unmapped": sorted({u[0] for u in units} - unit_map),
        "total_series": one(rcon, "select count(*) filter (where is_total), count(*) from s")[0:2],
        "measure_key_max_len": one(rcon, "select max(length(measure_key)) from m")[0],
        "keys_with_pipe": one(rcon, "select count(*) from m where contains(measure_key, '|')")[0],
    }


def curated_found(rcon, con) -> list:
    """Every row of the curated build_knf_banking looked up in the new tables by item, bank group, date and value."""
    rcon.register("cur", con.sql("select measure_codes, breakdown_json, frequency, date_key, value from curated").to_arrow_table())
    rcon.execute(
        "create or replace temp table cmap as select * from (values "
        + ", ".join(
            "('" + k + "', '" + s.replace("'", "''") + "', '" + p.replace("'", "''") + "')" for k, (s, p) in CURATED.items()
        )
        + ") as t (code, sheet, item_path)"
    )
    rcon.execute(
        "create or replace temp table gmap as select * from (values "
        + ", ".join(f"('{k}', '{v}')" for k, v in GROUPS.items())
        + ") as t (cur_breakdown, grp)"
    )
    rcon.execute(
        """create or replace temp table cur_found as
           select c.*, exists (
               select 1 from o as n
               where n.date_key = c.date_key
               and split_part(n.measure_key, ':', 4) = x.sheet
               and n.measure_key like '%:' || x.item_path || '@' || c.frequency
               and n.series_key like '%|PL|{"bank_group":"' || gm.grp || '"}'
               and abs(n.value - c.value) <= 1e-9 * greatest(1, abs(c.value))) as found
           from cur as c join cmap as x on c.measure_codes = x.code join gmap as gm on c.breakdown_json = gm.cur_breakdown"""
    )
    return [
        one(rcon, "select (select count(*) from cur), count(*), count(*) filter (where found) from cur_found"),
        rcon.execute("select measure_codes, count(*) from cur_found where not found group by all order by 2 desc").fetchall()[:8],
    ]


def poland(rcon) -> list:
    return rcon.execute(
        """select o.measure_key, o.date_key::varchar, o.value from o
           where o.series_key like '%|PL|{"bank_group":"banking_sector"}'
           and (o.measure_key like '%:Bilans:Aktywa, razem@M' or o.measure_key like '%:Adekwatność:Łączny współczynnik kapitałowy@Q'
                or o.measure_key like '%:Struktura:Zatrudnienie > Liczba zatrudnionych@M'
                or o.measure_key like '%:RZiS:Zysk (strata) roku bieżącego@M')
           qualify row_number() over (partition by o.measure_key order by o.date_key desc) = 1 order by 1"""
    ).fetchall()


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
    stage = obs.SOURCES[SOURCE]["tables"][TABLE][1]
    obs.SOURCES[SOURCE]["tables"] = {TABLE: ((NS, TABLE), stage)}  # the scratch Silver; workers need only the stage
    try:
        con = duckdb.connect()
        con.execute("SET memory_limit = '8GB'")
        try:
            silver = build_silver(cat, con)
            annotate("notice", "knfg trial: silver (scratch, from production Bronze)", {"cleanup_before": before, **silver})
            runs = {
                "guard": run("--continue-only"),
                "load": run(),
                "again": run(),
                "again_continue_only": run("--continue-only"),
                "model_digest": obs.model_digest(SOURCE),
            }
            rcon = connect()
            head = {"runs": runs, **views(rcon, cat), **checks(rcon)}
            annotate("notice", "knfg trial: gold", head)
            annotate(
                "notice",
                "knfg trial: curated rows found and Poland",
                {"curated_total_rows_found_misses": curated_found(rcon, con), "poland_newest": poland(rcon)},
            )
            rcon.close()
        except Exception:
            annotate("error", "knfg trial failed", {"traceback": traceback.format_exc()[-3000:]})
    finally:
        annotate("notice", "knfg trial: cleanup", cleanup(cat, client))


if __name__ == "__main__":
    main()
