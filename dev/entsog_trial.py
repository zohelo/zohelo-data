"""Dev trial for entsog (PL-ENV-021): Landing → Bronze → Silver → Gold into scratch zz_dev_entsog, each step twice.

Landing: the full backfill of the window (one request per gas month, 5 s apart) into the Landing prefix
zz_dev_entsog/, then a second load (only open months and one rotation month: must write nothing), inspect and
verify. Bronze: the source's loader into Iceberg namespace zz_dev_entsog and lake prefix zz_dev_entsog/, twice (the
second appends nothing). Silver, singular tests and the Gold build stage are rendered with Jinja from
warehouse/models, warehouse/tests and warehouse/macros/sources/entsog.sql and run in local DuckDB over that Bronze
table, each built twice. Compact annotations (the activities' own annotations are collected, not printed).
Everything written is removed in `finally`; never writes a production namespace or prefix.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import duckdb
from jinja2 import Environment

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.entsog import extract
from ingestion.to_bronze.entsog import load as bronze_load

SCRATCH = "zz_dev_entsog"
MACROS = Path("warehouse/macros/sources/entsog.sql").read_text()
COLLECTED: list = []


def collect(level, title, data):  # the activities' annotations, kept for the trial's own
    COLLECTED.append({"level": level, "title": title, "data": data})


extract.annotate = collect
bulk_bronze.annotate = collect


def render(path: str | None, refs: dict[str, str], sources: dict[str, str], text: str = "") -> str:
    text = MACROS + (Path(path).read_text() if path else text)
    captured = {}

    def ret(value):
        captured["value"] = value
        return ""

    out = Environment(extensions=["jinja2.ext.do"]).from_string(text).render(
        config=lambda **_k: "",
        ref=lambda name: refs.get(name, name),
        source=lambda _s, table: sources[table],
        **{"return": ret},
    )
    return captured.get("value", out)


def q(con, query: str, limit: int = 100) -> list:
    return [[str(v) for v in r] for r in con.execute(query).fetchall()][:limit]


def main() -> int:
    r: dict = {}
    status = 0
    try:
        # Landing: full backfill, then a second pass, inspect, verify
        args = ["--landing-root", SCRATCH, "--budget-minutes", "45"]
        r["landing_exit"] = [extract.main(["--mode", "load", *args]) for _ in (1, 2)]
        r["inspect_exit"] = extract.main(["--mode", "inspect", *args])
        r["verify_exit"] = extract.main(["--mode", "verify", *args])
        r["landing_runs"] = [c["data"] for c in COLLECTED]
        held = extract.landed_all(s3(), req("STORAGE_LANDING_BUCKET_NAME"), SCRATCH)
        r["landing_files"] = sum(len(v) for v in held.values())
        r["landing_bytes"] = sum(x["size_bytes"] for v in held.values() for x in v)
        r["entities_with_2_copies"] = [k for k, v in held.items() if len(v) > 1]
        COLLECTED.clear()
        annotate("notice", "entsog trial Landing", r)

        # Bronze twice
        b: dict = {"bronze_exit": [bronze_load.load(0, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]}
        b["bronze_runs"] = [c["data"] for c in COLLECTED]
        cat = catalog()
        table = cat.load_table((SCRATCH, bronze_load.TABLE))
        arrow = table.scan().to_arrow()
        b["bronze_files"] = len(list(table.scan().plan_files()))
        b["bronze_rows"] = arrow.num_rows
        try:
            rejected = cat.load_table((SCRATCH, bronze_load.REJECTED)).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001
            rejected = []
        b["rejected"] = [{k: x.get(k) for k in ("entity", "error")} for x in rejected][:5]
        con = duckdb.connect()
        con.register("bronze_rows", arrow)
        b["profile"] = q(con, """
            select count(*), count(distinct point_key || '|' || direction_key || '|' || period_from),
                   min(period_from), max(period_from), count(distinct entity),
                   count(*) filter (where point_key is null or direction_key is null or period_from is null),
                   count(*) filter (where value_text is null), count(*) filter (where value_text = ''),
                   count(*) filter (where is_na is not null), count(distinct point_key || '|' || direction_key)
            from bronze_rows""")
        b["profile_cols"] = "rows, distinct keys, min/max periodFrom, entities, null keys, value null, value '', isNA set, series"
        b["is_na x flow_status x value"] = q(con, """
            select is_na, flow_status, value_text = '' as empty, count(*) from bronze_rows group by all order by 4 desc""")
        b["non_numeric_values"] = q(con, """
            select value_text, count(*) from bronze_rows where try_cast(value_text as double) is null group by 1""")
        b["dup_keys_bronze"] = q(con, """
            select count(*) from (select point_key, direction_key, period_from from bronze_rows group by all
            having count(*) > 1)""")
        b["unit/indicator/periodType"] = q(con, "select unit, indicator, period_type, count(*) from bronze_rows group by all")
        COLLECTED.clear()
        annotate("notice", "entsog trial Bronze", b)

        # Silver twice
        s: dict = {}
        flow_sql = render("warehouse/models/silver/entsog/entsog_physical_flow.sql", {}, {bronze_load.TABLE: "bronze_rows"})
        points_sql = render("warehouse/models/silver/entsog/entsog_points.sql", {}, {})
        counts = []
        for _ in (1, 2):
            con.execute(f"create or replace table entsog_physical_flow as {flow_sql}")
            con.execute(f"create or replace table entsog_points as {points_sql}")
            counts.append(q(con, "select (select count(*) from entsog_physical_flow), (select count(*) from entsog_points)")[0])
        s["silver_rows_two_builds"] = counts
        grain = "operator_key, point_key, direction_key, indicator, period_type, period_from"
        s["dup_keys"] = q(con, f"select count(*) from (select {grain} from entsog_physical_flow group by all having count(*) > 1)")
        s["null_keys"] = q(con, "select count(*) from entsog_physical_flow where " + " or ".join(
            f"{k.strip()} is null" for k in (grain + ", gas_day, raw_sha256").split(",")))
        s["range"] = q(con, """select min(gas_day), max(gas_day), count(distinct gas_day), count(value_kwh_d),
            count(*) filter (where value_kwh_d is null), count(*) filter (where is_na),
            count(*) filter (where value_kwh_d = 0), max(value_kwh_d), min(value_kwh_d) from entsog_physical_flow""")
        s["range_cols"] = "first, last day, days, values, nulls, isNA, zeros, max, min"
        s["flow_status"] = q(con, "select flow_status, count(*) from entsog_physical_flow group by 1 order by 2 desc")
        s["provisional_by_month"] = q(con, """select strftime(gas_day, '%Y-%m'), count(*) from entsog_physical_flow
            where flow_status = 'Provisional' group by 1 order by 1""")
        s["tests"] = {
            t: q(con, f"select count(*) from ({render(f'warehouse/tests/{t}.sql', {}, {})})")[0][0]
            for t in ("entsog_gas_days", "entsog_physical_flow_complete", "entsog_point_groups")
        }
        s["complete_failures"] = q(con, f"select * from ({render('warehouse/tests/entsog_physical_flow_complete.sql', {}, {})}) limit 12")
        s["unmapped"] = q(con, f"select * from ({render('warehouse/tests/entsog_point_groups.sql', {}, {})})")
        annotate("notice", "entsog trial Silver", s)
        pts = q(con, """select point_key || '|' || direction_key || '|' || point_label || '|' || coalesce(point_type, '-')
            || '|' || first_gas_day || '|' || last_gas_day || '|' || days_with_value || '/' || gas_days
            from entsog_points order by point_key, direction_key""")
        half = (len(pts) + 1) // 2
        annotate("notice", "entsog trial points 1/2", [p[0] for p in pts[:half]])
        annotate("notice", "entsog trial points 2/2", [p[0] for p in pts[half:]])

        # Gold twice
        g: dict = {}
        con.execute("create or replace table geo as select * from (values ('PL', 'PL', 'POL', 'country'), "
                    "('DE', 'DE', 'DEU', 'country')) as t (geography_key, iso2, iso3, level_name)")
        con.execute(f"create or replace table gmap as with {render(None, {}, {}, text='{{ poc_geography__entsog() }}')} "
                    "select * from entsog")
        build_sql = render("warehouse/models/gold/poc_build_entsog.sql", {"poc_map_geography": "gmap"}, {})
        rows = []
        for _ in (1, 2):
            con.execute(f"create or replace table build as {build_sql}")
            rows.append(q(con, "select count(*) from build")[0][0])
        g["gold_rows_two_builds"] = rows
        g["registration"] = str(render(None, {}, {}, text="{{ poc_source__entsog() }}"))
        g["gold_dup_series_date"] = q(con, """select count(*) from (select measure_key || '|' || geography_key || '|' ||
            breakdown_json as sk, date_key from build group by all having count(*) > 1)""")
        g["measures"] = q(con, """select measure_key, frequency, count(distinct breakdown_json), count(*), min(date_key),
            max(date_key), max(value), count(*) filter (where observation_status_key = 'provisional')
            from build group by all order by 1""")
        g["geography"] = q(con, "select geography_key, geography_source_code, count(*) from build group by all")
        g["tests"] = {
            t: q(con, f"select count(*) from ({render(f'warehouse/tests/{t}.sql', {}, {})})")[0][0]
            for t in ("entsog_value_range",)
        }
        g["days_total_vs_silver"] = q(con, """
            select direction_key, count(distinct gas_day) as silver_days,
              (select count(*) from build b where b.frequency = 'D' and b.breakdown_json = '{}'
                 and b.measure_codes = f.direction_key) as complete_days
            from entsog_physical_flow f group by 1""")
        g["incomplete_days_sample"] = q(con, """
            with d as (select distinct direction_key, gas_day from entsog_physical_flow)
            select d.direction_key, strftime(d.gas_day, '%Y-%m') as m, count(*) from d
            left join build b on b.frequency = 'D' and b.breakdown_json = '{}' and b.measure_codes = d.direction_key
              and b.date_key = d.gas_day
            where b.date_key is null group by all order by 2 desc limit 15""")
        g["PL latest daily totals"] = q(con, """select measure_key, date_key, value, observation_status_key from build
            where breakdown_json = '{}' and frequency = 'D'
            qualify row_number() over (partition by measure_key order by date_key desc) = 1""")
        g["PL 2026-10-15 entry by group"] = q(con, """select breakdown_json, value, observation_status_key from build
            where frequency = 'D' and date_key = date '2026-10-15' and measure_codes = 'entry' order by value desc""")
        g["PL monthly totals 2026"] = q(con, """select measure_key, date_key, value, observation_status_key from build
            where breakdown_json = '{}' and frequency = 'M' and date_key >= date '2026-01-01' order by 1, 2""", 24)
        annotate("notice", "entsog trial Gold", g)
        lic = duckdb.connect()
        lic.execute("create table source as select * from read_csv('warehouse/seeds/source.csv', all_varchar=true)")
        lic.execute("create table licence as select * from read_csv('warehouse/seeds/licence.csv', all_varchar=true)")
        annotate("notice", "entsog trial catalogue licence row", q(lic, """select s.source_key, s.licence_key, l.name,
            l.commercial_ok from source s left join licence l on s.licence_key = l.licence_key
            where s.source_key = 'entsog'"""))
    except Exception as exc:  # noqa: BLE001
        status = 1
        annotate("error", "entsog trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:2500]}",
                                                  "collected": COLLECTED[-3:]})
    finally:
        cleanup()
    return status


def cleanup() -> None:
    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:  # noqa: BLE001
                cat.drop_table(table)
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:  # noqa: BLE001
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    for var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"), ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
        bucket = req(var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for v in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(v), Prefix=f"{SCRATCH}/")
    )
    annotate("notice", "entsog trial scratch removed", removed)


if __name__ == "__main__":
    sys.exit(main())
