"""Dev trial of ms_isws (register PLG-031, #2968): Landing and Bronze into zz_dev_ms_isws/ (both buckets) and namespace
zz_dev_ms_isws, each run twice; then the branch's Silver model, its tests (grain, not_null, accepted_values,
completeness) and the Gold build stage applied in memory with duckdb (Jinja-rendered with warehouse/macros), the
grain checked by series_key x date_key, the KPI bounds of warehouse/tests/metrics_value_range.sql applied to the
total series, Poland spot values annotated. Everything written is removed at the end (finally). Deleted before
merge."""

from __future__ import annotations

from pathlib import Path
import re
import sys

import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze import ms_isws as bronze
from ingestion.to_landing import ms_isws as landing

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_ms_isws"
MACROS = (ROOT / "warehouse/macros/xlsx_cells.sql").read_text()


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def objects(client, bucket: str, prefix: str) -> int:
    n = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        n += len(page.get("Contents", []))
    return n


def show(con, title, sql, limit=200):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:100] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def main(argv) -> int:
    client = s3()
    land_bucket, lake_bucket = req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    land = f"{SCRATCH}/01_landing"
    try:
        landed = []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            rc = landing.extract(60, landing_root=land)
            landed.append({"rc": rc, "objects": objects(client, land_bucket, f"{land}/")})
        entities = sorted(landing.held(land))
        loaded = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            rc = bronze.bronze_load(entities, landing_root=land, namespace=SCRATCH)
            t = cat.load_table((SCRATCH, "ms_isws_cells"))
            snap = t.current_snapshot()
            loaded.append(
                {"rc": rc, "rows": t.scan().to_arrow().num_rows, "snapshot": snap.snapshot_id if snap else None}
            )
        cells = cat.load_table((SCRATCH, "ms_isws_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "ms_isws_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001 - no rejected table means none rejected
            rejected = []
        con.register("ms_isws_cells", cells)
        n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
        per_entity = n(
            "select string_agg(entity || '=' || c, ' ' order by entity) from (select entity, count(*) c from ms_isws_cells group by 1)"
        )
        annotate(
            "notice",
            "ms_isws trial landing+bronze",
            {
                "entities": entities,
                "landing_objects_run1_run2": [x["objects"] for x in landed],
                "landing_rc": [x["rc"] for x in landed],
                "bronze_rows_run1_run2": [x["rows"] for x in loaded],
                "bronze_snapshot_unchanged": loaded[0]["snapshot"] == loaded[1]["snapshot"],
                "bronze_rc": [x["rc"] for x in loaded],
                "bronze_rows_per_entity": per_entity,
                "rejected": [(r.get("entity"), (r.get("error") or r.get("reason") or "")[:80]) for r in rejected],
            },
        )

        # Silver
        silver_sql = render(
            ROOT / "warehouse/models/silver/ms_isws/ms_isws_tables.sql", {"ms_isws_cells": "ms_isws_cells"}
        )
        con.sql("create table ms_isws_tables as " + silver_sql)
        s = "ms_isws_tables"
        spec = yaml.safe_load((ROOT / "warehouse/models/silver/ms_isws/_ms_isws.yml").read_text())["models"][0]
        failures = {}
        failures["unique_grain"] = n(
            f"select count(*) from (select 1 from {s} group by table_code, measure_code, breakdown_code, period_year having count(*) > 1)"
        )
        for col in spec["columns"]:
            for test in col["tests"]:
                if test == "not_null":
                    failures[f"not_null_{col['name']}"] = n(f"select count(*) from {s} where {col['name']} is null")
                elif isinstance(test, dict) and "accepted_values" in test:
                    vals = ", ".join(f"'{v}'" for v in test["accepted_values"]["arguments"]["values"])
                    failures[f"accepted_{col['name']}"] = n(
                        f"select count(*) from {s} where {col['name']} not in ({vals})"
                    )
        complete_sql = (
            jinja2.Environment()
            .from_string((ROOT / "warehouse/tests/ms_isws_tables_complete.sql").read_text())
            .render(ref=lambda m: s)
        )
        failures["complete"] = n(f"select count(*) from ({complete_sql})")
        failed = {k: v for k, v in failures.items() if v}
        stats = {
            "silver_rows": n(f"select count(*) from {s}"),
            "rebuild_same_rows": n(f"select count(*) from ({silver_sql})") == n(f"select count(*) from {s}"),
            "per_table": n(
                f"select string_agg(x, '; ' order by x) from (select table_code || ' ' || min(period_year) || '-' || max(period_year) || ' rows=' || count(*) as x from {s} group by table_code)"
            ),
            "status": n(
                f"select string_agg(cell_status || '=' || c, ' ') from (select cell_status, count(*) c from {s} group by 1)"
            ),
            "non_ok_cells": n(
                f"select string_agg(table_code || ':' || measure_code || ':' || breakdown_code || ':' || period_year || '=' || value_text, ' ') from {s} where cell_status <> 'ok'"
            ),
            "tests_run": len(failures),
            "tests_failed": failed,
        }
        annotate("error" if failed else "notice", "ms_isws trial silver", stats)
        if failed.get("complete"):
            show(con, "incomplete", f"select * from ({complete_sql}) order by all")
        if failed.get("unique_grain"):
            show(
                con,
                "duplicates",
                f"select s.* from {s} s join (select table_code, measure_code, breakdown_code, period_year from {s} group by all having count(*) > 1) d using (table_code, measure_code, breakdown_code, period_year) order by all",
            )

        # Gold
        con.sql("create table geo as select 'ms_isws' as source_system, 'PL' as source_code, 'PL' as geography_key")
        gold_sql = render(
            ROOT / "warehouse/models/gold/poc_build_ms_isws.sql", {"ms_isws_tables": s, "poc_map_geography": "geo"}
        )
        con.sql("create table gold as " + gold_sql)
        g = "gold"
        bounds = re.findall(
            r"\('(court_[a-z_]+)', 'ms_isws', '([^']*)', ([-0-9e.]+), ([-0-9e.]+)\)",
            (ROOT / "warehouse/tests/metrics_value_range.sql").read_text(),
        )
        kpis = [
            line.split(",")[:3]
            for line in (ROOT / "warehouse/seeds/kpi_measure.csv").read_text().splitlines()
            if ",ms_isws," in line
        ]
        out_of_bounds = 0
        for concept, _source, pattern in kpis:
            b = next((x for x in bounds if x[0] == concept), None)
            if b is None:
                out_of_bounds += 1000
                continue
            out_of_bounds += n(
                f"select count(*) from {g} where is_total and measure_key like '{pattern}' and (value < {b[2]} or value > {b[3]})"
            )
        gstats = {
            "gold_rows": n(f"select count(*) from {g}"),
            "silver_rows_with_value": n(f"select count(*) from {s} where value is not null"),
            "measures": n(f"select count(distinct measure_key) from {g}"),
            "series": n(
                f"select count(distinct measure_key || '|' || geography_key || '|' || breakdown_json) from {g}"
            ),
            "dup_series_date": n(
                f"select count(*) from (select 1 from {g} group by measure_key, geography_key, breakdown_json, date_key having count(*) > 1)"
            ),
            "unmatched_geo": n(f"select count(*) from {g} where geography_key <> 'PL'"),
            "null_measure_name": n(f"select count(*) from {g} where measure_name is null or breakdown_json is null"),
            "dates": n(f"select min(date_key) || '..' || max(date_key) from {g}"),
            "kpi_bound_violations": out_of_bounds,
            "kpis": [k[0] for k in kpis],
        }
        bad = gstats["dup_series_date"] or gstats["unmatched_geo"] or gstats["null_measure_name"] or out_of_bounds
        annotate("error" if bad else "notice", "ms_isws trial gold", gstats)
        spot = n(
            f"""select string_agg(measure_codes || ' ' || coalesce(json_extract_string(breakdown_json, '$.case_type'), json_extract_string(breakdown_json, '$.court_level'), 'total') || ' ' || period_label || '=' || value, '; ' order by measure_key, breakdown_json, period_label)
            from {g} where geography_key = 'PL' and (period_label in ('2011', '2019', '2025') or (measure_key like '%notarial%' and period_label in ('1991', '2021')))
            and (is_total or period_label = '2025')"""
        )
        annotate("notice", "ms_isws trial Poland", {"values": spot[:3800] if spot else None})
        show(
            con,
            "silver per table x measure x breakdown",
            f"select table_code, measure_code, breakdown_code, count(*), min(period_year), max(period_year), min(value), max(value) from {s} group by all order by all",
        )
        show(
            con,
            "silver sample",
            f"select * exclude (source_version, raw_sha256) from {s} order by table_code, measure_code, breakdown_code, period_year",
            limit=1000,
        )
        show(
            con,
            "gold sample",
            f"select measure_key, breakdown_json, period_label, value from {g} where is_total order by all",
            limit=400,
        )
    finally:
        removed = 0
        for bucket in (land_bucket, lake_bucket):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    removed += 1
        dropped = []
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
            cat.drop_namespace(SCRATCH)
        except Exception as exc:  # noqa: BLE001 - report and finish
            print("drop failed", exc)
        annotate(
            "notice",
            "ms_isws trial cleanup",
            {"objects_removed": removed, "tables_dropped": dropped, "duckdb": duckdb.__version__},
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
