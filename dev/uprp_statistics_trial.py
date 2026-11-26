"""Dev trial of uprp_statistics (#3265) on the final code: Landing and Bronze into zz_dev_uprp_statistics/ (both
buckets) and namespace zz_dev_uprp_statistics, each twice; Bronze profiled before Silver; then the branch's Silver SQL
and its tests (unique, not_null, accepted_values from _uprp_statistics.yml, tests/uprp_statistics_counts_complete.sql),
the geography macro, the Gold build stage and its tests, the grain by series_key x date_key and the value bounds of
the KPI measures, applied in memory (Jinja-rendered with the branch's macros). Everything written is removed at the
end (finally). Never merged."""

from __future__ import annotations

import csv
from pathlib import Path
import re
import sys

import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze import uprp_statistics as bronze
from ingestion.to_landing import uprp_statistics as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_uprp_statistics"
LAND = f"{SCRATCH}/01_landing"
MACROS = "\n".join(
    (ROOT / p).read_text()
    for p in ("warehouse/macros/xlsx_cells.sql", "warehouse/macros/sources/uprp_statistics.sql",
              "warehouse/macros/value_range/uprp_statistics.sql")
)  # fmt: skip


def env() -> jinja2.Environment:
    e = jinja2.Environment(extensions=["jinja2.ext.do"])
    e.globals["return"] = lambda x: x
    return e


def render(text: str, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", text, flags=re.S)
    return env().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def landed_state(client, bucket) -> list[tuple[str, int]]:
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{LAND}/"):
        out += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return sorted(out)


def yml_tests(con, n, path: Path) -> dict[str, int]:
    failures = {}
    for model in yaml.safe_load(path.read_text())["models"]:
        name = model["name"]
        if not n(f"select count(*) from information_schema.tables where table_name = '{name}'"):
            continue
        for test in model.get("tests", []):
            if "unique" in test:
                expr = test["unique"]["column_name"]
                failures[f"{name} unique"] = n(f"select count(*) from (select 1 from {name} group by {expr} having count(*) > 1)")
        for column in model.get("columns", []):
            for test in column.get("tests", []):
                c = column["name"]
                if test == "not_null":
                    failures[f"{name} not_null {c}"] = n(f"select count(*) from {name} where {c} is null")
                elif isinstance(test, dict) and "accepted_values" in test:
                    values = test["accepted_values"]["arguments"]["values"]
                    con.execute("create or replace temp table allowed as select unnest(?) as v", [values])
                    failures[f"{name} accepted_values {c}"] = n(
                        f"select count(*) from {name} where {c} is not null and cast({c} as varchar) not in (select v from allowed)"
                    )
    return failures


def main(argv) -> int:
    client = s3()
    land_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    try:
        # Landing twice: the second run must write nothing (Last-Modified unchanged: no download at all)
        states, codes = [], []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            codes.append(landing.extract(60, landing_root=LAND))
            states.append(landed_state(client, land_bucket))
        held = landing.held(LAND)
        entities = sorted(held)
        verify_rc = bulk_file.main("uprp_statistics", "verify", landing_root=LAND, specs=[{"entity": e} for e in entities])
        landing_stats = {
            "files": len([k for k, _ in states[0] if not k.endswith(".receipt.json")]),
            "bytes": sum(s for k, s in states[0] if not k.endswith(".receipt.json")),
            "entities": entities,
            "versions": {e: r[-1]["version_token"] for e, r in held.items()},
            "exit_codes": codes,
            "second_run_unchanged": states[0] == states[1],
            "verify_exit": verify_rc,
        }
        annotate("notice", "uprp_statistics trial landing", landing_stats)

        # Bronze twice: the second run must add nothing
        snapshots = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            bronze.bronze_load(entities, landing_root=LAND, namespace=SCRATCH)
            snap = cat.load_table((SCRATCH, "uprp_statistics_cells")).current_snapshot()
            snapshots.append(snap.snapshot_id if snap else None)
        rows = cat.load_table((SCRATCH, "uprp_statistics_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "uprp_statistics_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001  the table exists only after a rejection
            rejected = []
        con.register("b", rows)
        bronze_stats = {
            "rows": rows.num_rows,
            "per_entity": n("select string_agg(entity || '=' || c || '/ok ' || k, ' ' order by entity) from (select entity, count(*) c, count(*) filter (where starts_with(guard, 'ok')) k from b group by 1)"),
            "files_in_bronze": n("select count(distinct raw_sha256) from b"),
            "landed_not_in_bronze_or_rejected": len(set(entities) - set(n("select list(distinct entity) from b") or []) - {x["entity"] for x in rejected}),
            "rejected": [(x.get("entity"), (x.get("error") or "")[:100]) for x in rejected],
            "second_run_unchanged": snapshots[0] == snapshots[1],
            "null_keys": n("select count(*) from b where table_code is null or entity is null or raw_sha256 is null"),
            "dup_keys": n("select count(*) from (select 1 from b where row_code is not null group by entity, table_code, row_code, year_text having count(*) > 1)"),
            "years": n("select min(year_text) || '-' || max(year_text) from b"),
        }
        annotate("notice" if not rejected else "warning", "uprp_statistics trial bronze", bronze_stats)
        # look before modelling: guards by reason, values that are not numbers as printed, tables not read
        profile = {
            "guards": n("select string_agg(g || '=' || c, '; ' order by c desc) from (select regexp_replace(guard, ':.*', '') g, count(*) c from b group by 1)"),
            "value_null_ok": n("select count(*) from b where starts_with(guard, 'ok') and value_text is null"),
            "value_not_number": n("select count(*) from b where value_text is not null and not regexp_full_match(value_text, '\\d{1,3}( \\d{3})*|\\d+')"),
            "zero_values_ok": n("select count(*) from b where starts_with(guard, 'ok') and value_text = '0'"),
            "tables_not_read": n("select coalesce(string_agg(entity || ':' || table_code || ' ' || guard, '; '), 'none') from b where row_code is null"),
            "labelled_figure": n("select coalesce(string_agg(entity || ':' || year_text || '=' || value_text, ' '), 'none') from b where guard like 'ok: %'"),
            "not_ok_cells": n("select string_agg(entity || ':' || table_code || ':' || row_code || ':' || year_text || ' ' || left(guard, 60), ' | ') from (select * from b where not starts_with(guard, 'ok') and row_code in ('total', 'national', 'international') order by entity, table_code limit 20)"),
        }
        annotate("notice", "uprp_statistics trial bronze profile", profile)

        # Silver and its tests; a second rendering gives the same rows (idempotent rebuild)
        silver_sql = render((ROOT / "warehouse/models/silver/uprp_statistics/uprp_statistics_counts.sql").read_text(), {"uprp_statistics_cells": "b"})
        con.sql("create table uprp_statistics_counts as " + silver_sql)
        rebuilt = n(f"select count(*) from ({silver_sql}) x")
        failures = yml_tests(con, n, ROOT / "warehouse/models/silver/uprp_statistics/_uprp_statistics.yml")
        complete = render((ROOT / "warehouse/tests/uprp_statistics_counts_complete.sql").read_text(), {"uprp_statistics_counts": "uprp_statistics_counts"})
        failures["complete"] = n(f"select count(*) from ({complete}) x")
        s = "uprp_statistics_counts"
        silver_stats = {
            "rows": n(f"select count(*) from {s}"),
            "rebuild_rows": rebuilt,
            "years": n(f"select min(year) || '-' || max(year) from {s}"),
            "per_table": n(f"select string_agg(table_code || '=' || c || ' ' || y, '; ' order by table_code) from (select table_code, count(*) c, min(year) || '-' || max(year) y from {s} group by 1)"),
            "from_report": n(f"select string_agg(report_year || '=' || c, ' ' order by report_year) from (select report_year, count(*) c from {s} group by 1)"),
            "tests_run": len(failures),
            "test_failures": {k: v for k, v in failures.items() if v} or "none",
        }
        annotate("notice" if not any(failures.values()) else "error", "uprp_statistics trial silver", silver_stats)

        # Gold: geography macro, build stage, tests, grain by series_key x date_key, KPI bounds
        geo_sql = render("with geo as (select 1 as x), {{ poc_geography__uprp_statistics() }} select * from uprp_statistics", {})
        con.sql("create table poc_map_geography as " + geo_sql)
        g = "poc_build_uprp_statistics"
        con.sql(f"create table {g} as " + render((ROOT / f"warehouse/models/gold/{g}.sql").read_text(),
                                                 {"uprp_statistics_counts": s, "poc_map_geography": "poc_map_geography"}))
        gold_failures = yml_tests(con, n, ROOT / "warehouse/models/gold/_uprp_statistics.yml")
        series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
                  "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
        bounds = re.findall(r"\['(\w+)', '%', '([\d.e]+)', '([\d.e]+)'\]", (ROOT / "warehouse/macros/value_range/uprp_statistics.sql").read_text())
        measures = {r[0]: r[2] for r in csv.reader((ROOT / "warehouse/seed_rows/kpi_measure/uprp_statistics.csv").open()) if r[0] != "kpi_key"}
        outside = 0
        for kpi, lo, hi in bounds:
            outside += n(f"select count(*) from {g} where measure_key = '{measures[kpi]}' and (value < {lo} or value > {hi})")
        gold_stats = {
            "rows": n(f"select count(*) from {g}"),
            "measures": n(f"select count(distinct measure_key) from {g}"),
            "dup_series_date": n(f"select count(*) from (select 1 from {g} group by {series}, date_key having count(*) > 1)"),
            "unmatched_rows": n(f"select count(*) from {g} where geography_key = '_X'"),
            "null_values": n(f"select count(*) from {g} where value is null"),
            "kpi_measures_found": n(f"select count(distinct measure_key) from {g} where measure_key in ({', '.join(repr(m) for m in measures.values())})"),
            "kpi_rows_outside_bounds": outside,
            "bounds_checked": len(bounds),
            "dates": n(f"select min(date_key) || ' to ' || max(date_key) from {g}"),
            "test_failures": {k: v for k, v in gold_failures.items() if v} or "none",
            "tests_run": len(gold_failures),
        }
        annotate("notice" if not any(gold_failures.values()) and not gold_stats["dup_series_date"] else "error", "uprp_statistics trial gold", gold_stats)
        spot = {
            m.split(":")[3]: n(f"select string_agg(year(date_key) || '=' || cast(value as bigint), ' ' order by date_key) from {g} where measure_key = '{m}'")
            for m in sorted(set(n(f"select list(distinct measure_key) from {g} where measure_key like '%:total'")))
        }
        annotate("notice", "uprp_statistics trial spot values (totals, Poland)", spot)
        src = list(csv.DictReader((ROOT / "warehouse/seed_rows/source/uprp_statistics.csv").open()))
        lic = {r["licence_key"]: r for f in (ROOT / "warehouse/seed_rows/licence").glob("*.csv") for r in csv.DictReader(f.open())}
        annotate("notice", "uprp_statistics trial licence row", {"source": src, "licence": lic.get(src[0]["licence_key"])})
        print("STATS", landing_stats, bronze_stats, profile, silver_stats, gold_stats, spot, flush=True)
    finally:
        removed = 0
        for bucket in (land_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
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
        except Exception as exc:  # noqa: BLE001  report, do not hide
            print("drop failed", exc)
        left = sum(len(p.get("Contents", [])) for b in (land_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
                   for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"))
        annotate("notice", "uprp_statistics trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
