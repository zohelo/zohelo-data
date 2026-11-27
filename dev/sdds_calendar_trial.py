"""Dev trial of sdds_calendar (#3347, #3348) on the branch's code: Landing and Bronze into zz_dev_3347/ (both buckets)
and namespace zz_dev_3347, each twice; Bronze profiled before Silver; the branch's Silver SQL and its tests (unique,
not_null, accepted_values, the completeness test), the Gold entity table and build stage with their tests, the grain
check by series_key x date_key, spot values for Poland, and the source/licence/unit rows, applied in memory
(Jinja-rendered with warehouse/macros/sources/sdds_calendar.sql). Everything written is removed at the end (finally).
Never merged."""

from __future__ import annotations

import csv
import os
from pathlib import Path
import re
import sys
import time

import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze import sdds_calendar as bronze
from ingestion.to_landing import sdds_calendar as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_3347"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/sources/sdds_calendar.sql").read_text()
SM, EM, GM = "sdds_calendar_releases", "sdds_release", "poc_build_sdds_calendar"


def quiet(fn, *args, **kwargs):
    os.environ["GITHUB_ACTIONS"] = "false"
    try:
        return fn(*args, **kwargs)
    finally:
        os.environ["GITHUB_ACTIONS"] = "true"


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def landed_state(client, bucket) -> list[tuple[str, int]]:
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{LAND}/"):
        out += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return sorted(out)


def yml_tests(con, n, path: Path) -> dict[str, int]:
    failures = {}
    for model in yaml.safe_load(path.read_text())["models"]:
        name = model["name"]
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
                    values = [str(v) for v in test["accepted_values"]["arguments"]["values"]]
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
    os.environ["GITHUB_ACTIONS"] = "true"
    started = time.monotonic()
    try:
        states, codes = [], []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            codes.append(quiet(landing.extract, landing_root=LAND))
            states.append(landed_state(client, land_bucket))
            if attempt == 1:
                time.sleep(10)
        inspect_rc = quiet(landing.inspect, landing_root=LAND)
        held = sorted(landing.held(LAND))
        verify_rc = quiet(bulk_file.main, landing.SOURCE, "verify", landing_root=LAND, specs=[{"entity": e} for e in held])
        receipts = [r for e in held for r in bulk_file.landed(client, land_bucket, bulk_file.prefix(landing.SOURCE, e, LAND))]
        landing_stats = {
            "entities": held,
            "files": len([k for k, _ in states[0] if not k.endswith(".receipt.json")]),
            "bytes": sum(s for k, s in states[0] if not k.endswith(".receipt.json")),
            "exit_codes": codes, "inspect_exit": inspect_rc, "verify_exit": verify_rc,
            "second_run_unchanged": states[0] == states[1],
            "source_urls": [r["source_url"][-70:] for r in receipts],
            "version_basis": sorted({r["version_basis"] for r in receipts}),
        }
        annotate("notice" if not any(codes) and len(held) else "error", "sdds_calendar trial landing", landing_stats)
        if not held:
            return 1

        snapshots = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            quiet(bronze.load, held, landing_root=LAND, namespace=SCRATCH)
            snap = cat.load_table((SCRATCH, bronze.TABLE)).current_snapshot()
            snapshots.append(snap.snapshot_id if snap else None)
        rows = cat.load_table((SCRATCH, bronze.TABLE)).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, bronze.REJECTED)).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001
            rejected = []
        con.register("b", rows)
        bronze_stats = {
            "rows": rows.num_rows,
            "files": n("select count(distinct raw_sha256) from b"),
            "cells_per_sheet": n("select string_agg(sheet_name || '=' || c, ' ' order by sheet_name) from (select sheet_name, count(*) c from b group by 1)"),
            "dup_cells": n("select count(*) from (select 1 from b group by raw_sha256, sheet_name, row_number, column_number having count(*) > 1)"),
            "no_release_cells": n("select string_agg(v || '=' || c, ' ') from (select trim(value_text) v, count(*) c from b where column_number >= 3 and trim(value_text) in ('-', 'dziennie') group by 1)"),
            "multi_release_cells": n("select count(*) from b where column_number >= 3 and len(regexp_extract_all(value_text, '\\d{2}\\.\\d{2}\\.\\d{4}')) > 1"),
            "unclosed_bracket_cells": n("select count(*) from b where column_number >= 3 and regexp_matches(value_text, '\\([^)]*$')"),
            "rejected": [(x.get("entity"), (x.get("error") or "")[:300]) for x in rejected],
            "second_run_unchanged": snapshots[0] == snapshots[1],
        }
        annotate("notice" if not rejected else "error", "sdds_calendar trial bronze", bronze_stats)

        sql = render(ROOT / f"warehouse/models/silver/sdds_calendar/{SM}.sql", {bronze.TABLE: "b"})
        con.sql(f"create table {SM} as " + sql)
        failures = yml_tests(con, n, ROOT / "warehouse/models/silver/sdds_calendar/_sdds_calendar.yml")
        test = render(ROOT / f"warehouse/tests/{SM}_complete.sql", {SM: SM})
        found = con.sql(f"select * from ({test}) limit 5").fetchall()
        failures[f"{SM} complete"] = len(found)
        if found:
            print("COMPLETE-FAIL", found, flush=True)
        silver_stats = {
            "rows": n(f"select count(*) from {SM}"),
            "rebuild_same": n(f"select count(*) from ({sql})") == n(f"select count(*) from {SM}"),
            "by_publisher_status": n(f"select string_agg(publisher || '/' || check_status || '=' || c, ' ' order by publisher, check_status) from (select publisher, check_status, count(*) c from {SM} group by all)"),
            "flagged": [list(map(str, r)) for r in con.sql(f"select category, release_date, reference_period, column_month, check_status from {SM} where check_status <> 'ok' order by all").fetchall()],
            "range": n(f"select min(release_date) || '..' || max(release_date) from {SM}"),
            "calendar_years": n(f"select string_agg(distinct cast(calendar_year as varchar), ',') from {SM}"),
            "tests_run": len(failures),
            "test_failures": {k: v for k, v in failures.items() if v} or "none",
        }
        annotate("notice" if not any(failures.values()) else "error", "sdds_calendar trial silver", silver_stats)

        con.sql("create table geo as select 'PL' as geography_key, 'PL' as iso2, 'country' as level_name")
        macro = jinja2.Environment().from_string(MACROS + "{{ poc_geography__sdds_calendar() }}").render()
        con.sql(f"create table map as with {macro} select * from sdds_calendar")
        con.sql(f"create table {EM} as " + render(ROOT / f"warehouse/models/gold/{EM}.sql", {SM: SM}))
        con.sql(f"create table {GM} as " + render(ROOT / f"warehouse/models/gold/{GM}.sql", {EM: EM, "poc_map_geography": "map"}))
        gold_failures = yml_tests(con, n, ROOT / "warehouse/models/gold/_sdds_calendar.yml")
        series = "measure_key || '|' || geography_key || '|' || breakdown_json"
        gold_stats = {
            "entity_rows": n(f"select count(*) from {EM}"),
            "entity_by_publisher": n(f"select string_agg(publisher || '=' || c, ' ' order by publisher) from (select publisher, count(*) c from {EM} group by 1)"),
            "rows": n(f"select count(*) from {GM}"),
            "rows_by_measure": n(f"select string_agg(measure_codes || '=' || c, ' ' order by measure_codes) from (select measure_codes, count(*) c from {GM} group by 1)"),
            "dup_series_date": n(f"select count(*) from (select 1 from {GM} group by {series}, date_key having count(*) > 1)"),
            "frequencies_per_measure_max": n(f"select max(c) from (select measure_key, count(distinct frequency) c from {GM} group by 1)"),
            "date_range": n(f"select min(date_key) || '..' || max(date_key) from {GM}"),
            "status": n(f"select string_agg(observation_status_key || '=' || c, ' ') from (select observation_status_key, count(*) c from {GM} group by 1)"),
            "unmatched": n(f"select count(*) from {GM} where geography_key = '_X'"),
            "zero_values": n(f"select count(*) from {GM} where value = 0"),
            "publisher_sum_ne_total": n(f"""select count(*) from (select date_key, sum(value) v from {GM} where measure_codes = 'publisher' group by 1) x
                join (select date_key, value t from {GM} where is_total) y using (date_key) where x.v <> y.t"""),
            "ok_entity_eq_total": n(f"select count(*) from {EM} where check_status = 'ok'") == n(f"select sum(value) from {GM} where is_total"),
            "test_failures": {k: v for k, v in gold_failures.items() if v} or "none",
        }
        bad = any(gold_failures.values()) or gold_stats["dup_series_date"] or gold_stats["publisher_sum_ne_total"]
        annotate("error" if bad else "notice", "sdds_calendar trial gold", gold_stats)
        spot = {
            "pl_total_by_month": n(f"select string_agg(strftime(date_key, '%Y-%m') || '=' || value, ' ' order by date_key) from {GM} where is_total"),
            "mf_by_month": n(f"select string_agg(strftime(date_key, '%Y-%m') || '=' || value, ' ' order by date_key) from {GM} where breakdown_json like '%\"mf\"%'"),
            "nbp_by_month": n(f"select string_agg(strftime(date_key, '%Y-%m') || '=' || value, ' ' order by date_key) from {GM} where breakdown_json like '%\"nbp\"%'"),
            "mf_march_2026": [list(map(str, r)) for r in con.sql(f"select release_date, category, reference_period from {EM} where publisher = 'mf' and release_date between '2026-03-01' and '2026-03-31' order by all").fetchall()],
            "next_nbp_after_copy": [list(map(str, r)) for r in con.sql(f"select release_date, category, reference_period from {EM} where publisher = 'nbp' and release_date > copy_date order by all limit 4").fetchall()],
        }
        annotate("notice", "sdds_calendar trial spot values", spot)
        src = list(csv.DictReader((ROOT / "warehouse/seed_rows/source/sdds_calendar.csv").open()))
        lic = {r["licence_key"]: r for f in (ROOT / "warehouse/seed_rows/licence").glob("*.csv") for r in csv.DictReader(f.open())}
        units = {r["unit_key"]: r for f in (ROOT / "warehouse/seed_rows/unit").glob("*.csv") for r in csv.DictReader(f.open())}
        umap = list(csv.DictReader((ROOT / "warehouse/seed_rows/unit_map/sdds_calendar.csv").open()))
        annotate("notice", "sdds_calendar trial catalogue and licence", {
            "source_row": src[0], "licence_row": lic.get(src[0]["licence_key"], "MISSING"),
            "unit_codes_in_gold": n(f"select string_agg(distinct unit_code, ',') from {GM}"),
            "unit_map": [(u["unit_code"], u["unit_key"], u["unit_key"] in units) for u in umap],
            "seconds_total": round(time.monotonic() - started)})
        print("STATS", landing_stats, bronze_stats, silver_stats, gold_stats, spot, flush=True)
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
        except Exception as exc:  # noqa: BLE001
            print("drop failed", exc)
        left = sum(len(p.get("Contents", [])) for b in (land_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
                   for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"))
        annotate("notice", "sdds_calendar trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
