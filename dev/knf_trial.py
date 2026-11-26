"""Dev trial of knf_banking (PLG-017) and knf_pensions (PLG-018), #2871, on the final code: Landing and Bronze into
zz_dev_knf/ (both buckets) and namespace zz_dev_knf, each twice; then the branch's Silver SQL and its tests (unique,
not_null, accepted_values from the YAML, the custom tests), the Gold build stages, duplicates by series_key x date_key,
KPI rows and spot values, applied in memory with DuckDB (Jinja-rendered with warehouse/macros/xlsx_cells.sql).
Everything written is removed at the end (finally). Deleted before merge."""

from __future__ import annotations

import csv
import json
from pathlib import Path
import re
import sys

import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze, report
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze.knf_banking import load as bank_bronze
from ingestion.to_bronze.knf_pensions import load as ppk_bronze
from ingestion.to_landing import knf_pensions as ppk_landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_knf"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/xlsx_cells.sql").read_text()
ANNOTATE = report.annotate


def quiet(level, title, payload=None, *args, **kwargs):
    print(f"ANNOTATION {level} | {title} | {json.dumps(payload, default=str, ensure_ascii=False)[:3000]}", flush=True)


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def landed_state(client, bucket, source) -> list[tuple[str, int]]:
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{LAND}/{source}/"):
        out += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return sorted(out)


def seed(name: str, source: str) -> list[dict]:
    return list(csv.DictReader(open(ROOT / f"warehouse/seed_rows/{name}/{source}.csv", encoding="utf-8")))


def yaml_tests(con, n, table: str, path: Path) -> dict[str, int]:
    spec = yaml.safe_load(path.read_text())["models"][0]
    failures = {}
    for test in spec.get("tests", []):
        if "unique" in test:
            col = test["unique"]["column_name"]
            failures["unique"] = n(f"select count(*) from (select {col} k from {table} group by k having count(*) > 1)")
    for column in spec["columns"]:
        for test in column.get("tests", []):
            if test == "not_null":
                failures[f"not_null {column['name']}"] = n(f"select count(*) from {table} where {column['name']} is null")
            elif isinstance(test, dict) and "accepted_values" in test:
                values = test["accepted_values"]["arguments"]["values"]
                con.execute("create or replace temp table allowed as select unnest(?) as v", [[str(v) for v in values]])
                failures[f"accepted_values {column['name']}"] = n(
                    f"select count(*) from {table} where cast({column['name']} as varchar) not in (select v from allowed)"
                )
    return failures


def main(argv) -> int:
    client = s3()
    land_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    report.annotate = bulk_bronze.annotate = bulk_file.annotate = quiet
    ok = True
    try:
        # ---- Landing twice per source: the second run must write nothing
        landing = {}
        for source, run in (
            ("knf_banking", lambda: bulk_file.main("knf_banking", "load", landing_root=LAND)),
            ("knf_pensions", lambda: ppk_landing.extract(10, landing_root=LAND)),
        ):
            states, codes = [], []
            for attempt in (1, 2):
                print(f"\n##### {source} landing extract {attempt}", flush=True)
                codes.append(run())
                states.append(landed_state(client, land_bucket, source))
            entities = ["banking_monthly"] if source == "knf_banking" else [ppk_landing.ENTITY]
            verify = bulk_file.main(source, "verify", landing_root=LAND, specs=[{"entity": e} for e in entities])
            data = [(k, s) for k, s in states[0] if not k.endswith(".json")]
            landing[source] = {"exit": codes, "files": len(data), "bytes": sum(s for _, s in data),
                               "keys": [k.rsplit("/", 2)[-2:] for k, _ in data],
                               "second_run_unchanged": states[0] == states[1], "verify_exit": verify}
            good = landing[source]["second_run_unchanged"] and not verify and not any(codes) and data
            ok &= bool(good)
        ANNOTATE("notice" if ok else "error", "knf trial landing", landing)

        # ---- Bronze twice per source
        bronze = {}
        for source, mod in (("knf_banking", bank_bronze), ("knf_pensions", ppk_bronze)):
            snaps = []
            for attempt in (1, 2):
                print(f"\n##### {source} bronze load {attempt}", flush=True)
                mod.load(0, landing_root=LAND, namespace=SCRATCH)
                snap = cat.load_table((SCRATCH, f"{source}_cells")).current_snapshot()
                snaps.append(snap.snapshot_id if snap else None)
            cells = cat.load_table((SCRATCH, f"{source}_cells")).scan().to_arrow()
            try:
                rejected = cat.load_table((SCRATCH, f"{source}_rejected_files")).scan().to_arrow().to_pylist()
            except Exception:  # the table exists only after a rejection
                rejected = []
            con.register(f"{source}_cells", cells)
            bronze[source] = {
                "rows": cells.num_rows,
                "files": n(f"select count(distinct raw_sha256) from {source}_cells"),
                "sheets": n(f"select count(distinct sheet_name) from {source}_cells"),
                "rejected": [(r.get("entity"), (r.get("error") or "")[:100]) for r in rejected],
                "second_run_unchanged": snaps[0] == snaps[1],
            }
            ok &= bronze[source]["second_run_unchanged"] and not rejected
        ANNOTATE("notice" if ok else "error", "knf trial bronze", bronze)

        # ---- Silver, tests, Gold per source
        units = {}
        for source, silver, build, tests in (
            ("knf_banking", "knf_banking_series", "poc_build_knf_banking",
             ["knf_banking_entities_in_silver", "knf_banking_series_complete", "knf_banking_selected_items"]),
            ("knf_pensions", "knf_pensions_ppk_net_assets", "poc_build_knf_pensions",
             ["knf_pensions_entities_in_silver", "knf_pensions_ppk_net_assets_complete"]),
        ):
            tables = {f"{source}_cells": f"{source}_cells", silver: f"s_{source}", build: f"g_{source}"}
            sql = render(ROOT / f"warehouse/models/silver/{source}/{silver}.sql", tables)
            con.sql(f"create table s_{source} as " + sql)
            con.sql(f"create table s2_{source} as " + sql)
            con.sql(f"create table g_{source} as " + render(ROOT / f"warehouse/models/gold/{build}.sql", tables))
            failures = yaml_tests(con, n, f"s_{source}", ROOT / f"warehouse/models/silver/{source}/_{source}.yml")
            for t in tests:
                failures[t] = n(f"select count(*) from ({render(ROOT / f'warehouse/tests/{t}.sql', tables)})")
            s, g = f"s_{source}", f"g_{source}"
            stats = {
                "silver_rows": n(f"select count(*) from {s}"),
                "rebuild_same": n(f"select count(*) from (select * from {s} except select * from s2_{source})") == 0,
                "tests_run": len(failures),
                "test_failures": {k: v for k, v in failures.items() if v} or "none",
                "statuses": n(f"select string_agg(cell_status || '=' || c, ' ') from (select cell_status, count(*) c from {s} group by 1)"),
                "non_numeric_texts": n(f"select string_agg(distinct value_text, ' | ') from {s} where value is null"),
                "zero_values": n(f"select count(*) from {s} where value = 0"),
                "gold_rows": n(f"select count(*) from {g}"),
                "gold_measures": n(f"select string_agg(x, '; ') from (select measure_codes || ' ' || frequency || ' ' || min(period_label) || '..' || max(period_label) || ' n=' || count(*) x from {g} group by measure_codes, frequency order by 1)"),
                "dup_series_date": n(f"select count(*) from (select 1 from {g} group by measure_key || '|' || geography_key || '|' || breakdown_json, date_key having count(*) > 1)"),
                "frequencies_per_measure": n(f"select max(c) from (select count(distinct frequency) c from {g} group by measure_key)"),
                "null_values": n(f"select count(*) from {g} where value is null"),
                "unmapped_units": sorted({r[0] for r in con.sql(f"select distinct unit_code from {g}").fetchall()} - {r["unit_code"] for r in seed("unit_map", source)}),
                "kpi_rows": {k["kpi_key"]: n(f"select count(*) from {g} where measure_key like '{k['measure_key_pattern']}'") for k in seed("kpi_measure", source)},
                "kpi_max_per_period": n(f"select max(c) from (select count(*) c from {g} where is_total group by measure_key, date_key)"),
                "licence_key": seed("source", source)[0]["licence_key"],
            }
            good = not any(failures.values()) and stats["rebuild_same"] and not stats["dup_series_date"] and not stats["unmapped_units"]
            ok &= good
            ANNOTATE("notice" if good else "error", f"{source} trial silver and gold", stats)
            units[source] = stats

        spot = {
            "knf_banking": n("""select string_agg(measure_codes || ' ' || period_label || '=' || value, '; ' order by measure_codes, period_label)
                from g_knf_banking where is_total and period_label in ('2018-01', '2025-12', '2026-09', '2018-Q1', '2026-Q2')"""),
            "knf_banking_groups_2026_09_assets": n("""select string_agg(breakdown_json || '=' || value, '; ') from g_knf_banking
                where measure_codes = 'total_assets' and period_label = '2026-09'"""),
            "knf_pensions": n("""select string_agg(coalesce(breakdown_label_json, 'total') || ' ' || period_label || '=' || value, '; ' order by period_label)
                from g_knf_pensions where (is_total and period_label in ('2020-Q1', '2025-Q4', '2026-Q3')) or (period_label = '2026-Q3' and breakdown_json like '%pko%')"""),
        }
        ANNOTATE("notice", "knf trial spot values", spot)
        print("STATS", json.dumps({"landing": landing, "bronze": bronze, "units": units, "spot": spot}, default=str, ensure_ascii=False), flush=True)
    finally:
        report.annotate = bulk_bronze.annotate = bulk_file.annotate = ANNOTATE
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
        except Exception as exc:  # report, do not hide
            print("drop failed", exc)
        left = sum(len(p.get("Contents", [])) for b in (land_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
                   for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"))
        ANNOTATE("notice" if not left else "error", "knf trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
