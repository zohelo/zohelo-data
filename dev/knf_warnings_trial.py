"""Dev trial of knf_warnings (#3341) on the branch's code: Landing and Bronze into zz_dev_3341/ (both buckets) and
namespace zz_dev_3341, each twice; Bronze profiled before Silver; the branch's Silver SQL and its tests (unique,
not_null, accepted_values, the completeness test), the Gold build stage and the entity table with their tests, the
grain check by series_key x date_key, spot values, a check that no name of the page reached the reduced copy, and the
source/licence rows, applied in memory (Jinja-rendered with warehouse/macros/sources/knf_warnings.sql). Inner
activity annotations go to the log only; the trial's own annotations are compact. Everything written is removed at
the end (finally). Never merged."""

from __future__ import annotations

import csv
import json
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
from ingestion.to_bronze.knf_warnings import load as bronze
from ingestion.to_landing.knf_warnings import extract as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_3341"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/sources/knf_warnings.sql").read_text()
SM = "knf_warnings_entries"
GM = "poc_build_knf_warnings"
EM = "knf_warning"


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
                    values = [str(v) for v in test["accepted_values"]["arguments"]["values"]]
                    con.execute("create or replace temp table allowed as select unnest(?) as v", [values])
                    failures[f"{name} accepted_values {c}"] = n(
                        f"select count(*) from {name} where {c} is not null and cast({c} as varchar) not in (select v from allowed)"
                    )
    return failures


def names_leaked(doc_bytes: bytes) -> dict:
    """Parse the page again in memory: how many entity names (column 2) and free-text cells (columns 5, 6) of at
    least 12 characters occur in the reduced copy. Expected 0; nothing of them is printed."""
    raw, _ = landing.fetch()
    page = landing.Page()
    page.feed(raw.decode("utf-8"))
    doc = doc_bytes.decode("utf-8")
    checked = leaked = 0
    for table in page.tables:
        for cells in table["rows"][1:]:
            for j in (1, 4, 5):
                value = landing.text(cells[j])
                if len(value) >= 12:
                    checked += 1
                    leaked += value in doc
    return {"cells_checked": checked, "found_in_reduced_copy": leaked}


def main(argv) -> int:
    client = s3()
    land_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    os.environ["GITHUB_ACTIONS"] = "true"
    started = time.monotonic()
    try:
        states, codes, seconds = [], [], []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            t0 = time.monotonic()
            codes.append(quiet(landing.main, ["--mode", "load", "--landing-root", LAND]))
            seconds.append(round(time.monotonic() - t0))
            states.append(landed_state(client, land_bucket))
            if attempt == 1:
                time.sleep(20)
        inspect_rc = quiet(landing.main, ["--mode", "inspect", "--landing-root", LAND])
        verify_rc = quiet(landing.main, ["--mode", "verify", "--landing-root", LAND])
        held = bulk_file.landed(client, land_bucket, bulk_file.prefix(landing.SOURCE, landing.ENTITY, LAND))
        receipt = held[-1] if held else {}
        body = client.get_object(Bucket=land_bucket, Key=receipt["object"]["key"])["Body"].read() if held else b""
        doc = json.loads(body) if body else {}
        landing_stats = {
            "files": len([k for k, _ in states[0] if not k.endswith(".receipt.json")]),
            "bytes": sum(s for k, s in states[0] if not k.endswith(".receipt.json")),
            "exit_codes": codes, "inspect_exit": inspect_rc, "verify_exit": verify_rc, "seconds": seconds,
            "second_run_unchanged": states[0] == states[1], "copies": len(held),
            "stated_total": doc.get("stated_total"), "rows": len(doc.get("rows", [])),
            "row_keys": sorted(doc["rows"][0]) if doc.get("rows") else None,
            "top_keys": sorted(doc),
            "sections": [(s["position"], s["key"], s["entries"]) for s in doc.get("sections", [])],
        }
        annotate("notice" if not any(codes) else "error", "knf_warnings trial landing", landing_stats)
        if not body:
            return 1
        privacy = names_leaked(body)
        privacy["titles"] = [s["title"][:150] for s in doc["sections"]]
        annotate("notice" if privacy["found_in_reduced_copy"] == 0 else "error", "knf_warnings trial privacy", privacy)

        snapshots = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            quiet(bronze.load, landing_root=LAND, namespace=SCRATCH)
            snap = cat.load_table((SCRATCH, "knf_warnings_entries")).current_snapshot()
            snapshots.append(snap.snapshot_id if snap else None)
        rows = cat.load_table((SCRATCH, "knf_warnings_entries")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "knf_warnings_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001
            rejected = []
        con.register("b", rows)
        bronze_stats = {
            "rows": rows.num_rows,
            "files": n("select count(distinct raw_sha256) from b"),
            "dup_keys": n("select count(*) from (select 1 from b group by source_version, section_key, lp having count(*) > 1)"),
            "null_or_empty": n("""select count(*) from b where coalesce(section_key, '') = '' or coalesce(lp, '') = ''
                or coalesce(prosecutor_office, '') = '' or identifier_kind is null"""),
            "identifier_kinds": n("select string_agg(identifier_kind || '=' || c, ' ') from (select identifier_kind, count(*) c from b group by 1 order by 1)"),
            "krs_cells": n("select count(*) from b where krs_numbers <> ''"),
            "krs_numbers": n("select count(*) from (select unnest(string_split(krs_numbers, ' ')) k from b where krs_numbers <> '')"),
            "multi_krs": n("select count(*) from b where krs_numbers like '% %'"),
            "rulings": n("select string_agg(final_ruling_mentioned || '=' || c, ' ') from (select final_ruling_mentioned, count(*) c from b group by 1)"),
            "office_max_len": n("select max(length(prosecutor_office)) from b"),
            "rejected": [(x.get("entity"), (x.get("error") or "")[:300]) for x in rejected],
            "second_run_unchanged": snapshots[0] == snapshots[1],
        }
        annotate("notice" if not rejected else "error", "knf_warnings trial bronze", bronze_stats)

        sql = render(ROOT / f"warehouse/models/silver/knf_warnings/{SM}.sql", {"knf_warnings_entries": "b"})
        con.sql(f"create table {SM} as " + sql)
        failures = yml_tests(con, n, ROOT / "warehouse/models/silver/knf_warnings/_knf_warnings.yml")
        test = render(ROOT / f"warehouse/tests/{SM}_complete.sql", {SM: SM})
        found = con.sql(f"select * from ({test}) limit 5").fetchall()
        failures[f"{SM} complete"] = len(found)
        if found:
            print("COMPLETE-FAIL", found, flush=True)
        silver_stats = {
            "rows": n(f"select count(*) from {SM}"),
            "rebuild_same": n(f"select count(*) from ({sql})") == n(f"select count(*) from {SM}"),
            "snapshot_dates": n(f"select string_agg(distinct cast(snapshot_date as varchar), ',') from {SM}"),
            "levels": n(f"select string_agg(prosecutor_level || '=' || c, ' ') from (select prosecutor_level, count(*) c from {SM} group by 1 order by 1)"),
            "other_level_offices": n(f"select count(distinct prosecutor_office) from {SM} where prosecutor_level = 'other'"),
            "offices": n(f"select count(distinct prosecutor_office) from {SM}"),
            "krs_count_sum": n(f"select sum(krs_count) from {SM}"),
            "tests_run": len(failures),
            "test_failures": {k: v for k, v in failures.items() if v} or "none",
        }
        annotate("notice" if not any(failures.values()) else "error", "knf_warnings trial silver", silver_stats)

        con.sql("create table geo as select 'PL' as geography_key, 'PL' as iso2, 'country' as level_name")
        macro = jinja2.Environment().from_string(MACROS + "{{ poc_geography__knf_warnings() }}").render()
        con.sql(f"create table map as with {macro} select * from knf_warnings")
        con.sql(f"create table {GM} as " + render(ROOT / f"warehouse/models/gold/{GM}.sql", {SM: SM, "poc_map_geography": "map"}))
        con.sql(f"create table {EM} as " + render(ROOT / f"warehouse/models/gold/{EM}.sql", {SM: SM}))
        gold_failures = yml_tests(con, n, ROOT / "warehouse/models/gold/_knf_warnings.yml")
        series = "measure_key || '|' || geography_key || '|' || breakdown_json"
        gold_stats = {
            "rows": n(f"select count(*) from {GM}"),
            "entity_rows": n(f"select count(*) from {EM}"),
            "rows_by_measure": n(f"""select string_agg(m || '=' || c, ' ' order by m) from (select replace(measure_key,
                'knf_warnings:warning_list:entries:', '') m, count(*) c from {GM} group by 1)"""),
            "dup_series_date": n(f"select count(*) from (select 1 from {GM} group by {series}, date_key having count(*) > 1)"),
            "frequencies_per_measure_max": n(f"select max(c) from (select measure_key, count(distinct frequency) c from {GM} group by 1)"),
            "date_keys": n(f"select string_agg(distinct cast(date_key as varchar), ',') from {GM}"),
            "unmatched": n(f"select count(*) from {GM} where geography_key = '_X'"),
            "zero_values": n(f"select count(*) from {GM} where value = 0"),
            "breakdown_sums_ne_total": n(f"""select count(*) from (select measure_key, sum(value) v from {GM} where measure_codes in
                ('legal_basis', 'prosecutor_level', 'identifier_kind') group by 1) x cross join (select value t from {GM} where is_total) y
                where x.v <> y.t"""),
            "test_failures": {k: v for k, v in gold_failures.items() if v} or "none",
        }
        annotate("notice" if not any(gold_failures.values()) and not gold_stats["dup_series_date"] else "error", "knf_warnings trial gold", gold_stats)
        spot = {
            "pl_total": n(f"select value from {GM} where is_total"),
            "by_identifier_kind": n(f"select string_agg(breakdown_json || '=' || value, ' ' order by breakdown_json) from {GM} where measure_codes = 'identifier_kind'"),
            "by_prosecutor_level": n(f"select string_agg(breakdown_json || '=' || value, ' ' order by breakdown_json) from {GM} where measure_codes = 'prosecutor_level'"),
            "by_legal_basis": n(f"select string_agg(replace(breakdown_json, '{{\"legal_basis\":', '') || '=' || value, ' ' order by value desc) from {GM} where measure_codes = 'legal_basis'"),
            "final_ruling_mentioned": n(f"select value from {GM} where measure_codes = 'final_ruling_mentioned'"),
            "entity_columns": [r[0] for r in con.sql(f"describe {EM}").fetchall()],
        }
        annotate("notice", "knf_warnings trial spot values", spot)
        src = list(csv.DictReader((ROOT / "warehouse/seed_rows/source/knf_warnings.csv").open()))
        lic = {r["licence_key"]: r for f in (ROOT / "warehouse/seed_rows/licence").glob("*.csv") for r in csv.DictReader(f.open())}
        units = {r["unit_key"]: r for f in (ROOT / "warehouse/seed_rows/unit").glob("*.csv") for r in csv.DictReader(f.open())}
        umap = list(csv.DictReader((ROOT / "warehouse/seed_rows/unit_map/knf_warnings.csv").open()))
        annotate("notice", "knf_warnings trial catalogue and licence", {
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
        annotate("notice", "knf_warnings trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
