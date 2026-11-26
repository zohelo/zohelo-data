"""Dev trial of arimr_payments (#2487, register PLS-034) on the final code: (1) the Bronze parser over every report of
the gov.pl page dated 31.12.2025 or later and every year-end report (read only, one GET at a time, 2 s apart; nothing stored); (2) Landing and
Bronze into zz_dev_arimr/ (both buckets) and namespace zz_dev_arimr, each twice, and Landing verify; (3) the branch's
Silver, Gold and test SQL applied in memory (Jinja-rendered with warehouse/macros), with profiles, checks and spot
values annotated. Reads silver.teryt_terc (read-only) for the voivodeship keys. Everything written is removed at the
end (finally). Deleted before merge."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import re
import sys
import time

import duckdb
import jinja2
import requests
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_arimr"
SOURCE = "arimr_payments"
MACROS = "".join((ROOT / f"warehouse/macros/{m}.sql").read_text() for m in ("xlsx_cells", "sources/arimr_payments"))
# the Landing and Bronze modules write 5 notices (extract x2, verify, Bronze x2); GitHub keeps 10 per level
COUNTS = {"notice": 5, "warning": 0, "error": 0}


def note(title, data, level="notice"):
    order = [level] + [lv for lv in ("notice", "warning", "error") if lv != level]
    for lv in order:
        if COUNTS[lv] < 10:
            COUNTS[lv] += 1
            annotate(lv, title, data)
            return
    print(title, data, flush=True)


def render(path: Path, tables: dict[str, str]) -> str:
    env = jinja2.Environment(extensions=["jinja2.ext.do"])
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return env.from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def parser_check() -> dict:
    """Every report since 31.12.2025 through text_pages + parse: tables, cells, or the rejection reason."""
    from ingestion.to_bronze import arimr_payments as tb
    from ingestion.to_landing import arimr_payments as tl
    from ingestion.to_landing.patterns.bulk_file import USER_AGENT

    reports = tl.listed(tl._links(tl.PAGE))
    out, failed = {}, 0
    for d in sorted(x for x in reports if x >= date(2025, 12, 31) or (x.month, x.day) == (12, 31)):
        time.sleep(2.0)
        body = requests.get(reports[d], headers={"User-Agent": USER_AGENT}, timeout=120).content
        try:
            pages = tb.text_pages(body)
            cells = tb.parse(pages)
            tables = sorted({(c["table_number"], c["table_title"][:14], c["column_label"]) for c in cells})
            per = {}
            for n, title, col in tables:
                per.setdefault(f"{n}:{title}", []).append(col)
            out[f"{d:%Y%m%d}"] = f"{len(cells)} cells; " + "; ".join(f"{k} {v[0]}-{v[-1]}" for k, v in per.items())
        except tb.Layout as exc:
            failed += 1
            out[f"{d:%Y%m%d}"] = f"REJECTED {exc}"[:300]
            print(f"===== text layer of {d} ({len(body)} bytes)")
            for i, p in enumerate(tb.text_pages(body) if body.startswith(b"%PDF") else [], 1):
                print(f"--- page {i}")
                for line in p.splitlines():
                    if line.strip():
                        print("|" + line.rstrip())
    out["reports_listed"] = len(reports)
    out["checked"] = len(out) - 1
    out["rejected"] = failed
    return out


def main(argv) -> int:
    from ingestion.to_bronze import arimr_payments as tb
    from ingestion.to_landing import arimr_payments as tl
    from ingestion.to_landing.patterns import bulk_file

    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    land = f"{SCRATCH}/01_landing"
    out: dict = {}
    lb = req("STORAGE_LANDING_BUCKET_NAME")

    def landed_state() -> list[tuple[str, int]]:
        state = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=lb, Prefix=f"{land}/"):
            state += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
        return sorted(state)

    try:
        # one short request first: an unreachable host ends the trial in a minute, not after every retry
        started = time.monotonic()
        try:
            r = requests.get(tl.PAGE, headers={"User-Agent": bulk_file.USER_AGENT}, timeout=(20, 40))
            reach = {"status": r.status_code, "bytes": len(r.content), "server": r.headers.get("Server")}
        except requests.RequestException as exc:
            reach = {"error": f"{type(exc).__name__}: {str(exc)[:160]}"}
        reach["seconds"] = round(time.monotonic() - started, 1)
        print("www.gov.pl reachability", reach, flush=True)
        if reach.get("status") != 200:
            note("arimr www.gov.pl not reachable from the runner", reach, "warning")
            return 1
        pc = parser_check()
        note("arimr parser over reports since 2025-12-31", pc, "notice" if not pc["rejected"] else "warning")

        found, summary = tl.discover()
        out["page"] = summary
        states = []
        for attempt in (1, 2):
            out[f"landing_extract_{attempt}_rc"] = tl.extract(30, landing_root=land)
            states.append(landed_state())
        out["landing_second_run_unchanged"] = states[0] == states[1]
        held = tl.held(land)
        receipts = bulk_file.landed(client, lb, f"{land}/{SOURCE}/snapshot/data/")
        out["landing_files"] = len(receipts)
        out["landing_bytes"] = sum(r["size_bytes"] for r in receipts)
        out["landing_entities"] = sorted(held)
        out["landing_verify_rc"] = bulk_file.main(
            SOURCE, "verify", landing_root=land, specs=[{"entity": e} for e in sorted(held)]
        )
        snaps = []
        for attempt in (1, 2):
            out[f"bronze_load_{attempt}_rc"] = tb.bronze_load(sorted(held), landing_root=land, namespace=SCRATCH)
            t = cat.load_table((SCRATCH, f"{SOURCE}_cells"))
            snap = t.current_snapshot()
            snaps.append((snap.snapshot_id if snap else None, t.scan().to_arrow().num_rows))
        out["bronze_second_run_unchanged"] = snaps[0] == snaps[1]
        out["bronze_snapshot_rows"] = snaps
        cells_t = cat.load_table((SCRATCH, f"{SOURCE}_cells"))
        cells = cells_t.scan().to_arrow()
        out["bronze_snapshots"] = len(list(cells_t.metadata.snapshots))
        try:
            rejected = cat.load_table((SCRATCH, f"{SOURCE}_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001
            rejected = []
        con.register("cells_raw", cells)
        con.sql("create table cells as select * from cells_raw")

        def n(q):
            return con.sql(q).fetchall()[0][0]

        out["bronze_rows"] = cells.num_rows
        out["bronze_files"] = n("select count(distinct raw_sha256) from cells")
        out["bronze_per_entity"] = n(
            "select string_agg(x, ' ' order by x) from (select entity || ':' || count(*) || ' cells/'"
            " || count(distinct table_number) || ' tables as of ' || any_value(as_of_text) x from cells group by entity)"
        )
        held_sha = set(con.sql("select distinct raw_sha256 from cells").fetchnumpy()["raw_sha256"].tolist())
        out["landing_sha_not_in_bronze"] = [r["entity"] for r in receipts if r["sha256"] not in held_sha]
        out["rejected"] = [(r.get("entity"), (r.get("error") or "")[:160]) for r in rejected]
        out["bronze_null_values"] = n("select count(*) from cells where value_text is null or row_label is null")
        out["bronze_value_shapes"] = n(
            "select string_agg(distinct regexp_replace(value_text, '[0-9]', '9', 'g'), ' | ') from"
            " (select value_text from cells where not regexp_full_match(value_text, '[0-9]{1,3}( [0-9]{3})*(,[0-9]{1,2})?'))"
        )
        out["bronze_column_labels"] = n("select string_agg(distinct regexp_replace(column_label, '[0-9]', '', 'g'), ',') from cells")
        out["bronze_titles"] = n("select count(distinct table_title) from cells")
        out["bronze_total_column_cells"] = n("select count(*) from cells where column_label like 'Razem%'")
        note("arimr landing+bronze", out)

        silver_sql = render(
            ROOT / "warehouse/models/silver/arimr_payments/arimr_payments_direct_payments.sql",
            {f"{SOURCE}_cells": "cells"},
        )
        con.sql("create table silver as " + silver_sql)
        con.sql("create table silver2 as " + silver_sql)
        areas = (
            "polska dolnośląskie kujawsko-pomorskie lubelskie lubuskie łódzkie małopolskie mazowieckie opolskie"
            " podkarpackie podlaskie pomorskie śląskie świętokrzyskie warmińsko-mazurskie wielkopolskie zachodniopomorskie"
        ).split()
        s = {
            "silver_rows": n("select count(*) from silver"),
            "rebuild_same": n("select count(*) from (select * from silver except all select * from silver2)") == 0,
            "dup_grain": n(
                "select count(*) from (select 1 from silver group by measure_code, area_name, campaign_year having count(*) > 1)"
            ),
            "null_keys": n(
                "select count(*) from silver where measure_code is null or unit_code is null or area_name is null"
                " or campaign_year is null or as_of_date is null or column_note is null or value is null or raw_sha256 is null"
            ),
            "bad_measure": n("select count(*) from silver where measure_code not in ('applications', 'decisions', 'amount_paid')"),
            "bad_unit": n("select count(*) from silver where unit_code not in ('applications', 'decisions', 'PLN')"),
            "bad_area": n(f"select count(*) from silver where area_name not in ({', '.join(repr(a) for a in areas)})"),
            "bad_note": n("select count(*) from silver where column_note not in ('', '*', '**')"),
            "per_measure": n(
                "select string_agg(x, ' ' order by x) from (select measure_code || ':' || count(*) || ' rows '"
                " || min(campaign_year) || '-' || max(campaign_year) || ' from ' || string_agg(distinct strftime(as_of_date, '%Y-%m-%d'), '+') x"
                " from silver group by measure_code)"
            ),
            "source_report_by_campaigns": n(
                "select string_agg(x, '; ' order by x) from (select measure_code || ' ' || min(campaign_year) || '-' || max(campaign_year)"
                " || ' from ' || strftime(as_of_date, '%Y-%m-%d') x from silver group by measure_code, as_of_date)"
            ),
            "notes": n("select string_agg(distinct measure_code || ' ' || campaign_year || column_note, ',') from silver where column_note <> ''"),
        }
        # the YAML's own tests (unique on the grain, not_null, accepted_values), read from _arimr_payments.yml
        spec = yaml.safe_load((ROOT / "warehouse/models/silver/arimr_payments/_arimr_payments.yml").read_text())["models"][0]
        failures = {}
        for test in spec.get("tests", []):
            if "unique" in test:
                expr = test["unique"]["column_name"]
                failures["unique grain"] = n(f"select count(*) from (select 1 from silver group by {expr} having count(*) > 1)")
        for column in spec["columns"]:
            for test in column.get("tests", []):
                if test == "not_null":
                    failures[f"not_null {column['name']}"] = n(f"select count(*) from silver where {column['name']} is null")
                elif isinstance(test, dict) and "accepted_values" in test:
                    values = test["accepted_values"]["arguments"]["values"]
                    con.execute("create or replace temp table allowed as select unnest(?) as v", [values])
                    failures[f"accepted_values {column['name']}"] = n(
                        f"select count(*) from silver where {column['name']} not in (select v from allowed)"
                    )
        s["yaml_tests_run"] = len(failures)
        s["yaml_test_failures"] = {k: v for k, v in failures.items() if v} or "none"
        s["zero_values"] = n("select count(*) from silver where value = 0")
        complete = render(ROOT / "warehouse/tests/arimr_payments_complete.sql", {"arimr_payments_direct_payments": "silver"})
        s["complete_failures"] = n(f"select count(*) from ({complete})")
        s["complete_rows"] = n(
            f"select string_agg(measure_code || ' ' || coalesce(cast(campaign_year as varchar), '-') || '=' || found, '; ') from ({complete})"
        )
        bad = s["yaml_test_failures"] != "none" or s["complete_failures"] or not s["rebuild_same"]
        note("arimr silver", s, "error" if bad else "notice")

        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        con.sql(
            f"""create table geo as select '{SOURCE}' as source_system, lower(name_pl) as source_code,
                'TERC:' || terc_code as geography_key from terc where unit_level = 'wojewodztwo'
                union all select '{SOURCE}', 'polska', 'PL'"""
        )
        gold_sql = render(
            ROOT / "warehouse/models/gold/poc_build_arimr_payments.sql",
            {"arimr_payments_direct_payments": "silver", "poc_map_geography": "geo"},
        )
        con.sql("create table gold as " + gold_sql)
        vr = render(ROOT / "warehouse/tests/arimr_payments_value_range.sql", {"poc_build_arimr_payments": "gold"})
        g = {
            "gold_rows": n("select count(*) from gold"),
            "gold_measures": n("select string_agg(distinct measure_key, ' ') from gold"),
            "dup_series_date": n(
                "select count(*) from (select 1 from gold group by measure_key || '|' || geography_key || '|' || breakdown_json,"
                " date_key having count(*) > 1)"
            ),
            "not_null": n(
                "select count(*) from gold where measure_key is null or date_key is null or geography_key is null"
                " or value is null or raw_sha256 is null or observation_status_key is null"
            ),
            "unmatched_geo": n("select count(*) from gold where geography_key = '_X'"),
            "geos": n("select count(distinct geography_key) from gold"),
            "status": n("select string_agg(x, ' ') from (select observation_status_key || '=' || count(*) x from gold group by observation_status_key)"),
            "provisional": n("select string_agg(distinct measure_codes || ' ' || year(date_key), ',') from gold where observation_status_key = 'provisional'"),
            "date_range": n("select min(date_key) || ' ' || max(date_key) from gold"),
            "value_range_failures": n(f"select count(*) from ({vr})"),
            "spot_PL": n(
                "select string_agg(measure_codes || ' ' || year(date_key) || '=' || value, '; ' order by measure_codes, date_key)"
                " from gold where geography_key = 'PL' and year(date_key) in (2004, 2015, 2022, 2024, 2025, 2026)"
            ),
            "spot_maz_2025": n(
                "select string_agg(measure_codes || '=' || value, '; ' order by measure_codes) from gold"
                " where geography_key = 'TERC:1400000' and year(date_key) = 2025"
            ),
        }
        # D37 bound (metrics in million PLN): Poland and voivodeship amounts / 1e6 within the declared bound
        from warehouse.seed_rows import rows

        bound = re.search(r"\['direct_payments_paid', '%', '([^']+)', '([^']+)'\]",
                          (ROOT / "warehouse/macros/value_range/arimr_payments.sql").read_text())
        g["metrics_bound"] = bound.groups() if bound else None
        if bound:
            g["metrics_out_of_bound"] = n(
                f"select count(*) from gold where measure_codes = 'amount_paid' and geography_key <> '_X'"
                f" and (value / 1e6 < {bound.group(1)} or value / 1e6 > {bound.group(2)})"
            )
        g["metrics_max_per_area_campaign"] = n(
            "select max(c) from (select count(*) c from gold where measure_codes = 'amount_paid' group by geography_key, date_key)"
        )
        src = [r for r in rows("source") if r["source_key"] == SOURCE]
        lic = [r for r in rows("licence") if src and r["licence_key"] == src[0]["licence_key"]]
        g["catalogue_source_row"] = {k: src[0][k] for k in ("source_key", "licence_key", "url")} if src else None
        g["catalogue_licence_row"] = {k: lic[0][k] for k in ("licence_key", "commercial_ok", "share_alike", "url")} if lic else None
        note("arimr gold", g)
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
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
        left = sum(
            len(pg.get("Contents", []))
            for b in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
            for pg in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/")
        )
        note("arimr trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
