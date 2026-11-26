"""Dev trial of ure_prices (register PL-ENV-020, #2870) on the branch's final code: Landing → Bronze → Silver → Gold.

Landing: the source's own landing.extract (bulk_file, 5 s between pages, reduce: ure_prices) into
zz_dev_ure_prices/01_landing, twice (the second must write nothing), then verify and inspect. Bronze: the real loader
into the namespace zz_dev_ure_prices, twice (the second must append nothing), with a profile of the input. Silver and
Gold: the real `dbt run` and `dbt test` of the Silver model and the Gold block on a local DuckDB file attached as r2
(dev/ure_prices_dbt.py; nothing written to the catalog), twice; then the grain check by poc_series_key() x date_key
(warehouse/macros/poc_blocks.sql, as poc_fact_* builds series_key), geography keys against production
gold.dim_geography (read only), the KPI one-row and value-range checks, spot values against the raw page bytes, and
the licence gate on the seed rows. Everything written to R2 is removed in `finally`. Deleted before merge.
"""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import re
import sys
import tempfile
import time
import traceback

import duckdb
import jinja2

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "ure_prices"
SCRATCH = "zz_dev_ure_prices"
LANDING = f"{SCRATCH}/01_landing"
START = time.monotonic()


def log(message: str) -> None:
    print(f"[{round((time.monotonic() - START) / 60, 1)} min] {message}", file=sys.__stderr__, flush=True)


def q(con, sql: str):
    return con.sql(sql).fetchall()


def plain(obj):
    return json.loads(json.dumps(obj, default=str))


def stage(name: str, annotate, call):
    log(f"{name}: start")
    try:
        return call()
    except Exception:  # every stage reports; the clean-up always runs
        annotate("error", f"ure_prices trial · {name} failed", traceback.format_exc()[-1800:])
        return None
    finally:
        log(f"{name}: end")


def objects(client, bucket: str, prefix: str) -> tuple[int, int]:
    count = size = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            count, size = count + 1, size + o["Size"]
    return count, size


def captured(call) -> tuple[dict, int]:
    """Run a bulk_file mode; returns its printed summary and exit code."""
    old = sys.stdout
    sys.stdout = buf = io.StringIO()
    try:
        code = call()
    finally:
        sys.stdout = old
    print(buf.getvalue()[-3000:], flush=True)
    summary = json.loads([ln for ln in buf.getvalue().splitlines() if ln.startswith('{"files"') or ln.startswith('{"mode"')][-1])
    summary["exit"] = code
    return summary, code


def clean(client, cat, buckets) -> dict:
    removed, dropped = 0, []
    for bucket in buckets:
        keys = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys += [o["Key"] for o in page.get("Contents", [])]
        for key in keys:
            client.delete_object(Bucket=bucket, Key=key)
            removed += 1
    with contextlib.suppress(Exception):
        for ident in cat.list_tables(SCRATCH):
            cat.drop_table(ident)
            dropped.append(".".join(ident))
    with contextlib.suppress(Exception):
        cat.drop_namespace(SCRATCH)
    left = sum(objects(client, b, f"{SCRATCH}/")[0] for b in buckets)
    return {"objects_removed": removed, "objects_left": left, "tables_dropped": dropped}


def raw_row(body: bytes, table: int, year: str) -> list[str]:
    """The cells of one year's row of a page table, read from the raw bytes with a plain regex (independent of the
    Bronze parser)."""
    html = body.decode("utf-8")
    tables = re.findall(r"(?s)<table.*?</table>", html)
    for tr in re.findall(r"(?s)<tr[^>]*>(.*?)</tr>", tables[table - 1]):
        cells = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", c).replace("&nbsp;", " ")).strip()
                 for c in re.findall(r"(?s)<td[^>]*>(.*?)</td>", tr)]
        if cells and cells[0] == year:
            return cells
    return []


def main() -> int:
    from ingestion.common import bronze_files as bf
    from ingestion.common import bulk_bronze
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.common.report import annotate
    from ingestion.to_bronze.ure_prices import load
    from ingestion.to_landing import ure_prices as landing
    from ingestion.to_landing.patterns import bulk_file

    from dev import ure_prices_dbt as trial_dbt

    # GitHub keeps 10 annotations per step: the pattern's and the loader's own are silenced (summaries are in these).
    bulk_file.annotate = bulk_bronze.annotate = lambda *_args, **_kw: None
    client, cat = s3(), bf.retry(catalog, "catalog")
    landing_bucket = req("STORAGE_LANDING_BUCKET_NAME")
    buckets = (landing_bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
    annotate("notice", "ure_prices trial · scratch cleaned before the start", clean(client, cat, buckets))
    try:
        def landing_stage():
            first, _ = captured(lambda: landing.extract(LANDING))
            between = objects(client, landing_bucket, f"{LANDING}/")
            second, _ = captured(lambda: landing.extract(LANDING))
            after = objects(client, landing_bucket, f"{LANDING}/")
            verify, _ = captured(lambda: bulk_file.main(SOURCE, "verify", LANDING))
            inspect, _ = captured(lambda: bulk_file.main(SOURCE, "inspect", LANDING))
            receipts = {s["entity"]: bulk_file.landed(client, landing_bucket, bulk_file.prefix(SOURCE, s["entity"], LANDING))
                        for s in bulk_file.declaration(SOURCE)}
            files = {e: [{"bytes": r["size_bytes"], "sha": r["sha256"][:12], "version": r["version_token"],
                          "reduced": r["detail"].get("reduced"), "ctype": r["detail"]["http"].get("content_type")}
                         for r in rs] for e, rs in receipts.items()}
            ok = (first["exit"] == 0 and second["exit"] == 0 and between == after and verify["exit"] == 0
                  and all(f["outcome"] == "unchanged" for f in second["files"]))
            annotate("notice" if ok else "warning", "ure_prices trial · Landing (twice), verify, inspect", plain({
                "first": [[f["entity"], f["outcome"], f.get("download_seconds")] for f in first["files"]],
                "second": [[f["entity"], f["outcome"]] for f in second["files"]],
                "objects_bytes_after_first_and_after_second": [between, after],
                "verify": [[f["entity"], f["missing_data"]] for f in verify["files"]], "verify_exit": verify["exit"],
                "inspect": [[f["entity"], f.get("state"), f.get("server_version")] for f in inspect["files"]],
                "files": files}))
            return receipts

        receipts = stage("landing", annotate, landing_stage)
        if not receipts:
            return 1
        table_id = (SCRATCH, load.TABLE)

        def bronze_stage():
            load.load(0, landing_root=LANDING, namespace=SCRATCH)
            t1 = cat.load_table(table_id)
            files1, rows1 = len(bf.loaded_hashes(t1)), t1.scan().to_arrow().num_rows
            load.load(0, landing_root=LANDING, namespace=SCRATCH)
            t2 = cat.load_table(table_id)
            bronze = t2.scan().to_arrow()
            files2 = len(bf.loaded_hashes(t2))
            try:
                rejected = cat.load_table((SCRATCH, load.REJECTED)).scan().to_arrow().to_pylist()
            except Exception:  # no rejections table when nothing was rejected
                rejected = []
            newest = {rs[-1]["sha256"] for rs in receipts.values() if rs}
            con = duckdb.connect()
            con.register("b", bronze)
            profile = {
                "rows_entities_files": q(con, "select count(*), count(distinct entity), count(distinct raw_sha256) from b")[0],
                "per_entity_table": q(con, "select entity, table_number, count(*), count(distinct row_label), min(row_label), max(row_label), "
                                           "any_value(page_modified) from b group by all order by all"),
                "null_keys": q(con, "select count(*) from b where entity is null or row_label is null or column_header is null "
                                    "or cell_text is null or raw_sha256 is null")[0][0],
                "duplicate_cells": q(con, "select count(*) from (select entity, table_number, row_number, column_number from b group by all having count(*) > 1)")[0][0],
                "non_numeric_texts": q(con, "select cell_text, count(*) from b where not starts_with(column_header, 'Nr Informacji') "
                                            "and not regexp_matches(cell_text, '^-?[0-9][0-9 ]*(,[0-9]+)?$') group by 1 order by 1"),
                "headers": q(con, "select entity, table_number, count(distinct column_header) from b group by all order by all"),
            }
            ok = not rejected and files1 == files2 and rows1 == bronze.num_rows and newest <= bf.loaded_hashes(t2)
            annotate("notice" if ok else "warning", "ure_prices trial · Bronze (twice) and profile", plain({
                "files_rows_after_first": [files1, rows1], "files_rows_after_second": [files2, bronze.num_rows],
                "newest_landing_files_in_bronze": [len(newest & bf.loaded_hashes(t2)), len(newest)],
                "rejected": len(rejected), "rejected_errors": [r.get("error") for r in rejected][:3], "profile": profile}))
            return bronze

        bronze = stage("bronze", annotate, bronze_stage)
        if bronze is None or bronze.num_rows == 0:
            return 1
        with tempfile.TemporaryDirectory(prefix="ure-dbt-") as workdir:
            built = stage("dbt", annotate, lambda: trial_dbt.build(bronze, workdir))
            if built is None:
                return 1
            path, runs = built
            failing = {k: [r for r in v if r[1] not in ("success", "pass")] for k, v in runs.items() if k.startswith(("run", "test"))}
            annotate("warning" if any(failing.values()) or runs["rows1"] != runs["rows2"] else "notice",
                     "ure_prices trial · dbt run and test (twice)", plain({
                         "rows_silver_gold_run1_run2": [runs["rows1"], runs["rows2"]], "models_run1": runs["run1"],
                         "tests_run1_pass": sum(r[1] == "pass" for r in runs["test1"]), "tests_run1_total": len(runs["test1"]),
                         "tests_run2_pass": sum(r[1] == "pass" for r in runs["test2"]), "tests_run2_total": len(runs["test2"]),
                         "tests_run1": [[n[:70], s, f] for n, s, f in runs["test1"]], "not_passing": failing}))

            def analysis():
                con = duckdb.connect()
                con.sql(f"attach '{path}' as r2 (read_only)")
                con.sql("create view silver as select * from r2.silver.ure_prices_indicators")
                con.sql("create view gold as select * from r2.gold.build_ure_prices")
                silver = {
                    "rows": q(con, "select count(*) from silver")[0][0],
                    "per_indicator_freq_unit_status": q(con, "select indicator_code, frequency, unit_code, value_status, count(*), "
                                                             "min(period_label), max(period_label) from silver group by all order by all"),
                    "duplicate_grain": q(con, "select count(*) from (select indicator_code, period_label from silver group by all having count(*) > 1)")[0][0],
                    "null_keys": q(con, "select count(*) from silver where indicator_code is null or period_label is null "
                                        "or period_start is null or raw_sha256 is null")[0][0],
                    "markers_value_null": q(con, "select value_status, cell_text, count(*), count(*) filter (where value is null) "
                                                 "from silver where value_status <> 'ok' group by all order by all"),
                    "zero_or_negative_values": q(con, "select count(*) from silver where value <= 0")[0][0],
                    "page_modified": q(con, "select entity, any_value(page_modified), count(distinct raw_sha256) from silver group by 1 order by 1"),
                }
                annotate("warning" if silver["duplicate_grain"] or silver["null_keys"] else "notice",
                         "ure_prices trial · Silver profile", plain(silver))
                macro = jinja2.Environment(extensions=["jinja2.ext.do"]).from_string(
                    (ROOT / "warehouse/macros/poc_blocks.sql").read_text()).module
                series = macro.poc_series_key()
                dim = cat.load_table(("gold", "dim_geography")).scan(selected_fields=("geography_key",)).to_arrow()
                con.register("dim_geography", dim)
                gold = {
                    "rows_measures_series_min_max": q(con, f"select count(*), count(distinct measure_key), count(distinct {series}), "
                                                           "min(date_key), max(date_key) from gold")[0],
                    "per_measure": q(con, "select measure_key, frequency, unit_name, count(*), min(period_label), max(period_label), "
                                          "min(value), max(value) from gold group by all order by all"),
                    "measures_with_two_frequencies": q(con, "select count(*) from (select measure_key from gold group by 1 having count(distinct frequency) > 1)")[0][0],
                    "grain_duplicates_series_key_x_date_key": q(con, f"select count(*) from (select {series}, date_key from gold group by all having count(*) > 1)")[0][0],
                    "geography_keys_and_not_in_prod_dim_geography": q(con, "select list(distinct geography_key), list(distinct geography_key) "
                                                                         "filter (where geography_key not in (select geography_key from dim_geography)) from gold")[0],
                    "gold_rows_vs_silver_ok": [q(con, "select count(*) from gold")[0][0], q(con, "select count(*) from silver where value_status = 'ok'")[0][0]],
                    "zero_values": q(con, "select count(*) from gold where value = 0")[0][0],
                }
                annotate("warning" if gold["grain_duplicates_series_key_x_date_key"] or gold["geography_keys_and_not_in_prod_dim_geography"][1]
                         else "notice", "ure_prices trial · Gold block", plain(gold))

                # Spot values: Gold against the raw cells of the newest Landing copies (read again from Landing).
                bodies = {e: bulk_bronze.read_raw(client, landing_bucket, SOURCE, rs[-1]) for e, rs in receipts.items()}
                newest_q = q(con, "select period_label, value from gold where measure_key like '%electricity_competitive_market.quarterly' "
                                  "order by date_key desc limit 1")[0]
                year_a = q(con, "select period_label, value from gold where measure_key like '%electricity_competitive_market.annual' "
                                "order by date_key desc limit 1")[0]
                house = q(con, "select period_label, value from gold where measure_key like '%electricity_households.annual' "
                               "order by date_key desc limit 1")[0]
                gas = q(con, "select period_label, value from gold where measure_key like '%pln_per_mwh:gas_import_eu_efta.quarterly' "
                             "order by date_key desc limit 1")[0]
                y, qn = newest_q[0].split("-Q")
                spots = {
                    "7852_newest_quarter": [newest_q[0], newest_q[1], raw_row(bodies["electricity_competitive_market"], 1, y)[1 + int(qn)]],
                    "7852_newest_year": [year_a[0], year_a[1], raw_row(bodies["electricity_competitive_market"], 1, year_a[0])[1]],
                    "7852_year_2015": [q(con, "select value from gold where measure_key like '%electricity_competitive_market.annual' and period_label = '2015'")[0][0],
                                       raw_row(bodies["electricity_competitive_market"], 1, "2015")],
                    "7853_newest_year": [house[0], house[1], raw_row(bodies["electricity_households"], 1, house[0])[1]],
                    "7859_eu_newest_quarter": [gas[0], gas[1], raw_row(bodies["gas_import_prices"], 1, gas[0].split("-Q")[0])],
                }
                spots["all_equal"] = all(float(str(v[2]).replace(",", ".")) == v[1] for k, v in spots.items()
                                         if k in ("7852_newest_quarter", "7852_newest_year", "7853_newest_year"))
                # KPI layer: rows per KPI pattern x area x period (max 1) and the value-range bounds, on the build stage.
                from warehouse import seed_rows
                from warehouse.publish import licensed_sources

                bounds = {b[0]: (float(b[2]), float(b[3])) for b in re.findall(
                    r"\['(\w+)', '([^']*)', '([^']*)', '([^']*)'\]", (ROOT / "warehouse/macros/value_range/ure_prices.sql").read_text())}
                kpi = {}
                for r in [r for r in seed_rows.rows("kpi_measure") if r["source_system"] == SOURCE]:
                    lo, hi = bounds[r["kpi_key"]]
                    kpi[r["kpi_key"]] = q(con, f"select count(*), max(n), sum(out) from (select geography_key, date_key, count(*) n, "
                                               f"count(*) filter (where value < {lo} or value > {hi}) out from gold "
                                               f"where measure_key like '{r['measure_key_pattern']}' group by all)")[0]
                sources, licences = seed_rows.rows("source"), seed_rows.rows("licence")
                row = next(r for r in sources if r["source_key"] == SOURCE)
                annotate("notice" if spots["all_equal"] else "warning", "ure_prices trial · spot values, KPI checks, licence gate", plain({
                    "spot_gold_vs_raw_cell": spots,
                    "kpi_periods_max_rows_per_area_period_out_of_bounds": kpi,
                    "source_licence_key": row["licence_key"],
                    "licence_row": next((r for r in licences if r["licence_key"] == row["licence_key"]), None),
                    "licensed_by_gate": SOURCE in licensed_sources(sources, licences)}))

            stage("silver and gold analysis", annotate, analysis)
    finally:
        annotate("notice", "ure_prices trial · cleanup", clean(client, cat, buckets))
        log("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
