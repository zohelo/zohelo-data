"""Dev trial (PLS-032, #2484): MRiRW ZSRIR, source mrirw_zsrir, Landing → Gold on the final code.

Landing extract of the newest XLSX issue of the three bulletins into the Landing prefix zz_dev_mrirw_zsrir/ twice (the
second must write nothing), inspect (newest issue vs scratch receipts) and verify; Bronze load into the Iceberg
namespace zz_dev_mrirw_zsrir and the lake prefix zz_dev_mrirw_zsrir/ twice (the second appends nothing); a profile of
the Bronze cells (titles, notes); the Silver model, its tests and the completeness test, the Gold build stage and the
metrics rows rendered with Jinja from warehouse/ and run in local DuckDB, each twice, with the grain check by
series_key x date_key, the value bounds and spot values for Poland. dim_geography is read (read-only) from production.
Everything written is removed in `finally`. At most 10 annotations.
"""

from __future__ import annotations

from pathlib import Path
import re

import duckdb
from jinja2 import Environment

from ingestion.common import report
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.mrirw_zsrir import load as zsrir_load
from ingestion.to_landing import mrirw_zsrir as landing
from ingestion.to_landing.patterns import bulk_file

SOURCE = "mrirw_zsrir"
SCRATCH = "zz_dev_mrirw_zsrir"
NOTES = {"left": 9}
KPIS = {  # kpi: (measure key, bound) as in seed_rows/kpi_measure and tests/metrics_value_range.sql
    "milk_purchase_price": ("mrirw_zsrir:purchase_prices:PLN_100KG:milk.raw_milk", 1000),
    "pig_purchase_price": ("mrirw_zsrir:purchase_prices:PLN_KG_LW:pork.live_pigs", 30),
    "wheat_purchase_price": ("mrirw_zsrir:purchase_prices:PLN_T:cereals.wheat_consumption", 5000),
}


def note(title: str, data, level: str = "notice") -> None:
    if NOTES["left"] > 0:
        NOTES["left"] -= 1
        annotate(level, title, data)
    print(f"[{title}] {data}", flush=True)


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def one(con, sql):
    return con.execute(sql).fetchone()[0]


def render(path: str, refs: dict[str, str], sources: dict[str, str], prefix: str = "") -> str:
    return Environment().from_string(prefix + Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda n: refs.get(n, n), source=lambda _s, t: sources[t]
    )


def trial(cat) -> None:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    runs = []
    for attempt in (1, 2):
        landing.extract(0, landing_root=SCRATCH)
        runs.append({"run": attempt, "last_extract": LAST.get("extract")})
    held = landing.held(SCRATCH)
    with landing.portal_tls():
        issues = landing.newest_issues()
    inspect = {m: "current" if m in held and held[m]["source_url"] == i["url"] else "stale/missing" for m, i in issues.items()}
    verify = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH, specs=[{"entity": m} for m in landing.MARKETS])
    receipts = {m: len(bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, m, SCRATCH))) for m in landing.MARKETS}
    note("zsrir landing (scratch)", {
        "extract_two_runs": runs, "inspect": inspect, "verify_exit": verify, "receipts_per_market": receipts,
        "issues": {m: {k: i[k] for k in ("issue", "published", "filename", "files_listed")} for m, i in issues.items()},
        "held": {m: {"size": r["size_bytes"], "version": r["version_token"], "basis": r.get("version_basis"),
                     "sha": r["sha256"][:12], "url": r["source_url"][-40:]} for m, r in held.items()}})

    timings = []
    for _ in (1, 2):
        code = zsrir_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        table = cat.load_table((SCRATCH, "mrirw_zsrir_cells"))
        files = table.inspect.files()
        timings.append({"exit": code, "files": files.num_rows, "rows": sum(files.column("record_count").to_pylist()),
                        "snapshots": len(table.metadata.snapshots)})
    paths = files.column("file_path").to_pylist()
    try:
        rejected = cat.load_table((SCRATCH, "mrirw_zsrir_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    note("zsrir bronze (scratch)", {
        "load_two_runs": timings, "parquet_bytes": sum(files.column("file_size_in_bytes").to_pylist()),
        "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3]})

    con = duckdb.connect()
    from warehouse import entity

    entity._remote(con)
    con.execute(f"create table bronze_b as select * from read_parquet({paths!r})")
    note("zsrir bronze profile", {
        "by_entity": q(con, "select entity, count(*), count(distinct sheet_name), count(distinct raw_sha256), "
                            "min(source_version) from bronze_b group by 1 order by 1"),
        "modelled_sheet_top_rows": q(con, "select entity, sheet_name, row_number, column_number, value_text from bronze_b "
                                          "where (regexp_matches(trim(sheet_name), '^(ZAKUP WYKRESY MCE|MIESIĘCZNE CENY "
                                          "SKUPU 2004-20[0-9]{2}|ZAKUP ŻYWIEC WYKRES MSC)$') and row_number <= 5) "
                                          "order by 1, 3, 4"),
        "grid_header_rows": q(con, "select entity, row_number, string_agg(value_text, ' | ' order by column_number) "
                                   "from bronze_b where regexp_matches(trim(sheet_name), '^(ZAKUP WYKRESY MCE|MIESIĘCZNE "
                                   "CENY SKUPU 2004-20[0-9]{2}|ZAKUP ŻYWIEC WYKRES MSC)$') and trim(value_text) in "
                                   "('MC/ROK', 'ROK', 'MIESIĄC') and column_number = 2 group by all"),
        "publication_note": q(con, "select entity, row_number, value_text from bronze_b where sheet_name = 'DODATKOWE "
                                   "INFO' and entity = 'milk' and row_number between 29 and 36 order by 2"),
        "monthly_sheets_other_markets": q(con, "select entity, sheet_name from bronze_b where sheet_name ilike '%MSC%' or "
                                               "sheet_name ilike '%MIESI%' or sheet_name ilike '%MCE%' group by all "
                                               "order by 1, 2")})

    macros = Path("warehouse/macros/xlsx_cells.sql").read_text()
    silver_sql = render("warehouse/models/silver/mrirw_zsrir/mrirw_zsrir_prices.sql", {},
                        {"mrirw_zsrir_cells": "bronze_b"}, macros)
    rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table silver_p as {silver_sql}")
        rows.append(one(con, "select count(*) from silver_p"))
    tests = {
        "unique_grain": one(con, "select count(*) from (select market, product_code, period_start from silver_p "
                                 "group by all having count(*) > 1)"),
        "not_null": q(con, "select " + ", ".join(f"count(*) filter (where {c} is null)" for c in (
            "market", "product_code", "period_start", "frequency", "unit_code", "raw_sha256")) + " from silver_p"),
        "complete": q(con, render("warehouse/tests/mrirw_zsrir_prices_complete.sql",
                                  {"mrirw_zsrir_prices": "silver_p"}, {})),
    }
    note("zsrir silver", {
        "rows_two_builds": rows, "tests": tests,
        "by_product": q(con, "select market, product_code, unit_text, unit_code, count(*), count(value), "
                             "min(period_start), max(period_start), min(value), max(value) from silver_p group by all "
                             "order by 1, 2"),
        "value_flags": q(con, "select market, value_flag, count(*) from silver_p where value_flag is not null "
                              "group by all order by 1, 2"),
        "null_keys_sample": q(con, "select market, sheet_name, source_row, source_column, product_name, value_flag "
                                   "from silver_p where product_code is null or period_start is null or unit_code is "
                                   "null limit 8"),
        "issue_labels": q(con, "select market, issue_label, count(*) from silver_p group by all")})

    geo_cols = ("geography_key", "level_name", "name_pl")
    con.register("geo_a", cat.load_table(("gold", "dim_geography")).scan(selected_fields=geo_cols).to_arrow())
    con.execute("create table geo as select * from geo_a")
    macro = Path("warehouse/macros/sources/mrirw_zsrir.sql").read_text()
    cte = Environment().from_string(macro + "{{ poc_geography__mrirw_zsrir() }}").render()
    con.execute(f"create table map_t as with geo as (select * from geo), {cte} select * from mrirw_zsrir")
    gold_sql = render("warehouse/models/gold/poc_build_mrirw_zsrir.sql",
                      {"poc_map_geography": "map_t", "mrirw_zsrir_prices": "silver_p"}, {})
    gold_rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table gold_t as {gold_sql}")
        gold_rows.append(one(con, "select count(*) from gold_t"))
    series = "measure_key || '|' || geography_key || '|' || breakdown_json"
    note("zsrir gold build_mrirw_zsrir", {
        "rows_two_builds": gold_rows, "silver_rows_with_value": one(con, "select count(value) from silver_p"),
        "dup_series_x_date": one(con, f"select count(*) from (select {series}, date_key from gold_t group by all "
                                      "having count(*) > 1)"),
        "measures": q(con, "select measure_key, unit_code, count(*), min(date_key), max(date_key) from gold_t "
                           "group by all order by 1"),
        "geography": q(con, "select geography_key, count(*) from gold_t group by 1"),
        "null_values": one(con, "select count(*) from gold_t where value is null"),
        "columns": [r[0] for r in con.execute("describe gold_t").fetchall()]})

    metrics = {}
    for kpi, (key, bound) in KPIS.items():
        r = con.execute(
            "select count(*), max(n), count(*) filter (where v < 0 or v > ?), min(date_key), max(date_key) from ("
            "select geography_key, date_key, count(*) n, max(value) v from gold_t where measure_key = ? group by all)",
            [bound, key]).fetchone()
        metrics[kpi] = {"rows": r[0], "max_rows_per_area_period": r[1], "out_of_bound": r[2], "from": str(r[3]),
                        "to": str(r[4])}
    note("zsrir gold spot values and metrics", {
        "metrics": metrics,
        "latest_poland": q(con, "select measure_key, date_key, value from gold_t qualify rank() over (partition by "
                                "measure_key order by date_key desc) = 1 order by 1"),
        "spot": q(con, "select measure_key, date_key, value from gold_t where (measure_key like '%milk.raw_milk' and "
                       "date_key in ('2004-04-01', '2022-11-01', '2026-09-01')) or (measure_key like '%live_pigs' and "
                       "date_key in ('2022-01-01', '2026-09-01')) or (measure_key like '%wheat_consumption' and "
                       "date_key in ('2025-10-01', '2026-10-01')) order by 1, 2")})

    from warehouse import seed_rows

    source_row = [r for r in seed_rows.rows("source") if r["source_key"] == SOURCE]
    lic = {r["licence_key"] for r in source_row}
    note("zsrir licence (catalogue rows)", {
        "source": source_row,
        "licence": [r for r in seed_rows.rows("licence") if r["licence_key"] in lic],
        "unit_map": [r for r in seed_rows.rows("unit_map") if r["source_system"] == SOURCE]})


LAST: dict = {}


def cleanup(cat) -> None:
    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
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
    for var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"),
                       ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
        bucket = req(var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/")
    )
    NOTES["left"] += 1
    note("scratch removed", removed)


def main() -> int:
    def capture(level, title, data):  # the activities' own annotations go to the log only (cap 10)
        LAST[title.rsplit(" ", 1)[-1]] = data
        print(f"[{title}] {data}", flush=True)

    report.annotate = capture
    bulk_file.annotate = lambda *_a, **_k: None
    zsrir_load.bulk_bronze.annotate = lambda *_a, **_k: None
    failed = 0
    cat = catalog()
    try:
        try:
            trial(cat)
        except Exception as exc:
            failed = 1
            NOTES["left"] += 1
            message = re.sub(r"\s+", " ", str(exc))[:1500]
            note("zsrir trial failed", {"error": f"{type(exc).__name__}: {message}"}, "error")
            raise
    finally:
        cleanup(cat)
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
