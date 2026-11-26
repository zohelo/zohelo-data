"""Dev trial (PL-ENV-003, #3097): GUGiK BDOT10k, source gugik_bdot10k, Landing → Gold on the final code.

Landing extract of the declared index answer into the Landing prefix zz_dev_gugik_bdot10k/ twice (the second must write
nothing: same SHA-256), inspect and verify; Bronze load into the Iceberg namespace zz_dev_gugik_bdot10k and the lake
prefix zz_dev_gugik_bdot10k/ twice (the second appends nothing); a profile of the Bronze rows; the Silver model, its
completeness test and the Gold build stage rendered with Jinja from warehouse/ and run in local DuckDB, each twice,
with the checks of their dbt tests, the grain check by series_key × date_key, the metrics rows and value bound, and spot
values for Poland. dim_geography is read (read-only) from production. Everything written is removed in `finally`. No
package is downloaded. At most 10 annotations.
"""

from __future__ import annotations

from pathlib import Path
import re
import time

import duckdb
from jinja2 import Environment

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gugik_bdot10k import load as bdot_load
from ingestion.to_landing.patterns import bulk_file

SOURCE = "gugik_bdot10k"
SCRATCH = "zz_dev_gugik_bdot10k"
NOTES = {"left": 9}


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
    specs = bulk_file.declaration(SOURCE)
    runs = []
    for attempt in (1, 2):
        for spec in specs:
            started = time.monotonic()
            result = bulk_file.load_file(client, bucket, SOURCE, spec, SCRATCH)
            runs.append({"run": attempt, "outcome": result.get("outcome"), "sha": result.get("sha256", "")[:12],
                         "version": result.get("version"), "seconds": round(time.monotonic() - started)})
            time.sleep(5)
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, specs[0]["entity"], SCRATCH))
    inspect = bulk_file.main(SOURCE, "inspect", landing_root=SCRATCH, specs=specs)
    verify = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH, specs=specs)
    note("bdot10k landing (scratch)", {
        "extract_two_runs": runs, "inspect_exit": inspect, "verify_exit": verify,
        "receipts": [{"size": r["size_bytes"], "version": r["version_token"], "basis": r.get("version_basis"),
                      "content_type": r["detail"]["http"].get("content_type"), "sha": r["sha256"][:12]} for r in held]})

    timings = []
    for _ in (1, 2):
        started = time.monotonic()
        code = bdot_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        table = cat.load_table((SCRATCH, "bdot10k_packages"))
        files = table.inspect.files()
        timings.append({"exit": code, "seconds": round(time.monotonic() - started), "files": files.num_rows,
                        "rows": sum(files.column("record_count").to_pylist()),
                        "snapshots": len(table.metadata.snapshots)})
    paths = files.column("file_path").to_pylist()
    try:
        rejected = cat.load_table((SCRATCH, "bdot10k_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    note("bdot10k bronze (scratch)", {
        "load_two_runs": timings, "parquet_bytes": sum(files.column("file_size_in_bytes").to_pylist()),
        "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3]})

    con = duckdb.connect()
    from warehouse import entity

    entity._remote(con)
    con.execute(f"create table bronze_b as select * from read_parquet({paths!r})")
    texts = list(bdot_load.ATTRIBUTES.values())
    note("bdot10k bronze profile", {
        "rows": q(con, "select count(*), count(distinct teryt), count(distinct url_gml), min(data_aktualizacji), "
                       "max(data_aktualizacji), count(distinct raw_sha256) from bronze_b"),
        "null_or_empty": {c: n for c, n in zip(texts, con.execute(
            "select " + ", ".join(f"count(*) filter (where {c} is null or trim({c}) = '')" for c in texts)
            + " from bronze_b").fetchone())},
        "shapes": {c: q(con, f"select regexp_replace(regexp_replace({c}, '[0-9]', '9', 'g'), '[a-ząćęłńóśźż]+', 'a',"
                             f" 'g') s, count(*) from bronze_b group by 1 order by 2 desc limit 4") for c in texts},
        "voivodeships": q(con, "select left(teryt, 2), count(*) from bronze_b group by 1 order by 1"),
        "years": q(con, "select left(data_aktualizacji, 4), count(*) from bronze_b group by 1 order by 1")})

    silver_sql = render("warehouse/models/silver/gugik_bdot10k/bdot10k_powiat_packages.sql", {},
                        {"bdot10k_packages": "bronze_b"})
    rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table silver_p as {silver_sql}")
        rows.append(one(con, "select count(*) from silver_p"))
    voiv = tuple(f"{n:02d}" for n in range(2, 33, 2))
    tests = {
        "unique_package_id": one(con, "select count(*) from (select package_id from silver_p group by 1 having "
                                      "count(*) > 1)"),
        "unique_url_gml": one(con, "select count(*) from (select url_gml from silver_p group by 1 having count(*) > 1)"),
        "not_null": q(con, "select " + ", ".join(f"count(*) filter (where {c} is null)" for c in (
            "package_id", "voivodeship_code", "powiat_name", "update_date", "update_year", "url_gml",
            "application_schema", "snapshot_date", "raw_sha256")) + " from silver_p"),
        "accepted_voivodeship_fails": q(con, f"select voivodeship_code, count(*) from silver_p where voivodeship_code "
                                             f"not in {voiv!r} group by 1"),
        "accepted_schema_fails": q(con, "select application_schema, count(*) from silver_p where application_schema "
                                        "<> '2021' group by 1"),
        "complete": q(con, render("warehouse/tests/gugik_bdot10k_powiat_packages_complete.sql",
                                  {"bdot10k_powiat_packages": "silver_p"}, {})),
    }
    note("bdot10k silver", {
        "rows_two_builds": rows, "tests": tests,
        "range": q(con, "select min(update_date), max(update_date), min(snapshot_date), max(snapshot_date), "
                        "count(distinct voivodeship_code) from silver_p"),
        "by_year": q(con, "select update_year, count(*) from silver_p group by 1 order by 1"),
        "sample": q(con, "select package_id, powiat_name, update_date, application_schema, url_gml from silver_p "
                         "where package_id in ('1465', '1261', '0264')")})

    geo_cols = ("geography_key", "level_name", "name_pl", "terc_code")
    con.register("geo_a", cat.load_table(("gold", "dim_geography")).scan(selected_fields=geo_cols).to_arrow())
    con.execute("create table geo as select * from geo_a")
    macro = Path("warehouse/macros/sources/gugik_bdot10k.sql").read_text()
    cte = Environment().from_string(macro + "{{ poc_geography__gugik_bdot10k() }}").render()
    con.execute(f"create table map_t as with geo as (select * from geo), {cte} select * from gugik_bdot10k")
    gold_sql = render("warehouse/models/gold/poc_build_gugik_bdot10k.sql",
                      {"poc_map_geography": "map_t", "bdot10k_powiat_packages": "silver_p"}, {})
    gold_rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table gold_t as {gold_sql}")
        gold_rows.append(one(con, "select count(*) from gold_t"))
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
              "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
    note("bdot10k gold build_gugik_bdot10k", {
        "rows_two_builds": gold_rows,
        "map_rows": q(con, "select count(*), count(distinct source_code) from map_t"),
        "dup_series_x_date": one(con, f"select count(*) from (select {series}, date_key from gold_t group by all "
                                      "having count(*) > 1)"),
        "by_measure": q(con, "select measure_key, unit_code, date_key, frequency, count(*), sum(is_total::int), "
                             "count(distinct geography_key) from gold_t group by all order by 1"),
        "unmatched": q(con, "select geography_source_code, count(*) from gold_t where geography_key = '_X' group by 1"),
        "null_or_nonpositive": one(con, "select count(*) from gold_t where value is null or value <= 0"),
        "share_sums": q(con, "select min(s), max(s) from (select geography_key, sum(value) s from gold_t where "
                             "measure_codes = 'share' group by 1)"),
        "count_sums_vs_total": one(con, "select count(*) from (select geography_key, sum(value) filter (where not "
                                        "is_total) a, sum(value) filter (where is_total) t from gold_t where "
                                        "measure_codes = 'count' group by 1) where a <> t"),
        "columns": [r[0] for r in con.execute("describe gold_t").fetchall()]})

    metrics = con.execute(
        "select geography_key, date_key, count(*) n, max(value) v from gold_t where measure_key = "
        "'gugik_bdot10k:powiat_packages:packages:count' and is_total and geography_key <> '_X' group by all").fetchall()
    note("bdot10k gold spot values and metrics", {
        "poland": q(con, "select measure_codes, breakdown_json, value from gold_t where geography_key = 'PL' "
                         "order by 1, 2"),
        "poland_total_vs_silver": [one(con, "select value from gold_t where geography_key = 'PL' and is_total"),
                                   one(con, "select count(*) from silver_p")],
        "voivodeships_total": q(con, "select g.name_pl, b.value from gold_t as b join geo as g using (geography_key) "
                                     "where b.is_total order by 2 desc"),
        "mazowieckie": q(con, "select measure_codes, breakdown_json, value from gold_t where geography_source_code = "
                              "'14' order by 1, 2"),
        "metrics_rows": len(metrics), "metrics_max_rows_per_area_period": max((r[2] for r in metrics), default=0),
        "metrics_out_of_bound_1_500": sum(1 for r in metrics if not (1 <= r[3] <= 500)),
        "metrics_max": max((r[3] for r in metrics), default=None)})

    from warehouse import seed_rows

    source_row = [r for r in seed_rows.rows("source") if r["source_key"] == SOURCE]
    lic = {r["licence_key"] for r in source_row}
    note("bdot10k licence (catalogue rows)", {
        "source": source_row,
        "licence": [r for r in seed_rows.rows("licence") if r["licence_key"] in lic],
        "unit_map": [r for r in seed_rows.rows("unit_map") if r["source_system"] == SOURCE]})


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
    bulk_file.annotate = lambda *_a, **_k: None  # their summaries go to the log only (cap 10)
    bdot_load.bulk_bronze.annotate = lambda *_a, **_k: None
    failed = 0
    cat = catalog()
    try:
        try:
            trial(cat)
        except Exception as exc:
            failed = 1
            NOTES["left"] += 1
            message = re.sub(r"\s+", " ", str(exc))[:1500]
            note("bdot10k trial failed", {"error": f"{type(exc).__name__}: {message}"}, "error")
            raise
    finally:
        cleanup(cat)
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
