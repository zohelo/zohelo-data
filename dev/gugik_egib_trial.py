"""Dev trial (PL-ENV-006, #3299): GUGiK KI EGiB, source gugik_egib, Landing → Gold on the final code.

Landing extract into the Landing prefix zz_dev_gugik_egib/ with a deadline of `minutes` (argument, default 75): the
whole building round (17 parts) and as many parcel parts as fit (a parcel request takes about 40 s); a second extract
with the deadline already passed (must write nothing: the building round is current, the open parcel round waits);
inspect and verify. Bronze load into the Iceberg namespace zz_dev_gugik_egib and the lake prefix zz_dev_gugik_egib/
twice (the second appends nothing); a profile of the Bronze rows and the reconciliation of the counts (national and
voivodeship hits against the sum of their powiats, zero counts); the Silver model, its completeness test and the Gold
build stage rendered with Jinja from warehouse/ and run in local DuckDB, each twice, with the checks of their dbt
tests, the grain check by series_key × date_key and spot values for Poland. dim_geography is read (read-only) from
production. Everything written is removed in `finally`. At most 10 annotations.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys
import time

import duckdb
from jinja2 import Environment

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gugik_egib import load as egib_load
from ingestion.to_landing import gugik_egib as landing
from ingestion.to_landing.patterns import bulk_file

SOURCE = "gugik_egib"
SCRATCH = "zz_dev_gugik_egib"
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


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    return Environment().from_string(Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda n: refs.get(n, n), source=lambda _s, t: sources[t]
    )


def trial(cat, minutes: float) -> None:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    landing.LANDING_ROOT = SCRATCH
    started = time.monotonic()
    status1, first = landing.extract(client, bucket, time.monotonic() + minutes * 60)
    seconds1 = round(time.monotonic() - started)
    status2, second = landing.extract(client, bucket, time.monotonic())
    held = landing.held(client, bucket)
    verify = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH, specs=landing.entities(held))
    from ingestion.common import report

    kept, report.annotate = report.annotate, lambda *_a, **_k: None  # inspect's summary to the log only
    try:
        inspect = landing._run("inspect", None)
    finally:
        report.annotate = kept
    summary = {
        "run1": {"exit": status1, "seconds": seconds1, **{t: {k: v for k, v in r.items() if k != "landed"}
                                                           | {"parts": len(r["landed"])} for t, r in first.items()}},
        "run2_deadline_passed": {"exit": status2, **{t: {k: v for k, v in r.items() if k != "landed"}
                                                      | {"parts": len(r["landed"])} for t, r in second.items()}},
        "inspect_exit": inspect, "verify_exit": verify, "files": len(held), "bytes": sum(r["size_bytes"] for r in held),
        "max_file_bytes": max(r["size_bytes"] for r in held),
        "parcel_parts_bytes": [[p["part"], p["bytes"]] for p in first["dzialki"]["landed"]][:5],
    }
    note("egib landing (scratch)", summary)

    timings = []
    for _ in (1, 2):
        t0 = time.monotonic()
        code = egib_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        table = cat.load_table((SCRATCH, "gugik_egib_hits"))
        files = table.inspect.files()
        timings.append({"exit": code, "seconds": round(time.monotonic() - t0), "files": files.num_rows,
                        "rows": sum(files.column("record_count").to_pylist()),
                        "snapshots": len(table.metadata.snapshots)})
    paths = files.column("file_path").to_pylist()
    try:
        rejected = cat.load_table((SCRATCH, "gugik_egib_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    note("egib bronze (scratch)", {
        "load_two_runs": timings, "parquet_bytes": sum(files.column("file_size_in_bytes").to_pylist()),
        "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3]})

    con = duckdb.connect()
    from warehouse import entity

    entity._remote(con)
    con.execute(f"create table bronze_b as select * from read_parquet({paths!r})")
    note("egib bronze profile", {
        "by_type_scope": q(con, "select type_name, property, scope, count(*), count(distinct code), "
                                "count(distinct entity), min(requested_at), max(requested_at) from bronze_b "
                                "group by all order by 1, 3"),
        "nulls": q(con, "select count(*) filter (where number_matched is null), count(*) filter (where "
                        "number_returned is null), count(*) filter (where time_stamp is null), count(*) filter "
                        "(where code is null) from bronze_b"),
        "non_numeric_matched": q(con, "select number_matched, count(*) from bronze_b where not "
                                      "regexp_full_match(number_matched, '[0-9]+') group by 1"),
        "number_returned": q(con, "select number_returned, count(*) from bronze_b group by 1"),
        "zero_counts": q(con, "select type_name, scope, code from bronze_b where number_matched = '0' order by 1, 3"),
        "duplicate_requests": one(con, "select count(*) from (select entity, scope, code from bronze_b group by all "
                                       "having count(*) > 1)")})

    con.execute("create table b as select type_name, scope, code, cast(number_matched as bigint) n from bronze_b")
    note("egib reconciliation (hits vs sum of powiats)", {
        "national_vs_powiat_sum": q(con, "select type_name, max(n) filter (where scope = 'PL') national, sum(n) "
                                         "filter (where scope = 'powiat') powiats, count(*) filter (where scope = "
                                         "'powiat') n_powiats, max(n) filter (where scope = 'PL') - sum(n) filter "
                                         "(where scope = 'powiat') outside from b group by 1"),
        "voivodeship_vs_powiat_sum": q(con, "select type_name, code, n, s, n - s from (select v.type_name, v.code, "
                                            "v.n, sum(p.n) s from b v join b p on p.type_name = v.type_name and "
                                            "p.scope = 'powiat' and left(p.code, 2) = v.code where v.scope = "
                                            "'voivodeship' group by v.type_name, v.code, v.n) order by 1, 2")})

    silver_sql = render("warehouse/models/silver/gugik_egib/gugik_egib_counts.sql", {}, {"gugik_egib_hits": "bronze_b"})
    rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table silver_c as {silver_sql}")
        rows.append(one(con, "select count(*) from silver_c"))
    tests = {
        "unique_grain": one(con, "select count(*) from (select snapshot_date, feature_type, scope, area_code from "
                                 "silver_c group by all having count(*) > 1)"),
        "not_null": q(con, "select " + ", ".join(f"count(*) filter (where {c} is null)" for c in (
            "snapshot_date", "feature_type", "scope", "area_code", "record_count", "raw_sha256")) + " from silver_c"),
        "accepted_feature_type_fails": q(con, "select feature_type, count(*) from silver_c where feature_type not in "
                                              "('dzialki', 'budynki') group by 1"),
        "accepted_scope_fails": q(con, "select scope, count(*) from silver_c where scope not in ('PL', "
                                       "'voivodeship', 'powiat') group by 1"),
        "complete": q(con, render("warehouse/tests/gugik_egib_counts_complete.sql",
                                  {"gugik_egib_counts": "silver_c"}, {})),
    }
    note("egib silver", {
        "rows_two_builds": rows, "tests": tests,
        "rounds": q(con, "select feature_type, snapshot_date, count(*), sum(record_count) filter (where scope = "
                         "'powiat') from silver_c group by all")})

    geo_cols = ("geography_key", "level_name", "name_pl", "terc_code")
    con.register("geo_a", cat.load_table(("gold", "dim_geography")).scan(selected_fields=geo_cols).to_arrow())
    con.execute("create table geo as select * from geo_a")
    macro = Path("warehouse/macros/sources/gugik_egib.sql").read_text()
    cte = Environment().from_string(macro + "{{ poc_geography__gugik_egib() }}").render()
    con.execute(f"create table map_t as with geo as (select * from geo), {cte} select * from gugik_egib")
    gold_sql = render("warehouse/models/gold/poc_build_gugik_egib.sql",
                      {"poc_map_geography": "map_t", "gugik_egib_counts": "silver_c"}, {})
    gold_rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table gold_t as {gold_sql}")
        gold_rows.append(one(con, "select count(*) from gold_t"))
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
              "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
    note("egib gold build_gugik_egib", {
        "rows_two_builds": gold_rows,
        "map_rows": q(con, "select count(*), count(distinct source_code) from map_t"),
        "dup_series_x_date": one(con, f"select count(*) from (select {series}, date_key from gold_t group by all "
                                      "having count(*) > 1)"),
        "by_measure": q(con, "select measure_key, date_key, frequency, observation_status_key, count(*), "
                             "count(distinct geography_key), min(value), max(value) from gold_t group by all order by 1"),
        "unmatched": q(con, "select geography_source_code, count(*) from gold_t where geography_key = '_X' group by 1"),
        "null_values": one(con, "select count(*) from gold_t where value is null"),
        "columns": [r[0] for r in con.execute("describe gold_t").fetchall()]})

    from warehouse import seed_rows

    source_row = [r for r in seed_rows.rows("source") if r["source_key"] == SOURCE]
    lic = {r["licence_key"] for r in source_row}
    note("egib gold spot values and licence (catalogue rows)", {
        "poland": q(con, "select measure_key, date_key, value, observation_status_key from gold_t where "
                         "geography_key = 'PL' order by 1"),
        "mazowieckie_warszawa": q(con, "select measure_key, geography_key, value from gold_t where "
                                       "geography_source_code in ('14', '1465') order by 1, 2"),
        "top_powiats": q(con, "select geography_source_code, value from gold_t where length(geography_source_code) "
                              "= 4 order by value desc limit 3"),
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


def main(args) -> int:
    minutes = float(args[0]) if args else 75.0
    bulk_file.annotate = lambda *_a, **_k: None  # their summaries go to the log only (cap 10)
    egib_load.bulk_bronze.annotate = lambda *_a, **_k: None
    failed = 0
    cat = catalog()
    try:
        try:
            trial(cat, minutes)
        except Exception as exc:
            failed = 1
            NOTES["left"] += 1
            message = re.sub(r"\s+", " ", str(exc))[:1500]
            note("egib trial failed", {"error": f"{type(exc).__name__}: {message}"}, "error")
            raise
    finally:
        cleanup(cat)
    return failed


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
