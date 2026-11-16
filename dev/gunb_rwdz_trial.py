"""Dev trial (PL-ENV-033 GUNB RWDZ, #2984): Landing → Bronze → Silver → Gold into scratch, on the final code.

Landing of the 16 voivodeship files into the Landing prefix zz_dev_gunb_rwdz/ (twice: the second must write
nothing), Bronze and Silver by the entity loader into the Iceberg namespace zz_dev_gunb_rwdz and lake prefix
zz_dev_gunb_rwdz/ (twice: the second must load nothing), bronze.verify, then the Gold build stage and its tests
rendered with Jinja from warehouse/ in local DuckDB (built twice), the geography map from the source's macro over
production gold.dim_geography (read-only). Everything written is removed in `finally`. At most 10 annotations.
No value of a dropped column is ever read; Bronze holds none.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
from jinja2 import Environment

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gunb_rwdz import entities
from ingestion.to_landing.patterns import bulk_file

SOURCE = "gunb_rwdz"
SCRATCH = "zz_dev_gunb_rwdz"


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def one(con, sql):
    return con.execute(sql).fetchall()[0][0]  # fetchall: DuckDB 2.0 keeps a half-read result open


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    return Environment().from_string(Path(path).read_text(encoding="utf-8")).render(
        config=lambda **_k: "", ref=lambda n: refs[n], source=lambda _s, t: sources[t]
    )


def geography_cte() -> str:
    text = Path("warehouse/macros/sources/gunb_rwdz.sql").read_text(encoding="utf-8")
    return Environment().from_string(text + "{{ poc_geography__gunb_rwdz() }}").render()


def trial() -> None:
    where = entities.Where(landing_root=SCRATCH, namespace=SCRATCH)
    bucket = req("STORAGE_LANDING_BUCKET_NAME")
    landing_exit = [bulk_file.main(SOURCE, "load", landing_root=SCRATCH, pause_seconds=3) for _ in (1, 2)]
    held = {}
    for spec in bulk_file.declaration(SOURCE):
        rec = bulk_file.landed(s3(), bucket, bulk_file.prefix(SOURCE, spec["entity"], SCRATCH))
        held[spec["entity"]] = [len(rec), rec[-1]["size_bytes"] if rec else 0, rec[-1]["version_token"] if rec else ""]
    verify_exit = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH)
    copies = sorted({v[0] for v in held.values()})
    annotate("notice", "gunb_rwdz landing (scratch)", {
        "exit_two_runs": landing_exit, "verify_exit": verify_exit, "files": len(held), "copies_per_file": copies,
        "bytes": sum(v[1] for v in held.values()), "versions": sorted({v[2] for v in held.values()})})

    loads = []
    for _ in (1, 2):
        code = entities.main(["--minutes", "150"], where=where)
        loads.append(code)
    bronze_verify = entities.verify(where=where)
    cat = catalog()
    bronze_t = cat.load_table(where.bronze)
    silver_t = cat.load_table(where.silver)
    b_files = len(list(bronze_t.scan().plan_files()))
    s_files = len(list(silver_t.scan().plan_files()))
    try:
        rejected = cat.load_table(where.rejected).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    snaps = [len(list(bronze_t.snapshots())), len(list(silver_t.snapshots()))]
    con = duckdb.connect()
    con.execute("SET memory_limit = '10GB'")
    con.register("bronze_a", bronze_t.scan(selected_fields=("entity", "numer_gunb", "terc", "data_wydania_decyzji",
                                                             "raw_sha256")).to_arrow())
    con.execute("CREATE TABLE b AS SELECT * FROM bronze_a")
    con.unregister("bronze_a")
    con.register("silver_a", silver_t.scan().to_arrow())
    con.execute("CREATE TABLE silver AS SELECT * FROM silver_a")
    con.unregister("silver_a")
    annotate("notice", "gunb_rwdz bronze (scratch)", {
        "loader_exit_two_runs": loads, "bronze_verify_exit": bronze_verify, "snapshots_bronze_silver": snaps,
        "bronze_files": b_files, "silver_files": s_files, "bronze_rows": one(con, "SELECT count(*) FROM b"),
        "rows_per_entity": q(con, "SELECT entity, count(*) FROM b GROUP BY 1 ORDER BY 1"),
        "null_numer_gunb": one(con, "SELECT count(*) FROM b WHERE numer_gunb IS NULL"),
        "null_terc_lines": one(con, "SELECT count(*) FROM b WHERE terc IS NULL"),
        "rejected": [{k: str(r.get(k))[:200] for k in ("entity", "error")} for r in rejected][:3]})

    annotate("notice", "gunb_rwdz silver (scratch)", {
        "rows": one(con, "SELECT count(*) FROM silver"),
        "columns": [r[0] for r in con.execute("DESCRIBE silver").fetchall()],
        "dup_key_in_entity": one(con, "SELECT count(*) FROM (SELECT entity, record_id FROM silver GROUP BY ALL "
                                      "HAVING count(*) > 1)"),
        "record_id_in_several_files": one(con, "SELECT count(*) FROM (SELECT record_id FROM silver GROUP BY 1 "
                                               "HAVING count(DISTINCT entity) > 1)"),
        "null_keys": q(con, "SELECT count(*) FILTER (WHERE entity IS NULL), count(*) FILTER (WHERE record_id IS NULL),"
                            " count(*) FILTER (WHERE voivodeship_code IS NULL), count(*) FILTER (WHERE raw_sha256 IS "
                            "NULL), count(*) FILTER (WHERE contract_problem IS NOT NULL) FROM silver"),
        "lines_vs_bronze": q(con, "SELECT (SELECT sum(source_lines) FROM silver), (SELECT count(*) FROM b)"),
        "conflicting": q(con, "SELECT conflicting_columns, count(*) FROM silver WHERE conflicting_columns IS NOT NULL "
                              "GROUP BY 1 ORDER BY 2 DESC LIMIT 6"),
        "null_terc_records": one(con, "SELECT count(*) FROM silver WHERE terc_code IS NULL"),
        "terc_other_voivodeship": one(con, "SELECT count(*) FROM silver WHERE left(terc_code, 2) <> voivodeship_code"),
        "decision_range": q(con, "SELECT min(decision_date), max(decision_date), count(*) FILTER (WHERE decision_date "
                                 "IS NULL) FROM silver"),
        "application_range": q(con, "SELECT min(application_date), max(application_date), count(*) FILTER (WHERE "
                                    "application_date IS NULL) FROM silver"),
        "decision_before_application": one(con, "SELECT count(*) FROM silver WHERE decision_date < application_date"),
        "kinds": q(con, "SELECT investment_kind, count(*) FROM silver GROUP BY 1 ORDER BY 2 DESC"),
        "categories": one(con, "SELECT count(DISTINCT object_category) FROM silver"),
        "records_from_2020": one(con, "SELECT count(*) FROM silver WHERE decision_date >= DATE '2020-01-01'")})

    geo = cat.load_table(("gold", "dim_geography")).scan(
        selected_fields=("geography_key", "terc_code", "level_name")).to_arrow()
    con.register("geo_a", geo)
    con.execute("CREATE TABLE dim_geo AS SELECT * FROM geo_a")
    con.execute(f"CREATE TABLE geo_map AS WITH geo AS (SELECT * FROM dim_geo), {geography_cte()} "
                "SELECT * FROM gunb_rwdz")
    build = render("warehouse/models/gold/poc_build_gunb_rwdz.sql", {"poc_map_geography": "geo_map"},
                   {"gunb_rwdz_records": "silver"})
    rows = []
    for _ in (1, 2):
        con.execute(f"CREATE OR REPLACE TABLE gold AS {build}")
        rows.append(one(con, "SELECT count(*) FROM gold"))
    series = ("measure_key || '|' || geography_key || CASE WHEN geography_key = '_X' THEN ':' || "
              "coalesce(geography_source_code, '') ELSE '' END || '|' || breakdown_json")
    tests = {}
    for name in ("gunb_rwdz_areas", "gunb_rwdz_value_range"):
        sql = render(f"warehouse/tests/{name}.sql", {"poc_build_gunb_rwdz": "gold"}, {})
        tests[name] = one(con, f"SELECT count(*) FROM ({sql})")
    annotate("notice", "gunb_rwdz gold build_gunb_rwdz (local DuckDB)", {
        "rows_two_builds": rows, "tests_failing_rows": tests,
        "dup_series_date": one(con, f"SELECT count(*) FROM (SELECT {series} AS s, date_key FROM gold GROUP BY ALL "
                                    "HAVING count(*) > 1)"),
        "series": one(con, f"SELECT count(DISTINCT {series}) FROM gold"),
        "unmatched": q(con, "SELECT geography_source_code, count(*) FROM gold WHERE geography_key = '_X' GROUP BY 1 "
                            "ORDER BY 2 DESC LIMIT 8"),
        "areas_per_level": q(con, "SELECT length(geography_source_code), count(DISTINCT geography_key) FROM gold "
                                  "GROUP BY 1 ORDER BY 1"),
        "date_range": q(con, "SELECT min(date_key), max(date_key), count(DISTINCT date_key) FROM gold"),
        "value_range": q(con, "SELECT min(value), max(value), sum(value) FILTER (WHERE geography_key = 'PL' AND "
                              "is_total) FROM gold"),
        "nulls": q(con, "SELECT count(*) FILTER (WHERE value IS NULL), count(*) FILTER (WHERE date_key IS NULL), "
                        "count(*) FILTER (WHERE raw_sha256 IS NULL) FROM gold"),
        "voiv_sum_vs_pl": q(con, "SELECT count(*) FROM (SELECT date_key, breakdown_json, sum(value) FILTER (WHERE "
                                 "length(geography_source_code) = 2) v, sum(value) FILTER (WHERE geography_key = 'PL') p"
                                 " FROM gold GROUP BY ALL) WHERE v <> p"),
        "kinds_sum_vs_total": q(con, "SELECT count(*) FROM (SELECT geography_key, date_key, sum(value) FILTER (WHERE "
                                     "NOT is_total) k, sum(value) FILTER (WHERE is_total) t FROM gold GROUP BY ALL) "
                                     "WHERE k <> t")})
    annotate("notice", "gunb_rwdz gold spot values (decisions per month)", {
        "PL_total": q(con, "SELECT period_label, value FROM gold WHERE geography_key = 'PL' AND is_total AND date_key IN "
                           "('2020-01-01', '2023-06-01', '2025-01-01', '2025-12-01') OR (geography_key = 'PL' AND "
                           "is_total AND date_key = (SELECT max(date_key) FROM gold)) ORDER BY 1"),
        "PL_2025_by_kind": q(con, "SELECT breakdown_json, sum(value) FROM gold WHERE geography_key = 'PL' AND "
                                  "year(date_key) = 2025 GROUP BY 1 ORDER BY 1"),
        "PL_by_year": q(con, "SELECT year(date_key), sum(value) FROM gold WHERE geography_key = 'PL' AND is_total "
                             "GROUP BY 1 ORDER BY 1"),
        "mazowieckie_2025_01": q(con, "SELECT breakdown_json, value FROM gold WHERE geography_key = 'TERC:14' AND "
                                      "date_key = '2025-01-01' ORDER BY 1"),
        "warszawa_1465_2025_01": q(con, "SELECT breakdown_json, value FROM gold WHERE geography_key = 'TERC:1465' AND "
                                        "date_key = '2025-01-01' ORDER BY 1"),
        "silver_check_PL_2025_01": one(con, "SELECT count(*) FROM silver WHERE decision_date >= DATE '2025-01-01' AND "
                                            "decision_date < DATE '2025-02-01'")})


def cleanup() -> None:
    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
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
    annotate("notice", "gunb_rwdz scratch removed", removed)


def main() -> int:
    printed = lambda level, title, data: print(f"[{level}] {title}: {json.dumps(data, default=str)[:3000]}", flush=True)  # noqa: E731
    bulk_file.annotate = printed
    entities.annotate = printed  # the loader's own summaries go to the log only (cap 10 annotations)
    failed = 0
    try:
        try:
            trial()
        except Exception as exc:  # noqa: BLE001
            failed = 1
            annotate("error", "gunb_rwdz trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
            raise
    finally:
        cleanup()
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
