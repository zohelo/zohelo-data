"""Dev trial (PL-ENV-007, #2870): PRG address points, entity `punkty_adresowe` of gugik_prg, Landing → Gold.

Landing extract of the declared address package into the Landing prefix zz_dev_gugik_prg/ twice (the second must
skip the download: Last-Modified and ETag unchanged), inspect and verify; Bronze load into the Iceberg namespace
zz_dev_gugik_prg and the lake prefix zz_dev_gugik_prg/ twice (the second appends nothing); a profile of the Bronze
file; the Silver model and the Gold build stage rendered with Jinja from warehouse/models and run in local DuckDB,
each twice, with the dbt tests' checks. dim_geography, map_geography, prg_units, teryt_terc, source and licence
are read (read-only) from production. Everything written is removed in `finally`. At most 10 annotations.
"""

from __future__ import annotations

import csv
from pathlib import Path
import time

import duckdb
from jinja2 import Environment

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gugik_prg import addresses
from ingestion.to_landing.patterns import bulk_file
from warehouse import entity

SOURCE = "gugik_prg"
SCRATCH = "zz_dev_gugik_prg"


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def one(con, sql):
    return con.execute(sql).fetchone()[0]


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    return Environment().from_string(Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda n: refs.get(n, n), source=lambda _s, t: sources[t]
    )


def prod(con, cat, name: str, table_id: tuple[str, str], columns: tuple[str, ...]) -> None:
    con.register(f"{name}_a", cat.load_table(table_id).scan(selected_fields=columns).to_arrow())
    con.execute(f"create table {name} as select * from {name}_a")


def trial() -> None:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    spec = next(s for s in bulk_file.declaration(SOURCE) if s["entity"] == addresses.ENTITY)
    runs = []
    for _ in (1, 2):
        started = time.monotonic()
        result = bulk_file.load_file(client, bucket, SOURCE, spec, SCRATCH)
        runs.append({**result, "seconds": round(time.monotonic() - started)})
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"], SCRATCH))
    inspect = bulk_file.main(SOURCE, "inspect", landing_root=SCRATCH, specs=[spec])
    verify = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH, specs=[spec])
    newest = held[-1]
    annotate("notice", "prg addresses landing (scratch)", {
        "extract_two_runs": runs, "receipts": len(held), "size_bytes": newest["size_bytes"],
        "version": newest["version_token"], "version_basis": newest["version_basis"],
        "etag": newest["detail"]["http"]["etag"], "members": len(newest["detail"].get("members", [])),
        "inspect_exit": inspect, "verify_exit": verify})

    timings = []
    for _ in (1, 2):
        started = time.monotonic()
        exit_code = addresses.load(0, landing_root=SCRATCH, namespace=SCRATCH, lake_root=SCRATCH)
        timings.append([exit_code, round(time.monotonic() - started)])
    cat = catalog()
    table = cat.load_table((SCRATCH, addresses.TABLE))
    files = table.inspect.files()
    paths = files.column("file_path").to_pylist()
    try:
        rejected = cat.load_table((SCRATCH, addresses.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:
        rejected = []
    snapshots = len(table.metadata.snapshots)
    annotate("notice", "prg addresses bronze (scratch)", {
        "load_exit_and_seconds_two_runs": timings, "files": len(paths),
        "rows_metadata": sum(files.column("record_count").to_pylist()),
        "parquet_bytes": sum(files.column("file_size_in_bytes").to_pylist()), "snapshots": snapshots,
        "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3]})

    con = duckdb.connect()
    con.execute("SET memory_limit = '8GB'; SET preserve_insertion_order = false")
    entity._remote(con)
    con.execute(f"create view bronze_b as select * from read_parquet({paths!r})")
    digits = "regexp_full_match({c}, '[0-9]{{{n}}}')"
    annotate("notice", "prg addresses bronze profile", {
        "rows": one(con, "select count(*) from bronze_b"),
        "distinct_id_iip": one(con, "select count(distinct id_iip) from bronze_b"),
        "id_iip_repeated": q(con, "select count(*), sum(n) from (select id_iip, count(*) n from bronze_b group by 1 "
                                  "having count(*) > 1)"),
        "versions": q(con, "select source_version, raw_sha256, count(*) from bronze_b group by all"),
        "nulls": q(con, "select " + ", ".join(f"count(*) filter (where {c} is null or trim({c}) = '')" for c in (
            "id_iip", "numer_porz", "kod_poczt", "data_nad", "nazwa_ulc", "id_ulic", "nazwa_gmi", "teryt_gmi",
            "nazwa_msc", "id_simc", "x", "y")) + " from bronze_b"),
        "teryt_gmi": q(con, f"select count(distinct teryt_gmi), count(*) filter (where not "
                            f"{digits.format(c='teryt_gmi', n=7)}) from bronze_b"),
        "gmina_type_digit": q(con, "select substr(teryt_gmi, 7, 1), count(distinct teryt_gmi), count(*) from bronze_b "
                                   "group by 1 order by 1"),
        "id_simc_not_7_digits": q(con, f"select count(*), list(distinct id_simc)[1:5] from bronze_b where id_simc is "
                                       f"not null and not {digits.format(c='id_simc', n=7)}"),
        "id_ulic_not_5_digits": q(con, f"select count(*), list(distinct id_ulic)[1:5] from bronze_b where id_ulic is "
                                       f"not null and not {digits.format(c='id_ulic', n=5)}"),
        "numer_porz_non_numeric_sample": q(con, "select numer_porz, count(*) from bronze_b where not "
                                                "regexp_full_match(numer_porz, '[0-9]+') group by 1 order by 2 desc "
                                                "limit 6"),
        "numer_porz_non_numeric": one(con, "select count(*) from bronze_b where not "
                                           "regexp_full_match(numer_porz, '[0-9]+')"),
        "same_address_repeated": q(con, "select count(*), sum(n) from (select teryt_gmi, id_simc, id_ulic, numer_porz, "
                                        "count(*) n from bronze_b group by all having count(*) > 1)"),
        "xy": q(con, "select min(try_cast(x as double)), max(try_cast(x as double)), min(try_cast(y as double)), "
                     "max(try_cast(y as double)), count(*) filter (where try_cast(x as double) is null) from bronze_b"),
        "crs": q(con, "select left(crs_wkt, 80), count(*) from bronze_b group by 1"),
        "data_nad_range": q(con, "select min(data_nad), max(data_nad) from bronze_b")})

    silver_sql = render("warehouse/models/silver/gugik_prg/prg_address_gminas.sql", {},
                        {"prg_address_points": "bronze_b"})
    silver_rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table silver_t as {silver_sql}")
        silver_rows.append(one(con, "select count(*) from silver_t"))
    annotate("notice", "prg addresses silver prg_address_gminas", {
        "rows_two_builds": silver_rows,
        "unique_terc_code_dups": one(con, "select count(*) from (select terc_code from silver_t group by 1 "
                                          "having count(*) > 1)"),
        "not_null_fails": q(con, "select count(*) filter (where terc_code is null), count(*) filter (where "
                                 "gmina_type_code is null), count(*) filter (where address_points is null), "
                                 "count(*) filter (where source_version is null), count(*) filter (where raw_sha256 "
                                 "is null) from silver_t"),
        "accepted_gmina_type_fails": q(con, "select gmina_type_code, count(*) from silver_t where gmina_type_code "
                                            "not in ('1', '2', '3') or gmina_type_code is null group by 1"),
        "sum_points_vs_bronze": q(con, "select sum(address_points), (select count(*) from bronze_b) from silver_t"),
        "totals": q(con, "select sum(points_with_street), sum(points_without_locality), sum(points_without_number), "
                         "min(address_points), max(address_points), median(address_points) from silver_t"),
        "smallest": q(con, "select terc_code, address_points from silver_t order by address_points limit 4")})

    prod(con, cat, "geo", ("gold", "dim_geography"), ("geography_key", "level_name", "name_pl"))
    prod(con, cat, "map_prod", ("gold", "map_geography"), ("source_system", "source_code", "geography_key"))
    prod(con, cat, "teryt", ("silver", "teryt_terc"), ("terc_code", "unit_level", "name_pl"))
    # map_geography as this branch builds it for gugik_prg: production's PRG codes + TERYT gminas (the new lines)
    con.execute("""create table map_t as
        select 'gugik_prg' as source_system, m.source_code, coalesce(g.geography_key, '_X') as geography_key
        from (select source_code, geography_key from map_prod where source_system = 'gugik_prg'
              union all select terc_code, 'TERC:' || terc_code from teryt where unit_level = 'gmina') as m
        left join geo as g on m.geography_key = g.geography_key and g.geography_key <> '_X'
        qualify row_number() over (partition by m.source_code order by g.geography_key nulls last) = 1""")
    gold_sql = render("warehouse/models/gold/poc_build_prg_addresses.sql",
                      {"prg_address_gminas": "silver_t", "poc_map_geography": "map_t"}, {})
    gold_rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table gold_t as {gold_sql}")
        gold_rows.append(one(con, "select count(*) from gold_t"))
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
              "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
    annotate("notice", "prg addresses gold build_prg_addresses", {
        "rows_two_builds": gold_rows,
        "dup_series_x_date": one(con, f"select count(*) from (select {series}, date_key from gold_t group by all "
                                      "having count(*) > 1)"),
        "dates": q(con, "select date_key, frequency, count(*) from gold_t group by all"),
        "rows_geography_unmatched": q(con, "select count(*), sum(value) from gold_t where geography_key = '_X'"),
        "unmatched_codes": q(con, "select geography_source_code, value from gold_t where geography_key = '_X' "
                                  "order by 2 desc limit 15"),
        "matched_levels": q(con, "select g.level_name, count(*) from gold_t as b join geo as g using (geography_key) "
                                 "group by 1"),
        "teryt_gminas_without_points": q(con, "select count(*), list(terc_code)[1:8] from teryt where unit_level = "
                                              "'gmina' and right(terc_code, 1) in ('1', '2', '3') and terc_code not "
                                              "in (select terc_code from silver_t)"),
        "spot": q(con, "select b.geography_key, g.name_pl, b.value, b.period_label from gold_t as b left join geo as g "
                       "using (geography_key) where geography_source_code in ('1465011', '1261011', '0264011', "
                       "'2205011', '1434102') order by 1"),
        "poland_total": q(con, "select sum(value), count(*) from gold_t"),
        "values_null_or_zero": one(con, "select count(*) from gold_t where value is null or value <= 0")})

    seeds = Path("warehouse/seeds")
    source_row = [r for r in csv.DictReader((seeds / "source.csv").open()) if r["source_key"] == SOURCE]
    lic_keys = {r["licence_key"] for r in source_row}
    lic_row = [r for r in csv.DictReader((seeds / "licence.csv").open()) if r["licence_key"] in lic_keys]
    try:
        prod_source = cat.load_table(("gold", "source")).scan().to_arrow().to_pylist()
        prod_source = [r for r in prod_source if r.get("source_key") == SOURCE]
    except Exception as exc:
        prod_source = [f"{type(exc).__name__}: {str(exc)[:200]}"]
    annotate("notice", "prg addresses licence (catalogue rows)", {
        "source_csv": source_row, "licence_csv": lic_row, "prod_gold_source": prod_source})


def cleanup() -> None:
    removed: dict = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    cat = catalog()
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:
                cat.drop_table(table)
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:
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
    annotate("notice", "scratch removed", removed)


def main() -> int:
    bulk_file.annotate = addresses.annotate = lambda *_a, **_k: None  # their summaries go to the log only (cap 10)
    failed = 0
    try:
        try:
            trial()
        except Exception as exc:
            failed = 1
            annotate("error", "prg addresses trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
            raise
    finally:
        cleanup()
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
