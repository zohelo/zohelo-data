"""Dev trial (CD-014 Companies House, #1502): Landing → Bronze → Silver → Gold into scratch.

Landing load of the one-file ZIP into the Landing prefix zz_dev_companies_house/ (twice: the second downloads the
file again and must write nothing), the entity loader into Iceberg namespace zz_dev_companies_house and lake
prefix zz_dev_companies_house/ (twice: the second must find everything current), verify, a profile of Bronze and
Silver, the two Gold models rendered with Jinja from warehouse/models in DuckDB (built twice) with the dbt tests'
checks, and a synthetic second snapshot (the real counts file copied under the next month's date) to show that
the counts table keeps one file per snapshot and the build stage one date per month. Only aggregates are
reported, plus names of companies whose country of origin is Poland. Everything written is removed in `finally`.
At most 10 compact annotations.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
import tempfile

from jinja2 import Environment

from ingestion.common import entity_tables as et
from ingestion.common import report
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze.companies_house import entities
from ingestion.to_landing.patterns import bulk_file
from warehouse import entity

SCRATCH = "zz_dev_companies_house"
SOURCE, NAME = entities.SOURCE, entities.NAME
annotate = report.annotate
captured: list = []


def collect(level, title, data):
    captured.append({"level": level, "title": title, "data": data})


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def render(path: str) -> str:
    return Environment().from_string(Path(path).read_text()).render(config=lambda **_k: "", source=lambda _s, t: t)


def tables(cat) -> dict:
    out = {}
    for name in (entities.BRONZE, entities.POLAND, entities.COUNTS):
        st = et.state(cat.load_table((SCRATCH, name)), "entity")
        out[name] = {e: [len(s["paths"]), s["rows"], s["bytes"], sorted(r[:12] + r[64:] for r in s["recipes"])] for e, s in st.items()}
    return out


def objects(client, bucket_var: str) -> list:
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=req(bucket_var), Prefix=f"{SCRATCH}/"):
        keys += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    return keys


def trial() -> None:  # noqa: PLR0915
    client, cat = s3(), catalog()
    lake_root = f"{SCRATCH}/"
    # Landing
    landing_runs = []
    for mode in ("inspect", "load", "load", "inspect", "verify"):
        captured.clear()
        code = bulk_file.main(SOURCE, mode, landing_root=SCRATCH)
        files = [c["data"]["files"][0] for c in captured if "files" in c["data"]]
        held = objects(client, "STORAGE_LANDING_BUCKET_NAME")
        landing_runs.append({"mode": mode, "exit": code, "file": files[0] if files else None, "objects": len(held),
                             "bytes": sum(s for _, s in held)})
    receipts = bulk_file.landed(client, req("STORAGE_LANDING_BUCKET_NAME"), bulk_file.prefix(SOURCE, NAME, SCRATCH))
    annotate("notice", "companies_house trial: landing (scratch)", {
        "runs": landing_runs, "receipts": len(receipts),
        "receipt": {k: receipts[-1].get(k) for k in ("version_token", "version_basis", "source_url", "sha256", "size_bytes")},
        "members": receipts[-1]["detail"].get("members")})
    # Bronze and Silver, twice, then verify
    loads = []
    for _ in (1, 2):
        captured.clear()
        code = entities.main(landing_root=SCRATCH, namespace=SCRATCH, lake_root=lake_root)
        lake = objects(client, "STORAGE_LAKEHOUSE_BUCKET_NAME")
        loads.append({"exit": code, "out": [c["data"] for c in captured if c["title"] == "companies_house entities"],
                      "other": [c["title"] for c in captured if c["title"] != "companies_house entities"],
                      "lake_parquet": len([k for k, _ in lake if k.endswith(".parquet") and "/data/" not in k]),
                      "tables": tables(cat)})
    captured.clear()
    verify = entities.verify(landing_root=SCRATCH, namespace=SCRATCH)
    try:
        rejected = cat.load_table((SCRATCH, entities.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    annotate("notice", "companies_house trial: bronze+silver loads (scratch)", {
        "first": loads[0], "second": loads[1], "verify_exit": verify, "verify": [c["data"] for c in captured],
        "rejected_files": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3]})

    con = entity._connect("8GB", None)
    entity._remote(con)
    for name, view in ((entities.BRONZE, "b"), (entities.POLAND, entities.POLAND), (entities.COUNTS, entities.COUNTS)):
        paths = [p for s in et.state(cat.load_table((SCRATCH, name)), "entity").values() for p in s["paths"]]
        files = ", ".join(f"'{p}'" for p in paths)
        con.execute(f"create table {view} as select * from read_parquet([{files}])")
    annotate("notice", "companies_house trial: bronze profile", {
        "rows_numbers_nullnumbers_snapshots": q(con, "select count(*), count(distinct company_number), count(*) filter (where coalesce(company_number,'')=''), list(distinct snapshot_date) from b"),
        "columns": [r[0] for r in con.execute("describe b").fetchall()],
        "nulls": q(con, "select count(*) filter (where company_name is null), count(*) filter (where company_category is null), count(*) filter (where company_status is null), count(*) filter (where country_of_origin is null), count(*) filter (where trim(country_of_origin) = ''), count(*) filter (where incorporation_date is null), count(*) filter (where dissolution_date is not null), count(*) filter (where sic_text_1 = 'None Supplied'), count(*) filter (where post_town is null), count(*) filter (where address_country is null) from b"),
        "nulls_legend": "name, category, status, origin null, origin blank, incorporation null, dissolution NOT null, SIC None Supplied, post town null, address country null",
        "incorporation_unparsed": q(con, "select count(*) from b where incorporation_date is not null and try_strptime(incorporation_date, '%d/%m/%Y') is null"),
        "incorporation_min_max": q(con, "select min(try_strptime(incorporation_date, '%d/%m/%Y'))::date, max(try_strptime(incorporation_date, '%d/%m/%Y'))::date from b"),
        "origin_distinct": q(con, "select count(distinct country_of_origin) from b"),
        "origin_like_pol": q(con, "select country_of_origin, count(*) from b where country_of_origin ilike '%POL%' group by 1"),
        "origin_top": q(con, "select country_of_origin, count(*) from b group by 1 order by 2 desc limit 6"),
        "status": q(con, "select company_status, count(*) from b group by 1 order by 2 desc"),
        "categories": q(con, "select count(distinct company_category) from b"),
        "provenance": q(con, "select count(distinct raw_sha256), count(distinct source_version), count(*) filter (where raw_sha256 is null) from b")})
    counts, poland = entities.COUNTS, entities.POLAND
    annotate("notice", "companies_house trial: silver snapshot_counts", {
        "rows": q(con, f"select count(*), count(distinct entity), min(snapshot_date), max(snapshot_date), min(reference_month) from {counts}"),
        "sum_companies_vs_bronze": q(con, f"select (select sum(companies) from {counts}), (select count(*) from b)"),
        "dup_grain": q(con, f"select count(*) from (select snapshot_date, country_of_origin, company_category, company_status from {counts} group by all having count(*) > 1)"),
        "null_keys": q(con, f"select count(*) filter (where snapshot_date is null), count(*) filter (where company_category is null), count(*) filter (where company_status is null), count(*) filter (where country_of_origin is null), count(*) filter (where raw_sha256 is null), count(*) filter (where companies is null or companies <= 0) from {counts}"),
        "null_keys_legend": "snapshot_date, category, status, origin (blank in the file: allowed, one group per category x status), raw_sha256, companies null or <= 0",
        "blank_origin_companies": q(con, f"select sum(companies) from {counts} where country_of_origin is null"),
        "incorporated_in_reference_month": q(con, f"select sum(incorporated_in_reference_month), sum(incorporation_date_missing) from {counts}"),
        "incorporated_check_from_bronze": q(con, "select count(*) from b where strftime(try_strptime(incorporation_date, '%d/%m/%Y'), '%Y-%m') = '2026-10'"),
        "poland": q(con, f"select company_category, company_status, companies, incorporated_in_reference_month from {counts} where country_of_origin = 'POLAND' order by 1, 2")})
    annotate("notice", "companies_house trial: silver poland_origin", {
        "rows_numbers": q(con, f"select count(*), count(distinct company_number) from {poland}"),
        "null_keys": q(con, f"select count(*) filter (where company_number is null), count(*) filter (where company_name is null), count(*) filter (where snapshot_date is null), count(*) filter (where raw_sha256 is null), count(*) filter (where incorporation_date is null), count(*) filter (where post_town is null), count(*) filter (where address_country is null) from {poland}"),
        "null_legend": "number, name, snapshot_date, raw_sha256, incorporation_date, post_town, address_country",
        "category_status": q(con, f"select company_category, company_status, count(*) from {poland} group by all order by 1"),
        "number_prefix": q(con, f"select left(company_number, 2), count(*) from {poland} group by 1 order by 2 desc"),
        "address_country": q(con, f"select address_country, count(*) from {poland} group by 1 order by 2 desc"),
        "incorporation_range": q(con, f"select min(incorporation_date), max(incorporation_date) from {poland}"),
        "sic": q(con, f"select sic_text_1, count(*) from {poland} group by 1"),
        "well_known_orlen_pko_kghm_cdprojekt_pzu_allegro": q(con, f"select company_number, company_name from {poland} where regexp_matches(upper(company_name), 'ORLEN|PKO|KGHM|CD PROJEKT|PZU|ALLEGRO')"),
        "spot": q(con, f"select company_number, company_name, company_category, company_status, incorporation_date, post_town, address_country from {poland} where regexp_matches(upper(company_name), 'LOTNICZE|POLIMEX|CITY INTERACTIVE|SOFTWARE MIND|INVESTMENT AND TRADE') order by 1")})

    # Gold: both models, built twice
    build_sql = render("warehouse/models/gold/poc_build_companies_house.sql")
    entity_sql = render("warehouse/models/gold/company_uk_poland_origin.sql")
    rows_builds = []
    for _ in (1, 2):
        con.execute(f"create or replace table build as {build_sql}")
        con.execute(f"create or replace table gold_entity as {entity_sql}")
        rows_builds.append(q(con, "select (select count(*) from build), (select count(*) from gold_entity)")[0])
    series = "measure_key || '|' || geography_key || '|' || breakdown_json"
    grain = f"select count(*) from (select {series} as series_key, date_key from build group by all having count(*) > 1)"
    statuses = [v for c in __import__("yaml").safe_load(Path("warehouse/models/gold/_companies_house.yml").read_text())["models"][1]["columns"]
                if c["name"] == "company_status" for v in c["tests"][0]["accepted_values"]["arguments"]["values"]]
    con.execute("create table accepted_status as select unnest(?) as v", [statuses])
    try:
        geo = cat.load_table(("gold", "dim_geography")).scan(row_filter="geography_key in ('GB', 'PL')").to_arrow().to_pylist()
        geo = [[g.get("geography_key"), g.get("name_en"), g.get("level_name")] for g in geo]
    except Exception as exc:  # noqa: BLE001
        geo = f"{type(exc).__name__}: {str(exc)[:200]}"
    seeds = {}
    for seed, key in (("source", "source_key"), ("licence", "licence_key"), ("unit_map", "source_system")):
        with Path(f"warehouse/seeds/{seed}.csv").open(encoding="utf-8") as handle:
            seeds[seed] = [r for r in csv.DictReader(handle) if r[key].startswith("companies_house")]
    annotate("notice", "companies_house trial: gold build_companies_house", {
        "rows_two_builds_build_entity": rows_builds,
        "dup_series_key_x_date_key": q(con, grain),
        "measures": q(con, "select measure_key, frequency, unit_code, geography_key, min(date_key), max(date_key), count(*), count(distinct breakdown_json) from build group by all order by 1"),
        "nulls": q(con, "select count(*) filter (where value is null), count(*) filter (where date_key is null), count(*) filter (where value <= 0), count(*) filter (where raw_sha256 is null) from build"),
        "totals": q(con, "select measure_codes, period_label, breakdown_json, value from build where is_total order by 1"),
        "poland": q(con, "select breakdown_json, value from build where breakdown_json like '%POLAND%' order by 1"),
        "by_status_total_origin": q(con, "select json_extract_string(breakdown_json, '$.company_status'), value from build where breakdown_json like '%\"company_category\":\"_T\"%' and breakdown_json like '%\"country_of_origin\":\"_T\"%' and measure_codes = 'on_register' order by 2 desc limit 6"),
        "status_sum_equals_total": q(con, "select sum(value) filter (where not is_total), max(value) filter (where is_total) from build where measure_codes = 'on_register' and breakdown_json like '%\"company_category\":\"_T\"%' and breakdown_json like '%\"country_of_origin\":\"_T\"%'"),
        "dim_geography": geo, "seed_rows": seeds})
    annotate("notice", "companies_house trial: gold company_uk_poland_origin", {
        "rows_keys": q(con, "select count(*), count(distinct company_key), count(distinct company_number) from gold_entity"),
        "null_keys": q(con, "select count(*) filter (where company_key is null), count(*) filter (where company_name is null), count(*) filter (where snapshot_date is null), count(*) filter (where raw_sha256 is null) from gold_entity"),
        "accepted_values_violations": q(con, "select count(*) filter (where country_of_origin <> 'POLAND'), count(*) filter (where company_status not in (select v from accepted_status)), count(*) filter (where company_category not in ('Other company type', 'Overseas Entity')) from gold_entity"),
        "whole_register_status_not_in_list": q(con, "select company_status, count(*) from b where company_status not in (select v from accepted_status) group by 1"),
        "sic_codes_non_null": q(con, "select count(sic_code_1), count(sic_code_2) from gold_entity"),
        "sic_parse_check_whole_register": q(con, "select count(*) filter (where regexp_matches(sic_text_1, '^[0-9]{4,5} - ')), count(*) filter (where sic_text_1 = 'None Supplied'), count(*) from b"),
        "origin_geography_key": q(con, "select origin_geography_key, count(*) from gold_entity group by 1"),
        "columns": [r[0] for r in con.execute("describe gold_entity").fetchall()],
        "spot": q(con, "select company_key, company_name, company_category, incorporation_date, post_town from gold_entity where regexp_matches(upper(company_name), 'LOTNICZE|POLIMEX|CITY INTERACTIVE') order by 1")})
    try:  # how a join to GLEIF would work: GLEIF's registration authority RA000585 (Companies House) and its entity id
        from pyiceberg.expressions import EqualTo

        g = cat.load_table(("silver", "gleif_legal_entities")).scan(
            row_filter=EqualTo("registration_authority_id", "RA000585"),
            selected_fields=("lei", "registration_authority_entity_id", "legal_jurisdiction")).to_arrow()
        con.register("gleif_uk", g)
        gleif = {"gleif_entities_RA000585": g.num_rows,
                 "poland_origin_numbers_with_lei": q(con, "select count(*) from gold_entity as e join gleif_uk as g on g.registration_authority_entity_id = e.company_number"),
                 "jurisdictions_of_matches": q(con, "select g.legal_jurisdiction, count(*) from gold_entity as e join gleif_uk as g on g.registration_authority_entity_id = e.company_number group by 1")}
    except Exception as exc:  # noqa: BLE001
        gleif = {"error": f"{type(exc).__name__}: {str(exc)[:300]}"}

    # Synthetic second snapshot: the real counts file under the next month's date, as its own entity
    synthetic: dict = {}
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "synthetic.parquet"
        con.execute(
            f"copy (select '2026-12-01' as entity, date '2026-12-01' as snapshot_date, date '2026-11-01' as reference_month, "
            f"* exclude (entity, snapshot_date, reference_month) from {counts}) to '{path}' (format parquet, compression zstd)")
        key = f"{SCRATCH}/03_silver/{SOURCE}/{counts}/{'0' * 64}.synthetic.parquet"
        from ingestion.common import bronze_files as bf

        uri = bf.upload(client, req("STORAGE_LAKEHOUSE_BUCKET_NAME"), key, path, "0" * 64)
        captured.clear()
        synthetic["replace"] = et.replace(cat, (SCRATCH, counts), "entity", {"2026-12-01": [uri]}, ["entity", "raw_sha256"], "trial-synthetic")
    st = et.state(cat.load_table((SCRATCH, counts)), "entity")
    synthetic["counts_entities"] = {e: [len(s["paths"]), s["rows"]] for e, s in st.items()}
    captured.clear()
    synthetic["third_load_exit"] = entities.main(landing_root=SCRATCH, namespace=SCRATCH, lake_root=lake_root)
    synthetic["third_load"] = [{k: c["data"].get(k) for k in ("current", "loaded", "silver")} for c in captured if c["title"] == "companies_house entities"]
    synthetic["verify_exit"] = entities.verify(landing_root=SCRATCH, namespace=SCRATCH)
    st = et.state(cat.load_table((SCRATCH, counts)), "entity")
    synthetic["counts_entities_after"] = {e: [len(s["paths"]), s["rows"]] for e, s in st.items()}
    files = ", ".join(f"'{p}'" for s in st.values() for p in s["paths"])
    con.execute(f"create or replace table {counts} as select * from read_parquet([{files}])")
    con.execute(f"create or replace table build as {build_sql}")
    synthetic["build_rows"] = q(con, "select count(*), count(distinct date_key), min(date_key), max(date_key) from build")
    synthetic["dup_series_key_x_date_key"] = q(con, grain)
    synthetic["dates_per_series"] = q(con, f"select n, count(*) from (select {series} as s, count(distinct date_key) as n from build group by 1) group by 1")
    annotate("notice", "companies_house trial: synthetic second snapshot and GLEIF", {"synthetic": synthetic, "gleif": gleif})


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
    for var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"), ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
        bucket = req(var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = len(objects(client, "STORAGE_LANDING_BUCKET_NAME")) + len(objects(client, "STORAGE_LAKEHOUSE_BUCKET_NAME"))
    annotate("notice", "companies_house trial: scratch removed", removed)


def main() -> int:
    # summaries of the shared code go to `captured` and the log, not to annotations (cap 10)
    bulk_file.annotate = entities.annotate = report.annotate = collect
    failed = 0
    try:
        try:
            trial()
        except Exception as exc:  # noqa: BLE001
            import traceback

            traceback.print_exc()
            failed = 1
            annotate("error", "companies_house trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "captured": captured[-3:]})
    finally:
        cleanup()
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
