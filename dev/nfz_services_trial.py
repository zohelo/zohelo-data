"""Dev trial of the NFZ statistics API, services (register PLS-012, #2929): Landing → Bronze → Silver → Gold into
scratch. Landing prefix and lake prefix zz_dev_nfz_services/, Iceberg namespace zz_dev_nfz_services; everything
written is removed in `finally`. Landing and Bronze each run twice (the second run must write nothing). Silver and
the Gold build stage are rendered with Jinja from warehouse/ and run in local DuckDB over the scratch Bronze tables,
each built twice, with the schema tests of the YAML files and the source's singular tests. Read-only reads of
production: the row count of gold.fact_annual (snapshot summary) and silver.teryt_terc names. Deleted before merge.

Subset (declared): catalogs 1d and 1w whole, and 29 benefits of 1a, 1b and 1c (SUBSET) including the four KPI groups.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import duckdb
from jinja2 import Environment
import yaml

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.nfz_services import load as nfz_load
from ingestion.to_landing.nfz_services import extract

SCRATCH = "zz_dev_nfz_services"
SOURCE = "nfz_services"
CAPTURED: list = []
A1 = ["0013001", "0008013", "0008015", "0001048", "0013002", "0001094", "0001049", "0001050", "0001051", "0002101",
      "0002019", "0002102", "0004018", "0005023", "0005053", "0005054", "0005055", "0006058", "0008004", "0008005",
      "0009002", "0011064", "0001001"]
SUBSET = ["1d", "1w", *[f"1a_5.51.01.{c}" for c in A1], "1b_5.52.01.0001492", "1b_5.52.01.0001497",
          "1b_5.52.01.0001398", "1c_5.53.01.0001465", "1c_5.53.01.0005004", "1c_5.53.01.0003060"]
FILTER = ",".join(SUBSET)
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
NFZ_NAMES = {"01": "dolnośląski", "02": "kujawsko-pomorski", "03": "lubelski", "04": "lubuski", "05": "łódzki",
             "06": "małopolski", "07": "mazowiecki", "08": "opolski", "09": "podkarpacki", "10": "podlaski",
             "11": "pomorski", "12": "śląski", "13": "świętokrzyski", "14": "warmińsko-mazurski", "15": "wielkopolski",
             "16": "zachodniopomorski"}
VALUE_COLUMNS = [c for _, c in nfz_load.VALUES]


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:3000]}", flush=True)


extract.annotate = _capture
bulk_bronze.annotate = _capture


def note(title: str, data) -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, json.loads(text) if len(text) <= 3900 else {"truncated": text[:3800]})


def one(con, q):
    return con.execute(q).fetchone()[0]


def rows(con, q, limit=60):
    return [[str(v) for v in r] for r in con.execute(q).fetchall()][:limit]


def landing_objects() -> tuple[int, int, int]:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    files = size = receipts = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".receipt.json"):
                receipts += 1
            else:
                files += 1
                size += o["Size"]
    return files, size, receipts


def landing() -> dict:
    out: dict = {"filter_entities": len(SUBSET) - 2, "catalogs_whole": ["1d", "1w"]}
    runs = []
    for n in (1, 2):
        CAPTURED.clear()
        start = time.monotonic()
        code = extract.main(["--mode", "load", "--filter", FILTER, "--budget-minutes", "70", "--landing-root", SCRATCH])
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        keep = ("benefits", "missing", "held", "canary", "share_this_week", "reread_this_week", "landed_new",
                "landed_changed", "unchanged", "checked_current", "benefit_requests", "dictionary_landed_new",
                "dictionary_unchanged", "not_started_time_budget", "failed", "failed_sample", "release_detected")
        runs.append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                     **{k: summary.get(k) for k in keep if k in summary},
                     "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    out["runs"] = runs
    CAPTURED.clear()
    out["inspect_exit"] = extract.main(["--mode", "inspect", "--filter", FILTER, "--landing-root", SCRATCH])
    out["inspect"] = {k: v for k, v in next((d for t, d in CAPTURED if "inspect" in t), {}).items()
                      if k in ("missing", "held", "canary", "checked_current", "stale", "failed")}
    CAPTURED.clear()
    out["verify_exit"] = extract.main(["--mode", "verify", "--landing-root", SCRATCH])
    out["verify"] = next((d for t, d in CAPTURED if "verify" in t), {})
    return out


def bronze() -> tuple[dict, dict]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = nfz_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, "tables": [
            {k: d.get(k) for k in ("table", "entities_landed", "candidates", "appended_files", "rows_added", "rejected")}
            for t, d in CAPTURED]})
    cat = catalog()
    arrows = {}
    for table in (nfz_load.BENEFITS, nfz_load.BASIC):
        t = cat.load_table((SCRATCH, table))
        arrows[table] = t.scan().to_arrow()
        out[f"{table}_files"] = len(list(t.scan().plan_files()))
        out[f"{table}_rows"] = arrows[table].num_rows
    try:
        rej = cat.load_table((SCRATCH, nfz_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrows


def bronze_profile(con) -> dict:
    non_numeric = " union all ".join(
        f"select '{c}' as col, {c} as v from bronze_basic where {c} is not null and try_cast({c} as double) is null"
        for c in VALUE_COLUMNS)
    nulls = ", ".join(f"count(*) filter (where {c} is null)" for c in VALUE_COLUMNS)
    return {
        "rows_by_record": rows(con, "select record, count(*) from bronze_basic group by 1 order by 1"),
        "entities": one(con, "select count(distinct entity) from bronze_basic"),
        "tables": one(con, "select count(distinct entity || table_id) from bronze_basic where table_id is not null"),
        "tables_part_year": one(con, "select count(distinct entity || table_id) from bronze_basic where period_from is not null"),
        "year_min_max": rows(con, "select min(cast(year as int)), max(cast(year as int)) from bronze_basic"),
        "dup_keys": one(con, "select count(*) from (select entity, table_id, scope, branch from bronze_basic "
                             "where record = 'value' group by all having count(*) > 1)"),
        "null_keys": one(con, "select count(*) from bronze_basic where entity is null or raw_sha256 is null "
                              "or (record = 'value' and (table_id is null or year is null or scope is null))"),
        "branch_values": rows(con, "select coalesce(branch, 'null(Poland)'), count(*) from bronze_basic "
                                   "where record = 'value' group by 1 order by 1"),
        "value_nulls_in_value_rows": dict(zip(VALUE_COLUMNS, con.execute(
            f"select {nulls} from bronze_basic where record = 'value'").fetchone())),
        "value_zero_hospitalisations": one(con, "select count(*) from bronze_basic where number_of_hospitalizations = '0'"),
        "non_numeric_strings": rows(con, f"select col, v, count(*) from ({non_numeric}) group by all order by 3 desc limit 20"),
        "rows_per_table_value": rows(con, "select scope, min(c), avg(c)::decimal(6,2), max(c) from (select entity, table_id, scope, "
                                          "count(*) c from bronze_basic where record = 'value' group by all) group by 1"),
        "per_catalog": rows(con, "select catalog, count(distinct entity), count(distinct entity || coalesce(table_id, '')) "
                                 "filter (where table_id is not null), count(*) from bronze_basic group by 1 order by 1"),
        "dup_rows": rows(con, "select b.entity, b.year, b.period_from, b.table_id, b.scope, b.branch, b.source_row_number, "
                              "b.number_of_patients, b.number_of_hospitalizations, b.percentage, b.duration_median from bronze_basic b "
                              "join (select entity, table_id, scope, branch from bronze_basic where record = 'value' group by all "
                              "having count(*) > 1) d on b.entity = d.entity and b.table_id = d.table_id and b.scope = d.scope "
                              "and b.branch is not distinct from d.branch order by all", 20),
        "zero_rows_by_scope": rows(con, "select scope, count(*) from bronze_basic where number_of_hospitalizations = '0' group by 1"),
        "zero_rows_patterns": rows(con, "select number_of_patients, ratio_of_rehospitalizations, percentage, percentage_of_sections, "
                                        "duration_median, duration_mode, average_value_of_hospitalization, average_value_of_drg, count(*) "
                                        "from bronze_basic where number_of_hospitalizations = '0' group by all order by count(*) desc limit 10"),
        "zero_text_by_column": dict(zip(VALUE_COLUMNS, con.execute("select " + ", ".join(
            f"count(*) filter (where {c} in ('0', '0.0'))" for c in VALUE_COLUMNS) + " from bronze_basic").fetchone())),
        "median_zero_with_stays": one(con, "select count(*) from bronze_basic where duration_median = '0' and number_of_hospitalizations <> '0'"),
        "benefits_rows": one(con, "select count(*) from bronze_benefits"),
        "benefits_by_catalog": rows(con, "select catalog, count(*), count(distinct benefit_code) from bronze_benefits group by 1 order by 1"),
    }


def env() -> Environment:
    return Environment(extensions=["jinja2.ext.do"])


MACROS = Path("warehouse/macros/sources/nfz_services.sql").read_text()


def render(path: str, refs: dict[str, str]) -> str:
    sources = {"nfz_services_basic_data": "bronze_basic", "nfz_services_benefits": "bronze_benefits"}
    return env().from_string(MACROS + Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda name: refs.get(name, name), source=lambda _s, t: sources[t])


def schema_tests(con, yml: str, model: str, table: str) -> dict:
    spec = yaml.safe_load(Path(yml).read_text())
    m = next(x for x in spec["models"] if x["name"] == model)
    results = {}
    for t in m.get("tests", []):
        if "unique" in t:
            expr = t["unique"]["column_name"]
            where = (t["unique"].get("config") or {}).get("where") or "true"
            results[f"unique {expr[:40]} where {where}"] = one(
                con, f"select count(*) from (select {expr} k from {table} where {where} group by 1 having count(*) > 1)")
    for col in m.get("columns", []):
        for t in col.get("tests", []):
            if t == "not_null":
                results[f"not_null {col['name']}"] = one(con, f"select count(*) from {table} where {col['name']} is null")
            elif isinstance(t, dict) and "accepted_values" in t:
                vals = t["accepted_values"]["arguments"]["values"]
                lst = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)
                results[f"accepted_values {col['name']}"] = one(
                    con, f"select count(*) from {table} where {col['name']} is not null and cast({col['name']} as varchar) not in ({lst})")
    return results


def silver(con) -> dict:
    s: dict = {}
    for model in ("nfz_services_benefits", "nfz_services_basic_data"):
        sql = render(f"warehouse/models/silver/nfz_services/{model}.sql", {})
        counts = []
        for _ in (1, 2):
            con.execute(f"create or replace table {model} as {sql}")
            counts.append(one(con, f"select count(*) from {model}"))
        s[model] = {"rows_two_builds": counts,
                    "tests": schema_tests(con, "warehouse/models/silver/nfz_services/_nfz_services.yml", model, model)}
    test = render("warehouse/tests/nfz_services_tables_in_silver.sql", {})
    s["test nfz_services_tables_in_silver"] = one(con, f"select count(*) from ({test})")
    test = render("warehouse/tests/nfz_services_repeated_areas.sql", {})
    s["test nfz_services_repeated_areas (warn)"] = one(con, f"select count(*) from ({test})")
    b = "nfz_services_basic_data"
    s["profile"] = {
        "benefits": one(con, f"select count(distinct entity) from {b}"),
        "periods": rows(con, f"select period_kind, count(*), min(period_start), max(period_end) from {b} group by 1 order by 1"),
        "years": rows(con, f"select min(year), max(year) from {b}"),
        "area_codes": one(con, f"select count(distinct area_code) from {b}"),
        "rows_per_benefit_year": rows(con, f"select avg(c)::decimal(6,2), max(c) from (select entity, table_id, count(*) c from {b} group by all)"),
        "benefit_years": one(con, f"select count(distinct entity || table_id) from {b} where period_kind = 'year'"),
        "pct_max": rows(con, f"select max(percentage), max(percentage_of_sections), max(ratio_of_rehospitalizations), min(ratio_of_rehospitalizations) from {b}"),
        "nulls": rows(con, f"select count(*) filter (where number_of_hospitalizations is null), count(*) filter (where average_value_of_hospitalization_pln is null), "
                           f"count(*) filter (where percentage_of_sections is null), count(*) from {b}"),
        "avg_value_published_years": rows(con, f"select min(year), max(year) from {b} where average_value_of_hospitalization_pln is not null"),
        "without_hospitalisations": one(con, f"select count(*) from {b} where without_hospitalisations"),
        "repeated_area_rows": rows(con, f"select entity, year, table_id, area_code, area_records, source_row_number, number_of_hospitalizations "
                                        f"from {b} where area_records > 1 order by all", 20),
        "nonnull_measures_per_row": one(con, f"select avg(" + " + ".join(f"({c} is not null)::int" for c in (
            "number_of_patients", "number_of_hospitalizations", "ratio_of_rehospitalizations", "percentage", "percentage_of_sections",
            "duration_median_days", "duration_mode_days", "average_value_of_hospitalization_pln", "average_value_of_hospitalization_points",
            "average_value_of_drg_pln", "average_value_of_drg_points")) + f")::decimal(6,3) from {b} where area_records = 1"),
        "benefits_not_in_dictionary": one(con, f"select count(*) from (select distinct catalog, benefit_code from {b}) s "
                                               f"anti join nfz_services_benefits d using (catalog, benefit_code)"),
    }
    return s


def gold(con, prod: dict) -> dict:
    geo_cte = env().from_string(MACROS + "{{ poc_geography__nfz_services() }}").render()
    con.execute(f"create or replace table gmap as with {geo_cte} select * from nfz_services")
    blocks = env().from_string(MACROS + "{{ poc_nfz_services_blocks() }}").render()
    con.execute(f"create or replace table activated as {blocks}")
    sql = render("warehouse/models/gold/poc_build_nfz_services.sql", {"poc_map_geography": "gmap"})
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table build as {sql}")
        counts.append(one(con, "select count(*) from build"))
    g: dict = {"rows_two_builds": counts}
    g["columns_in_contract_order"] = [d[0] for d in con.execute("select * from build limit 0").description] == CONTRACT
    g["dup_series_date"] = one(con, "select count(*) from (select measure_key || '|' || geography_key || case when geography_key = '_X' "
                                    "then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json as series_key, date_key "
                                    "from build group by all having count(*) > 1)")
    g["measures_with_two_frequencies"] = one(con, "select count(*) from (select measure_key from build group by 1 having count(distinct frequency) > 1)")
    g["unmatched_rows"] = one(con, "select count(*) from build where geography_key = '_X'")
    g["null_values"] = one(con, "select count(*) from build where value is null")
    g["rows_per_measure_area_date_max"] = one(con, "select max(c) from (select count(*) c from build group by measure_key, geography_key, date_key)")
    g["measures"] = one(con, "select count(distinct measure_key) from build")
    g["series"] = one(con, "select count(distinct measure_key || geography_key) from build")
    g["by_unit"] = rows(con, "select unit_code, count(*), count(distinct measure_key), min(value), max(value) from build group by 1 order by 1")
    g["build_check"] = rows(con, "select a.block_id, count(o.block_id), count(distinct o.measure_key), count(*) filter (where o.geography_key = '_X'), "
                                 "min(o.date_key), max(o.date_key) from activated a left join build o on o.block_id = a.block_id group by all order by 1")
    g["geography_keys"] = rows(con, "select geography_key, count(*) from build group by 1 order by 1")
    test = render("warehouse/tests/nfz_services_gold_benefits.sql", {"poc_build_nfz_services": "build"})
    g["test nfz_services_gold_benefits"] = rows(con, f"select * from ({test})", 10)
    g["test_gold_not_null"] = schema_tests(con, "warehouse/models/gold/_nfz_services.yml", "poc_build_nfz_services", "build")
    # geography proof: every area code in Silver maps; names
    g["area_map"] = rows(con, "select s.area_code, m.geography_key, count(*) from (select area_code from nfz_services_basic_data) s "
                              "left join gmap m on m.source_code = s.area_code group by all order by 1")
    g["unmatched_area_codes"] = one(con, "select count(distinct area_code) from nfz_services_basic_data s anti join gmap m on m.source_code = s.area_code")
    names = prod.get("teryt_names") or {}
    g["branch_names"] = [[c, NFZ_NAMES.get(c), k, names.get(k.removeprefix("TERC:"))]
                         for c, k in con.execute("select source_code, geography_key from gmap where source_code <> 'PL' order by 1").fetchall()]
    g["spot_N01"] = rows(con, "select period_label, geography_key, measure_key, value from build where measure_codes like '5.51.01.0013001|%' "
                              "and period_label in ('2024', '2009') and geography_key in ('PL', 'TERC:14') "
                              "and (measure_codes like '%number-of-hospitalizations' or measure_codes like '%number-of-patients' "
                              "or measure_codes like '%mediana' or measure_codes like '%average-value-of-hospitalization') order by 1, 2, 3")
    g["spot_H13_PL"] = rows(con, "select period_label, value from build where measure_codes = '5.51.01.0008013|number-of-hospitalizations' "
                                 "and geography_key = 'PL' order by 1")
    g["kpi_patterns"] = rows(con, "select k.kpi_key, count(b.measure_key), count(distinct b.geography_key), min(b.period_label), max(b.period_label) "
                                  "from read_csv('warehouse/seeds/kpi_measure.csv', all_varchar=true) k left join build b on b.measure_key like k.measure_key_pattern "
                                  "where k.source_system = 'nfz_services' group by 1 order by 1")
    g["kpi_rows_per_area_period_max"] = one(con, "select max(c) from (select k.kpi_key, b.geography_key, b.date_key, count(*) c "
                                                 "from read_csv('warehouse/seeds/kpi_measure.csv', all_varchar=true) k join build b on b.measure_key like k.measure_key_pattern "
                                                 "where k.source_system = 'nfz_services' group by all)")
    return g


def production_reads() -> dict:
    out: dict = {}
    cat = catalog()
    try:
        snap = cat.load_table(("gold", "fact_annual")).current_snapshot()
        out["fact_annual_rows"] = int(snap.summary["total-records"]) if snap else None
    except Exception as exc:  # noqa: BLE001
        out["fact_annual_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    try:
        t = cat.load_table(("silver", "teryt_terc")).scan(selected_fields=("terc_code", "name_pl", "unit_level")).to_arrow()
        out["teryt_names"] = {r["terc_code"]: r["name_pl"] for r in t.to_pylist() if r["unit_level"] == "wojewodztwo"}
    except Exception as exc:  # noqa: BLE001
        out["teryt_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    return out


def licence() -> dict:
    from warehouse.publish import licensed_sources

    import csv

    def seed(name):
        with open(f"warehouse/seeds/{name}.csv", newline="", encoding="utf-8") as h:
            return list(csv.DictReader(h))

    src = [r for r in seed("source") if r["source_key"] == SOURCE]
    lic = [r for r in seed("licence") if src and r["licence_key"] == src[0]["licence_key"]]
    return {"source_row": src, "licence_row": lic,
            "licensed_by_gate": SOURCE in licensed_sources(seed("source"), seed("licence"))}


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
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/"))
    try:
        removed["namespace_left"] = SCRATCH in [n[0] for n in cat.list_namespaces()]
    except Exception:  # noqa: BLE001
        pass
    note("nfz_services scratch removed", removed)


def main(argv: list[str]) -> int:
    status = 0
    try:
        prod = production_reads()
        note("nfz_services production reads (read-only)", {k: v for k, v in prod.items() if k != "teryt_names"} | {"teryt_voivodeships": len(prod.get("teryt_names") or {})})
        land = landing()
        note("nfz_services landing (twice), inspect, verify", land)
        b, arrows = bronze()
        con = duckdb.connect()
        con.register("bronze_basic", arrows[nfz_load.BASIC])
        con.register("bronze_benefits", arrows[nfz_load.BENEFITS])
        note("nfz_services bronze (twice)", b)
        note("nfz_services bronze profile", bronze_profile(con))
        s = silver(con)
        note("nfz_services silver", s)
        g = gold(con, prod)
        main_g = {k: v for k, v in g.items() if k not in ("branch_names", "area_map", "spot_N01", "spot_H13_PL", "geography_keys", "kpi_patterns")}
        note("nfz_services gold", main_g)
        note("nfz_services geography and spot values", {k: g[k] for k in ("area_map", "branch_names", "spot_N01", "spot_H13_PL", "kpi_patterns", "kpi_rows_per_area_period_max")})
        note("nfz_services licence gate", licence())
    except Exception as exc:  # noqa: BLE001
        status = 1
        import traceback

        traceback.print_exc()
        annotate("error", "nfz_services trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
