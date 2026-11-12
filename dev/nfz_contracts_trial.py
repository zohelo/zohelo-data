"""Dev trial of the NFZ contracts API (register PLS-014, #2955): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_nfz_contracts/, Iceberg namespace zz_dev_nfz_contracts; everything written is
removed in `finally`. Landing runs the whole scope once (every year from FIRST_YEAR × 16 branches: the measured full
load), then again on a few entities (open and closed years: nothing may be written); Bronze runs twice (the second
must append nothing). Silver and the Gold build stage are rendered with Jinja from warehouse/ and run in local
DuckDB over the scratch Bronze table, each built twice, with the YAML schema tests and the source's singular tests.
Read-only production read: silver.teryt_terc voivodeship names (branch → voivodeship check). Deleted before merge.
Args: optional comma-separated entity filter for the first Landing run (default: the whole scope).
"""

from __future__ import annotations

import csv
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
from ingestion.to_bronze.nfz_contracts import load as nfz_load
from ingestion.to_landing.nfz_contracts import extract

SCRATCH = "zz_dev_nfz_contracts"
SOURCE = "nfz_contracts"
CAPTURED: list = []
SECOND = "agreements_2026_08,agreements_2025_10,agreements_2019_08"
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
BRANCH_NAMES = {"01": "dolnośląskie", "02": "kujawsko-pomorskie", "03": "lubelskie", "04": "lubuskie", "05": "łódzkie",
                "06": "małopolskie", "07": "mazowieckie", "08": "opolskie", "09": "podkarpackie", "10": "podlaskie",
                "11": "pomorskie", "12": "śląskie", "13": "świętokrzyskie", "14": "warmińsko-mazurskie",
                "15": "wielkopolskie", "16": "zachodniopomorskie"}


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


def landing(first_filter: str) -> dict:
    out: dict = {"first_filter": first_filter or "whole scope", "second_filter": SECOND}
    runs = []
    for n, flt in ((1, first_filter), (2, SECOND)):
        CAPTURED.clear()
        start = time.monotonic()
        code = extract.main(["--mode", "load", "--filter", flt, "--budget-minutes", "280", "--landing-root", SCRATCH])
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        runs.append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                     **{k: v for k, v in summary.items() if k != "mode"},
                     "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    out["runs"] = runs
    CAPTURED.clear()
    out["inspect_exit"] = extract.main(["--mode", "inspect", "--landing-root", SCRATCH])
    out["inspect"] = next((d for t, d in CAPTURED if "inspect" in t), {})
    CAPTURED.clear()
    out["verify_exit"] = extract.main(["--mode", "verify", "--landing-root", SCRATCH])
    out["verify"] = next((d for t, d in CAPTURED if "verify" in t), {})
    held = extract.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), SCRATCH)
    newest = [r[-1] for r in held.values()]
    out["api_agreements_by_year"] = {}
    for r in newest:
        out["api_agreements_by_year"][r["year"]] = out["api_agreements_by_year"].get(r["year"], 0) + r["agreements"]
    out["pages"] = sum(len(r["members"]) for r in newest)
    out["largest_entity"] = max(((r["entity"], r["agreements"], r["size_bytes"]) for r in newest), key=lambda x: x[1])
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        start = time.monotonic()
        code = nfz_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, "seconds": round(time.monotonic() - start),
                             **{k: v for t, d in CAPTURED for k, v in d.items() if k != "result"}})
    cat = catalog()
    t = cat.load_table((SCRATCH, nfz_load.TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, nfz_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    b = "bronze_agreements"
    return {
        "rows": one(con, f"select count(*) from {b}"),
        "entities": one(con, f"select count(distinct entity) from {b}"),
        "distinct_ids": one(con, f"select count(distinct agreement_id) from {b}"),
        "dup_ids": one(con, f"select count(*) from (select agreement_id from {b} group by 1 having count(*) > 1)"),
        "years": rows(con, f"select year_text, count(*), count(distinct branch) from {b} group by 1 order by 1"),
        "nulls": dict(zip(
            [c for _, c in nfz_load.ATTRIBUTES],
            con.execute("select " + ", ".join(f"count(*) filter (where {c} is null)" for _, c in nfz_load.ATTRIBUTES)
                        + f" from {b}").fetchone())),
        "amount_non_numeric": rows(con, f"select amount_text, count(*) from {b} where amount_text is not null "
                                        "and try_cast(amount_text as double) is null group by 1 order by 2 desc limit 10"),
        "amount_decimals": rows(con, f"select length(split_part(amount_text, '.', 2)) d, count(*) from {b} "
                                     "where amount_text is not null group by 1 order by 1"),
        "amount_zero_or_negative": rows(con, f"select sign(cast(amount_text as double)), count(*) from {b} "
                                             "where amount_text is not null group by 1 order by 1"),
        "amount_null_by_service": rows(con, f"select service_type, count(*) filter (where amount_text is null), count(*) "
                                            f"from {b} group by 1 order by 1"),
        "service_types": rows(con, f"select service_type, count(distinct service_name), min(service_name) from {b} group by 1 order by 1"),
        "provider_code_lengths": rows(con, f"select length(provider_code), count(*) from {b} group by 1 order by 1"),
        "provider_code_branch_prefix": rows(con, f"select count(*) filter (where lpad(provider_code, 9, '0') like branch || '%'), count(*) from {b}"),
        "updated_at_range": rows(con, f"select min(updated_at), max(updated_at) from {b}"),
        "updated_at_unparsable": one(con, f"select count(*) from {b} where try_cast(updated_at as timestamp) is null"),
        "same_code_in_years": rows(con, f"select n, count(*) from (select code, count(distinct year_text) n from {b} group by 1) group by 1 order by 1"),
    }


def env() -> Environment:
    return Environment(extensions=["jinja2.ext.do"])


MACROS = Path("warehouse/macros/sources/nfz_contracts.sql").read_text()


def render(path: str, refs: dict[str, str]) -> str:
    return env().from_string(MACROS + Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda name: refs.get(name, name), source=lambda _s, t: "bronze_agreements")


def schema_tests(con, yml: str, model: str, table: str) -> dict:
    spec = yaml.safe_load(Path(yml).read_text())
    m = next(x for x in spec["models"] if x["name"] == model)
    results = {}
    for t in m.get("tests", []):
        if "unique" in t:
            expr = t["unique"]["column_name"]
            results["unique"] = one(con, f"select count(*) from (select {expr} k from {table} group by 1 having count(*) > 1)")
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
    yml = "warehouse/models/silver/nfz_contracts/_nfz_contracts.yml"
    for model in ("nfz_contracts_agreements", "nfz_contracts_providers"):
        sql = render(f"warehouse/models/silver/nfz_contracts/{model}.sql", {})
        counts = []
        for _ in (1, 2):
            con.execute(f"create or replace table {model} as {sql}")
            counts.append(one(con, f"select count(*) from {model}"))
        s[model] = {"rows_two_builds": counts, "tests": schema_tests(con, yml, model, model)}
        test = render(f"warehouse/tests/{model}_complete.sql", {})
        s[model][f"test {model}_complete"] = rows(con, f"select * from ({test})", 10)
    a = "nfz_contracts_agreements"
    s["profile"] = {
        "years": rows(con, f"select contract_year, count(*), count(amount_pln), sum(amount_pln)::decimal(18,2), "
                           f"count(distinct branch_code || provider_code) from {a} group by 1 order by 1"),
        "amount_min_max": rows(con, f"select min(amount_pln), max(amount_pln) from {a}"),
        "service_types": rows(con, f"select service_type_code, count(*), count(amount_pln) from {a} group by 1 order by 1"),
        "provider_conflicts": one(con, "select count(*) from (select branch_code, provider_code from nfz_contracts_providers "
                                       "group by all having count(*) > 1)"),
        "providers_without_nip_or_regon": one(con, "select count(*) from nfz_contracts_providers where provider_nip is null and provider_regon is null"),
        "providers_same_nip_in_branches": one(con, "select count(*) from (select provider_nip from nfz_contracts_providers "
                                                   "where provider_nip is not null group by 1 having count(distinct branch_code) > 1)"),
    }
    return s


def gold(con, prod: dict) -> dict:
    geo_rows = ", ".join(f"('TERC:{k}', 'wojewodztwo')" for k in (prod.get("teryt_names") or {})) or "('TERC:02', 'wojewodztwo')"
    con.execute(f"create or replace table geo as select * from (values {geo_rows}) as t (geography_key, level_name)")
    geo_cte = env().from_string(MACROS + "{{ poc_geography__nfz_contracts() }}").render()
    con.execute(f"create or replace table gmap as with {geo_cte} select * from nfz_contracts")
    sql = render("warehouse/models/gold/poc_build_nfz_contracts.sql", {"poc_map_geography": "gmap"})
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
    g["measures"] = rows(con, "select measure_key, unit_code, count(*), count(distinct geography_key), min(value), max(value) from build group by 1, 2 order by 1")
    g["series"] = one(con, "select count(distinct measure_key || geography_key || breakdown_json) from build")
    g["date_range"] = rows(con, "select min(date_key), max(date_key) from build")
    g["tests"] = schema_tests(con, "warehouse/models/gold/_nfz_contracts.yml", "poc_build_nfz_contracts", "build")
    g["pl_equals_sum_of_branches"] = one(con, "select count(*) from (select measure_key, breakdown_json, date_key, "
                                              "sum(value) filter (where geography_key <> 'PL') b, sum(value) filter (where geography_key = 'PL') p "
                                              "from build group by all) where abs(b - p) > 0.001")
    g["total_equals_sum_of_types"] = one(con, "select count(*) from (select measure_key, geography_key, date_key, "
                                              "sum(value) filter (where not is_total) t, sum(value) filter (where is_total) a "
                                              "from build where measure_key not like '%providers%' group by all) where abs(t - a) > 0.01")
    names = prod.get("teryt_names") or {}
    g["branch_names"] = [[c, BRANCH_NAMES[c], k, names.get(k.removeprefix("TERC:"))]
                         for c, k in con.execute("select source_code, geography_key from gmap where source_code <> 'all_branches' order by 1").fetchall()]
    g["branch_name_mismatch"] = [r for r in g["branch_names"] if (r[3] or "").lower() != r[1]]
    g["spot_PL"] = rows(con, "select period_label, measure_codes, value from build where geography_key = 'PL' and is_total "
                             "and period_label in ('2019', '2024', '2025') order by 1, 2")
    g["spot_mazowieckie_2025"] = rows(con, "select measure_codes, value from build where geography_key = 'TERC:14' and is_total "
                                           "and period_label = '2025' order by 1")
    g["spot_hospital_PL_2025"] = rows(con, "select measure_codes, value, breakdown_label_json from build where geography_key = 'PL' "
                                           "and breakdown_json = '{\"service_type\":\"03\"}' and period_label = '2025' order by 1")
    g["kpi_patterns"] = rows(con, "select k.kpi_key, count(b.measure_key), count(distinct b.geography_key), min(b.period_label), max(b.period_label), "
                                  "max(b.value) from read_csv('warehouse/seeds/kpi_measure.csv', all_varchar=true) k left join build b "
                                  "on b.measure_key like k.measure_key_pattern and b.is_total where k.source_system = 'nfz_contracts' group by 1 order by 1")
    g["kpi_rows_per_area_period_max"] = one(con, "select max(c) from (select k.kpi_key, b.geography_key, b.date_key, count(*) c "
                                                 "from read_csv('warehouse/seeds/kpi_measure.csv', all_varchar=true) k join build b on b.measure_key like k.measure_key_pattern "
                                                 "and b.is_total where k.source_system = 'nfz_contracts' group by all)")
    return g


def production_reads() -> dict:
    out: dict = {}
    try:
        t = catalog().load_table(("silver", "teryt_terc")).scan(selected_fields=("terc_code", "name_pl", "unit_level")).to_arrow()
        out["teryt_names"] = {r["terc_code"]: r["name_pl"] for r in t.to_pylist() if r["unit_level"] == "wojewodztwo"}
    except Exception as exc:
        out["teryt_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    return out


def licence() -> dict:
    from warehouse.publish import licensed_sources

    def seed(name):
        with open(f"warehouse/seeds/{name}.csv", newline="", encoding="utf-8") as h:
            return list(csv.DictReader(h))

    src = [r for r in seed("source") if r["source_key"] == SOURCE]
    lic = [r for r in seed("licence") if src and r["licence_key"] == src[0]["licence_key"]]
    return {"source_row": src, "licence_row": lic,
            "licensed_by_gate": SOURCE in licensed_sources(seed("source"), seed("licence")),
            "kpi_v2_map_rows": [r for r in seed("kpi_v2_map") if r["source_key"] == SOURCE]}


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
    except Exception:
        pass
    note("nfz_contracts scratch removed", removed)


def main(argv: list[str]) -> int:
    status = 0
    try:
        prod = production_reads()
        note("nfz_contracts production read (read-only)", {"teryt_voivodeships": len(prod.get("teryt_names") or {}),
                                                            "error": prod.get("teryt_error")})
        land = landing(argv[0] if argv else "")
        note("nfz_contracts landing (twice), inspect, verify", land)
        b, arrow = bronze()
        note("nfz_contracts bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze_agreements", arrow)
        note("nfz_contracts bronze profile", bronze_profile(con))
        s = silver(con)
        note("nfz_contracts silver", s)
        g = gold(con, prod)
        spots = ("branch_names", "spot_PL", "spot_mazowieckie_2025", "spot_hospital_PL_2025", "kpi_patterns", "kpi_rows_per_area_period_max")
        note("nfz_contracts gold", {k: v for k, v in g.items() if k not in spots})
        note("nfz_contracts geography and spot values", {k: g[k] for k in spots})
        note("nfz_contracts licence gate", licence())
    except Exception as exc:
        status = 1
        import traceback

        traceback.print_exc()
        annotate("error", "nfz_contracts trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
