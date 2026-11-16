"""Dev trial of the NFZ contracts API (register PLS-014, #2955, rework of #3116): Landing → Bronze → Silver → Gold.

Landing prefix and lake prefix zz_dev_nfz_contracts/, Iceberg namespace zz_dev_nfz_contracts; everything written is
removed in `finally`. Landing runs the whole scope once (every year from FIRST_YEAR × 16 branches: the measured full
load), then a second unfiltered run, which is the weekly run (the two open years read again; nothing may be written
unless NFZ changed a list meanwhile). Bronze runs twice (the second must append nothing). Silver and the Gold build
stage are built with dbt itself (`dbt build`, the project in warehouse/, models + their generic and singular tests,
twice) against a local DuckDB file attached as `r2`, which holds the scratch Bronze table and a stand-in
`poc_map_geography` made from this source's registration macro and the production TERYT voivodeships (read-only
read). Not exercised: the full poc_map_geography union, poc_build_check, poc_grain_check, poc_fact_annual.
Deleted before merge. Args: optional comma-separated entity filter for the first Landing run (default: whole scope).
"""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
import subprocess
import sys
import tempfile
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
SCHEMA = "nfz_trial"  # schema of the dbt models in the local r2 file (target.schema of a non-prod target)
MODELS = ["nfz_contracts_agreements", "nfz_contracts_providers", "poc_build_nfz_contracts"]
PII = {"provider_nip", "provider_regon", "provider_registry_number", "provider_name", "provider_place"}
CAPTURED: list = []
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
    """One annotation (at most 9 in the run: GitHub keeps 10 per step); the full JSON is in the log."""
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
    out: dict = {"first_filter": first_filter or "whole scope", "second": "unfiltered (= the weekly run)"}
    runs = []
    for n, flt in ((1, first_filter), (2, "")):
        CAPTURED.clear()
        start = time.monotonic()
        code = extract.main(["--mode", "load", "--filter", flt, "--budget-minutes", "250", "--landing-root", SCRATCH])
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        runs.append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                     **{k: v for k, v in summary.items() if k != "mode"},
                     "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    out["runs"] = runs
    CAPTURED.clear()
    out["verify_exit"] = extract.main(["--mode", "verify", "--landing-root", SCRATCH])
    out["verify"] = next((d for t, d in CAPTURED if "verify" in t), {})
    held = extract.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), SCRATCH)
    newest = [r[-1] for r in held.values()]
    out["api_agreements_by_year"] = {}
    for r in newest:
        out["api_agreements_by_year"][r["year"]] = out["api_agreements_by_year"].get(r["year"], 0) + r["agreements"]
    out["pages"] = sum(len(r["members"]) for r in newest)
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
    out["columns"] = arrow.schema.names
    out["identity_columns_stored"] = sorted(PII & set(arrow.schema.names))
    try:
        rej = cat.load_table((SCRATCH, nfz_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    b = "bronze_agreements"
    kept = [c for _, c in nfz_load.ATTRIBUTES]
    return {
        "rows": one(con, f"select count(*) from {b}"),
        "entities": one(con, f"select count(distinct entity) from {b}"),
        "distinct_ids": one(con, f"select count(distinct agreement_id) from {b}"),
        "dup_ids": one(con, f"select count(*) from (select agreement_id from {b} group by 1 having count(*) > 1)"),
        "years_rows_branches": rows(con, f"select year_text, count(*), count(distinct branch) from {b} group by 1 order by 1"),
        "nulls": dict(zip(kept, con.execute("select " + ", ".join(f"count(*) filter (where {c} is null)" for c in kept)
                                            + f" from {b}").fetchone())),
        "amount_non_numeric": rows(con, f"select amount_text, count(*) from {b} where amount_text is not null "
                                        "and try_cast(amount_text as double) is null group by 1 order by 2 desc limit 10"),
        "amount_decimals": rows(con, f"select length(split_part(amount_text, '.', 2)) d, count(*) from {b} "
                                     "where amount_text is not null group by 1 order by 1"),
        "amount_texts_equal_zero": rows(con, f"select amount_text, count(*) from {b} where try_cast(amount_text as double) = 0 "
                                             "group by 1 order by 2 desc"),
        "amount_negative": one(con, f"select count(*) from {b} where try_cast(amount_text as double) < 0"),
        "updated_at_unparsable": one(con, f"select count(*) from {b} where try_cast(updated_at as timestamp) is null"),
    }


def zero_profile(con) -> dict:
    """Published amounts of exactly 0.00: per service type (total, null, zero, share of zero among amounts), per year,
    whether a zero agreement's provider also has non-zero agreements of the type, and the Gold rows of value 0."""
    a = f"{SCHEMA}.nfz_contracts_agreements"
    return {
        "by_service_type [code, agreements, null, zero, zero_share_of_amounts, max_amount]": rows(
            con, f"select service_type_code, count(*), count(*) - count(amount_pln), count(*) filter (where amount_pln = 0), "
                 "round(count(*) filter (where amount_pln = 0) / nullif(count(amount_pln), 0), 3), max(amount_pln) "
                 f"from {a} group by 1 order by 1"),
        "by_year [year, zero, amounts]": rows(con, f"select contract_year, count(*) filter (where amount_pln = 0), "
                                                    f"count(amount_pln) from {a} group by 1 order by 1"),
        "zero_rows_whose_code_has_nonzero_in_other_year": one(
            con, f"select count(*) from {a} z where z.amount_pln = 0 and exists (select 1 from {a} o where o.agreement_code = "
                 "z.agreement_code and o.branch_code = z.branch_code and o.contract_year <> z.contract_year and o.amount_pln > 0)"),
        "zero_rows_smallest_positive_in_same_type": rows(
            con, f"select service_type_code, min(amount_pln) filter (where amount_pln > 0) from {a} "
                 f"where service_type_code in (select service_type_code from {a} where amount_pln = 0) group by 1 order by 1"),
        "gold_pln_rows_of_value_0 [breakdown, is_total, rows]": rows(
            con, f"select breakdown_json, is_total, count(*) from {SCHEMA}.poc_build_nfz_contracts "
                 "where unit_code = 'pln' and value = 0 group by 1, 2 order by 1"),
        "gold_pln_rows": one(con, f"select count(*) from {SCHEMA}.poc_build_nfz_contracts where unit_code = 'pln'"),
    }


def env() -> Environment:
    return Environment(extensions=["jinja2.ext.do"])


def dbt_build(arrow, teryt: dict) -> tuple[dict, Path]:
    """dbt build of the three models and their tests, twice, into a local DuckDB file attached as r2."""
    tmp = Path(tempfile.mkdtemp(prefix="nfz_dbt_"))
    r2 = tmp / "r2.duckdb"
    con = duckdb.connect(str(r2))
    con.register("arrow_bronze", arrow)
    con.execute("create schema bronze")
    con.execute("create table bronze.nfz_contracts_agreements as select * from arrow_bronze")
    con.execute(f"create schema {SCHEMA}")
    keys = ", ".join(f"('TERC:{k}', 'wojewodztwo')" for k in teryt) or "('TERC:02', 'wojewodztwo')"
    macros = Path("warehouse/macros/sources/nfz_contracts.sql").read_text()
    cte = env().from_string(macros + "{{ poc_geography__nfz_contracts() }}").render()
    con.execute(f"create table {SCHEMA}.poc_map_geography as with geo as (select * from (values {keys}) as t "
                f"(geography_key, level_name)), {cte} select * from nfz_contracts")
    con.close()
    profile = {"zohelo": {"target": "local", "outputs": {"local": {
        "type": "duckdb", "path": str(tmp / "main.duckdb"), "schema": SCHEMA, "threads": 4,
        "attach": [{"path": str(r2), "alias": "r2"}]}}}}
    (tmp / "profiles.yml").write_text(yaml.safe_dump(profile))
    out: dict = {"command": "dbt build --select " + " ".join(MODELS) + " --indirect-selection cautious", "builds": []}
    for n in (1, 2):
        start = time.monotonic()
        proc = subprocess.run(["dbt", "build", "--project-dir", "warehouse", "--profiles-dir", str(tmp), "--target", "local",
                               "--select", *MODELS, "--indirect-selection", "cautious"], capture_output=True, text=True)
        print(proc.stdout[-2500:], proc.stderr[-2000:], flush=True)
        results = json.loads(Path("warehouse/target/run_results.json").read_text())["results"]
        status = {r["unique_id"].split(".")[-1 if r["unique_id"].startswith("model.") else 2]: (
            r["status"] + (f" ({r['failures']})" if r.get("failures") else "")) for r in results}
        print(f"dbt build {n} node status: {json.dumps(status)}", flush=True)
        out["builds"].append({
            "run": n, "exit": proc.returncode, "seconds": round(time.monotonic() - start),
            "models": sum(r["unique_id"].startswith("model.") for r in results),
            "tests": sum(r["unique_id"].startswith("test.") for r in results),
            "status_counts": dict(Counter(r["status"] for r in results)),
            "singular_tests": {k: v for k, v in status.items() if k.startswith("nfz_contracts_")},
            "not_pass_or_success": {k: v for k, v in status.items() if not v.startswith(("pass", "success"))},
        })
    return out, r2


def silver(con) -> dict:
    a, p = f"{SCHEMA}.nfz_contracts_agreements", f"{SCHEMA}.nfz_contracts_providers"
    return {
        "rows": {"agreements": one(con, f"select count(*) from {a}"), "providers": one(con, f"select count(*) from {p}")},
        "columns": {"agreements": [d[0] for d in con.execute(f"select * from {a} limit 0").description],
                    "providers": [d[0] for d in con.execute(f"select * from {p} limit 0").description]},
        "dup_agreement_id": one(con, f"select count(*) from (select agreement_id from {a} group by 1 having count(*) > 1)"),
        "dup_provider_key": one(con, f"select count(*) from (select branch_code, provider_code from {p} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {a} where agreement_id is null or raw_sha256 is null or provider_code is null"),
        "years [year, agreements, with amount, sum, provider codes]": rows(
            con, f"select contract_year, count(*), count(amount_pln), sum(amount_pln)::decimal(18,2), "
                 f"count(distinct branch_code || provider_code) from {a} group by 1 order by 1"),
        "amount_min_max": rows(con, f"select min(amount_pln), max(amount_pln) from {a}"),
        "service_type_codes": rows(con, f"select list(distinct service_type_code order by service_type_code) from {a}"),
    }


def gold(con, teryt: dict) -> dict:
    b = f"{SCHEMA}.poc_build_nfz_contracts"
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [d[0] for d in con.execute(f"select * from {b} limit 0").description] == CONTRACT
    g["dup_series_date"] = one(con, f"select count(*) from (select measure_key || '|' || geography_key || case when geography_key = '_X' "
                                    "then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json as series_key, date_key "
                                    f"from {b} group by all having count(*) > 1)")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["unmatched_rows"] = one(con, f"select count(*) from {b} where geography_key = '_X'")
    g["measures"] = rows(con, f"select measure_key, unit_code, count(*), count(distinct geography_key), min(value), max(value) from {b} group by 1, 2 order by 1")
    g["series"] = one(con, f"select count(distinct measure_key || geography_key || breakdown_json) from {b}")
    g["totals_rows"] = one(con, f"select count(*) from {b} where is_total")
    g["date_range"] = rows(con, f"select min(date_key), max(date_key) from {b}")
    g["pl_equals_sum_of_branches"] = one(con, f"select count(*) from (select measure_key, breakdown_json, date_key, "
                                              "sum(value) filter (where geography_key <> 'PL') s, sum(value) filter (where geography_key = 'PL') p "
                                              f"from {b} group by all) where abs(s - p) > 0.001")
    g["total_equals_sum_of_types"] = one(con, "select count(*) from (select measure_key, geography_key, date_key, "
                                              "sum(value) filter (where not is_total) t, sum(value) filter (where is_total) a "
                                              f"from {b} where measure_key not like '%providers%' group by all) where abs(t - a) > 0.01")
    g["branch_name_mismatch"] = [
        [c, BRANCH_NAMES[c], k] for c, k in con.execute(f"select source_code, geography_key from {SCHEMA}.poc_map_geography "
                                                         "where source_code <> 'all_branches' order by 1").fetchall()
        if (teryt.get(k.removeprefix("TERC:")) or "").lower() != BRANCH_NAMES[c]]
    g["pl_amount_total_min_before_newest"] = one(con, f"select min(value) from {b} where unit_code = 'pln' and is_total and geography_key = 'PL' "
                                                       f"and date_key < (select max(date_key) from {b})")
    g["spot_PL"] = rows(con, f"select period_label, measure_codes, value from {b} where geography_key = 'PL' and is_total "
                             "and period_label in ('2019', '2025', '2026') order by 1, 2")
    g["spot_mazowieckie_2025"] = rows(con, f"select measure_codes, value from {b} where geography_key = 'TERC:14' and is_total "
                                           "and period_label = '2025' order by 1")
    g["amount_measure_name"] = one(con, f"select any_value(measure_name) from {b} where unit_code = 'pln'")
    km = "read_csv('warehouse/seed_rows/kpi_measure/nfz_contracts.csv', all_varchar=true)"
    g["kpi_patterns"] = rows(con, f"select k.kpi_key, count(b.measure_key), count(distinct b.geography_key), min(b.period_label), "
                                  f"max(b.period_label) from {km} k left join {b} b on b.measure_key like k.measure_key_pattern "
                                  "and b.is_total group by 1 order by 1")
    g["kpi_rows_per_area_period_max"] = one(con, f"select max(c) from (select k.kpi_key, b.geography_key, b.date_key, count(*) c "
                                                 f"from {km} k join {b} b on b.measure_key like k.measure_key_pattern and b.is_total group by all)")
    return g


def production_reads() -> dict:
    try:
        t = catalog().load_table(("silver", "teryt_terc")).scan(selected_fields=("terc_code", "name_pl", "unit_level")).to_arrow()
        return {r["terc_code"]: r["name_pl"] for r in t.to_pylist() if r["unit_level"] == "wojewodztwo"}
    except Exception as exc:
        print(f"teryt read failed: {type(exc).__name__}: {str(exc)[:200]}", flush=True)
        return {}


def licence() -> dict:
    from warehouse.publish import licensed_sources
    from warehouse.seed_rows import rows as seed

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


def analyse(arrow, teryt: dict) -> int:
    """Everything after Bronze (also run locally on a synthetic Bronze table)."""
    d, r2 = dbt_build(arrow, teryt)
    con = duckdb.connect(str(r2), read_only=True)
    con.register("bronze_agreements", arrow)
    note("nfz_contracts bronze profile", bronze_profile(con))
    note("nfz_contracts dbt build (twice) and silver", {**d, "silver": silver(con)})
    g = gold(con, teryt)
    spots = ("spot_PL", "spot_mazowieckie_2025", "amount_measure_name", "kpi_patterns", "kpi_rows_per_area_period_max")
    note("nfz_contracts gold", {k: v for k, v in g.items() if k not in spots})
    note("nfz_contracts spot values and zero amounts", {**{k: g[k] for k in spots}, "zero_amounts": zero_profile(con)})
    note("nfz_contracts licence gate", licence())
    return 0 if all(b["exit"] == 0 for b in d["builds"]) else 1


def main(argv: list[str]) -> int:
    status = 0
    try:
        teryt = production_reads()
        land = landing(argv[0] if argv else "")
        note("nfz_contracts landing (twice), verify", {"teryt_voivodeships_read": len(teryt), **land})
        b, arrow = bronze()
        note("nfz_contracts bronze (twice)", b)
        status = analyse(arrow, teryt)
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
