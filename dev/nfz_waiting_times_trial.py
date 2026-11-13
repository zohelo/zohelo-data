"""Dev trial of NFZ waiting times (register PLS-015, #2956): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_nfz_waiting_times/, Iceberg namespace zz_dev_nfz_waiting_times; everything
written is removed in `finally`. A full snapshot (every case and voivodeship) is read once, timed; Landing and
Bronze each run twice (the second run must write nothing). Silver, the Gold build stage and the metrics rows are
rendered with Jinja from warehouse/ and run in local DuckDB over the scratch Bronze table, each built twice, with
the schema tests of the YAML files and the source's singular tests. Also reads the v1.4 site's terms page
(swagger-ui/statute.html). Read-only reads of production: silver.teryt_terc (voivodeship codes and names).
Deleted before merge.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
import re
import sys
import time

import duckdb
from jinja2 import Environment
import requests
import yaml

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.nfz_waiting_times import load as nfz_load
from ingestion.to_landing.nfz_waiting_times import extract

SCRATCH = "zz_dev_nfz_waiting_times"
SOURCE = "nfz_waiting_times"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("nfz_waiting_times_queues", "nfz_waiting_times_providers", "nfz_waiting_times_monthly")
TESTS = {"nfz_waiting_times_queues": "nfz_waiting_times_complete",
         "nfz_waiting_times_providers": "nfz_waiting_times_providers_complete",
         "nfz_waiting_times_monthly": "nfz_waiting_times_monthly_complete"}


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


def terms() -> dict:
    out = {}
    for path in ("swagger-ui/statute.html", "swagger-ui/changelog.html"):
        url = f"{extract.API}/{path}"
        time.sleep(1)
        r = requests.get(url, headers={"User-Agent": extract.USER_AGENT}, timeout=60)
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"<(script|style).*?</\1>", " ", r.text, flags=re.S)))
        print(f"=== {url} HTTP {r.status_code}\n{text}", flush=True)
        out[path] = {"status": r.status_code, "chars": len(text), "head": text[:1500]}
    return out


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


def landing(filter_text: str) -> dict:
    out: dict = {"filter": filter_text or "all"}
    runs = []
    for n in (1, 2):
        CAPTURED.clear()
        start = time.monotonic()
        code = extract.main(["--mode", "load", "--filter", filter_text, "--budget-minutes", "300",
                             "--landing-root", SCRATCH])
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        runs.append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1), **summary,
                     "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    out["runs"] = runs
    CAPTURED.clear()
    out["inspect_exit"] = extract.main(["--mode", "inspect", "--filter", filter_text, "--landing-root", SCRATCH])
    out["inspect"] = next((d for t, d in CAPTURED if "inspect" in t), {})
    CAPTURED.clear()
    out["verify_exit"] = extract.main(["--mode", "verify", "--landing-root", SCRATCH])
    out["verify"] = next((d for t, d in CAPTURED if "verify" in t), {})
    held = extract.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), SCRATCH)
    details = [r[-1]["detail"] | {"entity": e, "bytes": r[-1]["size_bytes"]} for e, r in sorted(held.items())]
    out["reads_max"] = max((d["reads"] for d in details), default=None)
    out["inconsistent"] = [d for d in details if not d["consistent"]]
    out["largest"] = max(details, key=lambda d: d["bytes"], default=None)
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = nfz_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for t, d in CAPTURED for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, nfz_load.TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, nfz_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    ints = ("awaiting", "removed", "average_period", "type_building", "computed_average_period", "case_code")
    non_int = " union all ".join(
        f"select '{c}' as col, {c} as v from bronze where {c} is not null and try_cast({c} as integer) is null"
        for c in ints)
    nulls = ", ".join(f"count(*) filter (where {c} is null)" for c in ("awaiting", "removed", "average_period",
                                                                        "statistics_month", "pcus", "teryt_place",
                                                                        "provider_code", "benefit", "queue_id"))
    flags = ("many_places", "anesthesia", "benefits_for_children", "covid_19", "queue_in_cer", "type_building",
             "toilet", "ramp", "car_park", "elevator", "air_conditioning", "automatic_door", "wheelchairs",
             "corridors", "dates_applicable", "computed_update", "benefits_provided")
    return {
        "rows": one(con, "select count(*) from bronze"),
        "entities": one(con, "select count(distinct entity) from bronze"),
        "distinct_queue_ids": one(con, "select count(distinct queue_id) from bronze"),
        "dup_queue_ids": one(con, "select count(*) from (select queue_id from bronze group by 1 having count(*) > 1)"),
        "rows_by_case": rows(con, "select case_code, count(*) from bronze group by 1 order by 1"),
        "nulls(awaiting,removed,avg,stat_month,pcus,teryt_place,provider,benefit,id)": rows(con, f"select {nulls} from bronze"),
        "non_integer": rows(con, f"select col, v, count(*) from ({non_int}) group by all order by 3 desc limit 10"),
        "statistics_month": rows(con, "select statistics_month, count(*) from bronze group by 1 order by 2 desc limit 8"),
        "awaiting_min_max_zero": rows(con, "select min(cast(awaiting as int)), max(cast(awaiting as int)), "
                                           "count(*) filter (where awaiting = '0') from bronze"),
        "flags": {f: rows(con, f"select {f}, count(*) from bronze group by 1 order by 2 desc limit 6") for f in flags},
        "pcus_top": rows(con, "select pcus, count(*) from bronze group by 1 order by 2 desc limit 8"),
        "pcus_shapes": rows(con, "select regexp_replace(coalesce(pcus, 'null'), '[0-9]+', 'n', 'g'), count(*) "
                                 "from bronze group by 1 order by 2 desc limit 12"),
        "as_at_min_max": rows(con, "select min(date_situation_as_at), max(date_situation_as_at) from bronze"),
        "provider_codes_two_names": one(con, "select count(*) from (select entity[15:], provider_code from bronze "
                                             "group by all having count(distinct provider_name) > 1 "
                                             "or count(distinct teryt_provider) > 1)"),
        "provider_two_names_sample": rows(con, "select entity[15:], provider_code, list(distinct provider_name), "
                                               "list(distinct teryt_provider) from bronze group by all "
                                               "having count(distinct provider_name) > 1 or count(distinct teryt_provider) > 1 limit 3"),
        "same_place_benefit_case_twice": one(con, "select count(*) from (select provider_code, benefit, case_code, "
                                                  "registry_number, resort_part_vii, resort_part_viii, place_name, locality "
                                                  "from bronze group by all having count(*) > 1)"),
        "teryt_place_outside": one(con, "select count(*) from bronze where coalesce(teryt_place, '') <> '' and left(teryt_place, 2) "
                                        "<> lpad(cast(cast(right(entity, 2) as int) * 2 as varchar), 2, '0')"),
    }


def env() -> Environment:
    return Environment(extensions=["jinja2.ext.do"])


MACROS = Path("warehouse/macros/sources/nfz_waiting_times.sql").read_text()


def render(path: str, refs: dict[str, str] | None = None) -> str:
    return env().from_string(MACROS + Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda name: (refs or {}).get(name, name), source=lambda _s, _t: "bronze")


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
    for model in SILVER:
        sql = render(f"warehouse/models/silver/{SOURCE}/{model}.sql")
        counts = []
        for _ in (1, 2):
            con.execute(f"create or replace table {model} as {sql}")
            counts.append(one(con, f"select count(*) from {model}"))
        test = render(f"warehouse/tests/{TESTS[model]}.sql")
        s[model] = {"rows_two_builds": counts,
                    "tests": schema_tests(con, f"warehouse/models/silver/{SOURCE}/_{SOURCE}.yml", model, model),
                    f"test {TESTS[model]}": rows(con, f"select * from ({test})", 10)}
    q = "nfz_waiting_times_queues"
    s["profile"] = {
        "snapshot_month": rows(con, f"select snapshot_month, count(*) from {q} group by 1"),
        "by_province_case": rows(con, f"select province_code, case_code, count(*) from {q} group by all order by 1, 2", 32),
        "awaiting_by_stat_month": rows(con, f"select statistics_month, count(*), count(awaiting), sum(awaiting) from {q} group by 1 order by 1"),
        "forecast_applicable": rows(con, f"select forecast_applicable, count(*) from {q} group by 1"),
        "providers": one(con, "select count(*) from nfz_waiting_times_providers"),
        # many places: entries of one province x provider x benefit x case with many_places = 'Y'
        "many_places_groups(entries_Y,groups,groups_same_count,groups_differing,entries_in_differing)": rows(
            con, f"with g as (select province_code, case_code, provider_code, benefit, count(*) n, "
                 f"count(distinct awaiting) d, count(awaiting) a, count(distinct statistics_month) m from {q} "
                 f"where many_places = 'Y' group by all) select sum(n), count(*), count(*) filter (where d = 1 and a = n and m = 1), "
                 f"count(*) filter (where not (d = 1 and a = n and m = 1)), sum(n) filter (where not (d = 1 and a = n and m = 1)) from g"),
        "many_places_differing_sample": rows(
            con, f"select province_code, case_code, provider_code, benefit, list(awaiting order by awaiting), "
                 f"list(distinct statistics_month) from {q} where many_places = 'Y' group by all "
                 f"having count(distinct awaiting) > 1 or count(awaiting) < count(*) limit 3"),
        "naive_sum_vs_lists_PL_case1": rows(
            con, f"select (select sum(awaiting) from {q} where case_code = 1 and statistics_month = snapshot_month - interval 1 month), "
                 f"(select value from nfz_waiting_times_monthly where province_code = '00' and case_code = 1 and measure_code = 'persons_awaiting')"),
        "suppression_markers_awaiting_non_numeric": rows(
            con, "select awaiting, count(*) from bronze where awaiting is not null and try_cast(awaiting as bigint) is null group by 1 limit 10"),
        "monthly_by_measure": rows(con, "select measure_code, case_code, period_month, count(*), sum(value) filter (where province_code = '00') "
                                        "from nfz_waiting_times_monthly group by all order by all"),
    }
    return s


def gold(con, teryt: dict) -> dict:
    con.execute("create or replace table geo as select 'wojewodztwo' as level_name, terc_code, 'TERC:' || terc_code as "
                f"geography_key from (values {', '.join(repr((k,)) [:-2] + ')' for k in sorted(teryt))}) as t (terc_code)")
    geo_cte = env().from_string(MACROS + "{{ poc_geography__nfz_waiting_times() }}").render()
    con.execute(f"create or replace table gmap as with {geo_cte} select * from nfz_waiting_times")
    sql = render("warehouse/models/gold/poc_build_nfz_waiting_times.sql", {"poc_map_geography": "gmap"})
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
    g["measures"] = rows(con, "select measure_key, count(*), min(date_key), max(date_key), min(value), max(value) from build group by 1 order by 1")
    g["tests"] = schema_tests(con, f"warehouse/models/gold/_{SOURCE}.yml", f"poc_build_{SOURCE}", "build")
    g["map"] = [[c, k, teryt.get(k.removeprefix("TERC:"))] for c, k in con.execute("select source_code, geography_key from gmap order by 1").fetchall()]
    g["spot_PL"] = rows(con, "select measure_key, period_label, value from build where geography_key = 'PL' order by 1, 2")
    g["spot_mazowieckie_case1"] = rows(con, "select measure_key, period_label, value from build where geography_key = 'TERC:1400000' "
                                            "and measure_key like '%case1' order by 1")
    km = "read_csv('warehouse/seeds/kpi_measure.csv', all_varchar=true)"
    g["kpi_patterns"] = rows(con, f"select k.kpi_key, k.measure_key_pattern, count(b.measure_key), count(distinct b.geography_key) "
                                  f"from {km} k left join build b on b.measure_key like k.measure_key_pattern "
                                  f"where k.source_system = '{SOURCE}' group by all order by 1")
    g["kpi_rows_per_area_period_max"] = one(con, f"select max(c) from (select k.kpi_key, b.geography_key, b.date_key, count(*) c "
                                                 f"from {km} k join build b on b.measure_key like k.measure_key_pattern "
                                                 f"where k.source_system = '{SOURCE}' group by all)")
    bounds = re.findall(r"\('(waiting_list[a-z_]*)', 'nfz_waiting_times', '%', ([0-9e.]+), ([0-9e.]+)\)",
                        Path("warehouse/tests/metrics_value_range.sql").read_text())
    g["bounds"] = bounds
    g["outside_bounds"] = {
        k: one(con, f"select count(*) from {km} m join build b on b.measure_key like m.measure_key_pattern "
                    f"where m.kpi_key = '{k}' and m.source_system = '{SOURCE}' and (b.value < {lo} or b.value > {hi})")
        for k, lo, hi in bounds}
    return g


def teryt_names() -> dict:
    t = catalog().load_table(("silver", "teryt_terc")).scan(selected_fields=("terc_code", "name_pl", "unit_level")).to_arrow()
    return {r["terc_code"]: r["name_pl"] for r in t.to_pylist() if r["unit_level"] == "wojewodztwo"}


def licence() -> dict:
    from warehouse.publish import licensed_sources

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
    note(f"{SOURCE} scratch removed", removed)


def main(argv: list[str]) -> int:
    filter_text = argv[0] if argv else ""
    status = 0
    try:
        terms_page = terms()
        teryt = teryt_names()
        land = landing(filter_text)
        note(f"{SOURCE} landing (twice), inspect, verify", land)
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        note(f"{SOURCE} bronze profile", bronze_profile(con))
        s = silver(con)
        note(f"{SOURCE} silver", {k: v for k, v in s.items() if k != "profile"})
        many = {k: s["profile"].pop(k) for k in list(s["profile"]) if k.startswith(("many_places", "naive", "suppression"))}
        note(f"{SOURCE} silver profile", s["profile"])
        note(f"{SOURCE} many places and markers", many)
        g = gold(con, teryt)
        note(f"{SOURCE} gold", {k: v for k, v in g.items() if k not in ("map", "spot_PL", "spot_mazowieckie_case1")})
        note(f"{SOURCE} gold geography and spot values", {k: g[k] for k in ("map", "spot_PL", "spot_mazowieckie_case1")})
        note(f"{SOURCE} licence gate and v1.4 terms page", {**licence(), "terms_pages": {
            k: {"status": v["status"], "head": v["head"][:600]} for k, v in terms_page.items()}})
    except Exception as exc:  # noqa: BLE001
        status = 1
        import traceback

        traceback.print_exc()
        annotate("error", f"{SOURCE} trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
