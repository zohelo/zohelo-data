"""Dev trial of KG PSP interventions (register EX-003, #2970): Landing → Bronze → Silver → Gold into scratch.

Args: ``profile`` (reachability, Landing twice, Bronze twice, layout of every workbook: members, sheets, titles and
the first rows of the sheets whose title names voivodeships) or ``full`` (default: the same without the row dump,
then the Silver model and its tests, the completeness test and the Gold build stage rendered with Jinja from
warehouse/ and run in local DuckDB over the scratch Bronze table, each built twice).
Landing prefix and lake prefix zz_dev_kgpsp/, Iceberg namespace zz_dev_kgpsp; everything written is removed in
`finally`. Deleted before merge (test branch only).
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import socket
import sys

import duckdb
from jinja2 import Environment
import yaml

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze import kgpsp_interventions as kb
from ingestion.to_landing import kgpsp_interventions as kl
from ingestion.to_landing.patterns import bulk_file

SCRATCH = "zz_dev_kgpsp"
SOURCE = "kgpsp_interventions"
CAPTURED: list = []


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:3000]}", flush=True)


bulk_bronze.annotate = _capture


def note(title: str, data) -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, json.loads(text) if len(text) <= 3800 else {"truncated": text[:3700]})


def one(con, q):
    return con.execute(q).fetchone()[0]


def rows(con, q, limit=60):
    return [[str(v) for v in r] for r in con.execute(q).fetchall()][:limit]


def reachable() -> dict:
    out = {}
    try:
        addr = socket.getaddrinfo("www.gov.pl", 443, socket.AF_INET)[0][4]
        s = socket.create_connection(addr[:2], timeout=20)
        s.close()
        out["tcp_v4"] = "ok"
    except OSError as exc:
        out["tcp_v4"] = f"{type(exc).__name__}: {str(exc)[:80]}"
    return out


def landing() -> dict:
    import ingestion.to_landing.kgpsp_interventions as mod
    from ingestion.common import report

    out: dict = {"extract_exit": []}
    report_annotate = report.annotate
    report.annotate = _capture
    try:
        for _ in (1, 2):
            CAPTURED.clear()
            out["extract_exit"].append(mod.extract(40, landing_root=SCRATCH))
            out.setdefault("runs", []).append({k: v for t, d in CAPTURED for k, v in d.items()
                                               if k in ("planned", "landed", "unchanged", "refused", "deferred_to_next_run")})
    finally:
        report.annotate = report_annotate
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = {}
    for entity in sorted(kl.held(SCRATCH)):
        copies = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, entity, SCRATCH))
        held[entity] = [len(copies), copies[-1]["size_bytes"], copies[-1]["version_token"], copies[-1]["version_basis"]]
    out["held"] = held
    out["files"] = len(held)
    out["bytes"] = sum(v[1] for v in held.values())
    out["verify_exit"] = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH, specs=[{"entity": e} for e in held])
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"exit": [], "loads": []}
    for _ in (1, 2):
        CAPTURED.clear()
        out["exit"].append(kb.bronze_load(kb._entities(SCRATCH), landing_root=SCRATCH, namespace=SCRATCH))
        out["loads"].append([{k: d.get(k) for k in ("candidates", "appended_files", "rows_added", "rejected")} for _t, d in CAPTURED])
    cat = catalog()
    table = cat.load_table((SCRATCH, f"{SOURCE}_cells"))
    arrow = table.scan().to_arrow()
    out["data_files"] = len(list(table.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rejected = cat.load_table((SCRATCH, f"{SOURCE}_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or r.get("reason") or "")[:300]} for r in rejected]
    return out, arrow


def layout(con, dump: bool) -> None:
    con.execute("""create or replace table titles as
        select entity, member, sheet_name, max(row_number) as nrows, max(column_number) as ncols, count(*) as cells,
               string_agg(value_text, ' ' order by row_number, column_number) filter (where row_number <= 4) as title
        from bronze_cells group by all""")
    per_entity = rows(con, "select entity, count(distinct member), count(*), sum(cells) from titles group by 1 order by 1")
    note("kgpsp layout per year", {"entity_members_sheets_cells": per_entity})
    allt = con.execute("select entity, member, sheet_name, nrows, ncols, left(title, 220) from titles order by all").fetchall()
    for r in allt:
        print("SHEET", json.dumps([str(v) for v in r], ensure_ascii=False), flush=True)
    newest = max(r[0] for r in allt)
    oldest = min(r[0] for r in allt)
    for ent in (newest, oldest):
        sample = [[r[1][-50:], r[2], r[3], r[4], (r[5] or "")[:120]] for r in allt if r[0] == ent]
        note(f"kgpsp sheets {ent}", {"n": len(sample), "sheets": sample[:24]})
    if dump:
        for ent in (newest, oldest):
            for r in [x for x in allt if x[0] == ent]:
                cells = con.execute(
                    "select row_number, string_agg('c' || column_number || '=' || left(value_text, 40), ' | ' order by column_number) "
                    "from bronze_cells where entity = ? and member = ? and sheet_name = ? and row_number <= 75 and column_number <= 30 "
                    "group by 1 order by 1", [ent, r[1], r[2]]).fetchall()
                print(f"=== {ent} :: {r[1]} :: {r[2]}", flush=True)
                for rn, line in cells:
                    print(f"  r{rn}: {line}", flush=True)


def bronze_profile(con) -> dict:
    return {
        "dup_cell_keys": one(con, "select count(*) from (select entity, member, sheet_name, row_number, column_number from bronze_cells group by all having count(*) > 1)"),
        "null_keys": one(con, "select count(*) from bronze_cells where entity is null or row_number is null or column_number is null or raw_sha256 is null"),
        "cell_types": rows(con, "select cell_type, count(*) from bronze_cells group by 1 order by 1"),
        "non_numeric_short": rows(con, "select value_text, count(*) from bronze_cells where length(value_text) <= 3 "
                                       "and not regexp_matches(value_text, '^-?[0-9]+$') group by 1 order by 2 desc limit 25"),
    }


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    macros = "\n".join(p.read_text() for p in sorted(Path("warehouse/macros").rglob("*.sql")))
    return Environment(extensions=["jinja2.ext.do"]).from_string(macros + Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda name: refs.get(name, name), source=lambda _s, t: sources[t],
        var=lambda *_a, **_k: None, return_=None)


def schema_tests(con, model: str, spec_path: str) -> dict:
    spec = yaml.safe_load(Path(spec_path).read_text())
    m = next(x for x in spec["models"] if x["name"] == model)
    results = {}
    for t in m.get("tests", []):
        if isinstance(t, dict) and "unique" in t:
            expr = t["unique"]["column_name"]
            results["unique"] = one(con, f"select count(*) from (select {expr} k from {model} group by 1 having count(*) > 1)")
    for col in m.get("columns", []):
        for t in col.get("tests", []) or []:
            if t == "not_null":
                results[f"not_null {col['name']}"] = one(con, f"select count(*) from {model} where {col['name']} is null")
            elif isinstance(t, dict) and "accepted_values" in t:
                vals = t["accepted_values"]["arguments"]["values"]
                lst = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)
                results[f"accepted_values {col['name']}"] = one(
                    con, f"select count(*) from {model} where {col['name']} is not null and cast({col['name']} as varchar) not in ({lst})")
    return results


CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code", "unit_name",
            "geography_source_code", "geography_key", "frequency", "date_key", "period_label", "breakdown_json",
            "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key", "source_version", "raw_sha256"]


def silver_gold(con) -> None:
    model = f"{SOURCE}_tables"
    sql = render(f"warehouse/models/silver/{SOURCE}/{model}.sql", {}, {f"{SOURCE}_cells": "bronze_cells"})
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table {model} as {sql}")
        counts.append(one(con, f"select count(*) from {model}"))
    s = {"rows_two_builds": counts, "tests": schema_tests(con, model, f"warehouse/models/silver/{SOURCE}/_{SOURCE}.yml")}
    complete = render(f"warehouse/tests/{model}_complete.sql", {model: model}, {})
    s["complete_failures"] = rows(con, f"select * from ({complete})", 20)
    s["per_measure"] = rows(con, f"select measure_code, count(*), count(distinct area_name), min(period_year), max(period_year), "
                                 f"min(value), max(value), count(*) filter (where value is null) from {model} group by all order by all")
    s["status"] = rows(con, f"select cell_status, count(*) from {model} group by 1 order by 1")
    s["PL"] = rows(con, f"select measure_code, period_year, value from {model} where area_name = 'polska' and period_year in (2016, 2024, 2025) order by all")
    note("kgpsp silver", s)
    # Gold: geography CTE of the source's registration macro over the TERYT voivodeship names
    geo = Path(f"warehouse/macros/sources/{SOURCE}.sql").read_text()
    cte_macro = Environment().from_string(geo + "{{ poc_geography__" + SOURCE + "() }}").render()
    cte = re.search(r"\(\s*(select.*)\)\s*$", cte_macro.strip(), re.S).group(1)
    con.execute("""create or replace table geo as
        select * from (values
        ('dolnośląskie','TERC:02'),('kujawsko-pomorskie','TERC:04'),('lubelskie','TERC:06'),('lubuskie','TERC:08'),
        ('łódzkie','TERC:10'),('małopolskie','TERC:12'),('mazowieckie','TERC:14'),('opolskie','TERC:16'),
        ('podkarpackie','TERC:18'),('podlaskie','TERC:20'),('pomorskie','TERC:22'),('śląskie','TERC:24'),
        ('świętokrzyskie','TERC:26'),('warmińsko-mazurskie','TERC:28'),('wielkopolskie','TERC:30'),
        ('zachodniopomorskie','TERC:32')) as t (name_pl, geography_key)""")
    con.execute("create or replace view geo_v as select name_pl, geography_key, 'wojewodztwo' as level_name from geo")
    con.execute(f"create or replace table gmap as {cte.replace('from geo as g', 'from geo_v as g')}")
    build_sql = render(f"warehouse/models/gold/poc_build_{SOURCE}.sql", {model: model, "poc_map_geography": "gmap"}, {})
    gold = []
    for _ in (1, 2):
        con.execute(f"create or replace table build as {build_sql}")
        gold.append(one(con, "select count(*) from build"))
    g = {"rows_two_builds": gold}
    g["dup_series_date"] = one(con, "select count(*) from (select measure_key || '|' || geography_key || '|' || breakdown_json, date_key "
                                    "from build group by all having count(*) > 1)")
    g["freq_per_measure"] = one(con, "select count(*) from (select measure_key from build group by 1 having count(distinct frequency) > 1)")
    g["unmatched"] = rows(con, "select geography_source_code, count(*) from build where geography_key = '_X' group by 1")
    g["null_value"] = one(con, "select count(*) from build where value is null")
    g["measures"] = rows(con, "select measure_key, unit_code, count(*), count(distinct geography_key), min(period_label), max(period_label) from build group by all order by 1")
    g["PL"] = rows(con, "select split_part(measure_key, ':', 4), period_label, value from build where geography_key = 'PL' "
                        "and period_label in ('2016', '2025') order by all")
    g["columns_in_contract_order"] = [d[0] for d in con.execute("select * from build limit 0").description] == CONTRACT
    g["regions_sum_vs_PL_max_abs_diff"] = rows(con, "select measure_key, max(abs(r - p)) from (select measure_key, date_key, "
                                                    "sum(value) filter (where geography_key like 'TERC:%') r, max(value) filter (where geography_key = 'PL') p "
                                                    "from build group by all) group by 1 order by 1")
    note("kgpsp gold", g)


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
    note("kgpsp scratch removed", removed)


def main(argv: list[str]) -> int:
    mode = argv[0] if argv else "full"
    status = 0
    try:
        r = reachable()
        if r["tcp_v4"] != "ok":
            note("kgpsp www.gov.pl not reachable", r)
            return 1
        land = landing()
        note("kgpsp landing", land)
        b, arrow = bronze()
        con = duckdb.connect()
        con.register("bronze_cells", arrow)
        b["profile"] = bronze_profile(con)
        note("kgpsp bronze", b)
        layout(con, dump=mode == "profile")
        if mode != "profile":
            silver_gold(con)
    except Exception as exc:  # noqa: BLE001
        status = 1
        annotate("error", "kgpsp trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
        raise
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
