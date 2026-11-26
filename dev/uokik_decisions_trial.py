"""Dev trial for #3061/#3064 (UOKiK decisions, PLG-020 and PLG-021; test branch only, never on main).

Landing -> Bronze -> Silver -> Gold into scratch, each step twice. Landing through the source's own extractor into the
prefix zz_dev_uokik/ (pass 2 must be all `unchanged`), then landing inspect and verify. Bronze into the Iceberg
namespace zz_dev_uokik and lake prefix zz_dev_uokik/ (twice: the second appends nothing). Silver, the Gold entity table
and the build stage are the dbt SQL rendered with Jinja and run in local DuckDB over the scratch Bronze table, each
built twice, with the model tests; the licence rows come from warehouse/seed_rows. Everything written is removed in
`finally`. Only decyzje.uokik.gov.pl is read (never uokik.gov.pl); no party names are printed.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
from jinja2 import Environment
import yaml

from ingestion.common import report as report_module

SOURCE = "uokik_decisions"
SCRATCH = "zz_dev_uokik"
MACROS = Path(f"warehouse/macros/sources/{SOURCE}.sql").read_text()
COLLECTED: dict[str, list] = {}
ANNOTATE = report_module.annotate


def collect(level: str, title: str, data) -> None:
    COLLECTED.setdefault(title, []).append({"level": level, **(data if isinstance(data, dict) else {"data": data})})
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:3000]}", flush=True)


def note(title: str, data, level: str = "notice", quiet: bool = False) -> None:
    print(f"FULL {title}: {json.dumps(data, default=str, ensure_ascii=False)}", flush=True)
    if not quiet:
        ANNOTATE(level, title, data)


def q(con, sql: str, limit: int = 80) -> list:
    return [[v if isinstance(v, int | float) or v is None else str(v) for v in r] for r in con.execute(sql).fetchall()][
        :limit
    ]


def count(con, sql: str) -> int:
    return con.execute(f"select count(*) from ({sql})").fetchall()[0][0]


def render(path_or_text: str, refs: dict[str, str] | None = None, sources: dict[str, str] | None = None) -> str:
    text = Path(path_or_text).read_text() if path_or_text.endswith(".sql") else path_or_text
    return (
        Environment(extensions=["jinja2.ext.do"])
        .from_string(MACROS + text)
        .render(
            config=lambda **_k: "",
            ref=lambda name: (refs or {}).get(name, name),
            source=lambda _s, table: (sources or {})[table],
        )
    )


def shape(column: str) -> str:
    return f"left(regexp_replace(regexp_replace(coalesce({column}, '<null>'), '[0-9]', 'N', 'g'), '\\p{{L}}+', 'L', 'g'), 50)"


def model_tests(con, table: str, spec_path: str, model: str, extra: dict[str, str]) -> dict[str, str]:
    spec = yaml.safe_load(Path(spec_path).read_text())
    tests = dict(extra)
    for m in spec["models"]:
        if m["name"] != model:
            continue
        for column in m.get("columns", []):
            for test in column.get("tests", []):
                name = column["name"]
                if test == "not_null":
                    tests[f"not_null {name}"] = f"select 1 from {table} where {name} is null"
                elif test == "unique":
                    tests[f"unique {name}"] = f"select {name} from {table} group by 1 having count(*) > 1"
                elif isinstance(test, dict) and "accepted_values" in test:
                    values = test["accepted_values"]["arguments"]["values"]
                    listed = ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)
                    severity = (test["accepted_values"].get("config") or {}).get("severity", "error")
                    tests[f"accepted_values {name} ({severity})"] = (
                        f"select {name}, count(*) from {table} where {name} not in ({listed}) group by 1"
                    )
        for test in m.get("tests", []):
            if isinstance(test, dict) and "unique" in test:
                expr = test["unique"]["column_name"]
                tests[f"unique {expr[:40]}"] = f"select {expr} from {table} group by 1 having count(*) > 1"
    return tests


def trial() -> int:
    from ingestion.common import bulk_bronze
    from ingestion.common.r2 import catalog, req, s3
    from ingestion.to_bronze.uokik_decisions import load as bronze
    from ingestion.to_landing.patterns import bulk_file
    from ingestion.to_landing.uokik_decisions import extract

    for module in (extract, bulk_file, bulk_bronze):
        module.annotate = collect
        module.step_summary = lambda _m: None
    # --- Landing ---------------------------------------------------------------------------------------------
    exits = [extract.load(45, landing_root=SCRATCH) for _ in (1, 2)]
    loads = COLLECTED.get(f"{SOURCE} landing load", [])
    inspect_exit = extract.inspect(landing_root=SCRATCH)
    verify_exit = extract.verify(landing_root=SCRATCH)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    entities = extract.landed_entities(client, bucket, SCRATCH)
    receipts = [bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, e, SCRATCH))[-1] for e in entities]
    note(
        "uokik trial Landing (scratch zz_dev_uokik)",
        {
            "exits_load_load_inspect_verify": [*exits, inspect_exit, verify_exit],
            "loads": [{k: r.get(k) for k in ("categories", "read", "outcomes", "decisions_read", "requests", "retries",
                                             "slowest_seconds", "seconds", "failed", "labels_seen")} for r in loads],
            "inspect": COLLECTED.get(f"{SOURCE} landing inspect"),
            "verify": [r.get("files") for r in COLLECTED.get(f"{SOURCE} landing verify", [])],
            "files": len(receipts),
            "bytes": sum(r["size_bytes"] for r in receipts),
            "per_category": [[r["entity"], r["detail"].get("rows"), r["detail"].get("descendants"), r["size_bytes"]]
                             for r in receipts],
        },
    )
    # --- Bronze ----------------------------------------------------------------------------------------------
    bexits = [bronze.load(landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]
    cat = catalog()
    try:
        rejected = cat.load_table((SCRATCH, f"{SOURCE}_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:
        rejected = []
    con = duckdb.connect()
    b = "b_index"
    con.register(b, cat.load_table((SCRATCH, bronze.TABLE)).scan().to_arrow())
    note(
        "uokik trial Bronze (scratch)",
        {
            "exits": bexits,
            "loads": [{k: r.get(k) for k in ("candidates", "appended_files", "rows_added", "rejected")}
                      for r in COLLECTED.get(f"{SOURCE} Bronze from Landing", [])],
            "rows": count(con, f"select * from {b}"),
            "files": count(con, f"select distinct raw_sha256 from {b}"),
            "rejected_count": len(rejected),
            "rejected": [{k: r.get(k) for k in ("entity", "error")} for r in rejected][:3],
        },
    )
    note(
        "uokik trial Bronze profile",
        {
            "keys": q(con, f"select count(*), count(distinct unid), count(distinct decision_number), "
                           f"count(*) filter (where nullif(trim(unid), '') is null), "
                           f"count(*) filter (where nullif(trim(decision_number), '') is null), "
                           f"count(*) filter (where nullif(trim(decision_date), '') is null), "
                           f"count(*) filter (where nullif(trim(department), '') is null), "
                           f"min(year_category), max(year_category) from {b}"),
            "categories": q(con, f"select category, count(*), count(distinct year_category), min(year_category), "
                                 f"max(year_category), count(*) filter (where upper(decision_number) like '%DKK%') "
                                 f"from {b} group by 1 order by 1"),
            "unid_in_two_categories": count(con, f"select unid from {b} group by 1 having count(distinct category) > 1"),
            "number_repeated": q(con, f"select count(*), sum(c) from (select decision_number, count(*) c from {b} "
                                      f"group by 1 having count(*) > 1)"),
            "number_shapes": q(con, f"select {shape('decision_number')}, count(*), min(decision_number), "
                                    f"max(decision_number) from {b} group by 1 order by 2 desc", 25),
            "date_shapes": q(con, f"select {shape('decision_date')}, count(*), min(decision_date), max(decision_date) "
                                  f"from {b} group by 1 order by 2 desc", 10),
            "department": q(con, f"select department, count(*) from {b} group by 1 order by 2 desc", 40),
            "year_vs_category": q(con, f"select count(*) filter (where year_category <> right(trim(decision_date), 4) "
                                       f"and year_category <> left(trim(decision_date), 4)) from {b}"),
        },
        quiet=True,
    )
    # --- Silver ----------------------------------------------------------------------------------------------
    sources = {"uokik_decisions_index": b}
    sql = render("warehouse/models/silver/uokik_decisions/uokik_decisions.sql", {}, sources)
    silver_rows = []
    for _ in (1, 2):
        con.execute(f"create or replace table uokik_decisions as {sql}")
        silver_rows.append(count(con, "select * from uokik_decisions"))
    tests = model_tests(
        con, "uokik_decisions", "warehouse/models/silver/uokik_decisions/_uokik_decisions.yml", "uokik_decisions",
        {"complete": render("warehouse/tests/uokik_decisions_complete.sql", {"uokik_decisions": "uokik_decisions"})},
    )
    failing = {k: count(con, s) for k, s in tests.items()}
    note(
        "uokik trial Silver (local DuckDB over scratch Bronze)",
        {
            "rows_two_builds": silver_rows,
            "tests_failing_rows": failing,
            "failing_samples": {k: q(con, tests[k], 8) for k, c in failing.items() if c},
            "range": q(con, "select count(*), count(distinct unid), min(decision_date), max(decision_date), "
                            "count(*) filter (where decision_date is null), count(*) filter (where is_concentration), "
                            "count(distinct category_key) from uokik_decisions"),
            "per_year": q(con, "select decision_year, count(*), count(*) filter (where is_concentration) "
                               "from uokik_decisions group by 1 order by 1"),
            "concentration_by_category": q(con, "select category, count(*) filter (where is_concentration), count(*) "
                                                "from uokik_decisions group by 1 order by 2 desc"),
            "spot": q(con, "select decision_number, decision_date, department, category_key from uokik_decisions "
                           "where decision_number in ('DOZIK-6/2025') or decision_number like 'DKK-1/2025%' "
                           "order by 1", 10),
        },
    )
    # --- Gold ------------------------------------------------------------------------------------------------
    con.execute("create or replace table geo as select 'PL' as iso2, 'PL' as geography_key, 'country' as level_name")
    con.execute(
        f"create or replace view gmap as with {render('{{ poc_geography__uokik_decisions() }}')} "
        "select * from uokik_decisions"
    )
    refs = {"poc_map_geography": "gmap", "uokik_decisions": "uokik_decisions"}
    gold = {}
    for name in ("uokik_decision", "poc_build_uokik_decisions"):
        gsql = render(f"warehouse/models/gold/{name}.sql", refs)
        gold[name] = []
        for _ in (1, 2):
            con.execute(f"create or replace table {name} as {gsql}")
            gold[name].append(count(con, f"select * from {name}"))
    p = "poc_build_uokik_decisions"
    gtests = {}
    for model, table in (("uokik_decision", "uokik_decision"), (p, p)):
        gtests.update({f"{model}: {k}": v for k, v in model_tests(
            con, table, "warehouse/models/gold/_uokik_decisions.yml", model, {}).items()})
    gfail = {k: count(con, s) for k, s in gtests.items()}
    note(
        "uokik trial Gold build_uokik_decisions and uokik_decision",
        {
            "rows_two_builds": gold,
            "tests_failing_rows": gfail,
            "dup_series_date": count(
                con,
                f"select measure_key || '|' || geography_key || '|' || breakdown_json, date_key from {p} "
                "group by all having count(*) > 1",
            ),
            "entity_dup_unid": count(con, "select unid from uokik_decision group by 1 having count(*) > 1"),
            "unmatched_geography": count(con, f"select 1 from {p} where geography_key = '_X'"),
            "null_or_bad": count(con, f"select 1 from {p} where value is null or isnan(value) or value < 1 "
                                      "or date_key is null"),
            "measures": q(con, f"select block_id, measure_key, frequency, count(*), count(distinct breakdown_json), "
                               f"min(date_key), max(date_key), max(value) from {p} group by all order by 2"),
            "totals_match": q(con, f"select date_key, sum(value) filter (where not is_total), "
                                   f"sum(value) filter (where is_total) from {p} where block_id = 'decisions' "
                                   "group by 1 having sum(value) filter (where not is_total) <> "
                                   "sum(value) filter (where is_total) order by 1", 10),
            "concentration_years": q(con, f"select period_label, value, observation_status_key from {p} "
                                          "where block_id = 'concentration' order by 1"),
            "spot_2024": q(con, f"select measure_key, breakdown_json, value from {p} where period_label = '2024' "
                                "order by value desc", 20),
        },
    )
    from warehouse import seed_rows

    source_row = next(r for r in seed_rows.rows("source") if r["source_key"] == SOURCE)
    licence_row = next((r for r in seed_rows.rows("licence") if r["licence_key"] == source_row["licence_key"]), None)
    unit_map = [r for r in seed_rows.rows("unit_map") if r["source_system"] == SOURCE]
    units = {r["unit_key"] for r in seed_rows.rows("unit")}
    note(
        "uokik trial licence and units",
        {
            "source_licence_key": source_row["licence_key"],
            "licence_row": licence_row,
            "unit_map": unit_map,
            "unit_codes_in_build": q(con, f"select distinct unit_code from {p}"),
            "mapped_units_exist": all(r["unit_key"] in units for r in unit_map),
        },
    )
    return 0


def cleanup() -> None:
    from ingestion.common.r2 import catalog, req, s3

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
    for bucket_var, label in (
        ("STORAGE_LANDING_BUCKET_NAME", "landing_objects"),
        ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects"),
    ):
        bucket = req(bucket_var)
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
    note("uokik trial scratch removed", removed)


def main() -> int:
    status = 1
    try:
        status = trial()
    except Exception as exc:
        import traceback

        note(
            "uokik trial failed",
            {"error": f"{type(exc).__name__}: {str(exc)[:1500]}", "tb": traceback.format_exc()[-1800:]},
            "error",
        )
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main())
