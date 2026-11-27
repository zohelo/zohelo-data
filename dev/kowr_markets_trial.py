"""Dev trial of KOWR weekly grain reports (register PLS-033, #457): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_kowr_markets/, Iceberg namespace zz_dev_kowr_markets; everything written is
removed in `finally`. Landing reads both gov.pl pages and every grain report (5 s apart) twice (the second run may only
re-read the latest page's weeks and must land nothing), Bronze loads twice (the second must append nothing). Then a
real dbt build, twice, of the three Silver models, the eight code lists, poc_map_geography and poc_build_kowr_markets
with every dbt test attached to them (metrics_value_range excluded: no metrics model reads this source). dbt writes only
to a DuckDB file on the runner that plays the database "r2" (target prod); the production catalog is attached read-only
as "lake" and the other parents are views onto it; the Bronze table is a copy of the scratch table. Deleted before merge.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.secrets import Secret
import duckdb
import jinja2
import yaml

from ingestion.common import bulk_bronze, report
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze import kowr_markets as bronze_mod
from ingestion.to_landing import kowr_markets as landing_mod
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_kowr_markets"
SOURCE = "kowr_markets"
TABLE = "kowr_markets_cells"
REJECTED = "kowr_markets_rejected_files"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("kowr_markets_fob_prices", "kowr_markets_eu_prices", "kowr_markets_exchange_offers")
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_kowr_markets"]


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:3000]}", flush=True)


report.annotate = _capture
bulk_bronze.annotate = _capture
bulk_file.annotate = _capture


def note(title: str, data) -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, json.loads(text) if len(text) <= 3900 else {"truncated": text[:3800]})


def run(con, q):
    return con.execute(q).fetchall()


def one(con, q):
    return con.execute(q).fetchall()[0][0]


def rows(con, q, limit=60):
    return [[str(v) for v in r] for r in con.execute(q).fetchall()][:limit]




def rendered(value):
    env = jinja2.Environment()
    env.globals["env_var"] = lambda name, default=None: os.environ.get(name, default)
    if isinstance(value, dict):
        return {k: rendered(v) for k, v in value.items()}
    return env.from_string(value).render() if isinstance(value, str) else value


def dbt(work: Path, *args: str) -> tuple[int, dict]:
    cmd = ["dbt", *args, "--project-dir", str(WAREHOUSE), "--profiles-dir", str(work / "profiles"),
           "--target", "prod", "--target-path", str(work / "target"), "--no-partial-parse"]
    proc = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, "DUCKDB_TEMP_DIRECTORY": str(work / "tmp")})
    print(proc.stdout[-6000:], proc.stderr[-3000:], flush=True)
    results = {}
    path = work / "target" / "run_results.json"
    if args[0] != "parse" and path.exists():
        for r in json.loads(path.read_text())["results"]:
            results[r["unique_id"]] = (r["status"], r.get("failures"), (r.get("message") or "")[:200])
        path.unlink()
    return proc.returncode, results


def setup_dbt(work: Path, arrow) -> tuple[dict, list[str], list[str]]:
    """Profile with the local file as r2 and the catalog as lake; the Bronze copy and views for the other parents."""
    profiles = yaml.safe_load((WAREHOUSE / "profiles.yml").read_text())
    prod = dict(profiles["zohelo"]["outputs"]["prod"])
    attach = dict(prod["attach"][0], alias="lake")
    prod["path"] = str(work / "r2.duckdb")
    (work / "profiles").mkdir()
    (work / "tmp").mkdir()
    con = duckdb.connect(str(work / "r2.duckdb"))
    for ext in prod.get("extensions", []):
        run(con, f"install {ext}")
        run(con, f"load {ext}")
    for s in prod["secrets"]:
        s = {k: rendered(v) for k, v in s.items()}
        run(con, Secret.create(secret_type=s.pop("type"), name=s.pop("name"), **s).to_sql())
    a = {k: rendered(v) for k, v in attach.items()}
    try:
        run(con, Attachment(**{**a, "read_only": True}).to_sql())
        attach["read_only"] = True
    except duckdb.Error as exc:
        print("read-only attach refused, attaching without:", str(exc)[:300], flush=True)
        run(con, Attachment(**a).to_sql())
    prod["attach"] = [attach]
    (work / "profiles" / "profiles.yml").write_text(yaml.safe_dump({"zohelo": {"target": "prod", "outputs": {"prod": prod}}}))
    code, _ = dbt(work, "parse")
    if code:
        raise RuntimeError("dbt parse failed")
    manifest = json.loads((work / "target" / "manifest.json").read_text())
    nodes, sources = manifest["nodes"], manifest["sources"]
    selected = {f"model.zohelo.{m}" for m in SELECT}
    tests = [k for k, n in nodes.items() if n["resource_type"] == "test" and selected & set(n["depends_on"]["nodes"])
             and "metrics_value_range" not in k]
    parents = {p for k in [*selected, *tests] for p in nodes[k]["depends_on"]["nodes"]} - selected
    for schema in ("bronze", "silver", "gold"):
        run(con, f"create schema if not exists {schema}")
    con.register("bronze_arrow", arrow)
    run(con, f"create table bronze.{TABLE} as select * from bronze_arrow")
    con.unregister("bronze_arrow")
    views, missing = [], set()
    for p in sorted(parents):
        if p.startswith(f"source.zohelo.{SOURCE}."):
            continue
        n = nodes.get(p) or sources[p]
        schema, name = n["schema"], n.get("alias") or n.get("identifier") or n["name"]
        run(con, f"create schema if not exists {schema}")
        try:
            run(con, f'create view {schema}."{name}" as select * from lake.{schema}."{name}"')
            views.append(f"{schema}.{name}")
        except duckdb.CatalogException:  # not in production yet (an optional parent, poc_optional_ref)
            views.append(f"missing:{schema}.{name}")
            missing.add(p)
    con.close()
    aliases = {m: (nodes[f"model.zohelo.{m}"]["schema"], nodes[f"model.zohelo.{m}"]["alias"]) for m in SELECT}
    skipped = sorted(nodes[k]["name"] for k in tests if missing & set(nodes[k]["depends_on"]["nodes"]))
    own_tests = sorted(nodes[k]["name"] for k in tests if SOURCE in k)
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"], skipped, own_tests




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
    out: dict = {"runs": []}
    for n in (1, 2):
        CAPTURED.clear()
        start = time.monotonic()
        code = landing_mod.extract(60, landing_root=SCRATCH)
        summary = next((d for t, d in CAPTURED if "landing extract" in t), {})
        files, size, receipts = landing_objects()
        out["runs"].append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                            **{k: v for k, v in summary.items() if k != "gap_weeks"},
                            "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    found, latest, summary = landing_mod.discover()
    held = landing_mod.held(SCRATCH)
    out["inspect"] = {"listed": len(found), "latest": len(latest), "held": len(held),
                      "missing": sorted(set(found) - held)[:10], "gap_weeks": summary["gap_weeks"]}
    CAPTURED.clear()
    out["verify_exit"] = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH, specs=[{"entity": e} for e in sorted(held)])
    out["verify"] = [d for _, d in CAPTURED][-1:] if CAPTURED else []
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    entities = sorted(landing_mod.held(SCRATCH))
    for n in (1, 2):
        CAPTURED.clear()
        code = bronze_mod.bronze_load(entities, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for t, d in CAPTURED for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected", "loaded", "files")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected_n"] = len(rej)
    reasons: dict = {}
    for r in rej:
        text = str(r.get("error") or r.get("reason") or "")
        key = re.sub(r"\d", "9", text.split(":", 2)[-1] if text.count(":") >= 2 else text)[:90]
        reasons.setdefault(key, []).append(str(r.get("entity", ""))[6:])
    out["rejected_by_reason"] = {k: [len(v), v[:6]] for k, v in sorted(reasons.items(), key=lambda kv: -len(kv[1]))}
    diagnose(rej, arrow)
    return out, arrow


def diagnose(rej, arrow) -> None:
    """Logs only: every rejection with the text of the pages it concerns; the positions of the EU table's labels of
    one accepted and up to three rejected files."""
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    receipts = {r["entity"]: r for r in bulk_file.landed(client, bucket, f"{SCRATCH}/{SOURCE}/snapshot/data/")}
    accepted = sorted(set(arrow.column("entity").to_pylist()))[:1]
    shown_eu = 0
    for n, r in enumerate([*[{"entity": e, "error": "accepted"} for e in accepted], *rej]):
        entity, error = r.get("entity"), str(r.get("error") or r.get("reason"))
        print(f"@@@@@ {entity} :: {error}", flush=True)
        if n > 60 or entity not in receipts:
            continue
        try:
            texts, frags = bronze_mod.text_pages(bulk_bronze.read_raw(client, bucket, SOURCE, receipts[entity]))
        except Exception as exc:  # noqa: BLE001
            print(f"  unreadable: {exc}", flush=True)
            continue
        for i, t in enumerate(texts):
            keys = ("Eksportowe", "krajach UE", "giełdach towarowych")
            if error == "accepted" or any(k in t for k in keys):
                if error != "accepted":
                    print(f"----- page {i + 1}\n{t}", flush=True)
                if "krajach UE" in t and (error == "accepted" or "eu" in error) and shown_eu < 4:
                    shown_eu += 1
                    labels = [(f[0].strip()[:20], round(f[1]), round(f[2])) for f in frags[i]
                              if f[0].strip()[:6] in ("Cena w", "Pszeni", "Jęczmi", "Kukury", "Polska", "Austri")]
                    print(f"  positions page {i + 1}: {labels}", flush=True)
                if "krajach UE" in t and "values for" in error:
                    print(f"  all fragments page {i + 1}: {[(f[0].strip()[:14], round(f[1]), round(f[2])) for f in frags[i]]}", flush=True)


def bronze_profile(con) -> dict:
    v = "bronze"
    return {
        "rows": one(con, f"select count(*) from {v}"),
        "entities": one(con, f"select count(distinct entity) from {v}"),
        "weeks(min,max)": rows(con, f"select min(entity), max(entity) from {v}"),
        "rows_by_table(table,rows,files,pages)": rows(con, f"select table_code, count(*), count(distinct raw_sha256), "
                                                          f"string_agg(distinct page_number::varchar, ',') from {v} group by 1 order by 1"),
        "dup_keys": one(con, f"select count(*) from (select entity, table_code, row_group, row_label, column_label from {v} "
                             f"group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {v} where entity is null or table_code is null or row_label is null "
                              f"or column_label is null or value_text is null or raw_sha256 is null"),
        "non_numeric_values(table,column,value,n)": rows(con, f"select table_code, column_label, value_text, count(*) from {v} "
                                                             f"where column_label <> 'kontrola' and not regexp_full_match(value_text, '-?[0-9][0-9 ]*(,[0-9]+)?') "
                                                             f"group by all order by all", 40),
        "kontrola(table,group,row,files)": rows(con, f"select table_code, row_group, row_label, count(*) from {v} where column_label = 'kontrola' group by all order by 4 desc", 15),
        "zeros(table,column,n)": rows(con, f"select table_code, column_label, count(*) from {v} where value_text = '0' group by all order by all"),
        "stated_sources": rows(con, f"select table_code, stated_source, count(distinct entity) from {v} group by all order by all"),
        "fob_markets(group,market,files)": rows(con, f"select row_group, row_label, count(distinct entity) from {v} "
                                                    f"where table_code = 'fob' and column_label = 'cena' group by all order by all"),
        "exchange_goods(good,files)": rows(con, f"select row_label, count(distinct entity) from {v} where table_code = 'exchange' "
                                               f"and column_label = 'średnia' group by all order by all"),
        "fob_quote_dates_in_two_reports(date,n,distinct_values)": rows(con, f"""
            select period_text, count(distinct entity), count(distinct row_group || row_label || value_text) - count(distinct row_group || row_label)
            from {v} where table_code = 'fob' and column_label = 'cena' group by 1 having count(distinct entity) > 1 order by 1""", 20),
        "eu_pln_row_empty(files)": one(con, f"select count(distinct entity) - count(distinct entity) filter (where column_label = 'Cena w PLN/t') "
                                            f"from {v} where table_code = 'eu'"),
    }



def build(arrow, work: Path) -> tuple[dict, dict, dict, dict, list[str]]:
    """dbt build twice; returns (dbt summary, silver profile, gold checks, spot values, failures)."""
    failures: list[str] = []
    aliases, views, skipped, own_tests = setup_dbt(work, arrow)
    runs, fingerprints = [], []
    for attempt in (1, 2):
        code, results = dbt(work, "build", "--select", *SELECT, "--exclude", "metrics_value_range", *skipped)
        by_status: dict[str, int] = {}
        for st, _, _ in results.values():
            by_status[st] = by_status.get(st, 0) + 1
        bad = {k: v for k, v in results.items() if v[0] not in ("success", "pass")}
        runs.append({"exit": code, "nodes": len(results), "tests": sum(1 for k in results if k.startswith("test.")),
                     "status": by_status, "not_passing": bad,
                     "own_tests": {k.split(".")[2]: v[0] for k, v in results.items() if k.startswith("test.") and SOURCE in k}})
        if code or {k: v for k, v in bad.items() if v[0] != "warn"}:
            failures.append(f"dbt build {attempt}: exit {code}, {bad}")
        local = duckdb.connect(str(work / "r2.duckdb"), read_only=True)
        fp = {}
        for m, (s, a) in aliases.items():
            if m == "poc_map_geography" or m in SEED_ROWS:
                continue
            try:
                fp[m] = local.sql(f'select count(*), sum(hash(t)) from {s}."{a}" as t').fetchall()[0]
            except duckdb.Error:
                fp[m] = None
        fingerprints.append(fp)
        local.close()
    if fingerprints[0] != fingerprints[1]:
        failures.append("second dbt build changed a table")
    if any(v is None for v in fingerprints[-1].values()):
        failures.append(f"models not built: {[m for m, v in fingerprints[-1].items() if v is None]}")
        return {"runs": runs, "tests_skipped_parent_missing": skipped}, {}, {}, {}, failures
    con = duckdb.connect(str(work / "r2.duckdb"), read_only=True)
    rel = {m: f'{s}."{a}"' for m, (s, a) in aliases.items()}
    fob, eu, ex, b = (rel["kowr_markets_fob_prices"], rel["kowr_markets_eu_prices"], rel["kowr_markets_exchange_offers"],
                      rel["poc_build_kowr_markets"])
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped, "own_tests_selected": own_tests,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "fob": {"rows": one(con, f"select count(*) from {fob}"),
                "with_value": one(con, f"select count(value) from {fob}"),
                "dates(min,max,n)": rows(con, f"select min(quote_date), max(quote_date), count(distinct quote_date) from {fob}"),
                "by_measure(commodity,market,rows,values,first,last)": rows(con, f"select commodity_code, market_code, count(*), count(value), "
                                                                                 f"min(quote_date), max(quote_date) from {fob} group by all order by all"),
                "dup_grain": one(con, f"select count(*) from (select commodity_code, market_code, quote_date from {fob} group by all having count(*) > 1)"),
                "flags": rows(con, f"select coalesce(value_flag, 'value'), count(*) from {fob} group by 1 order by 1"),
                "null_keys": one(con, f"select count(*) from {fob} where commodity_code is null or market_code is null or quote_date is null or area_code is null"),
                "quote_minus_week_end_days(min,max)": rows(con, f"select min(quote_date - (report_week + 6)), max(quote_date - (report_week + 6)) from {fob}"),
                "value(min,max)": rows(con, f"select min(value), max(value) from {fob}")},
        "eu": {"rows": one(con, f"select count(*) from {eu}"),
               "weeks(min,max,n)": rows(con, f"select min(report_week), max(report_week), count(distinct report_week) from {eu}"),
               "flags": rows(con, f"select coalesce(value_flag, 'value'), count(*) from {eu} group by 1 order by 1"),
               "dup_grain": one(con, f"select count(*) from (select commodity_code, country_name, period_start from {eu} group by all having count(*) > 1)"),
               "periods(n,two_week,not_report_week)": rows(con, f"select count(distinct period_start), count(distinct period_start) filter (where period_end - period_start = 13), "
                                                               f"count(distinct period_start) filter (where period_start <> report_week) from {eu}"),
               "null_keys": one(con, f"select count(*) from {eu} where commodity_code is null or country_name is null or report_week is null"),
               "value(min,max)": rows(con, f"select min(value), max(value) from {eu}"),
               "poland_values": one(con, f"select count(value) from {eu} where country_name = 'Polska'")},
        "exchange": {"rows": one(con, f"select count(*) from {ex}"),
                     "weeks(min,max,n)": rows(con, f"select min(report_week), max(report_week), count(distinct report_week) from {ex}"),
                     "by_good(good,rows,avg_values)": rows(con, f"select commodity_name, count(*), count(price_avg) from {ex} group by 1 order by 1"),
                     "dup_grain": one(con, f"select count(*) from (select commodity_name, period_start from {ex} group by all having count(*) > 1)"),
                     "flags": rows(con, f"select coalesce(value_flag, 'value'), count(*) from {ex} group by 1"),
                     "no_stated_source": one(con, f"select count(*) from {ex} where stated_source is null"),
                     "avg(min,max)": rows(con, f"select min(price_avg), max(price_avg) from {ex}")},
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') "
              "else '' end || '|' || breakdown_json")
    g["dup_series_date"] = one(con, f"select count(*) from (select {series} as series_key, date_key from {b} group by all having count(*) > 1)")
    g["dup_series_date_without_source_code"] = one(con, f"select count(*) from (select measure_key, geography_key, breakdown_json, date_key "
                                                        f"from {b} group by all having count(*) > 1)")
    g["measures"] = one(con, f"select count(distinct measure_key) from {b}")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["series"] = one(con, f"select count(distinct {series}) from {b}")
    g["dates(min,max)"] = rows(con, f"select min(date_key), max(date_key) from {b}")
    g["by_geography(key,code,rows)"] = rows(con, f"select geography_key, geography_source_code, count(*) from {b} group by all order by all")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["silver_rows_with_value_not_in_gold"] = one(con, f"select count(value) from {fob}") - g["rows"]
    g["map_rows"] = rows(con, f"select source_code, geography_key from {rel['poc_map_geography']} where source_system = '{SOURCE}' order by 1")
    g["value_range_outside(50..1000)"] = one(con, f"select count(*) from {b} where value < 50 or value > 1000")
    k = f"from {b} where measure_key = 'kowr_markets:fob_export_prices:EUR_T:wheat.rouen' and is_total and geography_key <> '_X'"
    g["kpi_wheat_export_price_fob"] = {"rows": one(con, f"select count(*) {k}"),
                                       "max_rows_per_area_period": one(con, f"select coalesce(max(n), 0) from (select count(*) n {k} group by geography_key, date_key)"),
                                       "min": rows(con, f"select arg_min(period_label || ' ' || value, value) {k}"),
                                       "max": rows(con, f"select arg_max(period_label || ' ' || value, value) {k}")}
    spots = {
        "gold_fob(date,codes,geo,value)": rows(con, f"select period_label, measure_codes, geography_key, value from {b} "
                                                    f"where period_label in ('2023-03-29', '2025-07-02', '2026-10-21', '2026-11-19') order by 1, 2"),
        "silver_fob_null_2026-10-21": rows(con, f"select commodity_code, market_code, value_flag from {fob} where quote_date = '2026-10-21' and value is null"),
        "silver_fob_flagged": rows(con, f"select quote_date, commodity_code, market_code, value_flag from {fob} where value_flag = 'zl_t_mismatch' order by 1", 30),
        "eu_poland(week,commodity,value)": rows(con, f"select report_week, commodity_code, value from {eu} where country_name = 'Polska' "
                                                     f"and report_week in ('2023-03-20', '2025-06-23', '2026-10-12', '2026-11-09') order by 1, 2"),
        "eu_eu_average(week,commodity,value)": rows(con, f"select report_week, commodity_code, value from {eu} where country_name = 'Średnio w UE' "
                                                         f"and report_week in ('2026-10-12', '2026-11-09') order by 1, 2"),
        "eu_szwecja_wheat_2025-06-23": rows(con, f"select value, value_flag from {eu} where country_name = 'Szwecja' and commodity_code = 'wheat' and report_week = '2025-06-23'"),
        "exchange(week,good,min,max,avg)": rows(con, f"select report_week, commodity_name, price_min, price_max, price_avg from {ex} "
                                                     f"where report_week in ('2023-03-20', '2026-10-12', '2026-11-09') order by 1, 2"),
    }
    con.close()
    if g["dup_series_date"] or g["dup_series_date_without_source_code"] or g["null_values"] or not g["rows"]:
        failures.append("gold checks")
    if g["measures_with_two_frequencies"]:
        failures.append("a measure with two frequencies")
    if g["value_range_outside(50..1000)"]:
        failures.append("values outside 50..1000 EUR/t")
    return dbt_summary, s, g, spots, failures


def licence() -> dict:
    from warehouse.publish import licensed_sources
    from warehouse.seed_rows import rows as seed

    src = [r for r in seed("source") if r["source_key"] == SOURCE]
    lic = [r for r in seed("licence") if src and r["licence_key"] == src[0]["licence_key"]]
    return {"source_row": src, "licence_row": lic,
            "licensed_by_gate": SOURCE in licensed_sources(seed("source"), seed("licence")),
            "kpi_v2_map_rows": [r for r in seed("kpi_v2_map") if r.get("source_key") == SOURCE]}


def main(argv: list[str]) -> int:
    del argv
    status = 0
    try:
        land = landing()
        runs = land["runs"]
        landing_failed = (not runs[0].get("landed") or runs[1].get("landed") or runs[0].get("refused")
                          or runs[0].get("deferred_to_next_run")
                          or runs[1].get("landing_files_after") != runs[0].get("landing_files_after")
                          or land["inspect"]["missing"] or land["verify_exit"])
        note(f"{SOURCE} landing (twice), inspect, verify", land)
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        note(f"{SOURCE} bronze profile", bronze_profile(con))
        con.close()
        work = Path(tempfile.mkdtemp(prefix="kowr-dbt-"))
        try:
            d, s, g, spots, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append("landing: first run must land every week, second must land nothing, verify must pass")
        if b["loads"][1].get("appended_files"):
            failures.append("bronze: second load appended")
        note(f"{SOURCE} dbt build x2", d)
        note(f"{SOURCE} silver profile", s)
        note(f"{SOURCE} gold", {**g, "failures": failures})
        note(f"{SOURCE} spot values", spots)
        note(f"{SOURCE} licence gate", licence())
        if failures:
            status = 1
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
