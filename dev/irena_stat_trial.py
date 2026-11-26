"""Dev trial of IRENASTAT (register GL-035, #1268): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_irena_stat/, Iceberg namespace zz_dev_irena_stat; everything written is removed
in `finally`. The table is read from the PxWeb API twice (the second read must write nothing), Bronze loads twice (the
second must append nothing). Then a real dbt build, twice, of the Silver model, the eight code lists,
poc_map_geography (with this source's registration) and poc_build_irena_stat with every dbt test attached to them
(metrics_value_range excluded: the metrics models read the shared facts; their rows, one row per source x area x
period and the value bounds are checked on the build stage with the same filter, is_total and not _X). dbt writes only
to a DuckDB file on the runner that plays the database "r2" (target prod); the production catalog is attached
read-only as "lake" and the other parents are views onto it; the Bronze table is a copy of the scratch table. Deleted
before merge (never on the PR branch).
"""

from __future__ import annotations

from collections import Counter
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile

from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.secrets import Secret
import duckdb
import jinja2
import yaml

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.irena_stat import load as irena_load
from ingestion.to_landing.irena_stat import extract

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_irena_stat"
SOURCE = "irena_stat"
ENTITY = "country_elecstat"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("irena_stat_electricity",)
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_irena_stat"]
KPIS = {"renewable_capacity": "irena_stat:elecstat:MW:capacity.total_renewable",
        "renewable_generation": "irena_stat:elecstat:GWh:generation.total_renewable"}


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:3000]}", flush=True)


extract.annotate = _capture
bulk_bronze.annotate = _capture


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
        code = extract.main(["--mode", "load", "--budget-minutes", "120", "--landing-root", SCRATCH])
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        detail = summary.get(ENTITY) or {}
        out["runs"].append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                            "published": summary.get("published"), "states": summary.get("states"),
                            "error": summary.get("error"),
                            **{k: v for k, v in detail.items() if k not in ("sha256", "key", "variables")},
                            "sha256": str(detail.get("sha256", ""))[:16],
                            "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    for mode in ("inspect", "verify"):
        CAPTURED.clear()
        out[f"{mode}_exit"] = extract.main(["--mode", mode, "--landing-root", SCRATCH])
        out[mode] = {k: v for k, v in next((d for t, d in CAPTURED if mode in t), {}).items() if k != "mode"}
    return out


def landing_markers() -> dict:
    """Cells of the landed answers by value and status: the PxWeb signs and how many cells carry them."""
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = extract.landed(client, bucket, SCRATCH, ENTITY)
    body = bulk_bronze.read_raw(client, bucket, SOURCE, held[-1])
    counts: Counter = Counter()
    members = 0
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        for name in archive.namelist():
            if name == "metadata.json":
                continue
            members += 1
            data = json.loads(archive.read(name))
            values, status = data["value"], data.get("status") or {}
            for i, v in enumerate(values):
                flag = status[i] if isinstance(status, list) else status.get(str(i))
                counts[f"{'null' if v is None else ('zero' if v == 0 else 'number')}|{flag}"] += 1
    return {"data_members": members, "cells_by_value_and_status": dict(counts), "cells": sum(counts.values())}


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = irena_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for t, d in CAPTURED for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, irena_load.TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, irena_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    v = "bronze"
    return {
        "rows": one(con, f"select count(*) from {v}"),
        "countries": one(con, f"select count(distinct country_code) from {v}"),
        "technologies": one(con, f"select count(distinct technology_label) from {v}"),
        "years(min,max)": rows(con, f"select min(year_label), max(year_label) from {v}"),
        "rows_by_data_type_grid": rows(con, f"select data_type_label, grid_label, count(*) from {v} group by all order by all"),
        "dup_keys": one(con, f"select count(*) from (select country_code, technology_label, data_type_label, grid_label, year_label "
                             f"from {v} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {v} where country_code is null or technology_label is null or data_type_label is null "
                              f"or grid_label is null or year_label is null"),
        "null_values(status)": rows(con, f"select status, count(*) from {v} where value is null group by 1"),
        "status_on_values": rows(con, f"select status, count(*) from {v} where value is not null and status is not null group by 1"),
        "non_numeric_values": rows(con, f"select value, count(*) from {v} where value is not null "
                                        f"and not regexp_full_match(value, '-?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?') group by 1 order by 2 desc limit 15"),
        "zeros": one(con, f"select count(*) from {v} where try_cast(value as double) = 0"),
        "negatives": one(con, f"select count(*) from {v} where try_cast(value as double) < 0"),
        "codes_per_label(tech,datatype,grid,year)": rows(con, f"select count(distinct technology_code || technology_label), count(distinct data_type_code || data_type_label), "
                                                              f"count(distinct grid_code || grid_label), count(distinct year_code || year_label) from {v}"),
        "members": one(con, f"select count(distinct member) from {v}"),
    }


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
    run(con, f"create table bronze.{irena_load.TABLE} as select * from bronze_arrow")
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
    own_tests = sorted(nodes[k]["name"] for k in tests if "irena_stat" in k)
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"], skipped, own_tests


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
                     "own_tests": {k.split(".")[-2] if k.count(".") > 2 else k: v[0] for k, v in results.items()
                                   if k.startswith("test.") and "irena_stat" in k}})
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
    ob, b = rel["irena_stat_electricity"], rel["poc_build_irena_stat"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped, "own_tests_selected": own_tests,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "rows": one(con, f"select count(*) from {ob}"),
        "by_measure(measure,unit,rows,countries,techs,first,last)": rows(con, f"select measure, unit_code, count(*), count(distinct country_code), "
                                                                            f"count(distinct technology), min(year), max(year) from {ob} group by all order by all"),
        "by_grid(grid,rows)": rows(con, f"select grid_connection, count(*) from {ob} group by 1 order by 1"),
        "release": rows(con, f"select release, table_id, count(*) from {ob} group by all"),
        "null_values(status)": rows(con, f"select status, count(*) from {ob} where value is null group by 1"),
        "dup_grain": one(con, f"select count(*) from (select country_code, technology, data_type, grid_connection, year from {ob} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {ob} where country_code is null or technology is null or measure is null or unit_code is null "
                              f"or grid_connection is null or year is null"),
        "zeros(measure,rows)": rows(con, f"select measure, count(*) from {ob} where value = 0 group by 1 order by 1"),
        "technologies_without_values": rows(con, f"select technology from (select distinct technology from {ob}) except "
                                                 f"select distinct technology from {ob} where value is not null"),
        "on_plus_off_vs_all(n,max_rel_diff,over_1pct)": rows(con, f"""
            with g as (select country_code, technology, measure, year,
                              max(value) filter (where grid_connection = 'All') a,
                              sum(value) filter (where grid_connection <> 'All') p
                       from {ob} group by all)
            select count(*), max(abs(a - p) / nullif(a, 0)), count(*) filter (where abs(a - p) > 0.01 * abs(a) + 0.01)
            from g where a is not null and p is not null"""),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') "
              "else '' end || '|' || breakdown_json")
    g["dup_series_date"] = one(con, f"select count(*) from (select {series} as series_key, date_key from {b} group by all having count(*) > 1)")
    g["dup_series_date_without_source_code"] = one(con, f"select count(*) from (select measure_key, geography_key, breakdown_json, date_key "
                                                        f"from {b} where geography_key <> '_X' group by all having count(*) > 1)")
    g["measures"] = one(con, f"select count(distinct measure_key) from {b}")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["series"] = one(con, f"select count(distinct {series}) from {b}")
    g["dates(min,max)"] = rows(con, f"select min(date_key), max(date_key) from {b}")
    g["unmatched(code,rows)"] = rows(con, f"select geography_source_code, count(*) from {b} where geography_key = '_X' group by 1 order by 1", 40)
    g["countries_matched"] = one(con, f"select count(distinct geography_key) from {b} where geography_key <> '_X'")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["status"] = rows(con, f"select observation_status_key, count(*) from {b} group by 1")
    g["is_total(rows)"] = rows(con, f"select is_total, count(*) from {b} group by 1 order by 1")
    g["silver_rows_with_value_not_in_gold"] = one(con, f"select count(*) from {ob} where value is not null") - g["rows"]
    g["units_in_gold"] = rows(con, f"select unit_code, count(distinct measure_key) from {b} group by 1 order by 1")
    g["map_rows"] = one(con, f"select count(*) from {rel['poc_map_geography']} where source_system = '{SOURCE}'")
    kpi = {}
    for k, pattern in KPIS.items():
        f = f"from {b} where measure_key like '{pattern}' and is_total and geography_key <> '_X'"
        kpi[k] = {"metrics_rows": one(con, f"select count(*) {f}"),
                  "max_rows_per_area_period": one(con, f"select coalesce(max(n), 0) from (select count(*) n {f} group by geography_key, date_key)"),
                  "min": rows(con, f"select arg_min(geography_key || ' ' || period_label || ' ' || value, value) {f}"),
                  "max": rows(con, f"select arg_max(geography_key || ' ' || period_label || ' ' || value, value) {f}"),
                  "areas": one(con, f"select count(distinct geography_key) {f}")}
    g["kpi_checks"] = kpi
    spots = {
        "PL_total_renewable(measure,year,value)": rows(con, f"select measure_codes, period_label, value from {b} where geography_key = 'PL' "
                                                             f"and is_total and measure_codes like '%.total_renewable' and period_label >= '2019' order by 1, 2"),
        "PL_2024_by_technology(codes,value)": rows(con, f"select measure_codes, value from {b} where geography_key = 'PL' and is_total "
                                                         f"and period_label = '2024' order by 1", 60),
        "PL_2024_capacity_by_grid(tech,grid,value)": rows(con, f"select measure_codes, breakdown_json, value from {b} where geography_key = 'PL' "
                                                              f"and period_label = '2024' and measure_codes in ('capacity.total_renewable', 'capacity.solar_photovoltaic') order by 1, 2"),
        "DE_2024_total_renewable": rows(con, f"select measure_codes, value from {b} where geography_key = 'DE' and is_total and period_label = '2024' "
                                             f"and measure_codes like '%.total_renewable' order by 1"),
    }
    con.close()
    if g["dup_series_date"] or g["dup_series_date_without_source_code"] or g["null_values"] or not g["rows"]:
        failures.append("gold checks")
    if g["measures_with_two_frequencies"]:
        failures.append("a measure with two frequencies")
    if any(v["max_rows_per_area_period"] > 1 for v in kpi.values()):
        failures.append("kpi: more than one row per area x period")
    return dbt_summary, s, g, spots, failures


def licence() -> dict:
    from warehouse.publish import licensed_sources
    from warehouse.seed_rows import rows as seed

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
    del argv
    status = 0
    try:
        land = landing()
        runs = land["runs"]
        landing_failed = (runs[0].get("outcome") != "landed" or runs[1].get("outcome") not in ("current", "unchanged")
                          or runs[1].get("landing_files_after") != runs[0].get("landing_files_after")
                          or land["verify_exit"])
        land["markers"] = landing_markers() if not runs[0].get("error") else {}
        note(f"{SOURCE} landing (twice), inspect, verify, markers", land)
        if runs[0].get("error"):
            raise RuntimeError(f"landing failed: {runs[0]['error']}")
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        note(f"{SOURCE} bronze profile", bronze_profile(con))
        con.close()
        work = Path(tempfile.mkdtemp(prefix="irena-dbt-"))
        try:
            d, s, g, spots, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append("landing: first read must land, second must write nothing, verify must pass")
        if b["loads"][1].get("appended_files") or b["rejected"]:
            failures.append("bronze: second load appended or a file was rejected")
        note(f"{SOURCE} dbt build x2", d)
        note(f"{SOURCE} silver profile", s)
        note(f"{SOURCE} gold", {**g, "failures": failures})
        note(f"{SOURCE} gold spot values", spots)
        if failures:
            status = 1
        note(f"{SOURCE} licence gate", licence())
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
