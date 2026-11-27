"""Dev trial of EPO patent statistics (register EX-004, #3320): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_epo_statistics/, Iceberg namespace zz_dev_epo_statistics; everything written is
removed in `finally`. Landing runs twice (the second must write nothing), Bronze loads twice (the second must append
nothing). Then a real dbt build, twice, of the Silver model, the code lists, poc_map_geography and
poc_build_epo_statistics with every dbt test attached to them (the completeness test included); dbt writes only to a
DuckDB file on the runner that plays "r2"; the production catalog is attached read-only as "lake" and other parents
are views onto it. Deleted before merge (never on the PR branch).
"""

from __future__ import annotations

import json
import os
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
from ingestion.to_bronze import epo_statistics as epo_bronze
from ingestion.to_landing import epo_statistics as landing_mod
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_epo_statistics"
SOURCE = "epo_statistics"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("epo_statistics_counts",)
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_epo_statistics"]


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:3000]}", flush=True)


bulk_file.annotate = _capture
bulk_bronze.annotate = _capture
report.annotate = _capture


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
        code = landing_mod.extract(30, landing_root=SCRATCH)
        summary = next((d for t, d in CAPTURED if "landing extract" in t), {})
        files, size, receipts = landing_objects()
        out["runs"].append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                            "landed": len(summary.get("landed", [])), "unchanged": len(summary.get("unchanged", [])),
                            "refused": summary.get("refused"), "not_matched_once": summary.get("not_matched_once"),
                            "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    found, listed = landing_mod.discover()
    out["listed"] = {k: v for k, v in listed.items()}
    CAPTURED.clear()
    held = sorted(landing_mod.held(SCRATCH))
    out["verify_exit"] = bulk_file.main(SOURCE, "verify", SCRATCH, specs=[{"entity": e} for e in held])
    out["held"] = held
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    entities = sorted(landing_mod.held(SCRATCH))
    for n in (1, 2):
        CAPTURED.clear()
        code = epo_bronze.load(entities, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for t, d in CAPTURED for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, epo_bronze.TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, epo_bronze.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:400]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    v = "bronze"
    return {
        "rows": one(con, f"select count(*) from {v}"),
        "by_entity(entity,files,sheets,cells,text_cells_col3+)": rows(con, f"select entity, count(distinct raw_sha256), count(distinct sheet_name), count(*), "
                                                                         f"count(*) filter (where cell_type <> 'n' and column_number >= 3 and row_number > 3) from {v} group by 1 order by 1"),
        "cell_types": rows(con, f"select cell_type, count(*) from {v} group by 1 order by 1"),
        "non_numeric_in_value_area(value,n)": rows(con, f"select value_text, count(*) from {v} where cell_type <> 'n' and column_number >= 3 and row_number > 4 "
                                                       f"and entity not like 'poland%' and not regexp_full_match(value_text, '.*[A-Za-z].*') group by 1 order by 2 desc", 20),
        "null_keys": one(con, f"select count(*) from {v} where entity is null or raw_sha256 is null or sheet_name is null or row_number is null or column_number is null"),
        "empty_text": one(con, f"select count(*) from {v} where trim(value_text) = ''"),
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


def setup_dbt(work: Path, arrow):
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
    run(con, f"create table bronze.{epo_bronze.TABLE} as select * from bronze_arrow")
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
    own_tests = sorted(nodes[k]["name"] for k in tests if "epo_statistics" in k)
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"], skipped, own_tests


def build(arrow, work: Path):
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
                     "own_tests": {k.split(".")[2]: v[0] for k, v in results.items()
                                   if k.startswith("test.") and "epo_statistics" in k}})
        if code or {k: v for k, v in bad.items() if v[0] != "warn"}:
            failures.append(f"dbt build {attempt}: exit {code}, {str(bad)[:600]}")
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
    ob, b, mp = rel["epo_statistics_counts"], rel["poc_build_epo_statistics"], rel["poc_map_geography"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped, "own_tests_selected": own_tests,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    grain = "edition, table_code, area_label, coalesce(field_name, '_T'), period_year"
    s = {
        "rows": one(con, f"select count(*) from {ob}"),
        "by_table(table,edition,rows,areas,fields,first,last,not_ok)": rows(con, f"select table_code, edition, count(*), count(distinct area_label), count(distinct field_name), "
                                                                               f"min(period_year), max(period_year), count(*) filter (where cell_status <> 'ok') from {ob} group by 1, 2 order by 1, 2"),
        "dup_grain": one(con, f"select count(*) from (select {grain} from {ob} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {ob} where edition is null or table_code is null or area_label is null or period_year is null or raw_sha256 is null"),
        "cell_status": rows(con, f"select cell_status, count(*) from {ob} group by 1 order by 1"),
        "not_ok_cells(table,area,year,published)": rows(con, f"select table_code, area_label, period_year, value_published from {ob} where cell_status <> 'ok' order by all", 10),
        "zeros": one(con, f"select count(*) from {ob} where value = 0"),
        "status_dates": rows(con, f"select table_code, string_agg(distinct status_date, ', ') from {ob} group by 1 order by 1"),
        "PL_totals(table,year,value)": rows(con, f"select table_code, period_year, value from {ob} where area_code = 'PL' order by 1, 2"),
        "PL_regions_2025(region,value)": rows(con, f"select area_label, value from {ob} where table_code = 'poland_regions' and period_year = 2025 order by value desc"),
        "world_totals_2025": rows(con, f"select table_code, value from {ob} where area_label = 'Total' and period_year = 2025 and field_name is null order by 1"),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    # exactly poc_series_key() (macros/poc_blocks.sql), as tests/fact_grain.sql sees it
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || "
              "coalesce(geography_source_code, '') else '' end || '|' || breakdown_json")
    g["dup_series_date"] = one(con, f"select count(*) from (select {series} as series_key, date_key from {b} group by all having count(*) > 1)")
    g["by_block(block,rows,series,first,last)"] = rows(con, f"select block_id, count(*), count(distinct {series}), min(date_key), max(date_key) from {b} group by 1 order by 1")
    g["measures"] = rows(con, f"select measure_key, unit_code, count(*) from {b} group by 1, 2 order by 1")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["unmatched(_X: code,rows)"] = rows(con, f"select geography_source_code, count(*) from {b} where geography_key = '_X' group by 1 order by 1", 40)
    g["geographies"] = one(con, f"select count(distinct geography_key) from {b}")
    g["region_keys"] = rows(con, f"select geography_source_code, geography_key from {b} where block_id = 'regions' group by all order by 2")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["map_rows(matched,rows)"] = rows(con, f"select matched, count(*) from {mp} where source_system = '{SOURCE}' group by 1")
    g["silver_rows_not_in_gold"] = s["rows"] - g["rows"]
    g["max_rows_per_kpi_area_year"] = rows(con, f"select measure_key, max(n) from (select measure_key, geography_key, date_key, count(*) n from {b} "
                                                f"where measure_key like 'epo_statistics:by_country:%' group by all) group by 1")
    g["kpi_max(measure,max,where)"] = rows(con, f"select measure_key, max(value), arg_max(geography_key, value) from {b} where measure_key like 'epo_statistics:by_country:%' group by 1")
    spots = {
        "PL_by_country(measure,year,value)": rows(con, f"select measure_key, period_label, value from {b} where geography_key = 'PL' and block_id = 'by_country' order by 1, 2"),
        "PL_field_2025_top5": rows(con, f"select measure_key, breakdown_json, value from {b} where geography_key = 'PL' and block_id = 'by_field_country' and period_label = '2025' "
                                        f"order by measure_key, value desc limit 10"),
        "PL_regions(key,year,value)": rows(con, f"select geography_key, period_label, value from {b} where block_id = 'regions' order by 2, 3 desc"),
        "world_2025": rows(con, f"select measure_key, value from {b} where geography_key = 'WB:WLD' and period_label = '2025' and is_total"),
    }
    con.close()
    if g["dup_series_date"] or g["null_values"] or not g["rows"] or not g["columns_in_contract_order"]:
        failures.append("gold checks")
    if g["measures_with_two_frequencies"]:
        failures.append("a measure with two frequencies")
    if s["dup_grain"] or s["null_keys"]:
        failures.append("silver grain")
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
        landing_failed = (runs[0]["exit"] or runs[1]["exit"] or runs[1]["landed"] or not runs[0]["landed"]
                          or runs[1]["landing_files_after"] != runs[0]["landing_files_after"] or land["verify_exit"])
        note(f"{SOURCE} landing (twice), verify", land)
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        note(f"{SOURCE} bronze profile", bronze_profile(con))
        con.close()
        work = Path(tempfile.mkdtemp(prefix="epo-dbt-"))
        try:
            d, s, g, spots, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append("landing: first run must land, second must write nothing, verify must pass")
        if b["loads"][1].get("appended_files") or b["rejected"]:
            failures.append("bronze: second load appended, or a file was rejected")
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
