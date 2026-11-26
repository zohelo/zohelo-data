"""Dev trial of CCKP ERA5 (register GL-036, #2421): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_cckp_era5/, Iceberg namespace zz_dev_cckp_era5; everything written is removed
in `finally`. The ERA5 selection is read from the API twice (the second read must write nothing), Bronze loads twice
(the second must append nothing). Then a real dbt build, twice, of the two Silver models, the eight code lists,
poc_map_geography (with this source's registration) and poc_build_cckp_era5 with every dbt test attached to them
(metrics_value_range excluded: no KPI of this source). dbt writes only to a DuckDB file on the runner that plays the
database "r2" (target prod); the production catalog is attached read-only as "lake" and the other parents are views
onto it; the Bronze table is a copy of the scratch table. Deleted before merge.
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

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.cckp_era5 import load as era5_load
from ingestion.to_landing.cckp_era5 import extract

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_cckp_era5"
SOURCE = "cckp_era5"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("cckp_era5_observations",)
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_cckp_era5"]


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


extract.annotate = _capture
bulk_bronze.annotate = _capture

def landing() -> dict:
    out: dict = {"runs": []}
    for n in (1, 2):
        CAPTURED.clear()
        start = time.monotonic()
        code = extract.main(["--mode", "load", "--budget-minutes", "120", "--landing-root", SCRATCH])
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        out["runs"].append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                            **{k: v for k, v in summary.items() if k != "sha256"}, "sha256": summary.get("sha256", "")[:16],
                            "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    for mode in ("inspect", "verify"):
        CAPTURED.clear()
        out[f"{mode}_exit"] = extract.main(["--mode", mode, "--landing-root", SCRATCH])
        out[mode] = next((d for t, d in CAPTURED if mode in t), {})
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = era5_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for t, d in CAPTURED for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, era5_load.TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, era5_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    v = "bronze"
    return {
        "rows": one(con, "select count(*) from bronze"),
        "values_by_member(member,rows,geocodes,min,max,dataset_period)": rows(con, f"select member, count(*), count(distinct geocode), min(period), max(period), "
                                                                               f"string_agg(distinct dataset_period, ',') from {v} group by 1 order by 1"),
        "pol_adm1_codes": rows(con, f"select string_agg(distinct geocode, ',' order by geocode) from {v} where member like '%POL_adm1%'"),
        "dup_keys": one(con, f"select count(*) from (select variable, aggregation, geocode, period from {v} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {v} where variable is null or aggregation is null or geocode is null or period is null"),
        "null_values": one(con, f"select count(*) from {v} where value is null"),
        "non_numeric_values": rows(con, f"select value, count(*) from {v} where value is not null "
                                        f"and not regexp_full_match(value, '-?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?') group by 1 order by 2 desc limit 15"),
        "value_shapes": rows(con, f"select regexp_replace(coalesce(value, 'null'), '[0-9]+', 'n', 'g'), count(*) from {v} group by 1 order by 2 desc limit 8"),
        "zeros_by_variable": rows(con, f"select variable, aggregation, count(*) from {v} where try_cast(value as double) = 0 group by all order by all"),
        "annual_keys_not_07": one(con, f"select count(*) from {v} where aggregation = 'annual' and right(period, 3) <> '-07'"),
        "country_codes_not_3_letters": rows(con, f"select geocode, count(*) from {v} where member like '%all_countries%' "
                                                 f"and not regexp_full_match(geocode, '[A-Z]{{3}}') group by 1", 20),
        "pr_annual_vs_sum_of_months(n,max_abs_diff,over_0.05)": rows(con, f"""
            with m as (select geocode, left(period, 4) y, sum(cast(value as double)) s, count(*) n from {v}
                       where variable = 'pr' and aggregation = 'monthly' group by all),
                 a as (select geocode, left(period, 4) y, cast(value as double) x from {v} where variable = 'pr' and aggregation = 'annual')
            select count(*), max(abs(a.x - m.s)), count(*) filter (where abs(a.x - m.s) > 0.05) from a join m using (geocode, y) where m.n = 12"""),
        "tas_annual_vs_mean_of_months(n,max_abs_diff,over_0.05)": rows(con, f"""
            with m as (select geocode, left(period, 4) y, avg(cast(value as double)) s, count(*) n from {v}
                       where variable = 'tas' and aggregation = 'monthly' group by all),
                 a as (select geocode, left(period, 4) y, cast(value as double) x from {v} where variable = 'tas' and aggregation = 'annual')
            select count(*), max(abs(a.x - m.s)), count(*) filter (where abs(a.x - m.s) > 0.05) from a join m using (geocode, y) where m.n = 12"""),
    }


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
    # duckdb 2.0: a statement's result must be fetched before register() (ResourceInUseException otherwise)
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
    con.register("queues_arrow", arrow)
    run(con, f"create table bronze.{era5_load.TABLE} as select * from queues_arrow")
    con.unregister("queues_arrow")
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
    # other sources' tests that read a table production does not have yet cannot run here
    skipped = sorted(nodes[k]["name"] for k in tests if missing & set(nodes[k]["depends_on"]["nodes"]))
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"], skipped


def build(arrow, work: Path) -> tuple[dict, dict, dict, list[str]]:
    """dbt build twice; returns (dbt summary, silver profile, gold checks, failures)."""
    failures: list[str] = []
    aliases, views, skipped = setup_dbt(work, arrow)
    runs, fingerprints = [], []
    for attempt in (1, 2):
        code, results = dbt(work, "build", "--select", *SELECT, "--exclude", "metrics_value_range", *skipped)
        by_status: dict[str, int] = {}
        for st, _, _ in results.values():
            by_status[st] = by_status.get(st, 0) + 1
        bad = {k: v for k, v in results.items() if v[0] not in ("success", "pass")}
        runs.append({"exit": code, "nodes": len(results), "tests": sum(1 for k in results if k.startswith("test.")),
                     "status": by_status, "not_passing": bad})
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
        return {"runs": runs, "tests_skipped_parent_missing": skipped}, {}, {}, failures
    con = duckdb.connect(str(work / "r2.duckdb"), read_only=True)
    con = duckdb.connect(str(work / "r2.duckdb"), read_only=True)
    rel = {m: f'{s}."{a}"' for m, (s, a) in aliases.items()}
    ob, b = rel["cckp_era5_observations"], rel["poc_build_cckp_era5"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "by_variable(var,agg,level,rows,areas,first,last)": rows(con, f"select variable, aggregation, geo_level, count(*), count(distinct geocode), "
                                                                    f"min(period_label), max(period_label) from {ob} group by all order by all"),
        "null_values": one(con, f"select count(*) from {ob} where value is null"),
        "dataset_period": rows(con, f"select dataset_period, count(*) from {ob} group by 1"),
        "dup_grain": one(con, f"select count(*) from (select variable, aggregation, geocode, period_label from {ob} group by all having count(*) > 1)"),
        "range(var,agg,level,min,max)": rows(con, f"select variable, aggregation, geo_level, min(value), max(value) from {ob} group by all order by all"),
        "extremes": rows(con, f"select variable, aggregation, arg_min(geocode || ' ' || period_label || ' ' || value, value), "
                              f"arg_max(geocode || ' ' || period_label || ' ' || value, value) from {ob} group by all order by all"),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    g["dup_series_date"] = one(con, f"select count(*) from (select measure_key || '|' || geography_key || case when geography_key = '_X' "
                                    f"then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json as series_key, date_key "
                                    f"from {b} group by all having count(*) > 1)")
    g["dup_series_date_without_source_code"] = one(con, f"select count(*) from (select measure_key, geography_key, breakdown_json, date_key "
                                                        f"from {b} where geography_key <> '_X' group by all having count(*) > 1)")
    g["measures"] = rows(con, f"select measure_key, frequency, count(*), count(distinct geography_key), min(date_key), max(date_key) from {b} group by all order by 1")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["unmatched(code,rows)"] = rows(con, f"select geography_source_code, count(*) from {b} where geography_key = '_X' group by 1 order by 1", 40)
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["map_rows"] = rows(con, f"select geography_key like 'TERC:%', count(*) from {rel['poc_map_geography']} where source_system = '{SOURCE}' group by 1")
    g["areas"] = rows(con, f"select case when geography_key like 'TERC:%' then 'voivodeship' when geography_key = '_X' then '_X' "
                           f"else 'country' end, count(distinct geography_key), count(*) from {b} group by 1 order by 1")
    g["silver_rows_not_in_gold"] = one(con, f"select count(*) from {ob}") - g["rows"]
    g["spot_PL"] = rows(con, f"select measure_codes, period_label, value from {b} where geography_key = 'PL' and "
                             "(period_label in ('2024', '2025', '2024-07', '2025-01') ) order by 1, 2")
    g["voivodeships_2024(key,code,tas,pr)"] = rows(con, f"select geography_key, any_value(geography_source_code), "
                                                        f"max(value) filter (where measure_codes = 'tas.annual'), max(value) filter (where measure_codes = 'pr.annual') "
                                                        f"from {b} where geography_key like 'TERC:%' and period_label = '2024' group by 1 order by 1", 20)
    g["voivodeship_mean_vs_PL_tas_annual(max_abs_diff)"] = one(con, f"select max(abs(v - p)) from (select date_key, avg(value) v from {b} "
                                                                    f"where geography_key like 'TERC:%' and measure_codes = 'tas.annual' group by 1) "
                                                                    f"join (select date_key, value p from {b} where geography_key = 'PL' and measure_codes = 'tas.annual') using (date_key)")
    g["KSV"] = rows(con, f"select geography_key, count(*) from {b} where geography_source_code = 'KSV' group by 1")
    g["value_range_by_level(measure,level,min,max)"] = rows(con, f"select measure_codes, geography_key like 'TERC:%', min(value), max(value) from {b} "
                                                                f"where geography_key <> '_X' group by all order by all")
    g["units_in_gold"] = rows(con, f"select unit_code, count(distinct measure_key) from {b} group by 1 order by 1")
    con.close()
    if g["dup_series_date"] or g["dup_series_date_without_source_code"] or g["null_values"] or not g["rows"]:
        failures.append("gold checks")
    return dbt_summary, s, g, failures


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
        note(f"{SOURCE} landing (twice), inspect, verify", land)
        runs = land["runs"]
        landing_failed = (runs[0].get("outcome") != "landed" or runs[1].get("outcome") != "unchanged"
                          or land["verify_exit"])
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        prof = bronze_profile(con)
        detail = ("values_by_member(member,rows,geocodes,min,max,dataset_period)", "pol_adm1_codes")
        note(f"{SOURCE} bronze profile", {k: v for k, v in prof.items() if k not in detail})
        note(f"{SOURCE} bronze members and codes", {k: prof[k] for k in detail})
        con.close()
        work = Path(tempfile.mkdtemp(prefix="cckp-dbt-"))
        try:
            d, s, g, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append("landing: first read must land, second must be unchanged, verify must pass")
        if b["loads"][1].get("appended_files") or b["rejected"]:
            failures.append("bronze: second load appended or a file was rejected")
        note(f"{SOURCE} dbt build x2", d)
        note(f"{SOURCE} silver profile", s)
        spots = ("spot_PL", "voivodeships_2024(key,code,tas,pr)")
        note(f"{SOURCE} gold", {**{k: v for k, v in g.items() if k not in spots}, "failures": failures})
        note(f"{SOURCE} gold spot values", {k: g.get(k) for k in spots})
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
