"""Dev trial of GUS STRATEG (register PLS-003, #2430): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_gus_strateg/, Iceberg namespace zz_dev_gus_strateg; everything written is
removed in `finally`. The SOR selection is read from the API twice (the second read must write nothing), Bronze
loads twice (the second must append nothing). Then a real dbt build, twice, of the two Silver models, the eight code
lists, poc_map_geography (with this source's registration) and poc_build_gus_strateg with every dbt test attached to
them (metrics_value_range excluded: its metrics parents are not built here; this source has no KPI). dbt writes only
to a DuckDB file on the runner that plays the database "r2" (target prod); the production catalog is attached
read-only as "lake" and the other parents are views onto it; the Bronze table is a copy of the scratch table. Also
reads the licence of the STRATEG API spec (strateg_en.json, strateg_pl.json). Deleted before merge.
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
import requests
import yaml

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gus_strateg import load as strateg_load
from ingestion.to_landing.gus_strateg import extract

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_gus_strateg"
SOURCE = "gus_strateg"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("gus_strateg_indicators", "gus_strateg_observations")
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_gus_strateg"]


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


def terms() -> dict:
    out = {}
    for name in ("strateg_en.json", "strateg_pl.json"):
        url = f"https://strateg.stat.gov.pl/{name}"
        extract.pace(time.monotonic() + 3600)  # counted with the reads (quota: an over-quota request gets 404)
        try:
            r = requests.get(url, headers={"User-Agent": extract.USER_AGENT}, timeout=(30, 60))
            info = r.json().get("info", {})
            out[name] = {"status": r.status_code, "license": info.get("license"), "title": info.get("title"),
                         "quota_text": (info.get("description") or "")[:420]}
        except (requests.RequestException, ValueError) as exc:
            out[name] = {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}
        print(f"=== {url}: {json.dumps(out[name], ensure_ascii=False)}", flush=True)
    return out


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
        code = strateg_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for t, d in CAPTURED for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, strateg_load.TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, strateg_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    v = "(select * from bronze where record_kind = 'value')"
    i = "(select * from bronze where record_kind = 'indicator')"
    return {
        "rows_by_kind": rows(con, "select record_kind, count(*) from bronze group by 1 order by 1"),
        "indicator_dimensions": one(con, f"select count(distinct indicator_id || '.' || dimension_id) from {i}"),
        "with_values": one(con, f"select count(distinct indicator_id || '.' || dimension_id) from {v}"),
        "values_by_level": rows(con, f"select level_id, count(*), count(distinct indicator_id), count(distinct territory_code), "
                                     f"min(period), max(period) from {v} group by 1 order by 1"),
        "dup_keys": one(con, f"select count(*) from (select indicator_id, dimension_id, level_id, territory_code, period "
                             f"from {v} group by all having count(*) > 1)"),
        "null_keys_or_values": rows(con, f"select count(*) filter (where indicator_id is null or territory_code is null or period is null), "
                                         f"count(*) filter (where value is null) from {v}"),
        "non_numeric_values": rows(con, f"select value, count(*) from {v} where value is null "
                                        f"or not regexp_full_match(value, '-?[0-9]+(\\.[0-9]+)?') group by 1 order by 2 desc limit 15"),
        "value_shapes": rows(con, f"select regexp_replace(coalesce(value, 'null'), '[0-9]+', 'n', 'g'), count(*) from {v} "
                                  "group by 1 order by 2 desc limit 10"),
        "zeros": one(con, f"select count(*) from {v} where try_cast(value as double) = 0"),
        "zeros_by_indicator(id,unit,zeros,values,zero_texts,areas)": rows(con, f"select v.indicator_id || chr(46) || v.dimension_id, any_value(i.unit_pl), "
            f"count(*) filter (where try_cast(v.value as double) = 0), count(*), string_agg(distinct v.value, chr(124)) filter (where try_cast(v.value as double) = 0), "
            f"string_agg(distinct v.territory_code, chr(124)) filter (where try_cast(v.value as double) = 0) from {v} v join {i} i using (indicator_id, dimension_id) "
            "group by 1 having count(*) filter (where try_cast(v.value as double) = 0) > 0 order by 3 desc", 25),
        "non_year_periods": rows(con, f"select period, count(*) from {v} where not regexp_full_match(period, '[0-9]{{4}}') group by 1 limit 5"),
        "level0_codes": rows(con, f"select territory_code, count(*) from {v} where level_id = '0' group by 1 order by 1", 70),
        "level2_codes": rows(con, f"select territory_code, count(*) from {v} where level_id = '2' group by 1 order by 1", 30),
        "poland_level2_vs_level0": rows(con, f"select count(*), count(*) filter (where a.value is distinct from b.value) from {v} a "
                                             f"left join {v} b on a.indicator_id = b.indicator_id and a.dimension_id = b.dimension_id "
                                             "and a.period = b.period and b.level_id = '0' and b.territory_code = a.territory_code "
                                             "where a.level_id = '2' and a.territory_code = '100000000000'"),
        "units": rows(con, f"select unit_pl, unit_en, count(*) from {i} group by all order by 3 desc", 40),
        "with_target": one(con, f"select count(*) from {i} where target_value_json is not null"),
        "levels_offered": rows(con, f"select list_sort(json_keys(cast(territory_levels_json as json))), count(*) from {i} group by 1 order by 2 desc limit 12"),
        "no_english_name": one(con, f"select count(*) from {i} where indicator_name_en is null"),
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
    run(con, f"create table bronze.{strateg_load.TABLE} as select * from queues_arrow")
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
    rel = {m: f'{s}."{a}"' for m, (s, a) in aliases.items()}
    ob, ind, b = rel["gus_strateg_observations"], rel["gus_strateg_indicators"], rel["poc_build_gus_strateg"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "observations_by_level": rows(con, f"select level_id, count(*), count(distinct indicator_id), count(distinct territory_code), "
                                           f"min(period_year), max(period_year) from {ob} group by 1 order by 1"),
        "value_status": rows(con, f"select value_status, count(*) from {ob} group by 1"),
        "not_numeric_texts": rows(con, f"select value_text, count(*) from {ob} where value is null group by 1 order by 2 desc limit 10"),
        "indicators_without_values": rows(con, f"select i.indicator_id, i.dimension_id from {ind} i anti join {ob} o "
                                                "using (indicator_id, dimension_id)"),
        "voivodeships_per_indicator_year(min,max)": rows(con, f"select min(c), max(c) from (select indicator_id, period_year, count(*) c "
                                                              f"from {ob} where level_id = '2' and territory_code <> '100000000000' group by all)"),
        "targets_sample": rows(con, f"select indicator_id, unit_code, base_value_json, target_value_json from {ind} "
                                    "where target_value_json is not null order by indicator_id limit 4"),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    g["dup_series_date"] = one(con, f"select count(*) from (select measure_key || '|' || geography_key || case when geography_key = '_X' "
                                    f"then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json as series_key, date_key "
                                    f"from {b} group by all having count(*) > 1)")
    g["dup_series_date_without_source_code"] = one(con, f"select count(*) from (select measure_key, geography_key, breakdown_json, date_key "
                                                        f"from {b} group by all having count(*) > 1)")
    g["measures"] = one(con, f"select count(distinct measure_key) from {b}")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["unmatched"] = rows(con, f"select geography_source_code, count(*) from {b} where geography_key = '_X' group by 1 order by 1")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["map_rows"] = one(con, f"select count(*) from {rel['poc_map_geography']} where source_system = '{SOURCE}'")
    g["areas"] = rows(con, f"select case when geography_key like 'TERC:%' then 'voivodeship' when geography_key like 'EUROSTAT:%' "
                           f"then geography_key else 'country' end, count(distinct geography_key), count(*) from {b} group by 1 order by 1")
    g["dates"] = rows(con, f"select min(date_key), max(date_key) from {b}")
    g["silver_rows_not_in_gold"] = one(con, f"select count(*) from {ob}") - g["rows"]
    g["spot_PL"] = rows(con, f"select measure_key, period_label, value from {b} where geography_key = 'PL' and measure_codes in "
                             "('14.5', '4570.6735', '2400.2929', '517.578') and date_key >= date '2021-01-01' order by 1, 2")
    g["spot_mazowieckie"] = rows(con, f"select measure_key, period_label, value from {b} where geography_key = 'TERC:14' "
                                      "order by date_key desc, measure_key limit 6")
    g["units_in_gold"] = rows(con, f"select unit_code, count(distinct measure_key) from {b} group by 1 order by 2 desc", 30)
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
        detail = ("level0_codes", "zeros_by_indicator(id,unit,zeros,values,zero_texts,areas)")
        note(f"{SOURCE} bronze profile", {k: v for k, v in prof.items() if k not in detail})
        note(f"{SOURCE} bronze codes and zeros", {k: prof[k] for k in detail})
        con.close()
        work = Path(tempfile.mkdtemp(prefix="strateg-dbt-"))
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
        note(f"{SOURCE} gold", {**g, "failures": failures})
        if failures:
            status = 1
        note(f"{SOURCE} licence gate and STRATEG spec licence", {**licence(), "spec": terms()})
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
