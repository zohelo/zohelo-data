"""Dev trial of OTOMOTO Insights (register CD-034, #3264): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_otomoto/, Iceberg namespace zz_dev_otomoto; everything written is removed in
`finally`. The seeded editions are read twice (the second read must write nothing), then inspect and verify; Bronze
loads twice (the second must append nothing). Then a real dbt build, twice, of silver.otomoto_series, the code lists,
poc_map_geography (with this source's registration) and poc_build_otomoto with every dbt test attached to them
(metrics_value_range excluded: no metrics model is built here; this source has no KPI). dbt writes only to a DuckDB
file on the runner that plays the database "r2" (target prod); the production catalog is attached read-only as
"lake" and the other parents are views onto it; the Bronze table is a copy of the scratch table. Deleted before merge.
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
from ingestion.to_bronze.otomoto import load as otomoto_load
from ingestion.to_landing import otomoto as otomoto_landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_otomoto"
SOURCE = "otomoto"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("otomoto_series",)
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_otomoto"]


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((level, title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:3000]}", flush=True)


bulk_file.annotate = _capture
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
        code = bulk_file.main(SOURCE, "load", landing_root=SCRATCH, pause_seconds=5.0)
        summary = next((d for _, t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        out["runs"].append({"run": n, "exit": code, "seconds": round(time.monotonic() - start),
                            "files": [{k: (v[:16] if k == "sha256" else v) for k, v in f.items()} for f in summary.get("files", [])],
                            "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    for mode in ("inspect", "verify"):
        CAPTURED.clear()
        out[f"{mode}_exit"] = bulk_file.main(SOURCE, mode, landing_root=SCRATCH)
        out[mode] = next((d for _, t, d in CAPTURED if mode in t), {}).get("files")
    CAPTURED.clear()
    otomoto_landing.annotate = _capture
    out["edition_check_missing"] = otomoto_landing.check_editions()
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, "edition_2026_08", SCRATCH))
    if held:
        r = held[-1]
        out["receipt"] = {"version": r["version_token"], "basis": r["version_basis"], "size": r["size_bytes"],
                          "original_sha256": str(r.get("original_sha256"))[:16], "reduced": r.get("detail", {}).get("reduced"),
                          "key": r["object"]["key"]}
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = otomoto_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for _, t, d in CAPTURED for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, otomoto_load.TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, otomoto_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    return {
        "rows_by_kind": rows(con, "select record_kind, count(*), count(distinct tab_number), count(distinct element_id) from bronze group by 1 order by 1"),
        "editions": rows(con, "select entity, edition, count(*) from bronze group by all"),
        "null_keys": one(con, "select count(*) from bronze where record_kind is null or tab_number is null or element_id is null or value is null"),
        "dup_cells": one(con, "select count(*) from (select element_id, sheet_number, row_number, column_number from bronze "
                              "where record_kind = 'cell' group by all having count(*) > 1)"),
        "cell_value_shapes": rows(con, "select regexp_replace(regexp_replace(value, '[0-9]+', 'n', 'g'), '[a-ząćęłńóśźż]+', 'a', 'gi'), count(*) "
                                       "from bronze where record_kind = 'cell' group by 1 order by 2 desc limit 14"),
        "segment_headings": rows(con, "select tab_number, value, count(*) from bronze where record_kind = 'text' "
                                      "and value like 'Ogłoszenia na OTOMOTO (%' group by all order by 1"),
        "titles_by_tab": rows(con, "select tab_number, count(*) filter (where value = 'Średnia dzienna liczba aktywnych ogłoszeń'), "
                                   "count(*) filter (where value like 'Statystyki ruchu (jako %') from bronze where record_kind = 'text' group by 1 order by 1"),
        "texts_stored": one(con, "select count(distinct value) from bronze where record_kind = 'text'"),
        "texts_with_at_sign": one(con, "select count(*) from bronze where record_kind = 'text' and value like '%@%'"),
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
    con.register("cells_arrow", arrow)
    run(con, f"create table bronze.{otomoto_load.TABLE} as select * from cells_arrow")
    con.unregister("cells_arrow")
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
        except duckdb.CatalogException:
            views.append(f"missing:{schema}.{name}")
            missing.add(p)
    con.close()
    aliases = {m: (nodes[f"model.zohelo.{m}"]["schema"], nodes[f"model.zohelo.{m}"]["alias"]) for m in SELECT}
    skipped = sorted(nodes[k]["name"] for k in tests if missing & set(nodes[k]["depends_on"]["nodes"]))
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"], skipped


def build(arrow, work: Path) -> tuple[dict, dict, dict, list[str]]:
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
                     "status": by_status, "not_passing": bad,
                     "otomoto_tests": sorted(k.split(".")[2][:70] for k in results if k.startswith("test.") and "otomoto" in k)})
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
    se, b = rel["otomoto_series"], rel["poc_build_otomoto"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "series(measure,segment,rows,first,last,min,max)": rows(con, f"select measure_code, segment_label, count(*), min(period_month), max(period_month), "
                                                                 f"min(value), max(value) from {se} group by all order by 1, 2"),
        "value_status": rows(con, f"select value_status, count(*) from {se} group by 1 order by 1"),
        "not_numeric_texts": rows(con, f"select value_text, count(*) from {se} where value_status = 'not_numeric' group by 1 limit 10"),
        "editions": rows(con, f"select edition, count(*) from {se} group by 1"),
        "dup_keys": one(con, f"select count(*) from (select measure_code, segment_label, period_month from {se} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {se} where measure_code is null or segment_label is null or period_month is null or raw_sha256 is null"),
        "zeros": one(con, f"select count(*) from {se} where value = 0"),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    g["dup_series_date"] = one(con, f"select count(*) from (select measure_key || '|' || geography_key || case when geography_key = '_X' "
                                    f"then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json as series_key, date_key "
                                    f"from {b} group by all having count(*) > 1)")
    g["measures"] = rows(con, f"select measure_key, unit_code, count(distinct breakdown_json), count(*) from {b} group by all order by 1")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["geography"] = rows(con, f"select geography_source_code, geography_key, count(*) from {b} group by all")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["map_rows"] = rows(con, f"select source_code, geography_key from {rel['poc_map_geography']} where source_system = '{SOURCE}'")
    g["dates"] = rows(con, f"select min(date_key), max(date_key) from {b}")
    g["silver_rows_not_in_gold"] = one(con, f"select count(*) from {se}") - g["rows"]
    g["spot_PL_2026_08"] = rows(con, f"select measure_codes, breakdown_json, value from {b} where geography_key = 'PL' "
                                     "and date_key = date '2026-08-01' order by 1, 2")
    g["spot_PL_2025_01"] = rows(con, f"select measure_codes, breakdown_json, value from {b} where geography_key = 'PL' "
                                     "and date_key = date '2025-01-01' and measure_codes = 'active_listings' order by 1, 2")
    g["breakdown_sample"] = one(con, f"select min(breakdown_json) from {b}")
    con.close()
    if g["dup_series_date"] or g["null_values"] or not g["rows"] or g["measures_with_two_frequencies"]:
        failures.append("gold checks")
    if s["dup_keys"] or s["null_keys"]:
        failures.append("silver checks")
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
        for bkt in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(bkt), Prefix=f"{SCRATCH}/"))
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
        note(f"{SOURCE} landing (twice), inspect, verify, edition check", land)
        r1 = (land["runs"][0]["files"] or [{}])[0].get("outcome")
        r2 = (land["runs"][1]["files"] or [{}])[0].get("outcome")
        landing_failed = r1 != "landed_new" or r2 != "unchanged" or land["verify_exit"]
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        note(f"{SOURCE} bronze profile", bronze_profile(con))
        con.close()
        work = Path(tempfile.mkdtemp(prefix="otomoto-dbt-"))
        try:
            d, s, g, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append(f"landing: first read must land ({r1}), second unchanged ({r2}), verify must pass")
        if b["loads"][1].get("appended_files") or b["rejected"]:
            failures.append("bronze: second load appended or a file was rejected")
        note(f"{SOURCE} dbt build x2", d)
        note(f"{SOURCE} silver profile", s)
        note(f"{SOURCE} gold", {**g, "failures": failures})
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
