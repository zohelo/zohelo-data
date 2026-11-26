"""Dev trial of GDDKiA rest areas (register PL-ENV-023, #2995): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_gddkia_mop/, Iceberg namespace zz_dev_gddkia_mop; everything written is removed
in `finally`. Landing extract twice (the second must write nothing), inspect (nothing left to land) and verify;
Bronze loads twice (the second must append nothing); a profile of the Bronze cells; then a real dbt build, twice, of
the Silver model, the code lists, poc_map_geography, poc_build_gddkia_mop and gddkia_mop_rest_area with every test
attached (metrics_value_range excluded: no metrics model). dbt writes only to a DuckDB file on the runner that plays
"r2" (target prod); the production catalog is attached read-only as "lake" and the other parents are views onto it;
the Bronze table is a copy of the scratch table. Manager names are never printed (counts only). Deleted before merge.
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
from ingestion.to_bronze import gddkia_mop as mop_bronze
from ingestion.to_landing import gddkia_mop as mop_landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_gddkia_mop"
SOURCE = "gddkia_mop"
CAPTURED: list = []
ANNOTATE = report.annotate
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("gddkia_mop_rest_areas",)
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_gddkia_mop", "gddkia_mop_rest_area"]


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:3000]}", flush=True)


report.annotate = _capture
bulk_bronze.annotate = _capture
bulk_file.annotate = _capture


def note(title: str, data) -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    ANNOTATE("notice", title, json.loads(text) if len(text) <= 3900 else {"truncated": text[:3800]})


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
        code = mop_landing.extract(60, landing_root=SCRATCH)
        summary = next((d for t, d in CAPTURED if "landing extract" in t), {})
        files, size, receipts = landing_objects()
        out["runs"].append({"run": n, "exit": code, "seconds": round(time.monotonic() - start), **summary,
                            "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    found = mop_landing.discover()
    out["inspect_to_land"] = [s["entity"] for s in mop_landing.plan(found, mop_landing.held(SCRATCH))]
    out["urls"] = {e: s["url"][-40:] for e, s in found.items()}
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    out["receipts"] = [{k: r.get(k) for k in ("entity", "version_token", "version_basis", "size_bytes")}
                       | {"sha256": r["sha256"][:16], "last_modified": r["detail"]["http"]["last_modified"]}
                       for r in bulk_file.landed(client, bucket, f"{SCRATCH}/{SOURCE}/snapshot/data/")]
    out["verify_exit"] = bulk_file.main(SOURCE, "verify", landing_root=SCRATCH,
                                        specs=[{"entity": e} for e in sorted(mop_landing.held(SCRATCH))])
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    entities = sorted(mop_landing.held(SCRATCH))
    for n in (1, 2):
        CAPTURED.clear()
        code = mop_bronze.bronze_load(entities, landing_root=SCRATCH, namespace=SCRATCH)
        totals = next((d for t, d in CAPTURED if "Bronze from Landing" in t), {})
        out["loads"].append({"run": n, "exit": code, **{k: v for k, v in totals.items() if k != "result"}})
    cat = catalog()
    t = cat.load_table((SCRATCH, f"{SOURCE}_cells"))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, f"{SOURCE}_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    head = ("(select entity, sheet_name, min(row_number) r from bronze where column_number = 1 "
            "and lower(value_text) like 'lp%' group by all)")
    col = ("(select b.column_number, any_value(b.value_text) h from bronze b join {head} h using (entity, sheet_name) "
           "where b.row_number = h.r group by 1)").format(head=head)
    data = ("(select b.* from bronze b join {head} h using (entity, sheet_name) where b.row_number > h.r)").format(head=head)

    def values(header_like: str, limit: int = 30):
        return rows(con, f"select trim(d.value_text), count(*) from {data} d join {col} c using (column_number) "
                         f"where lower(c.h) like '{header_like}' group by 1 order by 2 desc", limit)

    return {
        "cells_by_entity_sheet": rows(con, "select entity, sheet_name, count(*), max(row_number), max(column_number) "
                                           "from bronze group by all order by 1"),
        "header_row": rows(con, f"select * from {head}"),
        "headers": rows(con, f"select column_number, h from {col} order by 1"),
        "cell_types": rows(con, f"select cell_type, count(*) from {data} group by 1 order by 1"),
        "lp": rows(con, f"select count(*), count(distinct d.value_text), min(try_cast(d.value_text as int)), "
                        f"max(try_cast(d.value_text as int)), count(*) filter (where not regexp_matches(d.value_text, '^[0-9]+$')) "
                        f"from {data} d where d.column_number = 1"),
        "lp_non_digits": rows(con, f"select d.value_text, d.row_number from {data} d where d.column_number = 1 "
                                   "and not regexp_matches(d.value_text, '^[0-9]+$') limit 10"),
        "rows_per_column(nulls_shown_as_missing)": rows(con, f"select column_number, count(*) from {data} group by 1 order by 1"),
        "voivodeship": values("wojew%"),
        "current_category": values("obecna%"),
        "target_category": values("kat%"),
        "branch": values("oddzia%"),
        "markers(-,x,brak,empty)": rows(con, f"select trim(value_text), count(*) from {data} where trim(value_text) "
                                            "in ('-', 'x', 'X', 'brak', '', '.', 'b.d.') group by 1"),
    }


def manager_profile(con) -> dict:
    head = ("(select entity, sheet_name, min(row_number) r from bronze where column_number = 1 "
            "and lower(value_text) like 'lp%' group by all)")
    mcol = (f"(select b.entity, b.column_number from bronze b join {head} h using (entity, sheet_name) "
            "where b.row_number = h.r and lower(b.value_text) like 'zarz%')")
    q = (f"select b.value_text v from bronze b join {mcol} m using (entity, column_number) join {head} h "
         "using (entity) where b.row_number > h.r")
    kind = ("case when regexp_matches(lower(v), '^gddkia') then 'gddkia' when lower(trim(v)) = 'prywatny' then 'private' "
            "when regexp_matches(lower(v), '(s\\.\\s?a\\.|sp\\.\\s?z\\s?o\\.?\\s?o|spółka|sp\\.\\s?k\\.|\\bse\\b)') then 'company' "
            "else 'other' end")
    return {
        "kinds(rows,distinct)": rows(con, f"select {kind} k, count(*), count(distinct v) from ({q}) group by 1 order by 1"),
        # names are printed only for GDDKiA and company forms; 'other' only as counts
        "company_names": rows(con, f"select v, count(*) from ({q}) where {kind} in ('company', 'gddkia') group by 1 order by 2 desc", 25),
        "other_shapes": rows(con, f"select length(v), regexp_matches(v, '[A-ZĄĆĘŁŃÓŚŹŻ]{{3,}}'), count(*) from ({q}) "
                                  f"where {kind} = 'other' group by all order by 3 desc", 15),
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
    run(con, f"create table bronze.{SOURCE}_cells as select * from cells_arrow")
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
        ours = sorted(k.split(".")[-1][:70] for k in results if k.startswith("test.") and "gddkia_mop" in k)
        runs.append({"exit": code, "nodes": len(results), "tests": sum(1 for k in results if k.startswith("test.")),
                     "status": by_status, "not_passing": bad, **({"own_tests": ours} if attempt == 1 else {})})
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
    sv, b, e = rel["gddkia_mop_rest_areas"], rel["poc_build_gddkia_mop"], rel["gddkia_mop_rest_area"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "rows_by_edition": rows(con, f"select entity, edition_month, count(*), min(lp), max(lp), count(distinct voivodeship_name) "
                                     f"from {sv} group by all order by 1"),
        "dup_grain": one(con, f"select count(*) from (select entity, lp from {sv} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {sv} where entity is null or lp is null or raw_sha256 is null"),
        "nulls(branch,voiv,locality,name,cur,target,manager)": rows(con, f"select count(*) filter (where branch is null), "
            "count(*) filter (where voivodeship is null), count(*) filter (where locality is null), "
            "count(*) filter (where rest_area_name is null), count(*) filter (where current_category is null), "
            f"count(*) filter (where target_category is null), count(*) filter (where manager_kind is null) from {sv}"),
        "current_category": rows(con, f"select current_category, count(*) from {sv} group by 1 order by 2 desc"),
        "target_category": rows(con, f"select target_category, count(*) from {sv} group by 1 order by 2 desc"),
        "manager_kind(rows,names_kept)": rows(con, f"select manager_kind, count(*), count(manager_name) from {sv} group by 1 order by 1"),
        "natural_key_repeats(branch,locality,name)": rows(con, f"select count(*), sum(n) from (select branch, locality, "
            f"rest_area_name, count(*) n from {sv} group by all having count(*) > 1)"),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    g["dup_series_date"] = one(con, f"select count(*) from (select measure_key || '|' || geography_key || case when geography_key = '_X' "
                                    f"then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json as series_key, date_key "
                                    f"from {b} group by all having count(*) > 1)")
    g["dup_series_date_without_source_code"] = one(con, f"select count(*) from (select measure_key, geography_key, breakdown_json, date_key "
                                                        f"from {b} group by all having count(*) > 1)")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["unmatched"] = rows(con, f"select geography_source_code, count(*) from {b} where geography_key = '_X' group by 1 order by 1")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["map_rows"] = one(con, f"select count(*) from {rel['poc_map_geography']} where source_system = '{SOURCE}'")
    g["areas_dates"] = rows(con, f"select count(distinct geography_key), min(date_key), max(date_key), count(distinct breakdown_json) from {b}")
    g["spot_PL"] = rows(con, f"select period_label, breakdown_json, value from {b} where geography_key = 'PL' order by 1, 2")
    g["voivodeships_sum_vs_PL"] = rows(con, f"select p.date_key, p.breakdown_json, p.value, sum(v.value) from {b} p join {b} v "
                                            "on v.date_key = p.date_key and v.breakdown_json = p.breakdown_json and v.geography_key like 'TERC:%' "
                                            "where p.geography_key = 'PL' group by all having p.value <> sum(v.value)")
    g["top_voivodeships_total"] = rows(con, f"select geography_key, geography_source_code, value from {b} where is_total "
                                            "and geography_key <> 'PL' order by value desc limit 5")
    g["entity_table"] = rows(con, f"select count(*), count(distinct lp), min(date_key), count(*) filter (where geography_key = '_X') from {e}")
    g["entity_sample"] = rows(con, f"select lp, branch, voivodeship, geography_key, locality, rest_area_name, current_category, "
                                   f"target_category, manager_kind from {e} order by lp limit 4")
    con.close()
    if (g["dup_series_date"] or g["dup_series_date_without_source_code"] or g["null_values"] or not g["rows"]
            or g["unmatched"] or g["voivodeships_sum_vs_PL"] or s["dup_grain"] or s["null_keys"]):
        failures.append("silver or gold checks")
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
        landing_failed = (not runs[0].get("landed") or runs[1].get("landed") or runs[1].get("planned")
                          or land["inspect_to_land"] or land["verify_exit"] or runs[0]["exit"] or runs[1]["exit"])
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        note(f"{SOURCE} bronze profile", bronze_profile(con))
        note(f"{SOURCE} manager column (names only for GDDKiA and companies)", manager_profile(con))
        con.close()
        work = Path(tempfile.mkdtemp(prefix="mop-dbt-"))
        try:
            d, s, g, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append("landing: first run must land, second must plan nothing, inspect empty, verify must pass")
        if (b["rejected"] or not b["rows"] or any(x["exit"] for x in b["loads"]) or b["loads"][1].get("appended_files")
                or not b["loads"][0].get("appended_files") or b["files"] != len(land["receipts"])):
            failures.append("bronze: a load failed, the second appended, or a file was rejected")
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
        ANNOTATE("error", f"{SOURCE} trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
