"""Dev trial of OpenAlex (register CD-028, #2996): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_openalex/, Iceberg namespace zz_dev_openalex; everything written is removed in
`finally`. Both entities are read from the API twice (the second read must write nothing), Bronze loads twice (the
second must append nothing). Then a real dbt build, twice, of the three Silver models, the eight code lists,
poc_map_geography (with this source's registration), poc_build_openalex and institution_openalex with every dbt test
attached to them (metrics_value_range excluded: its metrics parents are not built here; the KPI's rows are counted
from the build stage). dbt writes only to a DuckDB file on the runner that plays the database "r2" (target prod);
the production catalog is attached read-only as "lake" and the other parents are views onto it; the Bronze tables
are copies of the scratch tables. About 74 API requests in all (two reads of about 37). Deleted before merge.
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
from ingestion.to_bronze.openalex import load as oa_load
from ingestion.to_landing.openalex import extract

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_openalex"
SOURCE = "openalex"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("openalex_works_pl_counts", "openalex_institutions_pl", "openalex_institution_works_by_year")
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
GOLD = ("poc_build_openalex", "institution_openalex")
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", *GOLD]
TABLES = (oa_load.COUNTS, oa_load.INSTITUTIONS)


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
        code = extract.main(["--mode", "load", "--budget-minutes", "60", "--landing-root", SCRATCH])
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        ents = {e: {k: (v[:12] if k == "content_sha256" else v) for k, v in d.items() if k not in ("key", "newest")}
                for e, d in summary.get("entities", {}).items()}
        out["runs"].append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                            "entities": ents, "landing_files_after": files, "landing_bytes_after": size,
                            "receipts_after": receipts})
    for mode in ("inspect", "verify"):
        CAPTURED.clear()
        out[f"{mode}_exit"] = extract.main(["--mode", mode, "--landing-root", SCRATCH])
        out[mode] = next((d for t, d in CAPTURED if mode in t), {})
    return out


def bronze() -> tuple[dict, dict]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = oa_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, "steps": [
            {k: v for k, v in d.items() if k in ("table", "entities_landed", "candidates", "appended_files",
                                                  "rows_added", "rejected")} for t, d in CAPTURED]})
    cat = catalog()
    arrows = {}
    for name in TABLES:
        t = cat.load_table((SCRATCH, name))
        arrows[name] = t.scan().to_arrow()
        out[name] = {"files": len(list(t.scan().plan_files())), "rows": arrows[name].num_rows}
    try:
        rej = cat.load_table((SCRATCH, oa_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrows


def bronze_profile(con) -> dict:
    g = "(select * from counts where record_kind = 'group')"
    return {
        "counts_rows_by_kind": rows(con, "select record_kind, count(*) from counts group by 1 order by 1"),
        "requests": one(con, f"select count(distinct request) from {g}"),
        "groups_by_request_kind": rows(con, f"select case when request like 'works_by_year_field_%' then 'field' else request end, "
                                            f"count(distinct request), count(*), min(cast(group_key as int)), max(cast(group_key as int)) "
                                            f"from {g} group by 1 order by 1"),
        "dup_group_keys": one(con, f"select count(*) from (select request, group_key from {g} group by all having count(*) > 1)"),
        "null_keys_or_counts": rows(con, f"select count(*) filter (where group_key is null), count(*) filter (where works is null) from {g}"),
        "non_numeric_counts_or_years": rows(con, f"select group_key, works from {g} where not regexp_full_match(works, '[0-9]+') "
                                                 "or not regexp_full_match(group_key, '[0-9]{1,4}') limit 10"),
        "zero_counts": one(con, f"select count(*) from {g} where works = '0'"),
        "fields": rows(con, "select field_id, field_name, domain_id, domain_name from counts where record_kind = 'field' "
                            "order by cast(field_id as int)", 30),
        "meta_count_vs_groups_sum": rows(con, f"select request, any_value(meta_count), sum(cast(works as bigint)) from {g} "
                                              "where request in ('works_by_year', 'works_oa_by_year') group by 1 order by 1"),
        "institutions": rows(con, "select count(*), count(distinct institution_id), count(*) filter (where ror is null), "
                                  "count(*) filter (where geo_region is null), count(*) filter (where country_code <> 'PL' "
                                  "or country_code is null), any_value(meta_count) from inst"),
        "institution_types": rows(con, "select institution_type, count(*) from inst group by 1 order by 2 desc"),
        "institution_regions": rows(con, "select coalesce(geo_region, 'null'), count(*) from inst group by 1 order by 2 desc", 30),
        "institution_geo_country_not_pl": one(con, "select count(*) from inst where geo_country_code is distinct from 'PL'"),
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


def setup_dbt(work: Path, arrows: dict) -> tuple[dict, list[str], list[str]]:
    """Profile with the local file as r2 and the catalog as lake; the Bronze copies and views for the other parents."""
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
    for name, arrow in arrows.items():
        con.register("bronze_arrow", arrow)
        run(con, f"create table bronze.{name} as select * from bronze_arrow")
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
        except duckdb.CatalogException:
            views.append(f"missing:{schema}.{name}")
            missing.add(p)
    con.close()
    aliases = {m: (nodes[f"model.zohelo.{m}"]["schema"], nodes[f"model.zohelo.{m}"]["alias"]) for m in SELECT}
    skipped = sorted(nodes[k]["name"] for k in tests if missing & set(nodes[k]["depends_on"]["nodes"]))
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"], skipped


def ceilings() -> dict:
    return {m: v["rows"] for m, v in yaml.safe_load((ROOT / "ingestion/sources/openalex.yaml").read_text())["full_rebuild"].items()}


def build(arrows: dict, work: Path) -> tuple[dict, dict, dict, list[str]]:
    """dbt build twice; returns (dbt summary, silver profile, gold checks, failures)."""
    failures: list[str] = []
    aliases, views, skipped = setup_dbt(work, arrows)
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
    wc, inst, iy = rel["openalex_works_pl_counts"], rel["openalex_institutions_pl"], rel["openalex_institution_works_by_year"]
    b, ent = rel["poc_build_openalex"], rel["institution_openalex"]
    measured = {m: v[0] for m, v in fingerprints[0].items()}
    kpi_rows = one(con, f"select count(*) from {b} where measure_key = 'openalex:works_pl:works'")
    measured["poc_metrics_scholarly_works_pl_institution (rows of its measure)"] = kpi_rows
    ceil = ceilings()
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": measured,
                   "ceiling_over_rows": {m: round(c / max(1, measured.get(m, kpi_rows if m.startswith("poc_metrics") else 0) or 1), 2)
                                         for m, c in ceil.items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "works_by_selection(sel,rows,fields,min_year,max_year,sum)": rows(con, f"select selection, count(*), count(distinct field_id), "
                                                                               f"min(publication_year), max(publication_year), sum(works) from {wc} group by 1 order by 1"),
        "dup_grain": one(con, f"select count(*) from (select selection, field_id, publication_year from {wc} group by all having count(*) > 1)"),
        "years_after_read": rows(con, f"select selection, publication_year, sum(works) from {wc} where publication_year > "
                                      "cast(left(source_version, 4) as integer) group by all order by 1, 2"),
        "years_before_1900(rows,works)": rows(con, f"select count(*), sum(works) from {wc} where publication_year < 1900"),
        "all_oa_fieldsum_PL_2020_2026": rows(con, f"select a.publication_year, a.works, o.works, f.w, a.works - f.w from {wc} a "
                                                  f"left join {wc} o on o.selection = 'open_access' and o.publication_year = a.publication_year "
                                                  f"left join (select publication_year, sum(works) w from {wc} where selection = 'field' group by 1) f "
                                                  "on f.publication_year = a.publication_year where a.selection = 'all' and a.publication_year >= 2020 order by 1"),
        "fields_2024": rows(con, f"select field_id, field_name, works from {wc} where selection = 'field' and publication_year = 2024 "
                                 "order by works desc", 30),
        "institutions(rows,ror_null,region_null,parents)": rows(con, f"select count(*), count(*) filter (where ror is null), "
                                                                     f"count(*) filter (where geo_region is null), count(*) filter (where parent_ids_json <> '[]') from {inst}"),
        "institution_years(rows,institutions,min,max,zero_works,oa_gt_works)": rows(con, f"select count(*), count(distinct institution_id), min(year), max(year), "
                                                                                         f"count(*) filter (where works_count = 0), count(*) filter (where oa_works_count > works_count) from {iy}"),
        "institutions_without_years": one(con, f"select count(*) from {inst} i anti join {iy} y using (institution_id)"),
        "years_per_institution(min,median,max)": rows(con, f"select min(c), median(c), max(c) from (select institution_id, count(*) c from {iy} group by 1)"),
        "top_institutions_2024": rows(con, f"select i.institution_id, i.display_name, y.works_count, y.oa_works_count from {iy} y join {inst} i "
                                           "using (institution_id) where y.year = 2024 order by y.works_count desc limit 6"),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    g["dup_series_date"] = one(con, f"select count(*) from (select measure_key || '|' || geography_key || '|' || breakdown_json as series_key, "
                                    f"date_key from {b} group by all having count(*) > 1)")
    g["rows_by_block_measure"] = rows(con, f"select block_id, measure_key, count(*), count(distinct breakdown_json), min(date_key), max(date_key) "
                                          f"from {b} group by 1, 2 order by 1, 2")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["unmatched_geography"] = rows(con, f"select geography_source_code, count(*) from {b} where geography_key = '_X' group by 1")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["status"] = rows(con, f"select observation_status_key, min(date_key), max(date_key), count(*) from {b} group by 1 order by 1")
    g["map_rows"] = rows(con, f"select source_code, geography_key from {rel['poc_map_geography']} where source_system = '{SOURCE}'")
    g["silver_works_rows_not_in_gold"] = one(con, f"select count(*) from {wc}") - one(con, f"select count(*) from {b} where block_id = 'works_pl'")
    g["silver_institution_rows_x2_not_in_gold"] = 2 * one(con, f"select count(*) from {iy}") - one(con, f"select count(*) from {b} where block_id = 'institutions_pl'")
    g["spot_PL"] = rows(con, f"select measure_key, breakdown_json, period_label, value, observation_status_key from {b} where geography_key = 'PL' "
                             "and (breakdown_json in ('{}', '{\"field\":\"17\"}', '{\"institution\":\"I99542240\"}')) "
                             "and date_key >= date '2023-01-01' order by 1, 2, 3")
    g["max_value_kpi_measure"] = rows(con, f"select period_label, value from {b} where measure_key = 'openalex:works_pl:works' order by value desc limit 3")
    g["entity_table(rows,ids,types)"] = rows(con, f"select count(*), count(distinct institution_id), count(distinct institution_type) from {ent}")
    con.close()
    if g["dup_series_date"] or g["null_values"] or not g["rows"] or g["unmatched_geography"] or s["dup_grain"]:
        failures.append("gold or silver checks")
    over = [m for m, c in ceil.items() if c < measured.get(m, kpi_rows if m.startswith("poc_metrics") else 0)]
    if over:
        failures.append(f"rows above ceiling: {over}")
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
        r1, r2 = (r["entities"] for r in land["runs"])
        landing_failed = (any(d.get("outcome") != "landed" for d in r1.values())
                          or any(d.get("outcome") != "unchanged" for d in r2.values()) or land["verify_exit"])
        b, arrows = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("counts", arrows[oa_load.COUNTS])
        con.register("inst", arrows[oa_load.INSTITUTIONS])
        prof = bronze_profile(con)
        detail = ("fields", "institution_regions")
        note(f"{SOURCE} bronze profile", {k: v for k, v in prof.items() if k not in detail})
        note(f"{SOURCE} bronze fields and regions", {k: prof[k] for k in detail})
        con.close()
        work = Path(tempfile.mkdtemp(prefix="openalex-dbt-"))
        try:
            d, s, g, failures = build(arrows, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append("landing: first read must land, second must be unchanged, verify must pass")
        if any(st.get("appended_files") for st in b["loads"][1]["steps"]) or b["rejected"]:
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
