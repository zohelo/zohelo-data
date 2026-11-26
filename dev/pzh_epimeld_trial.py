"""Dev trial of NIZP PZH-PIB EPIMELD (register PLS-019, #443): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_pzh_epimeld/, Iceberg namespace zz_dev_pzh_epimeld; everything written is
removed in `finally`. Landing runs twice (the second must write nothing), Bronze loads twice (the second must append
nothing). The plain text of every landed bulletin is printed to the job log (zlib + base64) to check the parser by
hand. Then a real dbt build, twice, of the Silver model, the code lists, poc_map_geography and poc_build_pzh_epimeld
with every dbt test attached to them; dbt writes only to a DuckDB file on the runner that plays "r2"; the production
catalog is attached read-only as "lake" and other parents are views onto it. Deleted before merge (never on the PR
branch).
"""

from __future__ import annotations

import base64
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
import zlib

from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.secrets import Secret
import duckdb
import jinja2
import yaml

from ingestion.common import bulk_bronze, report
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.pzh_epimeld import load as pzh_load
from ingestion.to_landing import pzh_epimeld as landing_mod
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_pzh_epimeld"
SOURCE = "pzh_epimeld"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("pzh_epimeld_cases",)
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_pzh_epimeld"]
SPOT = ("A01.0", "A02", "A37", "A69.2", "B01", "A39")


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
        code = landing_mod.extract(60, landing_root=SCRATCH)
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        out["runs"].append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                            "outcomes": dict(Counter(f.get("outcome") for f in summary.get("files", []))),
                            "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    found, updates = landing_mod.discover()
    out["listed"] = {"bulletins": sorted(found), "updates_not_landed": updates}
    CAPTURED.clear()
    out["verify_exit"] = bulk_file.main(SOURCE, "verify", SCRATCH,
                                        specs=[{"entity": e} for e in sorted(landing_mod.held_entities(SCRATCH))])
    out["held"] = len(landing_mod.held_entities(SCRATCH))
    return out


def dump_texts() -> dict:
    """The plain text of every landed bulletin to the job log, and the page count."""
    from pypdf import PdfReader

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    pages = {}
    for entity in sorted(landing_mod.held_entities(SCRATCH)):
        receipt = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, entity, SCRATCH))[-1]
        body = bulk_bronze.read_raw(client, bucket, SOURCE, receipt)
        texts = [p.extract_text() or "" for p in PdfReader(io.BytesIO(body)).pages]
        pages[entity] = len(texts)
        enc = base64.b64encode(zlib.compress(json.dumps(texts).encode(), 9)).decode()
        print(f"PLAINTEXT {entity} {enc}", flush=True)
    return pages


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = pzh_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for t, d in CAPTURED for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, pzh_load.TABLE_ID[1]))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, pzh_load.REJECTED_ID[1])).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:400]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    v = "bronze"
    values = ["value_q1", "value_q2", "value_q3", "value_q4", "value_cases", "value_incidence", "value_hospitalised",
              "value_hospitalised_pct"]
    nonnum = " union all ".join(
        f"select '{c}' as col, {c} as v from {v} where not regexp_full_match({c}, '\\d+(,\\d+)?')" for c in values)
    return {
        "rows": one(con, f"select count(*) from {v}"),
        "by_edition(entity,tables,poland,voiv,prelim)": rows(con, f"select entity, count(distinct table_number), count(*) filter (where row_kind='poland'), "
                                                                 f"count(*) filter (where row_kind='voivodeship'), any_value(preliminary) from {v} group by 1 order by 1"),
        "distinct_disease_codes": one(con, f"select count(distinct upper(replace(disease_codes, ' ', ''))) from {v}"),
        "dup_keys_in_edition": rows(con, f"select entity, disease_codes, row_kind, period_year, area_number, count(*) from {v} "
                                         f"group by all having count(*) > 1 order by all", 20),
        "null_keys": one(con, f"select count(*) from {v} where disease_codes is null or row_kind is null or period_year is null "
                              f"or (row_kind = 'voivodeship' and area_number is null)"),
        "non_numeric(col,value,n)": rows(con, f"select col, v, count(*) from ({nonnum}) group by all order by all", 40),
        "incidence_headers": rows(con, f"select incidence_header, count(distinct entity || table_number) from {v} group by 1"),
        "voiv_sum_vs_poland(mismatching tables)": rows(con, f"""
            with t as (select entity, table_number, disease_codes,
                         max(case when row_kind='poland' and period_year=edition_year then value_cases end) pl,
                         sum(try_cast(value_cases as bigint)) filter (where row_kind='voivodeship') vs
                       from {v} group by all)
            select entity, disease_codes, pl, vs from t
            where coalesce(try_cast(pl as bigint), 0) <> coalesce(vs, 0) order by all""", 30),
        "revisions(prev-year value differs between editions)": rows(con, f"""
            with p as (select upper(replace(disease_codes,' ','')) d, period_year, edition_year, value_cases c from {v} where row_kind='poland')
            select count(*), count(*) filter (where a.c <> b.c) from p a join p b
            on a.d = b.d and a.period_year = b.period_year and a.edition_year = a.period_year and b.edition_year = a.period_year + 1"""),
        "revision_examples(codes,year,edition_Y,edition_Y+1)": rows(con, f"""
            with p as (select upper(replace(disease_codes,' ','')) d, period_year, edition_year, value_cases c from {v} where row_kind='poland')
            select a.d, a.period_year, a.c, b.c from p a join p b
            on a.d = b.d and a.period_year = b.period_year and a.edition_year = a.period_year and b.edition_year = a.period_year + 1
            where a.c <> b.c order by abs(try_cast(a.c as bigint) - try_cast(b.c as bigint)) desc nulls last limit 8"""),
        "codes_in_every_edition": one(con, f"select count(*) from (select upper(replace(disease_codes,' ','')) d from {v} "
                                           f"group by 1 having count(distinct entity) = (select count(distinct entity) from {v}))"),
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
    run(con, f"create table bronze.{pzh_load.TABLE_ID[1]} as select * from bronze_arrow")
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
    own_tests = sorted(nodes[k]["name"] for k in tests if "pzh_epimeld" in k)
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"], skipped, own_tests


def build(arrow, work: Path) -> tuple[dict, dict, dict, dict, list[str]]:
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
                                   if k.startswith("test.") and "pzh_epimeld" in k}})
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
    ob, b = rel["pzh_epimeld_cases"], rel["poc_build_pzh_epimeld"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped, "own_tests_selected": own_tests,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "rows": one(con, f"select count(*) from {ob}"),
        "years(min,max)": rows(con, f"select min(period_year), max(period_year) from {ob}"),
        "by_area_kind(kind,rows,diseases,first,last)": rows(con, f"select area_code = 'PL', count(*), count(distinct disease_codes), min(period_year), max(period_year) from {ob} group by 1 order by 1"),
        "rows_by_source_edition_for_PL(edition,rows)": rows(con, f"select source_edition, count(*) from {ob} where area_code='PL' group by 1 order by 1"),
        "PL_year_from_edition(same,next)": rows(con, f"select count(*) filter (where source_edition = period_year), count(*) filter (where source_edition = period_year + 1) from {ob} where area_code='PL'"),
        "dup_grain": one(con, f"select count(*) from (select period_year, disease_codes, area_code from {ob} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {ob} where period_year is null or disease_codes is null or area_code is null"),
        "null_incidence": one(con, f"select count(*) from {ob} where incidence is null"),
        "zeros(cases,incidence)": rows(con, f"select count(*) filter (where cases = 0), count(*) filter (where incidence = 0) from {ob}"),
        "preliminary(rows)": rows(con, f"select preliminary, count(*) from {ob} group by 1"),
        "max_cases(codes,year,area,cases)": rows(con, f"select disease_codes, period_year, area_code, cases from {ob} order by cases desc limit 3"),
        "max_incidence(codes,year,area,inc)": rows(con, f"select disease_codes, period_year, area_code, incidence from {ob} where incidence is not null order by incidence desc limit 3"),
        "diseases_PL_2024": one(con, f"select count(*) from {ob} where area_code='PL' and period_year=2024"),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    series = "measure_key || '|' || geography_key || '|' || breakdown_json"
    g["dup_series_date"] = one(con, f"select count(*) from (select {series} as series_key, date_key from {b} group by all having count(*) > 1)")
    g["measures"] = one(con, f"select count(distinct measure_key) from {b}")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["series"] = one(con, f"select count(distinct {series}) from {b}")
    g["dates(min,max)"] = rows(con, f"select min(date_key), max(date_key) from {b}")
    g["geography(key,rows)"] = rows(con, f"select geography_key, count(*) from {b} group by 1 order by 1", 20)
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["status"] = rows(con, f"select observation_status_key, count(*) from {b} group by 1")
    g["units"] = rows(con, f"select unit_code, count(*) from {b} group by 1 order by 1")
    g["silver_values_not_in_gold"] = one(con, f"select count(*) + count(incidence) from {ob}") - g["rows"]
    g["map_rows"] = one(con, f"select count(*) from {rel['poc_map_geography']} where source_system = '{SOURCE}'")
    spots = {}
    for code in SPOT:
        spots[code] = rows(con, f"select period_label, unit_code, value from {b} where geography_key = 'PL' and measure_codes = '{code}' "
                                f"and period_label in ('2006', '2015', '2023', '2024', '2025') order by 1, 2")
    spots["silver_PL_2023_2025(codes,year,cases,inc,edition)"] = rows(con, f"select disease_codes, period_year, cases, incidence, source_edition from {ob} "
                                                                          f"where area_code='PL' and disease_codes in ('A01.0','A02','A37','A69.2') and period_year >= 2023 order by 1, 2")
    spots["silver_voiv_A02_2024(area,cases,inc)"] = rows(con, f"select area_code, cases, incidence from {ob} where disease_codes='A02' and period_year=2024 order by 1")
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
        landing_failed = (runs[0]["exit"] or runs[1]["exit"] or set(runs[1]["outcomes"]) - {"closed year, held", "unchanged"}
                          or runs[1]["landing_files_after"] != runs[0]["landing_files_after"] or land["verify_exit"])
        note(f"{SOURCE} landing (twice), verify", land)
        note(f"{SOURCE} pages per bulletin", dump_texts())
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        note(f"{SOURCE} bronze profile", bronze_profile(con))
        con.close()
        work = Path(tempfile.mkdtemp(prefix="pzh-dbt-"))
        try:
            d, s, g, spots, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append("landing: first run must land, second must write nothing, verify must pass")
        if b["loads"][1].get("appended_files"):
            failures.append("bronze: second load appended")
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
