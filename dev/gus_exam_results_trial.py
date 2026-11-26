"""Dev trial of gus_exam_results (register PLS-025, #449): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_gus_exam_results/, Iceberg namespace zz_dev_gus_exam_results; everything
written is removed in `finally`. The GUS CSV is landed twice through the shared bulk_file pattern (the second run must
write nothing), Bronze loads twice (the second must append nothing). Then a real dbt build, twice, of the Silver model,
the eight code lists, poc_map_geography (with this source's registration) and poc_build_gus_exam_results with every
dbt test attached to them (metrics_value_range excluded: no metrics model; the KPI rows and the bounds are checked on
the build stage with the metrics filter, is_total and not _X). dbt writes only to a DuckDB file on the runner that plays
the database "r2" (target prod); the production catalog is attached read-only as "lake" and the other parents are views
onto it; the Bronze table is a copy of the scratch table. Copied from the IRENASTAT trial. Deleted before merge.
"""

from __future__ import annotations

from collections import Counter
import csv
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.secrets import Secret
import duckdb
import jinja2
import yaml

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gus_exam_results import load as gus_load
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_gus_exam_results"
SOURCE = "gus_exam_results"
ENTITY = "eighth_grade_means"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("gus_exam_results_eighth_grade",)
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_gus_exam_results"]
KPIS = {f"exam8_{s}": f"gus_exam_results:exam8:pct_max_points:{s}" for s in ("polish", "mathematics", "english")}


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
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
        code = bulk_file.main(SOURCE, "load", landing_root=SCRATCH)
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        detail = (summary.get("files") or [{}])[0]
        out["runs"].append({"run": n, "exit": code, **{k: v for k, v in detail.items() if k != "sha256"},
                            "sha256": str(detail.get("sha256", ""))[:16],
                            "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    for mode in ("inspect", "verify"):
        CAPTURED.clear()
        out[f"{mode}_exit"] = bulk_file.main(SOURCE, mode, landing_root=SCRATCH)
        out[mode] = [{k: v for k, v in f.items() if k not in ("url", "page_links")}
                     for f in next((d for t, d in CAPTURED if mode in t), {}).get("files", [])]
    return out


def landed_csv() -> list[dict]:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, ENTITY, SCRATCH))
    body = bulk_bronze.read_raw(client, bucket, SOURCE, held[-1])
    return list(csv.DictReader(io.StringIO(body.decode("utf-8-sig")), delimiter=";")), body, held[-1]


def landing_markers(rows: list[dict]) -> dict:
    """The GUS signs in the landed file and the value each carries; the Poland rows for the spot check."""
    return {
        "rows": len(rows),
        "flag_x_value(flag,value,rows)": sorted([list(k) + [v] for k, v in Counter((r["flaga"], r["wartosc"] if r["flaga"] else "number")
                                                                                     for r in rows).items()]),
        "unflagged_zeros": sum(1 for r in rows if not r["flaga"] and r["wartosc"].strip() in ("0", "0,0", "0.0")),
        "unflagged_non_numeric": sorted({r["wartosc"] for r in rows if not r["flaga"] and not r["wartosc"].strip().isdigit()})[:10],
    }


def rejection_proof(body: bytes, receipt: dict) -> dict:
    """The decoder on altered copies of the landed bytes (in memory only): each must be refused with a reason."""
    out = {}
    cases = {"renamed_header": body.replace(b"flaga", b"flag", 1), "extra_field_row": body + b"a;b;c;d;e;f;g;h;i;j\r\n",
             "header_only": body.split(b"\n", 1)[0] + b"\n", "cp1250": body.decode("utf-8-sig").encode("cp1250", "replace")}
    for name, altered in cases.items():
        try:
            gus_load.decode(altered, receipt)
            out[name] = "NOT REJECTED"
        except ValueError as exc:
            out[name] = str(exc)[:160]
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = gus_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for t, d in CAPTURED for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, gus_load.TABLE_ID[1]))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, gus_load.REJECTED_ID[1])).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    v = "bronze"
    return {
        "rows": one(con, f"select count(*) from {v}"),
        "distinct_keys": one(con, f"select count(distinct (area, subject, sex, period)) from {v}"),
        "dup_keys": one(con, f"select count(*) from (select area, subject, sex, period from {v} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {v} where area is null or subject is null or sex is null or period is null"),
        "periods(min,max,n)": rows(con, f"select min(period), max(period), count(distinct period) from {v}"),
        "areas": one(con, f"select count(distinct area) from {v}"),
        "subjects": rows(con, f"select subject, count(*) from {v} group by 1 order by 1"),
        "sexes": rows(con, f"select sex, count(*) from {v} group by 1 order by 1"),
        "variables_units": rows(con, f"select variable_name, unit_label, country, count(*) from {v} group by all"),
        "flags(flag,value,rows)": rows(con, f"select flag, case when flag is null then 'number' else value end, count(*) from {v} group by all order by all"),
        "null_values": one(con, f"select count(*) from {v} where value is null"),
        "non_numeric_values": rows(con, f"select value, count(*) from {v} where not regexp_full_match(value, '-?[0-9]+([.,][0-9]+)?') group by 1"),
        "unflagged_zeros": one(con, f"select count(*) from {v} where flag is null and try_cast(value as double) = 0"),
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
    run(con, f"create table bronze.{gus_load.TABLE_ID[1]} as select * from bronze_arrow")
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
                     "own_tests": sorted(k.split(".")[2] for k in results if k.startswith("test.") and SOURCE in k)})
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
    ob, b = rel["gus_exam_results_eighth_grade"], rel["poc_build_gus_exam_results"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped, "own_tests_selected": own_tests,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "rows": one(con, f"select count(*) from {ob}"),
        "years(min,max)": rows(con, f"select min(period_year), max(period_year) from {ob}"),
        "dup_grain": one(con, f"select count(*) from (select area_name, subject, sex, period_year from {ob} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {ob} where area_name is null or subject_code is null or sex_code is null or period_year is null "
                              f"or unit_code is null or raw_sha256 is null"),
        "status(status,flag,rows,with_value)": rows(con, f"select observation_status, flag, count(*), count(value) from {ob} group by all order by all"),
        "flagged_value_text": rows(con, f"select flag, value_text, count(*) from {ob} where flag is not null group by all"),
        "value_range(min,max)": rows(con, f"select min(value), max(value) from {ob}"),
        "zeros_with_value": one(con, f"select count(*) from {ob} where value = 0"),
        "rows_by_subject(subject,rows,with_value)": rows(con, f"select subject_code, count(*), count(value) from {ob} group by 1 order by 1"),
        "total_outside_sexes(n_checked,outside)": rows(con, f"""
            with p as (select area_name, subject_code, period_year,
                              max(value) filter (where sex_code = 'T') t, max(value) filter (where sex_code = 'F') f,
                              max(value) filter (where sex_code = 'M') m
                       from {ob} group by all)
            select count(*), count(*) filter (where t < least(f, m) - 1 or t > greatest(f, m) + 1)
            from p where t is not null and f is not null and m is not null"""),
        "poland_outside_voivodeships(n_checked,outside)": rows(con, f"""
            with p as (select subject_code, sex_code, period_year,
                              max(value) filter (where area_name = 'Ogółem') pl,
                              min(value) filter (where area_name <> 'Ogółem') lo,
                              max(value) filter (where area_name <> 'Ogółem') hi,
                              count(value) filter (where area_name <> 'Ogółem') nv
                       from {ob} group by all)
            select count(*), count(*) filter (where pl < lo - 1 or pl > hi + 1) from p where pl is not null and nv = 16"""),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    series = "measure_key || '|' || geography_key || '|' || breakdown_json"
    g["dup_series_date"] = one(con, f"select count(*) from (select {series} as series_key, date_key from {b} group by all having count(*) > 1)")
    g["measures"] = rows(con, f"select measure_key, count(*) from {b} group by 1 order by 1")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["series"] = one(con, f"select count(distinct {series}) from {b}")
    g["dates(min,max)"] = rows(con, f"select min(date_key), max(date_key) from {b}")
    g["areas_matched"] = one(con, f"select count(distinct geography_key) from {b} where geography_key <> '_X'")
    g["unmatched(code,rows)"] = rows(con, f"select geography_source_code, count(*) from {b} where geography_key = '_X' group by 1 order by 1", 40)
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["status"] = rows(con, f"select observation_status_key, count(*) from {b} group by 1")
    g["sex(is_total,sex_code,rows)"] = rows(con, f"select is_total, sex_code, count(*) from {b} group by all order by all")
    g["silver_rows_with_value_not_in_gold"] = one(con, f"select count(*) from {ob} where value is not null") - g["rows"]
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
        "PL_T(subject,year,value)": rows(con, f"select measure_codes, period_label, value from {b} where geography_key = 'PL' and sex_code = 'T' "
                                             f"and measure_codes in ('polish', 'mathematics', 'english') order by 1, 2"),
        "PL_2025_by_sex(subject,sex,value)": rows(con, f"select measure_codes, sex_code, value from {b} where geography_key = 'PL' "
                                                      f"and period_label = '2025' order by 1, 2", 30),
        "2025_maths_T_by_voivodeship(key,name,value)": rows(con, f"select geography_key, geography_source_code, value from {b} "
                                                               f"where period_label = '2025' and sex_code = 'T' and measure_codes = 'mathematics' order by 1"),
    }
    con.close()
    if g["dup_series_date"] or g["null_values"] or not g["rows"] or g["unmatched(code,rows)"]:
        failures.append("gold checks")
    if g["measures_with_two_frequencies"]:
        failures.append("a measure with two frequencies")
    if s["dup_grain"] or s["null_keys"] or s["zeros_with_value"]:
        failures.append("silver checks")
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
        landing_failed = (runs[0].get("outcome") != "landed_new" or runs[1].get("outcome") != "unchanged"
                          or runs[1].get("landing_files_after") != runs[0].get("landing_files_after") or land["verify_exit"])
        csv_rows, body, receipt = landed_csv()
        land["markers"] = landing_markers(csv_rows)
        land["decoder_rejects_altered_copies"] = rejection_proof(body, receipt)
        note(f"{SOURCE} landing (twice), inspect, verify, markers", land)
        note(f"{SOURCE} landed CSV rows, Poland ogółem 2025 (spot check source)",
             [[r["przedmiot"], r["wartosc"], r["flaga"]] for r in csv_rows
              if r["wojewodztwo"] == "Ogółem" and r["plec"] == "ogółem" and r["rok"] in ("2019", "2025")])
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        note(f"{SOURCE} bronze profile", bronze_profile(con))
        con.close()
        work = Path(tempfile.mkdtemp(prefix="gus-exam-dbt-"))
        try:
            d, s, g, spots, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append("landing: first read must land, second must write nothing, verify must pass")
        if b["loads"][1].get("appended_files") or b["rejected"]:
            failures.append("bronze: second load appended or a file was rejected")
        if any(v == "NOT REJECTED" for v in land["decoder_rejects_altered_copies"].values()):
            failures.append("decoder accepted an altered file")
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
