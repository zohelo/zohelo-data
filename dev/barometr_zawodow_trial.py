"""Dev trial of Barometr Zawodów (register PLS-030, #454): Landing → Bronze → Silver → Gold into scratch.

Landing and lake prefix zz_dev_barometr_zawodow/, Iceberg namespace zz_dev_barometr_zawodow; everything written is
removed in `finally`. Argument `bronze`: Landing twice (the second must write nothing new), inspect, verify, Bronze
twice (the second must append nothing), the Bronze profile and, for a rejected file, a dump of the failing page to
the job log. Argument `full`: the same, then a real dbt build, twice, of the Silver model, the code lists,
poc_map_geography and poc_build_barometr_zawodow with their tests, into a DuckDB file on the runner (production
catalog attached read-only as "lake" for the other parents). Deleted before merge (never on the PR branch).
"""

from __future__ import annotations

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
from ingestion.to_bronze.barometr_zawodow import load as bz_load
from ingestion.to_bronze.barometr_zawodow import posters
import ingestion.to_landing.barometr_zawodow as landing_mod
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_barometr_zawodow"
SOURCE = "barometr_zawodow"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("barometr_zawodow_forecasts",)
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_barometr_zawodow"]
KPIS = {"occupations_deficit_forecast": "barometr_zawodow:forecast:occupations:deficyt"}


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
        if n == 2 and landing_mod.specs.__name__ == "chosen":
            break
        code = landing_mod._run("load", landing_root=SCRATCH)
        files = next((d for t, d in CAPTURED if t.endswith("landing load")), {}).get("files", [])
        outcomes: dict = {}
        for f in files:
            outcomes[f.get("outcome")] = outcomes.get(f.get("outcome"), 0) + 1
        n_files, size, receipts = landing_objects()
        out["runs"].append({"run": n, "exit": code, "outcomes": outcomes,
                            "not_landed": [f for f in files if f.get("outcome") not in ("landed_new", "unchanged",
                                                                                      "closed year, held",
                                                                                      "not published yet")],
                            "landing_files_after": n_files, "landing_bytes_after": size, "receipts_after": receipts})
    for mode in ("inspect", "verify"):
        CAPTURED.clear()
        out[f"{mode}_exit"] = landing_mod._run(mode, landing_root=SCRATCH)
        out[mode] = [{k: f.get(k) for k in ("entity", "state", "landed_versions", "missing_data", "http")}
                     for t, d in CAPTURED if t.endswith(f"landing {mode}") for f in d.get("files", [])]
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = bz_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for t, d in CAPTURED if isinstance(d, dict)
                                                       for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, bz_load.TABLE_ID[1]))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, bz_load.REJECTED_ID[1])).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or r.get("reason") or str(r))[:300]}
                       for r in rej]
    return out, arrow


def dump_entities(entities: list[str], reasons: dict[str, str]) -> None:
    """For each file: every poster page's wide shapes, rotated text pieces and lines, to the job log only."""
    from pypdf import PdfReader

    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    for entity in entities:
        held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, entity, SCRATCH))
        if not held:
            print(f"===== DUMP {entity}: not landed", flush=True)
            continue
        body = bulk_bronze.read_raw(client, bucket, SOURCE, held[-1])
        reader = PdfReader(io.BytesIO(body))
        for number, page in enumerate(reader.pages, 1):
            print(f"===== DUMP {entity} page {number}: {reasons.get(entity, 'read')}", flush=True)
            try:
                shapes, texts = posters.drawing(page, reader)
                print("MEDIABOX", page.mediabox, flush=True)
                print("SHAPES", json.dumps([{k: (round(v, 1) if isinstance(v, float) else v) for k, v in s.items()}
                                            for s in shapes if (s["x1"] - s["x0"]) * (s["y1"] - s["y0"]) > 300][:80]), flush=True)
                for g in [t for t in texts if t["rotated"]]:
                    text = posters.decode(page, reader, [[g]], rotated=True)[0]
                    print(f"ROTATED {g['x']:.1f} {g['y']:.1f} {text!r}", flush=True)
                for line in posters.lines_of(page, reader, texts):
                    print(f"LINE {line['x']:.1f} {line['y']:.1f} {line['text'][:110]}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"DUMP FAILED {type(exc).__name__}: {exc}", flush=True)


def bronze_profile(con) -> dict:
    out = {}
    for k, q in _profile_queries().items():
        try:
            out[k] = q(con)
        except duckdb.Error as exc:
            out[k] = f"query failed: {str(exc)[:150]}"
    return out


def _profile_queries() -> dict:
    v = "bronze"
    return {
        "rows": lambda con: one(con, f"select count(*) from {v}"),
        "files(kind,files,first,last)": lambda con: rows(con, f"select case when entity like 'plakat_p%' then 'province' else 'national' end, count(distinct entity), min(edition_year), max(edition_year) from {v} group by 1"),
        "posters_per_year(year,files,areas)": lambda con: rows(con, f"select edition_year, count(distinct entity), count(distinct area_label) from {v} group by 1 order by 1", 30),
        "area_labels(label,files)": lambda con: rows(con, f"select area_label, count(distinct entity) from {v} group by 1 order by 1", 60),
        "province_page_to_area(page,areas)": lambda con: rows(con, f"select split_part(entity, '_', 2), string_agg(distinct area_label, ' | ') from {v} where entity like 'plakat_p%' group by 1 order by 1", 20),
        "category_labels(label,rows)": lambda con: rows(con, f"select category_label, count(*) from {v} group by 1 order by 1"),
        "poster_categories(set,posters)": lambda con: rows(con, f"select poster_categories, count(distinct entity) from {v} group by 1 order by 1"),
        "dup_keys": lambda con: one(con, f"select count(*) from (select entity, area_label, occupation_name from {v} group by all having count(*) > 1)"),
        "null_keys": lambda con: one(con, f"select count(*) from {v} where entity is null or area_label is null or occupation_name is null or category_label is null or edition_year is null"),
        "occupations_per_poster(min,max)": lambda con: rows(con, f"select min(n), max(n) from (select count(*) n from {v} group by entity)"),
        "poland_by_year(year,category,n)": lambda con: rows(con, f"select edition_year, category_label, count(*) from {v} where entity not like 'plakat_p%' group by all order by all", 60),
        "voiv_names_not_in_poland_list(year,n,sample)": lambda con: rows(con, f"""
            select r.edition_year, count(*), string_agg(distinct r.occupation_name, ' | ')[:300] from {v} r
            where r.entity like 'plakat_p%' and r.occupation_name not in
              (select p.occupation_name from {v} p where p.entity not like 'plakat_p%' and p.edition_year = r.edition_year)
            group by 1 order by 1"""),
        "lowercase_or_odd_names": lambda con: rows(con, f"select occupation_name, count(*) from {v} where not regexp_matches(occupation_name, '^[A-ZĄĆĘŁŃÓŚŹŻ]') or length(occupation_name) > 90 group by 1 limit 20"),
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
    run(con, f"create table bronze.{bz_load.TABLE_ID[1]} as select * from bronze_arrow")
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
    own_tests = sorted(nodes[k]["name"] for k in tests if SOURCE in k)
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
                     "own_tests": {k.split(".")[-2] if k.count(".") > 2 else k: v[0] for k, v in results.items()
                                   if k.startswith("test.") and SOURCE in k}})
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
    sv, b = rel["barometr_zawodow_forecasts"], rel["poc_build_barometr_zawodow"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped, "own_tests_selected": own_tests,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "rows": one(con, f"select count(*) from {sv}"),
        "editions(min,max,n)": rows(con, f"select min(forecast_year), max(forecast_year), count(distinct forecast_year) from {sv}"),
        "areas_per_edition(min,max)": rows(con, f"select min(n), max(n) from (select count(distinct area_code) n from {sv} group by forecast_year)"),
        "by_category(key,label,rows)": rows(con, f"select category_key, category_label, count(*) from {sv} group by all order by all"),
        "occupations_per_edition(year,PL,voiv_min,voiv_max)": rows(con, f"""
            select forecast_year, count(*) filter (where area_code = 'PL'),
                   min(n) filter (where area_code <> 'PL'), max(n) filter (where area_code <> 'PL')
            from (select forecast_year, area_code, count(*) over (partition by forecast_year, area_code) n from {sv})
            group by 1 order by 1"""),
        "dup_grain": one(con, f"select count(*) from (select forecast_year, area_code, occupation_name from {sv} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {sv} where forecast_year is null or area_code is null or occupation_name is null or category_key is null or raw_sha256 is null"),
        "unmapped_areas": rows(con, f"select area_label, count(*) from {sv} where area_code is null group by 1"),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    series = "measure_key || '|' || geography_key || '|' || breakdown_json"
    g["dup_series_date"] = one(con, f"select count(*) from (select {series} as series_key, date_key from {b} group by all having count(*) > 1)")
    g["measures"] = rows(con, f"select measure_key, count(*), min(value), max(value) from {b} group by 1 order by 1")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["series"] = one(con, f"select count(distinct {series}) from {b}")
    g["dates(min,max)"] = rows(con, f"select min(date_key), max(date_key) from {b}")
    g["unmatched(code,rows)"] = rows(con, f"select geography_source_code, count(*) from {b} where geography_key = '_X' group by 1 order by 1", 40)
    g["areas_matched"] = one(con, f"select count(distinct geography_key) from {b} where geography_key <> '_X'")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["counts_equal_silver"] = one(con, f"""select count(*) from (select forecast_year, area_code, category_key, count(*) n from {sv} group by all) s
        full join (select year(date_key) y, geography_source_code a, split_part(measure_key, ':', 4) c, value v from {b}) g
        on s.forecast_year = g.y and s.area_code = g.a and s.category_key = g.c
        where coalesce(s.n, 0) <> coalesce(g.v, 0)""")
    g["map_rows"] = one(con, f"select count(*) from {rel['poc_map_geography']} where source_system = '{SOURCE}'")
    kpi = {}
    for k, pattern in KPIS.items():
        f = f"from {b} where measure_key like '{pattern}' and is_total and geography_key <> '_X'"
        kpi[k] = {"metrics_rows": one(con, f"select count(*) {f}"),
                  "max_rows_per_area_period": one(con, f"select coalesce(max(n), 0) from (select count(*) n {f} group by geography_key, date_key)"),
                  "min": rows(con, f"select arg_min(geography_key || ' ' || period_label || ' ' || value, value) {f}"),
                  "max": rows(con, f"select arg_max(geography_key || ' ' || period_label || ' ' || value, value) {f}")}
    g["kpi_checks"] = kpi
    spots = {
        "PL_by_year(year,measure,value)": rows(con, f"select period_label, split_part(measure_key, ':', 4), value from {b} where geography_key = 'PL' order by 1, 2", 60),
        "voiv_2026(area,measure,value)": rows(con, f"select geography_key, split_part(measure_key, ':', 4), value from {b} where period_label = '2026' and geography_key <> 'PL' order by 1, 2", 60),
        "PL_2026_deficit_names": rows(con, f"select occupation_name from {sv} where forecast_year = 2026 and area_code = 'PL' and category_key = 'deficyt' order by 1", 40),
    }
    con.close()
    if g["dup_series_date"] or g["null_values"] or not g["rows"] or g["counts_equal_silver"]:
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
    return {"source_row": src, "licence_row": [{k: v for k, v in r.items() if k != "notes"} for r in lic],
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
    mode = argv[0] if argv else "full"
    only = argv[1].split(",") if mode == "diag" and len(argv) > 1 else []
    if only:  # diag: land and load only these files, once each
        every = landing_mod.specs

        def chosen(today=None):
            return [s for s in every(today) if s["entity"] in only]

        landing_mod.specs = chosen
    status = 0
    try:
        land = landing()
        r1, r2 = (land["runs"] * 2)[:2]
        landing_failed = (r1["not_landed"] or r2.get("outcomes", {}).get("landed_new") or r2.get("outcomes", {}).get("landed_changed")
                          or r2["landing_files_after"] != r1["landing_files_after"] or land["verify_exit"])
        note(f"{SOURCE} landing (twice), inspect, verify", land)
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        if b["rejected"]:
            dump_entities([r["entity"] for r in b["rejected"]], {r["entity"]: r["error"] for r in b["rejected"]})
        if mode == "diag":
            dump_entities([e for e in only if e not in {r["entity"] for r in b["rejected"]}], {})
        con = duckdb.connect()
        con.register("bronze", arrow)
        note(f"{SOURCE} bronze profile", bronze_profile(con))
        con.close()
        failures = []
        if landing_failed:
            failures.append("landing: second run must write nothing, verify must pass")
        if b["loads"][1].get("appended_files") or b["rejected"]:
            failures.append("bronze: second load appended or a file was rejected")
        if mode == "full":
            work = Path(tempfile.mkdtemp(prefix="bz-dbt-"))
            try:
                d, s, g, spots, more = build(arrow, work)
            finally:
                shutil.rmtree(work, ignore_errors=True)
            failures += more
            note(f"{SOURCE} dbt build x2", d)
            note(f"{SOURCE} silver profile", s)
            note(f"{SOURCE} gold", {**g, "failures": failures})
            note(f"{SOURCE} gold spot values", spots)
            note(f"{SOURCE} licence gate", licence())
        else:
            note(f"{SOURCE} failures", failures)
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
