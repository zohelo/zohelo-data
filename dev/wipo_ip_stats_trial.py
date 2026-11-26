"""Dev trial of WIPO IP Statistics (register GL-032, #1262): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_wipo_ip_stats/, Iceberg namespace zz_dev_wipo_ip_stats; everything written is
removed in `finally`. The two ZIPs are downloaded twice through the shared bulk_file pattern (the second run must land
nothing), Bronze loads twice (the second must append nothing). Then a real dbt build, twice, of the three Silver
models, the eight code lists, poc_map_geography (with this source's registration) and poc_build_wipo_ip_stats with
every dbt test attached to them (metrics_value_range excluded: the metrics models read the shared facts; their rows,
one row per source x area x period and the value bounds are checked on the build stage with the metrics filter,
is_total and not _X). dbt writes only to a DuckDB file on the runner that plays the database "r2" (target prod); the
production catalog is attached read-only as "lake" and the other parents are views onto it; the Bronze table is a copy
of the scratch table. Deleted before merge (never on the PR branch).
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
from ingestion.to_bronze.wipo_ip_stats import load as wipo_load
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_wipo_ip_stats"
SOURCE = "wipo_ip_stats"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("wipo_ip_stats_patent_publications", "wipo_ip_stats_patent_grants", "wipo_ip_stats_designs")
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_wipo_ip_stats"]
KPIS = {"patent_grants_filing_office": "wipo_ip_stats:patent_grants:grants:by_office",
        "patent_grants_applicant_origin": "wipo_ip_stats:patent_grants:grants:by_origin"}
BOUNDS = {"patent_grants_filing_office": (1, 2e6), "patent_grants_applicant_origin": (1, 2e6)}


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
        start = time.monotonic()
        code = bulk_file.main(SOURCE, "load", landing_root=SCRATCH, pause_seconds=10)
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        out["runs"].append({"run": n, "exit": code, "seconds": round(time.monotonic() - start),
                            "files": [{k: v for k, v in f.items() if k != "sha256"} for f in summary.get("files", [])],
                            "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
        time.sleep(10)
    for mode in ("inspect", "verify"):
        CAPTURED.clear()
        out[f"{mode}_exit"] = bulk_file.main(SOURCE, mode, landing_root=SCRATCH)
        summary = next((d for t, d in CAPTURED if f"landing {mode}" in t), {})
        out[mode] = [{k: v for k, v in f.items() if k in ("entity", "state", "http", "missing_data", "newest",
                                                           "server_version", "url")} for f in summary.get("files", [])]
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = wipo_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for t, d in CAPTURED for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, wipo_load.TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, wipo_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    v = "bronze"
    num = "regexp_full_match(count, '[0-9]+')"
    return {
        "rows": one(con, f"select count(*) from {v}"),
        "by_entity_indicator(entity,indicator,rows,years,offices,origins,classes,files)": rows(con, f"""
            select entity, indicator, count(*), min(year) || '-' || max(year), count(distinct office), count(distinct origin),
                   count(distinct class_id), count(distinct raw_sha256) from {v} group by all order by all"""),
        "dup_keys": one(con, f"select count(*) from (select entity, indicator, year, office, origin, class_id from {v} "
                             f"group by all having count(*) > 1)"),
        "null_keys(count rows)": one(con, f"select count(*) from {v} where indicator <> 'technology_field_names' and "
                                          f"(year is null or office is null or origin is null or class_id is null or count is null)"),
        "non_numeric_counts(value,rows)": rows(con, f"select count, count(*) from {v} where indicator <> 'technology_field_names' "
                                                    f"and not {num} group by 1 order by 2 desc limit 15"),
        "zero_counts": one(con, f"select count(*) from {v} where {num} and cast(count as bigint) = 0"),
        "non_year": one(con, f"select count(*) from {v} where indicator <> 'technology_field_names' and not regexp_full_match(year, '(19|20)[0-9]{{2}}')"),
        "star_codes(office **, origin **)": rows(con, f"select count(*) filter (where office = '**'), count(*) filter (where origin = '**') from {v}"),
        "field_names": rows(con, f"select class_id, class_label from {v} where indicator = 'technology_field_names' order by cast(class_id as int)", 40),
        "versions": rows(con, f"select entity, source_version, count(*) from {v} group by all order by all"),
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
    run(con, f"create table bronze.{wipo_load.TABLE} as select * from bronze_arrow")
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


def build(arrow, work: Path):
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
    b = rel["poc_build_wipo_ip_stats"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped, "own_tests_selected": own_tests,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s: dict = {}
    for m in SILVER:
        t = rel[m]
        key = ("measure, " if m.endswith("designs") else "") + "office_code, origin_code, " + (
            "locarno_class" if m.endswith("designs") else "technology_field_code") + ", year"
        s[m] = {
            "rows": one(con, f"select count(*) from {t}"),
            "by_measure(measure,rows,years,offices,origins,value_sum)": rows(con, f"select measure, count(*), min(year) || '-' || max(year), "
                                                                               f"count(distinct office_code), count(distinct origin_code), sum(value) from {t} group by 1 order by 1"),
            "dup_grain": one(con, f"select count(*) from (select {key} from {t} group by all having count(*) > 1)"),
            "null_keys_or_values": one(con, f"select count(*) from {t} where year is null or office_code is null or origin_code is null or value is null"),
            "min_max_value": rows(con, f"select min(value), max(value) from {t}"),
        }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') "
              "else '' end || '|' || breakdown_json")
    g["dup_series_date"] = one(con, f"select count(*) from (select {series} as series_key, date_key from {b} group by all having count(*) > 1)")
    g["dup_series_date_without_source_code"] = one(con, f"select count(*) from (select measure_key, geography_key, breakdown_json, date_key "
                                                        f"from {b} where geography_key <> '_X' group by all having count(*) > 1)")
    g["rows_by_measure(measure,rows,series,totals,first,last)"] = rows(con, f"""
        select measure_key, count(*), count(distinct {series}), count(*) filter (where is_total), min(date_key), max(date_key)
        from {b} group by 1 order by 1""")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["series"] = one(con, f"select count(distinct {series}) from {b}")
    g["unmatched(code,rows)"] = rows(con, f"select geography_source_code, count(*) from {b} where geography_key = '_X' group by 1 order by 2 desc", 60)
    g["unmatched_rows"] = one(con, f"select count(*) from {b} where geography_key = '_X'")
    g["areas_matched"] = one(con, f"select count(distinct geography_key) from {b} where geography_key <> '_X'")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["zero_or_negative"] = one(con, f"select count(*) from {b} where value <= 0")
    # Every Silver count reaches Gold once per view: the per-class sums of each view equal Silver's total.
    g["silver_vs_gold_sums(block,view,silver,gold_classes,gold_totals)"] = rows(con, f"""
        with sv as (
            select 'patent_publications' as block_id, sum(value) as v from {rel['wipo_ip_stats_patent_publications']}
            union all select 'patent_grants', sum(value) from {rel['wipo_ip_stats_patent_grants']}
            union all select 'design_' || measure, sum(value) from {rel['wipo_ip_stats_designs']} group by measure
        )
        select b.block_id, b.measure_codes, cast(any_value(sv.v) as bigint), cast(sum(b.value) filter (where not b.is_total) as bigint),
               cast(sum(b.value) filter (where b.is_total) as bigint)
        from {b} as b join sv on sv.block_id = b.block_id group by all order by all""")
    kpi = {}
    for k, pattern in KPIS.items():
        f = f"from {b} where measure_key like '{pattern}' and is_total and geography_key <> '_X'"
        lo, hi = BOUNDS[k]
        kpi[k] = {"metrics_rows": one(con, f"select count(*) {f}"),
                  "max_rows_per_area_period": one(con, f"select coalesce(max(n), 0) from (select count(*) n {f} group by geography_key, date_key)"),
                  "min": rows(con, f"select arg_min(geography_key || ' ' || period_label || ' ' || value, value) {f}"),
                  "max": rows(con, f"select arg_max(geography_key || ' ' || period_label || ' ' || value, value) {f}"),
                  "outside_bounds": one(con, f"select count(*) {f} and (value < {lo} or value > {hi})"),
                  "areas": one(con, f"select count(distinct geography_key) {f}")}
    g["kpi_checks"] = kpi
    pl = "geography_key = 'PL' and is_total"
    spots = {
        "PL_patent_totals(measure,year,value)": rows(con, f"select measure_key, period_label, value from {b} where {pl} "
                                                          f"and period_label >= '2015' order by 1, 2"),
        "PL_grants_office_2022_by_field(top10)": rows(con, f"select breakdown_label_json, value from {b} where geography_key = 'PL' "
                                                           f"and measure_key = 'wipo_ip_stats:patent_grants:grants:by_office' and period_label = '2022' "
                                                           f"and not is_total order by value desc limit 10"),
        "PL_designs_2018_2022(measure,year,classes,sum)": rows(con, f"select measure_key, period_label, count(*), sum(value) from {b} "
                                                                    f"where geography_key = 'PL' and block_id like 'design%' and period_label >= '2018' group by all order by all"),
        "office_codes_X_examples(EP,EM,**)": rows(con, f"select geography_source_code, measure_key, max(value) from {b} where geography_key = '_X' "
                                                       f"and geography_source_code in ('EP', 'EM', '**') and (is_total or block_id like 'design%') group by all order by all"),
        "US_CN_JP_DE_grants_office_2022": rows(con, f"select geography_key, value from {b} where measure_key = 'wipo_ip_stats:patent_grants:grants:by_office' "
                                                   f"and is_total and period_label = '2022' and geography_key in ('US', 'CN', 'JP', 'DE') order by 1"),
    }
    try:  # the UPRP annual reports in production Gold: the office's own count of patents granted
        spots["UPRP_vs_WIPO_PL_grants(year,uprp,wipo_office)"] = rows(con, f"""
            select u.period_label, u.value, w.value from lake.gold.build_uprp_statistics as u
            left join {b} as w on w.measure_key = 'wipo_ip_stats:patent_grants:grants:by_office' and w.geography_key = 'PL'
                and w.is_total and w.period_label = u.period_label
            where u.measure_key = 'uprp_statistics:annual_reports:rights:inventions_grants:total' order by 1""")
    except duckdb.Error as exc:
        spots["UPRP_vs_WIPO_PL_grants"] = f"not readable: {str(exc)[:200]}"
    con.close()
    if g["dup_series_date"] or g["dup_series_date_without_source_code"] or g["null_values"] or not g["rows"]:
        failures.append("gold checks")
    if g["measures_with_two_frequencies"]:
        failures.append("a measure with two frequencies")
    if any(v["max_rows_per_area_period"] > 1 or v["outside_bounds"] for v in kpi.values()):
        failures.append("kpi: more than one row per area x period, or a value outside its bound")
    for block, view, sv, classes, totals in g["silver_vs_gold_sums(block,view,silver,gold_classes,gold_totals)"]:
        if classes != sv or (block.startswith("patent") and totals != sv):
            failures.append(f"sums differ: {block} {view}")
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
        first = {f["entity"]: f.get("outcome") for f in runs[0]["files"]}
        second = {f["entity"]: f.get("outcome") for f in runs[1]["files"]}
        landing_failed = (runs[0]["exit"] or set(first.values()) != {"landed_new"} or set(second.values()) != {"unchanged"}
                          or runs[1]["landing_files_after"] != runs[0]["landing_files_after"] or land["verify_exit"])
        note(f"{SOURCE} landing (twice), inspect, verify", land)
        if runs[0]["exit"]:
            raise RuntimeError("landing failed")
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        note(f"{SOURCE} bronze profile", bronze_profile(con))
        con.close()
        work = Path(tempfile.mkdtemp(prefix="wipo-dbt-"))
        try:
            d, s, g, spots, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append("landing: first run must land both files, the second nothing, verify must pass")
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
