"""Dev trial of WTO Stats (register GL-020, #3317): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_wto_stats/, Iceberg namespace zz_dev_wto_stats; everything written is removed in
`finally`. The BaTIS BPM6 ZIP is downloaded twice through the shared bulk_file pattern (the second must write nothing),
Bronze loads twice (the second must append nothing). Then a real dbt build, twice, of the Silver model, the eight code
lists, poc_map_geography (with this source's registration) and poc_build_wto_stats with every dbt test attached to them
(metrics_value_range excluded: the metrics models read the shared facts; one row per source x area x period and the
value bounds are checked on the build stage with the same filter, is_total and not _X). dbt writes only to a DuckDB
file on the runner that plays the database "r2" (target prod); the production catalog is attached read-only as "lake"
and the other parents are views onto it; the Bronze tables are copies of the scratch tables. Deleted before merge
(never on the PR branch).
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
from ingestion.to_bronze.wto_stats import load as wto_load
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_wto_stats"
SOURCE = "wto_stats"
ENTITY = "batis_bpm6"
CAPTURED: list = []
CONTRACT = [
    "source_system",
    "block_id",
    "block_name",
    "measure_key",
    "measure_name",
    "measure_codes",
    "unit_code",
    "unit_name",
    "geography_source_code",
    "geography_key",
    "frequency",
    "date_key",
    "period_label",
    "breakdown_json",
    "breakdown_label_json",
    "sex_code",
    "is_total",
    "value",
    "observation_status_key",
    "source_version",
    "raw_sha256",
]
SILVER = ("wto_stats_services_trade",)
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_wto_stats"]
KPIS = {"exports_services": "wto_stats:batis:USD_MN:X.S.final", "imports_services": "wto_stats:batis:USD_MN:M.S.final"}
NUM = "-?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?"


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:3000]}", flush=True)


bulk_file.annotate = _capture
bulk_bronze.annotate = _capture
wto_load.annotate = _capture


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
        code = bulk_file.main(SOURCE, "load", landing_root=SCRATCH)
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        detail = (summary.get("files") or [{}])[0]
        out["runs"].append(
            {
                "run": n,
                "exit": code,
                "minutes": round((time.monotonic() - start) / 60, 1),
                **{k: v for k, v in detail.items() if k != "sha256"},
                "sha256": str(detail.get("sha256", ""))[:16],
                "landing_files_after": files,
                "landing_bytes_after": size,
                "receipts_after": receipts,
            }
        )
    for mode in ("inspect", "verify"):
        CAPTURED.clear()
        out[f"{mode}_exit"] = bulk_file.main(SOURCE, mode, landing_root=SCRATCH)
        files = next((d for t, d in CAPTURED if f"landing {mode}" in t), {}).get("files") or [{}]
        out[mode] = {k: v for k, v in files[0].items() if k not in ("page_links",)}
    return out


def bronze() -> tuple[dict, object, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = wto_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append(
            {
                "run": n,
                "exit": code,
                "tables": [
                    {
                        k: v
                        for k, v in d.items()
                        if k in ("table", "entities_landed", "candidates", "appended_files", "rows_added", "rejected")
                    }
                    for t, d in CAPTURED
                    if "Bronze from Landing" in t
                ],
                "csv": [d.get("files") for t, d in CAPTURED if "rows read and kept" in t],
            }
        )
    cat = catalog()
    arrows = {}
    for name in (wto_load.TABLE, wto_load.CODES):
        t = cat.load_table((SCRATCH, name))
        arrows[name] = t.scan().to_arrow()
        out[name] = {"files": len(list(t.scan().plan_files())), "rows": arrows[name].num_rows}
    try:
        rej = cat.load_table((SCRATCH, wto_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrows[wto_load.TABLE], arrows[wto_load.CODES]


def bronze_profile(con) -> dict:
    v = "bronze"
    key = "reporter, partner, flow, item_code, year"
    return {
        "rows": one(con, f"select count(*) from {v}"),
        "by_selection(pl_reporter,pl_partner,group_reporter)": rows(
            con,
            f"select count(*) filter (where reporter = 'PL'), "
            f"count(*) filter (where partner = 'PL'), count(*) filter (where type_reporter = 'g') from {v}",
        ),
        "reporters": one(con, f"select count(distinct reporter) from {v}"),
        "partners": one(con, f"select count(distinct partner) from {v}"),
        "group_reporters": rows(
            con, f"select reporter, count(*) from {v} where type_reporter = 'g' group by 1 order by 1"
        ),
        "years(min,max,n)": rows(con, f"select min(year), max(year), count(distinct year) from {v}"),
        "items(code,type,rows)": rows(
            con, f"select item_code, type_item, count(*) from {v} group by all order by 1", 40
        ),
        "flows": rows(con, f"select flow, count(*) from {v} group by 1 order by 1"),
        "methodology(code,rows)": rows(
            con, f"select final_value_methodology, count(*) from {v} group by 1 order by 2 desc", 40
        ),
        "dup_keys": one(con, f"select count(*) from (select {key} from {v} group by all having count(*) > 1)"),
        "null_keys": one(
            con,
            f"select count(*) from {v} where reporter is null or partner is null or flow is null or item_code is null or year is null",
        ),
        "nulls(reported,final,balanced,method)": rows(
            con,
            f"select count(*) filter (where reported_value is null), count(*) filter (where final_value is null), "
            f"count(*) filter (where balanced_value is null), count(*) filter (where final_value_methodology is null) from {v}",
        ),
        "non_numeric(column,value,rows)": rows(
            con,
            f"""
            select c, x, count(*) from (
                select 'reported' c, reported_value x from {v} union all select 'final', final_value from {v}
                union all select 'balanced', balanced_value from {v} union all select 'year', year from {v})
            where x is not null and not regexp_full_match(x, '{NUM}') group by all order by 3 desc limit 15""",
        ),
        "zeros(final,balanced,final_zero_by_method)": [
            one(con, f"select count(*) from {v} where try_cast(final_value as double) = 0"),
            one(con, f"select count(*) from {v} where try_cast(balanced_value as double) = 0"),
            rows(
                con,
                f"select final_value_methodology, count(*) from {v} where try_cast(final_value as double) = 0 group by 1 order by 2 desc limit 8",
            ),
        ],
        "negatives(final,balanced)": rows(
            con,
            f"select count(*) filter (where try_cast(final_value as double) < 0), count(*) filter (where try_cast(balanced_value as double) < 0) from {v}",
        ),
        "csv_member": rows(con, f"select csv_member, count(*) from {v} group by 1"),
    }


def codes_profile(con) -> dict:
    v = "codes"
    return {
        "rows_by_sheet": rows(con, f"select sheet_name, count(*), max(row_number) from {v} group by 1 order by 1"),
        "notes": rows(
            con,
            f"select row_number, column_number, left(value_text, 230) from {v} where sheet_name = 'notes' order by 1, 2",
            25,
        ),
        "methodology": rows(
            con,
            f"select max(value_text) filter (where column_number = 1), left(max(value_text) filter (where column_number = 2), 70) "
            f"from {v} where sheet_name = 'methodology codes' and row_number > 1 group by row_number order by row_number",
            30,
        ),
        "items": rows(
            con,
            f"select max(value_text) filter (where column_number = 1), left(max(value_text) filter (where column_number = 2), 45) "
            f"from {v} where sheet_name = 'service items' and row_number > 1 group by row_number order by row_number",
            35,
        ),
        "economy_codes_twice": one(
            con,
            f"select count(*) from (select value_text from {v} where sheet_name = 'economies' and column_number = 1 "
            f"and row_number > 1 group by 1 having count(*) > 1)",
        ),
        "bronze_codes_without_label": rows(
            con,
            f"select distinct x from (select reporter x from bronze union select partner from bronze) "
            f"where x not in (select value_text from {v} where sheet_name = 'economies' and column_number = 1)",
        ),
    }


def rendered(value):
    env = jinja2.Environment()
    env.globals["env_var"] = lambda name, default=None: os.environ.get(name, default)
    if isinstance(value, dict):
        return {k: rendered(v) for k, v in value.items()}
    return env.from_string(value).render() if isinstance(value, str) else value


def dbt(work: Path, *args: str) -> tuple[int, dict]:
    cmd = [
        "dbt",
        *args,
        "--project-dir",
        str(WAREHOUSE),
        "--profiles-dir",
        str(work / "profiles"),
        "--target",
        "prod",
        "--target-path",
        str(work / "target"),
        "--no-partial-parse",
    ]
    proc = subprocess.run(
        cmd, capture_output=True, text=True, env={**os.environ, "DUCKDB_TEMP_DIRECTORY": str(work / "tmp")}
    )
    print(proc.stdout[-6000:], proc.stderr[-3000:], flush=True)
    results = {}
    path = work / "target" / "run_results.json"
    if args[0] != "parse" and path.exists():
        for r in json.loads(path.read_text())["results"]:
            results[r["unique_id"]] = (r["status"], r.get("failures"), (r.get("message") or "")[:200])
        path.unlink()
    return proc.returncode, results


def setup_dbt(work: Path, arrows: dict) -> tuple[dict, list[str], list[str], list[str]]:
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
    (work / "profiles" / "profiles.yml").write_text(
        yaml.safe_dump({"zohelo": {"target": "prod", "outputs": {"prod": prod}}})
    )
    code, _ = dbt(work, "parse")
    if code:
        raise RuntimeError("dbt parse failed")
    manifest = json.loads((work / "target" / "manifest.json").read_text())
    nodes, sources = manifest["nodes"], manifest["sources"]
    selected = {f"model.zohelo.{m}" for m in SELECT}
    tests = [
        k
        for k, n in nodes.items()
        if n["resource_type"] == "test" and selected & set(n["depends_on"]["nodes"]) and "metrics_value_range" not in k
    ]
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
        except duckdb.CatalogException:  # not in production yet (an optional parent, poc_optional_ref)
            views.append(f"missing:{schema}.{name}")
            missing.add(p)
    con.close()
    aliases = {m: (nodes[f"model.zohelo.{m}"]["schema"], nodes[f"model.zohelo.{m}"]["alias"]) for m in SELECT}
    skipped = sorted(nodes[k]["name"] for k in tests if missing & set(nodes[k]["depends_on"]["nodes"]))
    own_tests = sorted(nodes[k]["name"] for k in tests if "wto_stats" in k)
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"], skipped, own_tests


def build(arrows: dict, work: Path) -> tuple[dict, dict, dict, dict, list[str]]:
    """dbt build twice; returns (dbt summary, silver profile, gold checks, spot values, failures)."""
    failures: list[str] = []
    aliases, views, skipped, own_tests = setup_dbt(work, arrows)
    runs, fingerprints = [], []
    for attempt in (1, 2):
        code, results = dbt(work, "build", "--select", *SELECT, "--exclude", "metrics_value_range", *skipped)
        by_status: dict[str, int] = {}
        for st, _, _ in results.values():
            by_status[st] = by_status.get(st, 0) + 1
        bad = {k: v for k, v in results.items() if v[0] not in ("success", "pass")}
        runs.append(
            {
                "exit": code,
                "nodes": len(results),
                "tests": sum(1 for k in results if k.startswith("test.")),
                "status": by_status,
                "not_passing": bad,
                "own_tests_passed": sum(
                    1 for k, v in results.items() if k.startswith("test.") and "wto_stats" in k and v[0] == "pass"
                ),
            }
        )
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
    ob, b = rel["wto_stats_services_trade"], rel["poc_build_wto_stats"]
    dbt_summary = {
        "runs": runs,
        "tests_skipped_parent_missing": skipped,
        "own_tests_selected": own_tests,
        "second_build_same_tables": fingerprints[0] == fingerprints[1],
        "rows": {m: v[0] for m, v in fingerprints[0].items()},
        "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
        "parents_missing_in_production": [v for v in views if v.startswith("missing:")],
    }
    s = {
        "rows": one(con, f"select count(*) from {ob}"),
        "release": rows(con, f"select release, count(*) from {ob} group by 1"),
        "years(min,max)": rows(con, f"select min(year), max(year) from {ob}"),
        "by_reporter_kind(pl_reporter,pl_partner,groups)": rows(
            con,
            f"select count(*) filter (where reporter_code = 'PL'), count(*) filter (where partner_code = 'PL'), "
            f"count(*) filter (where reporter_type = 'g') from {ob}",
        ),
        "dup_grain": one(
            con,
            f"select count(*) from (select reporter_code, partner_code, flow, item_code, year from {ob} group by all having count(*) > 1)",
        ),
        "null_keys": one(
            con,
            f"select count(*) from {ob} where reporter_code is null or partner_code is null or flow is null or item_code is null or year is null",
        ),
        "null_labels(reporter,partner,item,method)": rows(
            con,
            f"select count(*) filter (where reporter_name is null), count(*) filter (where partner_name is null), "
            f"count(*) filter (where item_name is null), count(*) filter (where final_value_methodology_label is null) from {ob}",
        ),
        "null_values(reported,final,balanced)": rows(
            con,
            f"select count(*) filter (where reported_value is null), count(*) filter (where final_value is null), "
            f"count(*) filter (where balanced_value is null) from {ob}",
        ),
        "pl_world_total_by_year(year,X_final,X_bal,M_final,M_bal)": rows(
            con,
            f"""
            select year, max(final_value) filter (where flow = 'X'), max(balanced_value) filter (where flow = 'X'),
                   max(final_value) filter (where flow = 'M'), max(balanced_value) filter (where flow = 'M')
            from {ob} where reporter_code = 'PL' and partner_code = 'WL' and item_code = 'S' and year >= 2019 group by 1 order by 1""",
        ),
        "pl_main_items_vs_total(n,max_rel_diff_final,over_1pct)": rows(
            con,
            f"""
            with g as (select partner_code, flow, year, max(final_value) filter (where item_code = 'S') t,
                              sum(final_value) filter (where item_code in ('SA','SB','SC','SD','SE','SF','SG','SH','SI','SJ','SK','SL')) p
                       from {ob} where reporter_code = 'PL' group by all)
            select count(*), max(abs(t - p) / nullif(abs(t), 0)), count(*) filter (where abs(t - p) > 0.01 * abs(t) + 0.01) from g where t is not null""",
        ),
        "pl_partners_vs_world_balanced(n,max_rel_diff,over_1pct)": rows(
            con,
            f"""
            with g as (select flow, item_code, year, max(balanced_value) filter (where partner_code = 'WL') w,
                              sum(balanced_value) filter (where partner_type = 'c') p
                       from {ob} where reporter_code = 'PL' group by all)
            select count(*), max(abs(w - p) / nullif(abs(w), 0)), count(*) filter (where abs(w - p) > 0.01 * abs(w) + 0.01) from g where w is not null""",
        ),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    series = (
        "measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') "
        "else '' end || '|' || breakdown_json"
    )
    g["dup_series_date"] = one(
        con, f"select count(*) from (select {series} as series_key, date_key from {b} group by all having count(*) > 1)"
    )
    g["dup_series_date_without_source_code"] = one(
        con,
        f"select count(*) from (select measure_key, geography_key, breakdown_json, date_key "
        f"from {b} where geography_key <> '_X' group by all having count(*) > 1)",
    )
    g["measures"] = one(con, f"select count(distinct measure_key) from {b}")
    g["measures_with_two_frequencies"] = one(
        con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)"
    )
    g["series"] = one(con, f"select count(distinct {series}) from {b}")
    g["partners"] = one(con, f"select count(distinct breakdown_json) from {b}")
    g["dates(min,max)"] = rows(con, f"select min(date_key), max(date_key) from {b}")
    g["geography(key,rows)"] = rows(con, f"select geography_key, count(*) from {b} group by 1 order by 1")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["status"] = rows(con, f"select observation_status_key, count(*) from {b} group by 1 order by 1")
    g["is_total(rows)"] = rows(con, f"select is_total, count(*) from {b} group by 1 order by 1")
    g["silver_pl_main_rows_x2"] = 2 * one(
        con,
        f"select count(*) from {ob} where reporter_code = 'PL' and item_code in "
        f"('S','SA','SB','SC','SD','SE','SF','SG','SH','SI','SJ','SK','SL')",
    )
    g["map_rows"] = one(con, f"select count(*) from {rel['poc_map_geography']} where source_system = '{SOURCE}'")
    kpi = {}
    for k, measure in KPIS.items():
        f = f"from {b} where measure_key = '{measure}' and is_total and geography_key <> '_X'"
        kpi[k] = {
            "metrics_rows": one(con, f"select count(*) {f}"),
            "max_rows_per_area_period": one(
                con, f"select coalesce(max(n), 0) from (select count(*) n {f} group by geography_key, date_key)"
            ),
            "min": rows(con, f"select arg_min(period_label || ' ' || value, value) {f}"),
            "max": rows(con, f"select arg_max(period_label || ' ' || value, value) {f}"),
            "status": rows(con, f"select observation_status_key, count(*) {f} group by 1"),
        }
    g["kpi_checks"] = kpi
    spots = {
        "PL_world_S(measure,year,value,status)": rows(
            con,
            f"select measure_codes, period_label, value, observation_status_key from {b} "
            f"where is_total and measure_codes like '%.S.%' and period_label >= '2021' order by 1, 2",
        ),
        "PL_2024_X_final_by_item(codes,value,status)": rows(
            con,
            f"select measure_codes, value, observation_status_key from {b} where is_total "
            f"and period_label = '2024' and measure_codes like 'X.%.final' order by 1",
        ),
        "PL_2024_X_S_final_top_partners": rows(
            con,
            f"select breakdown_label_json, value, observation_status_key from {b} where not is_total "
            f"and period_label = '2024' and measure_codes = 'X.S.final' order by value desc limit 8",
        ),
        "PL_2024_M_S_final_DE_vs_balanced": rows(
            con,
            f"select measure_codes, value from {b} where period_label = '2024' and breakdown_json = '{{\"partner\":\"DE\"}}' "
            f"and measure_codes like 'M.S.%' order by 1",
        ),
    }
    con.close()
    if g["dup_series_date"] or g["dup_series_date_without_source_code"] or g["null_values"] or not g["rows"]:
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
    return {
        "source_row": src,
        "licence_row": lic,
        "licensed_by_gate": SOURCE in licensed_sources(seed("source"), seed("licence")),
    }


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
    for var, label in (
        ("STORAGE_LANDING_BUCKET_NAME", "landing_objects"),
        ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects"),
    ):
        bucket = req(var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/")
    )
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
        landing_failed = (
            runs[0].get("outcome") not in ("landed_new",)
            or runs[1].get("outcome") != "unchanged"
            or runs[1].get("landing_files_after") != runs[0].get("landing_files_after")
            or land["verify_exit"]
            or land["inspect"].get("state") != "current"
        )
        note(f"{SOURCE} landing (twice), inspect, verify", land)
        if runs[0].get("exit"):
            raise RuntimeError("landing failed")
        b, arrow, codes = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        con.register("codes", codes)
        note(f"{SOURCE} bronze profile", {**bronze_profile(con), "codes": codes_profile(con)})
        con.close()
        work = Path(tempfile.mkdtemp(prefix="wto-dbt-"))
        try:
            d, s, g, spots, failures = build({wto_load.TABLE: arrow, wto_load.CODES: codes}, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append(
                "landing: first read must land, second must write nothing, inspect current, verify must pass"
            )
        if any(t.get("appended_files") for t in b["loads"][1]["tables"]) or b["rejected"]:
            failures.append("bronze: second load appended or a file was rejected")
        note(f"{SOURCE} dbt build x2", d)
        note(f"{SOURCE} silver profile", s)
        note(f"{SOURCE} gold", {**g, "failures": failures})
        note(f"{SOURCE} gold spot values and licence gate", {**spots, "licence": licence()})
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
