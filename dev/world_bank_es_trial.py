"""Dev trial of World Bank Enterprise Surveys via World Bank Data360 (register GL-010, #3315): Landing → Bronze →
Silver → Gold.

Landing prefix and lake prefix zz_dev_world_bank_es/, Iceberg namespace zz_dev_world_bank_es; everything written is
removed in `finally`. The bulk file WB_ES.csv is landed twice with the shared bulk_file pattern (the second run must
write nothing), Bronze loads twice (the second must append nothing). Then a real dbt build, twice, of the Silver model,
the eight code lists, poc_map_geography (with this source's registration) and poc_build_world_bank_es with every dbt
test attached to them (metrics_value_range excluded: the metrics models read the shared facts; their rows, one row per
source x area x period and the value bounds are checked on the build stage with the same filter, is_total and not
_X). dbt writes only to a DuckDB file on the runner that plays the database "r2" (target prod); the production catalog
is attached read-only as "lake" and the other parents are views onto it; the Bronze table is a copy of the scratch
table. Copied from dev/itu_datahub_trial.py (#2420). Deleted before merge (never on the PR branch).
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
from ingestion.to_bronze.world_bank_es import load as es_load
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_world_bank_es"
SOURCE = "world_bank_es"
ENTITY = "wb_es"
TABLE, REJECTED = es_load.TABLE_ID[1], es_load.REJECTED_ID[1]
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("world_bank_es_observations",)
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_world_bank_es"]
KPIS = {"firms_bribery_incidence": "world_bank_es:wb_es:PT_F:WB_ES_T_GRAFT3",
        "firms_finance_constraint": "world_bank_es:wb_es:PT_F:WB_ES_T_FIN16",
        "operating_licence_days": "world_bank_es:wb_es:D:WB_ES_T_BUS2"}
KEY = ("indicator, ref_area, sex, age, urbanisation, comp_breakdown_1, comp_breakdown_2, comp_breakdown_3, "
       "unit_measure, time_period")
NUMBER = "'-?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?'"


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
        try:
            code, error = bulk_file.main(SOURCE, "load", landing_root=SCRATCH), None
        except Exception as exc:  # noqa: BLE001
            code, error = 1, f"{type(exc).__name__}: {str(exc)[:300]}"
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        detail = (summary.get("files") or [{}])[0]
        out["runs"].append({"run": n, "exit": code, "error": error, "minutes": round((time.monotonic() - start) / 60, 1),
                            **{k: v for k, v in detail.items() if k not in ("sha256", "key", "object")},
                            "sha256": str(detail.get("sha256", ""))[:16],
                            "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    for mode in ("inspect", "verify"):
        CAPTURED.clear()
        out[f"{mode}_exit"] = bulk_file.main(SOURCE, mode, landing_root=SCRATCH)
        out[mode] = (next((d for t, d in CAPTURED if mode in t), {}).get("files") or [{}])[0]
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        start = time.monotonic()
        code = es_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                             **{k: v for t, d in CAPTURED for k, v in d.items()
                                if k in ("entities_landed", "candidates", "appended_files", "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, ENTITY, SCRATCH))
    body = bulk_bronze.read_raw(client, bucket, SOURCE, held[-1])
    out["raw_bytes"] = len(body)
    out["version"] = held[-1]["version_token"]
    with tempfile.TemporaryDirectory() as tmp:  # rows of the raw file by an independent CSV read
        path = Path(tmp) / "raw.csv"
        path.write_bytes(body)
        del body
        con = duckdb.connect()
        out["raw_csv_rows"] = one(con, f"select count(*) from read_csv('{path}', all_varchar = true, header = true)")
        con.close()
    return out, arrow


def bronze_profile(con) -> dict:
    v = "bronze"
    return {
        "rows": one(con, f"select count(*) from {v}"),
        "indicators(T,M,S)": rows(con, f"select left(indicator, 7), count(distinct indicator), count(*) from {v} group by 1 order by 1"),
        "economies": one(con, f"select count(distinct ref_area) from {v}"),
        "years(min,max,distinct)": rows(con, f"select min(time_period), max(time_period), count(distinct time_period) from {v}"),
        "time_formats": rows(con, f"select regexp_replace(time_period, '[0-9]', '9', 'g'), count(*) from {v} group by 1"),
        "dup_keys": one(con, f"select count(*) from (select {KEY} from {v} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {v} where indicator is null or ref_area is null or sex is null or age is null "
                              f"or urbanisation is null or comp_breakdown_1 is null or comp_breakdown_2 is null "
                              f"or comp_breakdown_3 is null or unit_measure is null or time_period is null"),
        "null_or_empty_values": one(con, f"select count(*) from {v} where obs_value is null or trim(obs_value) = ''"),
        "non_numeric_values(value,rows)": rows(con, f"select left(obs_value, 40), count(*) from {v} "
                                                    f"where not regexp_full_match(coalesce(obs_value, ''), {NUMBER}) group by all order by 2 desc limit 15"),
        "obs_status(code,label,rows)": rows(con, f"select obs_status, obs_status_label, count(*) from {v} group by all order by 3 desc"),
        "obs_conf": rows(con, f"select obs_conf, count(*) from {v} group by 1"),
        "unit_mult": rows(con, f"select unit_mult, count(*) from {v} group by 1"),
        "freq": rows(con, f"select freq, count(*) from {v} group by 1"),
        "action": rows(con, f"select action, count(*) from {v} group by 1"),
        "zeros(unit,rows)": rows(con, f"select unit_measure, count(*) from {v} where try_cast(obs_value as double) = 0 group by 1 order by 2 desc"),
        "negatives(unit,rows)": rows(con, f"select unit_measure, count(*) from {v} where try_cast(obs_value as double) < 0 group by 1"),
        "units(unit,indicators,rows,min,max)": rows(con, f"select unit_measure, count(distinct indicator), count(*), min(try_cast(obs_value as double)), "
                                                         f"max(try_cast(obs_value as double)) from {v} group by 1 order by 3 desc"),
        "indicators_with_two_units": one(con, f"select count(*) from (select indicator from {v} group by 1 having count(distinct unit_measure) > 1)"),
        "codes": rows(con, "select 'sex', string_agg(distinct sex, ' ') from bronze union all select 'age', string_agg(distinct age, ' ') from bronze "
                           "union all select 'urb', string_agg(distinct urbanisation, ' ') from bronze union all select 'cb1', cast(count(distinct comp_breakdown_1) as varchar) from bronze "
                           "union all select 'cb2', string_agg(distinct comp_breakdown_2, ' ') from bronze union all select 'cb3', string_agg(distinct comp_breakdown_3, ' ') from bronze"),
        "T_all_firm_totals": one(con, f"select count(*) from {v} where indicator like 'WB_ES_T_%' and comp_breakdown_1 = '_T' "
                                      f"and comp_breakdown_2 = '_T' and comp_breakdown_3 = '_T'"),
        # do the manufacturing-only indicators (WB_ES_M_x, sector 'all manufacturing') repeat WB_ES_T_x by major sector 1?
        "M_all_vs_T_major1(pairs,equal_values)": rows(con, f"""
            select count(*), count(*) filter (where abs(cast(m.obs_value as double) - cast(t.obs_value as double)) < 1e-6)
            from {v} as m join {v} as t
              on t.indicator = 'WB_ES_T_' || substr(m.indicator, 9) and t.ref_area = m.ref_area and t.time_period = m.time_period
             and t.comp_breakdown_1 = 'WB_ES_MAJOR_SECTOR_1' and t.comp_breakdown_2 = '_T' and t.comp_breakdown_3 = '_T'
            where m.indicator like 'WB_ES_M_%' and m.comp_breakdown_1 = 'WB_ES_M_SECTOR_1'"""),
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
    run(con, f"create table bronze.{TABLE} as select * from bronze_arrow")
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
        start = time.monotonic()
        code, results = dbt(work, "build", "--select", *SELECT, "--exclude", "metrics_value_range", *skipped)
        by_status: dict[str, int] = {}
        for st, _, _ in results.values():
            by_status[st] = by_status.get(st, 0) + 1
        bad = {k: v for k, v in results.items() if v[0] not in ("success", "pass")}
        runs.append({"exit": code, "minutes": round((time.monotonic() - start) / 60, 1), "nodes": len(results),
                     "tests": sum(1 for k in results if k.startswith("test.")),
                     "status": by_status, "not_passing": bad,
                     "own_tests_passing": sum(1 for k, v in results.items() if k.startswith("test.") and SOURCE in k and v[0] == "pass"),
                     "own_tests": sum(1 for k in results if k.startswith("test.") and SOURCE in k)})
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
    ob, b = rel["world_bank_es_observations"], rel["poc_build_world_bank_es"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped, "own_tests_selected": own_tests,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "rows": one(con, f"select count(*) from {ob}"),
        "indicators": one(con, f"select count(distinct indicator) from {ob}"),
        "economies": one(con, f"select count(distinct ref_area) from {ob}"),
        "years(min,max)": rows(con, f"select min(year), max(year) from {ob}"),
        "dup_grain": one(con, f"select count(*) from (select {KEY} from {ob} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {ob} where indicator is null or ref_area is null or sex is null or age is null "
                              f"or urbanisation is null or comp_breakdown_1 is null or comp_breakdown_2 is null "
                              f"or comp_breakdown_3 is null or unit_measure is null or year is null"),
        "null_raw_sha256": one(con, f"select count(*) from {ob} where raw_sha256 is null"),
        "value_null": one(con, f"select count(*) from {ob} where value is null"),
        "versions": rows(con, f"select source_version, count(*) from {ob} group by 1"),
        "zeros": one(con, f"select count(*) from {ob} where value = 0"),
        "POL(rows,years)": rows(con, f"select count(*), string_agg(distinct cast(year as varchar), ',' order by cast(year as varchar)) from {ob} where ref_area = 'POL'"),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    series = ("measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') "
              "else '' end || '|' || breakdown_json")
    g["dup_series_date"] = one(con, f"select count(*) from (select {series} as series_key, date_key from {b} group by all having count(*) > 1)")
    g["dup_series_date_without_source_code"] = one(con, f"select count(*) from (select measure_key, geography_key, breakdown_json, date_key "
                                                        f"from {b} where geography_key <> '_X' group by all having count(*) > 1)")
    g["measures"] = one(con, f"select count(distinct measure_key) from {b}")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["series"] = one(con, f"select count(distinct {series}) from {b}")
    g["dates(min,max)"] = rows(con, f"select min(date_key), max(date_key) from {b}")
    g["unmatched(code,rows)"] = rows(con, f"select geography_source_code, count(*) from {b} where geography_key = '_X' group by 1 order by 1", 40)
    g["economies_matched"] = one(con, f"select count(distinct geography_key) from {b} where geography_key <> '_X'")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["status"] = rows(con, f"select observation_status_key, count(*) from {b} group by 1")
    g["silver_T_totals_not_in_gold"] = one(con, f"select count(*) from {ob} where indicator like 'WB_ES_T_%' and comp_breakdown_1 = '_T' "
                                                f"and comp_breakdown_2 = '_T' and comp_breakdown_3 = '_T'") - g["rows"]
    g["units_in_gold(unit,measures,rows,mapped)"] = rows(con, f"select unit_code, count(distinct measure_key), count(*), unit_code in (select unit_code from {rel['unit_map']} "
                                                              f"where source_system = '{SOURCE}') from {b} group by 1 order by 1")
    g["map_rows"] = one(con, f"select count(*) from {rel['poc_map_geography']} where source_system = '{SOURCE}'")
    kpi = {}
    for k, pattern in KPIS.items():
        f = f"from {b} where measure_key like '{pattern}' and is_total and geography_key <> '_X'"
        kpi[k] = {"metrics_rows": one(con, f"select count(*) {f}"),
                  "max_rows_per_area_period": one(con, f"select coalesce(max(n), 0) from (select count(*) n {f} group by geography_key, date_key)"),
                  "min": rows(con, f"select arg_min(geography_key || ' ' || period_label || ' ' || value, value) {f}"),
                  "max": rows(con, f"select arg_max(geography_key || ' ' || period_label || ' ' || value, value) {f}"),
                  "areas": one(con, f"select count(distinct geography_key) {f}"),
                  "years": rows(con, f"select min(period_label), max(period_label) {f}")}
    g["kpi_checks"] = kpi
    spots = {
        "PL_kpis(measure,year,value)": rows(con, f"select measure_codes, period_label, value from {b} where geography_key = 'PL' "
                                                 f"and measure_key in ({', '.join(repr(p) for p in KPIS.values())}) order by 1, 2"),
        "PL_2025(measure,unit,value)": rows(con, f"select measure_codes, unit_code, value from {b} where geography_key = 'PL' and period_label = '2025' "
                                                 f"and measure_codes in ('WB_ES_T_CORR4', 'WB_ES_T_CORR11', 'WB_ES_T_OBST1', 'WB_ES_T_INFOR1', 'WB_ES_T_BREADY_FIN33') order by 1"),
        "PL_rows_by_year": rows(con, f"select period_label, count(*) from {b} where geography_key = 'PL' group by 1 order by 1"),
        "DE_CZ_latest_bribery": rows(con, f"select geography_key, period_label, value from {b} where geography_key in ('DE', 'CZ') "
                                          f"and measure_key = '{KPIS['firms_bribery_incidence']}' order by 1, 2"),
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
        landing_failed = (not str(runs[0].get("outcome", "")).startswith("landed") or runs[1].get("outcome") != "unchanged"
                          or runs[1].get("landing_files_after") != runs[0].get("landing_files_after")
                          or land["verify_exit"] or land["inspect"].get("state") != "current")
        note(f"{SOURCE} landing (twice), inspect, verify", land)
        if runs[0].get("error") or runs[0].get("exit"):
            raise RuntimeError(f"landing failed: {runs[0]}")
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        note(f"{SOURCE} bronze profile", bronze_profile(con))
        con.close()
        work = Path(tempfile.mkdtemp(prefix="wb-es-dbt-"))
        try:
            d, s, g, spots, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append("landing: first read must land, second must write nothing, inspect current, verify must pass")
        if b["loads"][1].get("appended_files") or b["rejected"] or b["rows"] != b["raw_csv_rows"]:
            failures.append("bronze: second load appended, a file was rejected, or rows differ from the raw CSV rows")
        note(f"{SOURCE} dbt build x2", d)
        note(f"{SOURCE} silver profile", s)
        note(f"{SOURCE} gold", {**g, "failures": failures})
        note(f"{SOURCE} gold spot values (Poland)", spots)
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
