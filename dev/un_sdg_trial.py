"""Dev trial of the UN Global SDG Indicators Database (register GL-018, #3316): Landing → Bronze → Silver → Gold.

Landing prefix and lake prefix zz_dev_un_sdg/, Iceberg namespace zz_dev_un_sdg; everything written is removed in
`finally`. The release's CSV ZIP is landed twice with the shared bulk_file pattern (the second run must write nothing),
Bronze loads twice (the second must append nothing). Then a real dbt build, twice, of the Silver model, the eight code
lists, poc_map_geography (with this source's registration) and poc_build_un_sdg with every dbt test attached to them
(metrics_value_range excluded: the metrics models read the shared facts; their rows, one row per source x area x period
and the value bound are checked on the build stage with the metrics filter, is_total and not _X). dbt writes only to a
DuckDB file on the runner that plays the database "r2" (target prod); the production catalog is attached read-only as
"lake" and the other parents are views onto it; the Bronze table is a copy of the scratch table. Deleted before merge
(never on the PR branch).
"""

from __future__ import annotations

import csv
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile

from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.secrets import Secret
import duckdb
import jinja2
import yaml

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.un_sdg import load as sdg_load
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_un_sdg"
SOURCE = "un_sdg"
ENTITY = "alldata_csv"
TABLE, REJECTED = sdg_load.TABLE, sdg_load.REJECTED
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("un_sdg_observations",)
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_un_sdg"]
KPIS = {"women_in_parliament": ("un_sdg:sdg_observations:PERCENT:SG_GEN_PARL", 0, 100)}
EU = ("('40','56','100','191','196','203','208','233','246','250','276','300','348','372','380','428','440','442',"
      "'470','528','616','620','642','703','705','724','752')")
NUMBER = "'-?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?'"
GRAIN = "series_code, geo_area_code, time_period, dimensions"


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
                            **{k: v for k, v in detail.items() if k not in ("sha256", "key", "object", "members")},
                            "sha256": str(detail.get("sha256", ""))[:16],
                            "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    for mode in ("inspect", "verify"):
        CAPTURED.clear()
        out[f"{mode}_exit"] = bulk_file.main(SOURCE, mode, landing_root=SCRATCH)
        out[mode] = (next((d for t, d in CAPTURED if mode in t), {}).get("files") or [{}])[0]
    return out


def raw_count(body: bytes) -> dict:
    """Records of the CSV counted by Python's csv module (footnotes may hold line breaks), independent of DuckDB."""
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        member = archive.infolist()[0]
        with archive.open(member) as handle:
            reader = csv.reader(io.TextIOWrapper(handle, encoding="utf-8", newline=""))
            header = next(reader)
            n = eu = 0
            area = header.index("GeoAreaCode")
            codes = set(EU.strip("()").replace("'", "").split(","))
            for record in reader:
                n += 1
                eu += record[area] in codes
    return {"member": member.filename, "csv_bytes": member.file_size, "columns": len(header), "records": n,
            "records_eu27": eu}


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        start = time.monotonic()
        code = sdg_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1),
                             **{k: v for t, d in CAPTURED for k, v in d.items()
                                if k in ("entities_landed", "candidates", "appended_files", "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["file_bytes"] = sum(f.file.file_size_in_bytes for f in t.scan().plan_files())
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, ENTITY, SCRATCH))
    body = bulk_bronze.read_raw(client, bucket, SOURCE, held[-1])
    out["raw"] = raw_count(body)
    out["version"] = held[-1]["version_token"]
    out["source_url"] = held[-1].get("source_url")
    return out, arrow


def bronze_profile(con) -> dict:
    v = "bronze"
    e = f"(select * from bronze where geo_area_code in {EU})"
    return {
        "rows": one(con, f"select count(*) from {v}"),
        "series": one(con, f"select count(distinct series_code) from {v}"),
        "areas": one(con, f"select count(distinct geo_area_code) from {v}"),
        "releases": rows(con, f"select release_name, count(*) from {v} group by 1"),
        "eu27(rows,series,observation_ids,min_year,max_year)": rows(con, f"select count(*), count(distinct series_code), count(distinct observation_id), "
                                                                        f"min(time_period), max(time_period) from {e}"),
        "eu27_null_keys": one(con, f"select count(*) from {e} where series_code is null or geo_area_code is null or time_period is null "
                                   f"or observation_id is null or dimensions is null"),
        "eu27_dup_grain_rows_by_observation(groups,max_rows)": rows(con, f"select count(*), max(n) from (select count(*) n from {e} group by {GRAIN} having n > 1)"),
        "eu27_dup_grain_after_observation_id": one(con, f"select count(*) from (select {GRAIN} from (select distinct observation_id, {GRAIN} from {e}) "
                                                        f"group by all having count(*) > 1)"),
        "eu27_value_markers(value,rows)": rows(con, f"select value, count(*) from {e} where not regexp_full_match(value, {NUMBER}) group by 1 order by 2 desc limit 12"),
        "eu27_zeros": one(con, f"select count(*) from {e} where try_cast(value as double) = 0"),
        "eu27_negatives": one(con, f"select count(*) from {e} where try_cast(value as double) < 0"),
        "eu27_nature(code,rows)": rows(con, f"select nature, count(*) from {e} group by 1 order by 2 desc"),
        "eu27_status(code,rows)": rows(con, f"select observation_status, count(*) from {e} group by 1 order by 2 desc"),
        "eu27_status_x_value(code,kind,rows)": rows(con, f"select observation_status, case when value = 'NaN' then 'NaN' when regexp_full_match(value, {NUMBER}) "
                                                         f"then 'number' else 'text' end, count(*) from {e} where observation_status in ('M','O','Q','L','F') "
                                                         f"or nature = 'N' group by all order by all"),
        "eu27_units": one(con, f"select count(distinct units) from {e}"),
        "eu27_dimension_names": rows(con, f"select count(distinct k) from (select unnest(json_keys(dimensions)) k from {e})"),
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
        code, results = dbt(work, "build", "--select", *SELECT, "--exclude", "metrics_value_range", *skipped)
        by_status: dict[str, int] = {}
        for st, _, _ in results.values():
            by_status[st] = by_status.get(st, 0) + 1
        bad = {k: v for k, v in results.items() if v[0] not in ("success", "pass")}
        runs.append({"exit": code, "nodes": len(results), "tests": sum(1 for k in results if k.startswith("test.")),
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
    ob, b = rel["un_sdg_observations"], rel["poc_build_un_sdg"]
    dbt_summary = {"runs": runs, "tests_skipped_parent_missing": skipped, "own_tests_selected": own_tests,
                   "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()},
                   "views_onto_production": len([v for v in views if not v.startswith("missing:")]),
                   "parents_missing_in_production": [v for v in views if v.startswith("missing:")]}
    s = {
        "rows": one(con, f"select count(*) from {ob}"),
        "series": one(con, f"select count(distinct series_code) from {ob}"),
        "areas": one(con, f"select count(distinct geo_area_code) from {ob}"),
        "years(min,max)": rows(con, f"select min(time_period), max(time_period) from {ob}"),
        "dup_grain": one(con, f"select count(*) from (select {GRAIN} from {ob} group by all having count(*) > 1)"),
        "null_keys": one(con, f"select count(*) from {ob} where series_code is null or geo_area_code is null or time_period is null "
                              f"or dimensions is null or observation_id is null"),
        "listed_rows(n,rows)": rows(con, f"select listed_rows, count(*) from {ob} group by 1 order by 1"),
        "value_kind(kind,rows)": rows(con, f"select value_kind, count(*) from {ob} group by 1 order by 1"),
        "below(value,rows)": rows(con, f"select value_text, count(*) from {ob} where value_kind = 'below' group by 1 order by 2 desc"),
        "zeros": one(con, f"select count(*) from {ob} where value = 0"),
        "multi_year_periods(rows,series)": rows(con, f"select count(*), count(distinct series_code) from {ob} "
                                                     f"where time_detail is not null and time_detail <> cast(time_period as varchar)"),
        "units(code,series,rows)": rows(con, f"select unit_code, count(distinct series_code), count(*) from {ob} group by 1 order by 3 desc", 70),
        "series_per_area(min,max)": rows(con, f"select min(n), max(n) from (select count(distinct series_code) n from {ob} where value is not null group by geo_area_code)"),
        "PL(rows,series_with_value)": rows(con, f"select count(*), count(distinct series_code) filter (where value is not null) from {ob} where geo_area_code = '616'"),
    }
    excl = rows(con, f"""
        select case
            when value_kind = 'no_value' then 'NaN (no value)'
            when value_kind = 'below' then 'censored (<x)'
            when not isfinite(value) then 'not finite'
            when coalesce(observation_status, '') in ('M', 'O', 'Q', 'L', 'F') then 'missing-value or forecast status'
            when nature = 'N' then 'nature N'
            when time_detail is not null and time_detail <> cast(time_period as varchar) then 'multi-year period'
            when time_period > release_year then 'after the release year'
            else 'loaded' end as reason, count(*), count(distinct series_code)
        from {ob} group by 1 order by 2 desc""")
    g: dict = {"rows": one(con, f"select count(*) from {b}"), "silver_rows_by_reason(reason,rows,series)": excl}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    series = "measure_key || '|' || geography_key || '|' || breakdown_json"
    g["dup_series_date"] = one(con, f"select count(*) from (select {series} as series_key, date_key from {b} group by all having count(*) > 1)")
    g["measures"] = one(con, f"select count(distinct measure_key) from {b}")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["series"] = one(con, f"select count(distinct {series}) from {b}")
    g["series_is_total_mixed"] = one(con, f"select count(*) from (select {series} from {b} group by 1 having count(distinct is_total) > 1)")
    g["total_rows_per_measure_area_date>1"] = one(con, f"select count(*) from (select measure_key, geography_key, date_key from {b} "
                                                      f"where is_total group by all having count(*) > 1)")
    g["dates(min,max)"] = rows(con, f"select min(date_key), max(date_key) from {b}")
    g["unmatched(code,rows)"] = rows(con, f"select geography_source_code, count(*) from {b} where geography_key = '_X' group by 1 order by 1", 40)
    g["areas_matched"] = one(con, f"select count(distinct geography_key) from {b} where geography_key <> '_X'")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["status(key,rows)"] = rows(con, f"select observation_status_key, count(*) from {b} group by 1 order by 2 desc")
    g["is_total(rows)"] = rows(con, f"select is_total, count(*) from {b} group by 1 order by 1")
    g["sex_code(rows)"] = rows(con, f"select sex_code, count(*) from {b} group by 1 order by 1")
    g["units_in_gold(mapped,units,rows)"] = rows(con, f"select unit_code in (select unit_code from {rel['unit_map']} where source_system = '{SOURCE}'), "
                                                      f"count(distinct unit_code), count(*) from {b} group by 1")
    g["map_rows"] = one(con, f"select count(*) from {rel['poc_map_geography']} where source_system = '{SOURCE}'")
    kpi = {}
    for k, (pattern, lo, hi) in KPIS.items():
        f = f"from {b} where measure_key like '{pattern}' and is_total and geography_key <> '_X'"
        kpi[k] = {"metrics_rows": one(con, f"select count(*) {f}"),
                  "max_rows_per_area_period": one(con, f"select coalesce(max(n), 0) from (select count(*) n {f} group by geography_key, date_key)"),
                  "outside_bound": one(con, f"select count(*) {f} and (value < {lo} or value > {hi})"),
                  "min": rows(con, f"select arg_min(geography_key || ' ' || period_label || ' ' || value, value) {f}"),
                  "max": rows(con, f"select arg_max(geography_key || ' ' || period_label || ' ' || value, value) {f}"),
                  "areas": one(con, f"select count(distinct geography_key) {f}"),
                  "years": rows(con, f"select min(period_label), max(period_label) {f}")}
    g["kpi_checks"] = kpi
    pl = f"from {b} where geography_key = 'PL'"
    spots = {
        "PL(rows,measures,first,last)": rows(con, f"select count(*), count(distinct measure_key), min(period_label), max(period_label) {pl}"),
        "SG_GEN_PARL(year,value,status)": rows(con, f"select period_label, value, observation_status_key {pl} and measure_codes = 'SG_GEN_PARL' order by 1 desc limit 4"),
        "SH_DYN_MORT_BOTHSEX(year,value)": rows(con, f"select period_label, value {pl} and measure_codes = 'SH_DYN_MORT' and sex_code = 'T' order by 1 desc limit 3"),
        "SI_POV_DAY1(year,value)": rows(con, f"select period_label, value {pl} and measure_codes = 'SI_POV_DAY1' order by 1 desc limit 3"),
        "EN_ATM_CO2(unit,year,value)": rows(con, f"select unit_code, period_label, value {pl} and measure_codes = 'EN_ATM_CO2' and is_total order by 2 desc limit 2"),
        "EG_ELC_ACCS(year,value)": rows(con, f"select period_label, value {pl} and measure_codes = 'EG_ELC_ACCS' and is_total order by 1 desc limit 2"),
        "SL_TLF_UEM_total(year,breakdown,value)": rows(con, f"select period_label, breakdown_json, value {pl} and measure_codes = 'SL_TLF_UEM' and is_total order by 1 desc limit 3"),
    }
    con.close()
    if g["dup_series_date"] or g["null_values"] or not g["rows"] or g["series_is_total_mixed"]:
        failures.append("gold checks")
    if g["measures_with_two_frequencies"]:
        failures.append("a measure with two frequencies")
    if any(v["max_rows_per_area_period"] > 1 or v["outside_bound"] for v in kpi.values()):
        failures.append("kpi: more than one row per area x period, or a value outside the bound")
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
        con = duckdb.connect()
        con.register("bronze", arrow)
        b["profile"] = bronze_profile(con)
        con.close()
        note(f"{SOURCE} bronze (twice) and profile", b)
        work = Path(tempfile.mkdtemp(prefix="un-sdg-dbt-"))
        try:
            d, s, g, spots, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if landing_failed:
            failures.append("landing: first read must land, second must write nothing, inspect current, verify must pass")
        if b["loads"][1].get("appended_files") or b["rejected"] or b["rows"] != b["raw"]["records"]:
            failures.append("bronze: second load appended, a file was rejected, or rows differ from the CSV records")
        note(f"{SOURCE} dbt build x2", d)
        note(f"{SOURCE} silver profile", s)
        note(f"{SOURCE} gold", {**g, "failures": failures})
        note(f"{SOURCE} gold spot values (Poland) and licence gate", {**spots, "licence": licence()})
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
