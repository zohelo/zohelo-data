"""Dev trial of NFZ waiting times (register PLS-015, #2956): Landing → Bronze → Silver → Gold into scratch.

Landing prefix and lake prefix zz_dev_nfz_waiting_times/, Iceberg namespace zz_dev_nfz_waiting_times; everything
written is removed in `finally`. A full snapshot (every case and voivodeship) is read once, timed; Landing and
Bronze each run twice (the second run must write nothing). Then a real dbt build, twice, of the three Silver models,
the eight code lists, poc_map_geography (with this source's registration) and poc_build_nfz_waiting_times with every
dbt test attached to them (metrics_value_range excluded: its other metrics parents are not built here; the KPI bounds
are checked on the build rows). dbt writes only to a DuckDB file on the runner that plays the database "r2" (target
prod); the production catalog is attached read-only as "lake" and the other parents are views onto it; the Bronze
table is a copy of the scratch table. Also reads the v1.4 site's terms page (swagger-ui/statute.html).
Deleted before merge.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time

from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.secrets import Secret
import duckdb
import jinja2
import requests
import yaml

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.nfz_waiting_times import load as nfz_load
from ingestion.to_landing.nfz_waiting_times import extract

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_nfz_waiting_times"
SOURCE = "nfz_waiting_times"
CAPTURED: list = []
CONTRACT = ["source_system", "block_id", "block_name", "measure_key", "measure_name", "measure_codes", "unit_code",
            "unit_name", "geography_source_code", "geography_key", "frequency", "date_key", "period_label",
            "breakdown_json", "breakdown_label_json", "sex_code", "is_total", "value", "observation_status_key",
            "source_version", "raw_sha256"]
SILVER = ("nfz_waiting_times_queues", "nfz_waiting_times_providers", "nfz_waiting_times_monthly")
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_nfz_waiting_times"]
FORBIDDEN = {"provider_name", "place_name", "registry_number", "teryt_provider", "regon", "nip"}


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str, ensure_ascii=False)[:3000]}", flush=True)


extract.annotate = _capture
bulk_bronze.annotate = _capture


def note(title: str, data) -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, json.loads(text) if len(text) <= 3900 else {"truncated": text[:3800]})


def one(con, q):
    return con.execute(q).fetchone()[0]


def rows(con, q, limit=60):
    return [[str(v) for v in r] for r in con.execute(q).fetchall()][:limit]


def terms() -> dict:
    out = {}
    for path in ("swagger-ui/statute.html", "swagger-ui/changelog.html"):
        url = f"{extract.API}/{path}"
        time.sleep(1)
        r = requests.get(url, headers={"User-Agent": extract.USER_AGENT}, timeout=60)
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"<(script|style).*?</\1>", " ", r.text, flags=re.S)))
        print(f"=== {url} HTTP {r.status_code}\n{text}", flush=True)
        out[path] = {"status": r.status_code, "chars": len(text), "head": text[:1500]}
    return out


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


def landing(filter_text: str) -> dict:
    out: dict = {"filter": filter_text or "all"}
    runs = []
    for n in (1, 2):
        CAPTURED.clear()
        start = time.monotonic()
        code = extract.main(["--mode", "load", "--filter", filter_text, "--budget-minutes", "300",
                             "--landing-root", SCRATCH])
        summary = next((d for t, d in CAPTURED if "landing load" in t), {})
        files, size, receipts = landing_objects()
        runs.append({"run": n, "exit": code, "minutes": round((time.monotonic() - start) / 60, 1), **summary,
                     "landing_files_after": files, "landing_bytes_after": size, "receipts_after": receipts})
    out["runs"] = runs
    CAPTURED.clear()
    out["inspect_exit"] = extract.main(["--mode", "inspect", "--filter", filter_text, "--landing-root", SCRATCH])
    out["inspect"] = next((d for t, d in CAPTURED if "inspect" in t), {})
    CAPTURED.clear()
    out["verify_exit"] = extract.main(["--mode", "verify", "--landing-root", SCRATCH])
    out["verify"] = next((d for t, d in CAPTURED if "verify" in t), {})
    held = extract.landed(s3(), req("STORAGE_LANDING_BUCKET_NAME"), SCRATCH)
    details = [r[-1]["detail"] | {"entity": e, "bytes": r[-1]["size_bytes"]} for e, r in sorted(held.items())]
    out["reads_max"] = max((d["reads"] for d in details), default=None)
    out["inconsistent"] = [d for d in details if not d["consistent"]]
    out["largest"] = max(details, key=lambda d: d["bytes"], default=None)
    return out


def bronze() -> tuple[dict, object]:
    out: dict = {"loads": []}
    for n in (1, 2):
        CAPTURED.clear()
        code = nfz_load.load(0, landing_root=SCRATCH, namespace=SCRATCH)
        out["loads"].append({"run": n, "exit": code, **{k: v for t, d in CAPTURED for k, v in d.items()
                                                       if k in ("entities_landed", "candidates", "appended_files",
                                                                "rows_added", "rejected")}})
    cat = catalog()
    t = cat.load_table((SCRATCH, nfz_load.TABLE))
    arrow = t.scan().to_arrow()
    out["files"] = len(list(t.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rej = cat.load_table((SCRATCH, nfz_load.REJECTED)).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rej = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:300]} for r in rej]
    return out, arrow


def bronze_profile(con) -> dict:
    ints = ("awaiting", "removed", "average_period", "type_building", "computed_average_period", "case_code")
    non_int = " union all ".join(
        f"select '{c}' as col, {c} as v from bronze where {c} is not null and try_cast({c} as integer) is null"
        for c in ints)
    nulls = ", ".join(f"count(*) filter (where {c} is null)" for c in ("awaiting", "removed", "average_period",
                                                                        "statistics_month", "pcus", "teryt_place",
                                                                        "provider_code", "benefit", "queue_id"))
    flags = ("many_places", "anesthesia", "benefits_for_children", "covid_19", "queue_in_cer", "type_building",
             "toilet", "ramp", "car_park", "elevator", "air_conditioning", "automatic_door", "wheelchairs",
             "corridors", "dates_applicable", "computed_update", "benefits_provided")
    return {
        "rows": one(con, "select count(*) from bronze"),
        "entities": one(con, "select count(distinct entity) from bronze"),
        "distinct_queue_ids": one(con, "select count(distinct queue_id) from bronze"),
        "dup_queue_ids": one(con, "select count(*) from (select queue_id from bronze group by 1 having count(*) > 1)"),
        "rows_by_case": rows(con, "select case_code, count(*) from bronze group by 1 order by 1"),
        "nulls(awaiting,removed,avg,stat_month,pcus,teryt_place,provider,benefit,id)": rows(con, f"select {nulls} from bronze"),
        "non_integer": rows(con, f"select col, v, count(*) from ({non_int}) group by all order by 3 desc limit 10"),
        "statistics_month": rows(con, "select statistics_month, count(*) from bronze group by 1 order by 2 desc limit 8"),
        "awaiting_min_max_zero": rows(con, "select min(cast(awaiting as int)), max(cast(awaiting as int)), "
                                           "count(*) filter (where awaiting = '0') from bronze"),
        "flags": {f: rows(con, f"select {f}, count(*) from bronze group by 1 order by 2 desc limit 6") for f in flags},
        "pcus_top": rows(con, "select pcus, count(*) from bronze group by 1 order by 2 desc limit 8"),
        "pcus_shapes": rows(con, "select regexp_replace(coalesce(pcus, 'null'), '[0-9]+', 'n', 'g'), count(*) "
                                 "from bronze group by 1 order by 2 desc limit 12"),
        "as_at_min_max": rows(con, "select min(date_situation_as_at), max(date_situation_as_at) from bronze"),
        "identifier_columns_in_bronze": sorted(FORBIDDEN & {r[0] for r in con.execute("describe bronze").fetchall()}),
        "same_place_benefit_case_twice": one(con, "select count(*) from (select entity, provider_code, benefit, case_code, "
                                                  "resort_part_vii, resort_part_viii, teryt_place, locality "
                                                  "from bronze group by all having count(*) > 1)"),
        "teryt_place_outside": one(con, "select count(*) from bronze where coalesce(teryt_place, '') <> '' and left(teryt_place, 2) "
                                        "<> lpad(cast(cast(right(entity, 2) as int) * 2 as varchar), 2, '0')"),
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


def setup_dbt(work: Path, arrow) -> tuple[dict, list[str]]:
    """Profile with the local file as r2 and the catalog as lake; the Bronze copy and views for the other parents."""
    profiles = yaml.safe_load((WAREHOUSE / "profiles.yml").read_text())
    prod = dict(profiles["zohelo"]["outputs"]["prod"])
    attach = dict(prod["attach"][0], alias="lake")
    prod["path"] = str(work / "r2.duckdb")
    (work / "profiles").mkdir()
    (work / "tmp").mkdir()
    con = duckdb.connect(str(work / "r2.duckdb"))
    for ext in prod.get("extensions", []):
        con.execute(f"install {ext}; load {ext}")
    for s in prod["secrets"]:
        s = {k: rendered(v) for k, v in s.items()}
        con.execute(Secret.create(secret_type=s.pop("type"), name=s.pop("name"), **s).to_sql())
    a = {k: rendered(v) for k, v in attach.items()}
    try:
        con.execute(Attachment(**{**a, "read_only": True}).to_sql())
        attach["read_only"] = True
    except duckdb.Error as exc:
        print("read-only attach refused, attaching without:", str(exc)[:300], flush=True)
        con.execute(Attachment(**a).to_sql())
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
        con.execute(f"create schema if not exists {schema}")
    con.register("queues_arrow", arrow)
    con.execute(f"create table bronze.{nfz_load.TABLE} as select * from queues_arrow")
    views = []
    for p in sorted(parents):
        if p.startswith(f"source.zohelo.{SOURCE}."):
            continue
        n = nodes.get(p) or sources[p]
        schema, name = n["schema"], n.get("alias") or n.get("identifier") or n["name"]
        con.execute(f"create schema if not exists {schema}")
        con.execute(f'create view {schema}."{name}" as select * from lake.{schema}."{name}"')
        views.append(f"{schema}.{name}")
    con.close()
    aliases = {m: (nodes[f"model.zohelo.{m}"]["schema"], nodes[f"model.zohelo.{m}"]["alias"]) for m in SELECT}
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"]


def build(arrow, work: Path) -> tuple[dict, dict, dict, list[str]]:
    """dbt build twice; returns (dbt summary, silver profile, gold checks, failures)."""
    failures: list[str] = []
    aliases, views = setup_dbt(work, arrow)
    runs, fingerprints = [], []
    for attempt in (1, 2):
        code, results = dbt(work, "build", "--select", *SELECT, "--exclude", "metrics_value_range")
        by_status: dict[str, int] = {}
        for st, _, _ in results.values():
            by_status[st] = by_status.get(st, 0) + 1
        bad = {k: v for k, v in results.items() if v[0] not in ("success", "pass")}
        runs.append({"exit": code, "nodes": len(results), "tests": sum(1 for k in results if k.startswith("test.")),
                     "status": by_status, "not_passing": bad})
        if code or bad:
            failures.append(f"dbt build {attempt}: exit {code}, {bad}")
        local = duckdb.connect(str(work / "r2.duckdb"), read_only=True)
        fingerprints.append({m: local.sql(f'select count(*), sum(hash(t)) from {s}."{a}" as t').fetchall()[0]
                             for m, (s, a) in aliases.items() if m != "poc_map_geography" and m not in SEED_ROWS})
        local.close()
    if fingerprints[0] != fingerprints[1]:
        failures.append("second dbt build changed a table")
    con = duckdb.connect(str(work / "r2.duckdb"), read_only=True)
    rel = {m: f'{s}."{a}"' for m, (s, a) in aliases.items()}
    q, mo, b = rel["nfz_waiting_times_queues"], rel["nfz_waiting_times_monthly"], rel["poc_build_nfz_waiting_times"]
    dbt_summary = {"runs": runs, "second_build_same_tables": fingerprints[0] == fingerprints[1],
                   "rows": {m: v[0] for m, v in fingerprints[0].items()}, "views_onto_production": len(views),
                   "silver_identifier_columns": {m: sorted(FORBIDDEN & {r[0] for r in con.sql(f"describe {rel[m]}").fetchall()})
                                                 for m in SILVER}}
    if any(dbt_summary["silver_identifier_columns"].values()):
        failures.append("identifier columns in Silver")
    s = {
        "snapshot_month": rows(con, f"select snapshot_month, count(*) from {q} group by 1"),
        "awaiting_by_stat_month": rows(con, f"select statistics_month, count(*), count(awaiting), sum(awaiting) from {q} group by 1 order by 1"),
        "providers": one(con, f"select count(*) from {rel['nfz_waiting_times_providers']}"),
        "places_outside_voivodeship": one(con, f"select count(*) from {q} where coalesce(teryt_place, '') <> '' and left(teryt_place, 2) <> lpad(cast(cast(province_code as integer) * 2 as varchar), 2, '0')"),
        "many_places_groups(entries_Y,groups,groups_same_count,groups_differing,entries_in_differing)": rows(
            con, f"with g as (select province_code, case_code, provider_code, benefit, count(*) n, "
                 f"count(distinct awaiting) d, count(awaiting) a, count(distinct statistics_month) m from {q} "
                 f"where many_places = 'Y' group by all) select sum(n), count(*), count(*) filter (where d = 1 and a = n and m = 1), "
                 f"count(*) filter (where not (d = 1 and a = n and m = 1)), sum(n) filter (where not (d = 1 and a = n and m = 1)) from g"),
        "naive_sum_vs_lists_PL_case1": rows(
            con, f"select (select sum(awaiting) from {q} where case_code = 1 and statistics_month = snapshot_month - interval 1 month), "
                 f"(select value from {mo} where province_code = '00' and case_code = 1 and measure_code = 'persons_awaiting')"),
        "monthly_by_measure": rows(con, f"select measure_code, case_code, period_month, count(*), sum(value) filter (where province_code = '00') "
                                        f"from {mo} group by all order by all"),
    }
    g: dict = {"rows": one(con, f"select count(*) from {b}")}
    g["columns_in_contract_order"] = [r[0] for r in con.sql(f"describe {b}").fetchall()] == CONTRACT
    g["dup_series_date"] = one(con, f"select count(*) from (select measure_key || '|' || geography_key || case when geography_key = '_X' "
                                    f"then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json as series_key, date_key "
                                    f"from {b} group by all having count(*) > 1)")
    g["measures_with_two_frequencies"] = one(con, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)")
    g["unmatched_rows"] = one(con, f"select count(*) from {b} where geography_key = '_X'")
    g["null_values"] = one(con, f"select count(*) from {b} where value is null")
    g["map_rows"] = one(con, f"select count(*) from {rel['poc_map_geography']} where source_system = '{SOURCE}'")
    g["measures"] = rows(con, f"select measure_key, count(*), min(date_key), max(date_key), min(value), max(value) from {b} group by 1 order by 1")
    g["spot_PL"] = rows(con, f"select measure_key, period_label, value from {b} where geography_key = 'PL' order by 1, 2")
    g["spot_mazowieckie_case1"] = rows(con, f"select measure_key, period_label, value from {b} where geography_key = 'TERC:14' "
                                            "and measure_key like '%case1' order by 1")
    km = rel["kpi_measure"]
    g["kpi_rows_per_area_period_max"] = one(con, f"select max(c) from (select k.kpi_key, b.geography_key, b.date_key, count(*) c "
                                                 f"from {km} k join {b} b on b.measure_key like k.measure_key_pattern "
                                                 f"where k.source_system = '{SOURCE}' group by all)")
    bounds = re.findall(r"\('(waiting_list[a-z_]*)', 'nfz_waiting_times', '%', ([0-9e.]+), ([0-9e.]+)\)",
                        (WAREHOUSE / "tests/metrics_value_range.sql").read_text())
    g["bounds"] = bounds
    g["outside_bounds"] = {
        k: one(con, f"select count(*) from {km} m join {b} b on b.measure_key like m.measure_key_pattern "
                    f"where m.kpi_key = '{k}' and m.source_system = '{SOURCE}' and (b.value < {lo} or b.value > {hi})")
        for k, lo, hi in bounds}
    con.close()
    if g["dup_series_date"] or g["unmatched_rows"] or g["null_values"] or any(g["outside_bounds"].values()) or not bounds:
        failures.append("gold checks")
    return dbt_summary, s, g, failures


def teryt_names() -> dict:
    t = catalog().load_table(("silver", "teryt_terc")).scan(selected_fields=("terc_code", "name_pl", "unit_level")).to_arrow()
    return {r["terc_code"]: r["name_pl"] for r in t.to_pylist() if r["unit_level"] == "wojewodztwo"}


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
    filter_text = argv[0] if argv else ""
    status = 0
    try:
        terms_page = terms()
        teryt = teryt_names()
        land = landing(filter_text)
        note(f"{SOURCE} landing (twice), inspect, verify", land)
        b, arrow = bronze()
        note(f"{SOURCE} bronze (twice)", b)
        con = duckdb.connect()
        con.register("bronze", arrow)
        prof = bronze_profile(con)
        note(f"{SOURCE} bronze profile", prof)
        con.close()
        work = Path(tempfile.mkdtemp(prefix="nfz-itl-dbt-"))
        try:
            d, s, g, failures = build(arrow, work)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if prof["identifier_columns_in_bronze"]:
            failures.append("identifier columns in Bronze")
        note(f"{SOURCE} dbt build x2", d)
        note(f"{SOURCE} silver profile", s)
        note(f"{SOURCE} gold", {**g, "teryt_names": {k: teryt.get(k) for k in ("14", "24")}, "failures": failures})
        if failures:
            status = 1
        note(f"{SOURCE} licence gate and v1.4 terms page", {**licence(), "terms_pages": {
            k: {"status": v["status"], "head": v["head"][:600]} for k, v in terms_page.items()}})
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
