"""Dev trial of gis_sanitary (PLS-018, #442) on the branch's final code: Landing (inspect, extract twice, verify,
page check) into zz_dev_gis/01_landing, Bronze (load twice) into namespace zz_dev_gis, a profile of the Bronze
cells, then a real dbt build (twice) of the Silver model, the code lists, poc_map_geography (with this source's
registration) and poc_build_gis_sanitary with every dbt test attached to them (the singular test
gis_sanitary_food_samples_complete included). dbt writes
only to a DuckDB file on the runner that plays the database "r2" (target prod); the production catalog is attached
read-only as "lake" and the other parents are views onto it; the Bronze tables are copies of the scratch tables.
Summaries are annotations at warning level. Everything written to R2 is removed at the end (finally).
Deleted before merge."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import traceback

from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.secrets import Secret
import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gis_sanitary import load as bronze
from ingestion.to_landing import gis_sanitary as landing
from ingestion.to_landing.patterns import bulk_file

SOURCE = "gis_sanitary"
ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_gis"
LAND = f"{SCRATCH}/01_landing"
TABLES = [f"{SOURCE}_cells", f"{SOURCE}_rejected_files"]
SILVER = [f"{SOURCE}_food_samples"]
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", f"poc_build_{SOURCE}"]
SKIP: list[str] = []  # tests of other sources whose own tables are not in production yet
NUMBER = r"^[0-9][0-9   ]*$|^[0-9]{1,3},[0-9]{2}%$|^100%$"


def note(title: str, data, level: str = "warning") -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"[{title}] {text}", flush=True)
    annotate(level, f"{SOURCE} trial: {title}", data if len(text) < 3800 else {"truncated": text[:3700]})


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


def setup_dbt(work: Path, arrow_tables: dict) -> tuple[dict, list[str]]:
    """Profile with the local file as r2 and the catalog as lake; Bronze copies and views for the other parents."""
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
    tests = [k for k, n in nodes.items() if n["resource_type"] == "test" and selected & set(n["depends_on"]["nodes"])]
    parents = {p for k in [*selected, *tests] for p in nodes[k]["depends_on"]["nodes"]} - selected
    for schema in ("bronze", "silver", "gold"):
        con.execute(f"create schema if not exists {schema}")
    for table, arrow in arrow_tables.items():
        con.register(f"{table}_arrow", arrow)
        con.execute(f"create table bronze.{table} as select * from {table}_arrow")
    views = []
    for p in sorted(parents):
        if p.startswith(f"source.zohelo.{SOURCE}."):
            continue
        n = nodes.get(p) or sources[p]
        schema, name = n["schema"], n.get("alias") or n.get("identifier") or n["name"]
        con.execute(f"create schema if not exists {schema}")
        try:
            con.execute(f'create view {schema}."{name}" as select * from lake.{schema}."{name}"')
            views.append(f"{schema}.{name}")
        except duckdb.Error:  # a parent not built in production yet (another source's new table)
            views.append(f"missing in production: {schema}.{name}")
            SKIP.extend(nodes[t]["name"] for t in tests if p in nodes[t]["depends_on"]["nodes"])
    con.close()
    aliases = {m: (nodes[f"model.zohelo.{m}"]["schema"], nodes[f"model.zohelo.{m}"]["alias"]) for m in SELECT}
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"]


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    bucket = req("STORAGE_LANDING_BUCKET_NAME")
    work = Path(tempfile.mkdtemp(prefix="gis-dbt-"))
    con = duckdb.connect()
    one = lambda c, q: c.sql(q).fetchall()[0][0]  # noqa: E731
    failures: list[str] = []
    try:
        # Landing: inspect, extract twice (HEAD first, 5 s between files), verify, page check
        status = {"inspect": bulk_file.main(SOURCE, "inspect", landing_root=LAND)}
        per_pass = []
        for attempt in (1, 2):
            specs = landing.changed_specs(LAND)  # landing.extract = these specs into the bulk_file load
            todo = [s["entity"] for s in specs]
            status[f"extract_{attempt}"] = bulk_file.main(SOURCE, "load", LAND, specs=specs, pause_seconds=landing.PAUSE_SECONDS)
            receipts = []
            for spec in bulk_file.declaration(SOURCE):
                receipts += bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, spec["entity"], LAND))
            per_pass.append({"to_download": len(todo), "receipts": len(receipts),
                             "bytes": sum(int(r.get("size_bytes") or 0) for r in receipts)})
        status["verify"] = bulk_file.main(SOURCE, "verify", landing_root=LAND)
        unknown = landing.check_page()
        entities = len({r["entity"] for r in receipts})
        landing_summary = {"exit": status, "per_extract": per_pass, "entities": entities,
                           "second_extract_new_copies": per_pass[1]["receipts"] - per_pass[0]["receipts"],
                           "page_attachments_unknown": unknown,
                           "versions": sorted((r["entity"], r["version_token"], r["size_bytes"]) for r in receipts)}
        if (any(status.values()) or per_pass[1]["receipts"] != per_pass[0]["receipts"] or per_pass[1]["to_download"]
                or entities != 14 or unknown):
            failures.append(f"landing {landing_summary}")

        # Bronze: load twice
        counts = []
        for attempt in (1, 2):
            started = time.monotonic()
            rc = bronze.load(10, landing_root=LAND, namespace=SCRATCH)
            t = cat.load_table((SCRATCH, TABLES[0]))
            counts.append({"exit": rc, "files": len(bf.loaded_hashes(t)), "rows": t.scan().to_arrow().num_rows,
                           "snapshots": len(t.snapshots()), "seconds": round(time.monotonic() - started)})
        try:
            rejected_arrow = cat.load_table((SCRATCH, TABLES[1])).scan().to_arrow()
        except Exception:  # noqa: BLE001
            rejected_arrow = None
        rejected = rejected_arrow.to_pylist() if rejected_arrow is not None else []
        arrow_tables = {TABLES[0]: cat.load_table((SCRATCH, TABLES[0])).scan().to_arrow()}
        if rejected_arrow is not None:
            arrow_tables[TABLES[1]] = rejected_arrow
        if any(c["exit"] for c in counts) or [c["rows"] for c in counts] != [counts[0]["rows"]] * 2 or counts[0]["snapshots"] != counts[1]["snapshots"] or rejected:
            failures.append(f"bronze {counts} rejected={len(rejected)}")
        con.register("cells_arrow", arrow_tables[TABLES[0]])
        con.sql("create table cells as select * from cells_arrow")
        profile = {
            "rows": one(con, "select count(*) from cells"),
            "editions": one(con, "select count(distinct entity) from cells"),
            "per_edition (entity, page, rows, first..last year, 3rd/4th group)": con.sql("""select entity, min(page_number), count(*),
                min(row_label) || '..' || max(row_label), max(column_code) filter (where column_number = 6) || '/' || max(column_code) filter (where column_number = 8)
                from cells group by 1 order by 1""").fetchall(),
            "distinct_keys_entity_year_column": one(con, "select count(*) from (select distinct entity, row_label, column_code from cells)"),
            "dup_keys": one(con, "select count(*) from (select 1 from cells group by entity, row_label, column_code having count(*) > 1)"),
            "null_or_empty_values": one(con, "select count(*) from cells where value_text is null or trim(value_text) = ''"),
            "null_keys": one(con, "select count(*) from cells where entity is null or row_label is null or column_code is null or raw_sha256 is null"),
            "non_numeric_values (distinct, top 10)": con.sql(f"select value_text, count(*) from cells where not regexp_matches(value_text, '{NUMBER}') group by 1 order by 2 desc limit 10").fetchall(),
            "suppression_markers ('-', 'x', '.', ':', 'nb', empty)": one(con, "select count(*) from cells where trim(value_text) in ('-', 'x', '.', ':', 'nb', '')"),
            "printed_zeros": one(con, "select count(*) from cells where value_text in ('0', '0,0', '0,00')"),
            "years": con.sql("select min(row_label), max(row_label), count(distinct row_label) from cells").fetchall()[0],
            "column_codes": con.sql("select column_code, count(*) from cells group by 1 order by 1").fetchall(),
        }
        if profile["dup_keys"] or profile["null_or_empty_values"] or profile["null_keys"] or profile["non_numeric_values (distinct, top 10)"]:
            failures.append("bronze profile")
        note("landing and bronze", {"landing": landing_summary, "bronze_after_load_1_2": counts,
                                    "rejected": [(r.get("entity"), (r.get("error") or r.get("reason") or "")[:300]) for r in rejected],
                                    "profile": profile})

        # dbt: build twice on the local r2 file, every attached test
        aliases, views = setup_dbt(work, arrow_tables)
        runs, fingerprints = [], []
        for attempt in (1, 2):
            code, results = dbt(work, "build", "--select", *SELECT, *(["--exclude", *SKIP] if SKIP else []))
            by_status: dict[str, int] = {}
            for st, _, _ in results.values():
                by_status[st] = by_status.get(st, 0) + 1
            bad = {k: v for k, v in results.items() if v[0] not in ("success", "pass")}
            runs.append({"exit": code, "nodes": len(results), "tests": sum(1 for k in results if k.startswith("test.")),
                         "status": by_status, "not_passing": bad,
                         "source_tests": sorted(k.split(".")[2][:60] for k in results if k.startswith("test.") and SOURCE in k)})
            if code or bad:
                failures.append(f"dbt build {attempt}: exit {code}, {bad}")
            local = duckdb.connect(str(work / "r2.duckdb"), read_only=True)
            fingerprints.append({m: local.sql(f'select count(*), sum(hash(t)) from {s}."{a}" as t').fetchall()[0]
                                 for m, (s, a) in aliases.items() if m in (*SILVER, f"poc_build_{SOURCE}")})
            local.close()
        if fingerprints[0] != fingerprints[1]:
            failures.append("second dbt build changed a table")
        local = duckdb.connect(str(work / "r2.duckdb"), read_only=True)
        rel = {m: f'{s}."{a}"' for m, (s, a) in aliases.items()}
        s = rel[SILVER[0]]
        silver = {
            "rows": one(local, f"select count(*) from {s}"),
            "editions": local.sql(f"select count(distinct entity), min(edition_year), max(edition_year) from {s}").fetchall()[0],
            "reference_years": local.sql(f"select count(distinct reference_year), min(reference_year), max(reference_year) from {s}").fetchall()[0],
            "report_year_rows": one(local, f"select count(*) from {s} where is_report_year"),
            "dup_grain": one(local, f"select count(*) from (select 1 from {s} group by entity, reference_year, origin_code having count(*) > 1)"),
            "null_keys": one(local, f"select count(*) from {s} where entity is null or reference_year is null or origin_code is null or raw_sha256 is null"),
            "components_minus_total_nonzero": local.sql(f"select entity, reference_year, components_minus_total from {s} where components_minus_total <> 0 order by 1, 2").fetchall(),
            "reprints_differing_from_own_report (year, origin, own, reprint, editions)": local.sql(f"""select r.reference_year, r.origin_code,
                o.samples_examined || ' / ' || o.disqualified_pct, r.samples_examined || ' / ' || r.disqualified_pct, string_agg(r.entity, ',' order by r.entity)
                from {s} as r inner join {s} as o on o.is_report_year and o.reference_year = r.reference_year and o.origin_code = r.origin_code
                where not r.is_report_year and (r.samples_examined <> o.samples_examined or r.disqualified_pct <> o.disqualified_pct)
                group by all order by 1, 2""").fetchall(),
        }
        b = rel[f"poc_build_{SOURCE}"]
        series = "measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json"
        gold = {
            "build_rows": one(local, f"select count(*) from {b}"),
            "dup_series_key_x_date_key": one(local, f"select count(*) from (select 1 from {b} group by {series}, date_key having count(*) > 1)"),
            "measures_with_two_frequencies": one(local, f"select count(*) from (select measure_key from {b} group by 1 having count(distinct frequency) > 1)"),
            "unmatched_X": one(local, f"select count(*) from {b} where geography_key = '_X'"),
            "null_values": one(local, f"select count(*) from {b} where value is null"),
            "frequencies": local.sql(f"select distinct frequency from {b}").fetchall(),
            "dates": local.sql(f"select min(date_key), max(date_key), count(distinct date_key) from {b}").fetchall()[0],
            "rows_per_measure": local.sql(f"select measure_key, count(*), min(value), max(value) from {b} group by 1 order by 1").fetchall(),
            "map_rows": local.sql(f"select source_code, geography_key from {rel['poc_map_geography']} where source_system = '{SOURCE}'").fetchall(),
            "catalogue_licence": local.sql(f"""select s.source_key, s.licence_key, l.licence_key is not null as in_licence, l.commercial_ok
                from {rel['source']} as s left join {rel['licence']} as l on s.licence_key = l.licence_key where s.source_key = '{SOURCE}'""").fetchall(),
            "kpi_rows": local.sql(f"select kpi_key, status, unit_key from {rel['kpi']} where kpi_key like 'food_samples%'").fetchall(),
            "unit_map": local.sql(f"select unit_code, unit_key from {rel['unit_map']} where source_system = '{SOURCE}' order by 1").fetchall(),
        }
        if (gold["dup_series_key_x_date_key"] or gold["unmatched_X"] or gold["null_values"] or gold["measures_with_two_frequencies"]
                or silver["dup_grain"] or silver["null_keys"]):
            failures.append("gold or silver checks")
        spot = local.sql(f"""select period_label, measure_codes, value from {b} where geography_key = 'PL'
            and period_label in ('2012', '2019', '2020', '2023', '2024', '2025') order by 1, 2""").fetchall()
        local.close()
        note("dbt build x2 (Silver, code lists, poc_map_geography, build stage)", {
            "runs": runs, "second_build_same_tables": fingerprints[0] == fingerprints[1], "fingerprints": fingerprints,
            "views_onto_production": views, "excluded_tests_of_missing_parents": SKIP})
        note("silver", silver)
        note("gold", gold)
        note("spot values PL", [list(r) for r in spot])
        note("result", {"failures": failures})
    except Exception as exc:  # noqa: BLE001
        failures.append(f"{type(exc).__name__}: {exc}")
        traceback.print_exc()
        annotate("error", f"{SOURCE} trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:2000]}"})
    finally:
        removed = 0
        for bkt in (bucket, req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bkt, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bkt, Key=obj["Key"])
                    removed += 1
        dropped = []
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
            cat.drop_namespace(SCRATCH)
        except Exception as exc:  # noqa: BLE001
            print("drop failed", exc)
        shutil.rmtree(work, ignore_errors=True)
        annotate("warning", f"{SOURCE} trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "failures": len(failures)})
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
