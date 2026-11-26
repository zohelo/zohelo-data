"""Dev trial of rpwdl year-end snapshots (#3139, PLS-020) on the branch's final code, in scratch zz_dev_rpwdl only.

1. Landing for the real day (2026): inspect, extract twice (the second writes nothing), verify.
2. Bronze load twice into namespace zz_dev_rpwdl (the second adds nothing).
3. A simulated year boundary: the same code with today = 5 Jan 2027 (Europe/Warsaw year 2027): inspect, extract once,
   verify. The 2026 entities are closed (no request); the 2027 entities land although their bytes may equal 2026's.
4. Bronze load twice with today = 2027: the 2027 copies load per entity, even with the SHA-256 of a 2026 copy.
5. dbt build twice (target prod on a local DuckDB file playing "r2"; production catalog attached read-only as "lake"
   for silver.teryt_terc and the parents of poc_map_geography): the five Silver models, the code lists,
   poc_map_geography and poc_build_rpwdl with every attached test. Bronze copies get the 2026 rows a second time under
   the entity names without a year (the copies production holds from before #3139) to show Silver leaves them out.
Annotations: own summaries only (the modules' notices are printed, not annotated), at most 8. Everything written to
R2 is removed at the end. Counts and code lists only, no record content. Deleted before merge."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import traceback

from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.secrets import Secret
import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common import report
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze.rpwdl import load as bronze
from ingestion.to_landing import rpwdl as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
SCRATCH = "zz_dev_rpwdl"
LAND = f"{SCRATCH}/01_landing"
SILVER = ["rpwdl_providers", "rpwdl_establishments", "rpwdl_units", "rpwdl_cells", "rpwdl_practices"]
SEED_ROWS = ["source", "licence", "kpi", "kpi_measure", "kpi_v2_map", "unit", "unit_map", "unit_decimals"]
SELECT = [*SILVER, *SEED_ROWS, "poc_map_geography", "poc_build_rpwdl"]
FORBIDDEN = {"book_id", "book_number", "regon", "practice_id", "founder_name", "nip", "name"}
YEAR_2027 = datetime(2027, 1, 5, 12, 0, tzinfo=UTC)
annotate = report.annotate


BRONZE_NOTICES: list[dict] = []


def quiet(level: str, title: str, data) -> None:
    print(f"[{level}] {title}: {json.dumps(data, default=str)[:3000]}", flush=True)
    if "Bronze from Landing" in title:
        BRONZE_NOTICES.append(data)


bulk_file.annotate = quiet  # the modules' own annotations would pass the limit of 10 per step
bulk_bronze.annotate = quiet


def note(title: str, data, level: str = "warning") -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"[{title}] {text}", flush=True)
    annotate(level, f"rpwdl years: {title}", data if len(text) < 3800 else {"truncated": text[:3700]})


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
    tail = proc.stdout[-6000:]
    print(tail, proc.stderr[-3000:], flush=True)
    results = {}
    path = work / "target" / "run_results.json"
    if args[0] != "parse" and path.exists():
        for r in json.loads(path.read_text())["results"]:
            results[r["unique_id"]] = (r["status"], r.get("failures"), (r.get("message") or "")[:200])
        path.unlink()
    return proc.returncode, results


def setup_dbt(work: Path, arrow_tables: dict) -> tuple[dict, list[str], dict]:
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
    try:  # read-only when the iceberg extension accepts it
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
    legacy = {}
    for table, arrow in arrow_tables.items():
        con.register(f"{table}_arrow", arrow)
        con.execute(f"create table bronze.{table} as select * from {table}_arrow")
        if table != bronze.REJECTED:  # the copies landed before #3139: same rows, entity without a year
            con.execute(
                f"insert into bronze.{table} select * replace (regexp_replace(entity, '_2026$', '') as entity) "
                f"from {table}_arrow where regexp_matches(entity, '_2026$')"
            )
            legacy[table] = con.sql(
                f"select count(*) from bronze.{table} where not regexp_matches(entity, '_[0-9]{{4}}$')"
            ).fetchall()[0][0]
    views = []
    for p in sorted(parents):
        if p.startswith("source.zohelo.rpwdl."):
            continue
        n = nodes.get(p) or sources[p]
        schema, name = n["schema"], n.get("alias") or n.get("identifier") or n["name"]
        con.execute(f"create schema if not exists {schema}")
        con.execute(f'create view {schema}."{name}" as select * from lake.{schema}."{name}"')
        views.append(f"{schema}.{name}")
    con.close()
    aliases = {m: (nodes[f"model.zohelo.{m}"]["schema"], nodes[f"model.zohelo.{m}"]["alias"]) for m in SELECT}
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"], legacy


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    work = Path(tempfile.mkdtemp(prefix="rpwdl-dbt-"))
    one = lambda c, q: c.sql(q).fetchall()[0][0]  # noqa: E731
    failures: list[str] = []
    try:
        bucket = req("STORAGE_LANDING_BUCKET_NAME")

        def receipts(today) -> list[dict]:
            out = []
            for spec in landing.specs(today):
                out += bulk_file.landed(client, bucket, bulk_file.prefix("rpwdl", spec["entity"], LAND))
            return out

        def landing_round(today, label: str, extracts: int) -> dict:
            status = {"inspect": landing._run("inspect", landing_root=LAND, today=today)}
            counts = []
            for attempt in range(1, extracts + 1):
                status[f"extract_{attempt}"] = landing._run("load", landing_root=LAND, today=today)
                counts.append(len(receipts(today)))
            status["verify"] = landing._run("verify", landing_root=LAND, today=today)
            held = receipts(today)
            by_year: dict[str, int] = {}
            for r in held:
                year = r["entity"].rsplit("_", 1)[1]
                by_year[year] = by_year.get(year, 0) + 1
            if any(status.values()):
                failures.append(f"landing {label} exit {status}")
            if len(counts) > 1 and counts[0] != counts[1]:
                failures.append(f"landing {label}: second extract wrote {counts[1] - counts[0]} copies")
            return {"exit": status, "receipts_after_extract_1_2": counts, "receipts_by_year": by_year,
                    "bytes": sum(r["size_bytes"] for r in held)}

        def bronze_round(today, label: str) -> list[dict]:
            rounds = []
            for attempt in (1, 2):
                BRONZE_NOTICES.clear()
                rc = bronze.load(0, landing_root=LAND, namespace=SCRATCH, today=today)
                snap = {"candidates": sum(n.get("candidates", 0) for n in BRONZE_NOTICES),
                        "rows_added": sum(n.get("rows_added", 0) for n in BRONZE_NOTICES)}
                for table in bronze.TABLES:
                    t = cat.load_table((SCRATCH, table))
                    rows = t.scan(selected_fields=("entity",)).to_arrow().column("entity").to_pylist()
                    years: dict[str, int] = {}
                    for e in rows:
                        years[e[-4:]] = years.get(e[-4:], 0) + 1
                    files = t.inspect.data_files().column("file_path").to_pylist()
                    snap[table] = {"data_files": len(files), "entity_sha_pairs": len(bf.loaded_pairs(t)),
                                   "rows_by_year": years}
                rounds.append({"exit": rc, **snap})
            first = {k: v for k, v in rounds[0].items() if k not in ("candidates", "rows_added")}
            second = {k: v for k, v in rounds[1].items() if k not in ("candidates", "rows_added")}
            if first != second or rounds[1]["candidates"] or any(r["exit"] for r in rounds):
                failures.append(f"bronze {label}: second load changed the tables or failed")
            return rounds

        # 1-2: the real year (2026)
        land_2026 = landing_round(None, "2026", 2)
        bronze_2026 = bronze_round(None, "2026")
        sha_2026 = {r["entity"][:-5]: r["sha256"] for r in receipts(None)}
        loaded_2026 = set()
        for table in bronze.TABLES:
            loaded_2026 |= bf.loaded_hashes(cat.load_table((SCRATCH, table)))
        note("2026: landing and bronze", {"landing": land_2026, "bronze_load_1_2": bronze_2026})

        # 3-4: simulated first runs of 2027 (same code, today = 5 Jan 2027)
        land_2027 = landing_round(YEAR_2027, "2027", 1)  # idempotency shown in 2026; one extract spares the server
        new_2027 = [r for r in receipts(YEAR_2027) if r["entity"].endswith("_2027")]
        same_bytes = sorted(r["entity"] for r in new_2027 if sha_2026.get(r["entity"][:-5]) == r["sha256"])
        hash_only_would_skip = sorted(r["entity"] for r in new_2027 if r["sha256"] in loaded_2026)
        closed_2026_copies = sum(1 for r in receipts(YEAR_2027) if r["entity"].endswith("_2026"))
        bronze_2027 = bronze_round(YEAR_2027, "2027")
        if len(new_2027) != 18 or closed_2026_copies != land_2026["receipts_by_year"].get("2026"):
            failures.append(f"year boundary: {len(new_2027)} copies for 2027, 2026 copies {closed_2026_copies}")
        note("2027 (simulated): landing and bronze", {
            "landing": land_2027, "copies_2027": len(new_2027), "copies_2026_unchanged": closed_2026_copies,
            "2027_bytes_equal_to_2026": len(same_bytes), "would_be_skipped_by_hash_only_bronze": len(hash_only_would_skip),
            "bronze_load_1_2": bronze_2027})

        try:
            rejected = cat.load_table((SCRATCH, bronze.REJECTED)).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001
            rejected = []
        if rejected:
            failures.append(f"rejected files {[(r.get('entity'), (r.get('error') or '')[:200]) for r in rejected]}")
        arrow_tables = {}
        for table in bronze.TABLES:
            arrow_tables[table] = cat.load_table((SCRATCH, table)).scan().to_arrow()
        bronze_rows = {t: a.num_rows for t, a in arrow_tables.items()}

        # dbt: build twice on the local r2 file, every attached test
        aliases, views, legacy = setup_dbt(work, arrow_tables)
        runs, fingerprints = [], []
        for attempt in (1, 2):
            code, results = dbt(work, "build", "--select", *SELECT)
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
        local = duckdb.connect(str(work / "r2.duckdb"), read_only=True)
        rel = {m: f'{s}."{a}"' for m, (s, a) in aliases.items()}
        silver = {m: local.sql(f"select snapshot_year, count(*) n, count(*) filter (is_active) active, count(*) filter (is_active and voivodeship_code is null) active_no_voiv, count(distinct voivodeship_code) voivs, min(snapshot_date), max(snapshot_date) from {rel[m]} group by 1 order by 1").fetchall() for m in SILVER}
        silver_vs_bronze = {m: [local.sql(f"select count(*) from {rel[m]}").fetchall()[0][0], bronze_rows[m]] for m in SILVER}
        if any(a != b for a, b in silver_vs_bronze.values()):
            failures.append(f"silver rows differ from yeared bronze rows (legacy rows not left out?) {silver_vs_bronze}")
        b = rel["poc_build_rpwdl"]
        gold = {
            "build_rows": one(local, f"select count(*) from {b}"),
            "dup_series_key_x_date_key": one(local, f"select count(*) from (select 1 from {b} group by measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json, date_key having count(*) > 1)"),
            "silver_identifier_columns": {m: sorted(FORBIDDEN & {r[0] for r in local.sql(f"select column_name from (describe {rel[m]})").fetchall()}) for m in SILVER},
            "code_list_rows_rpwdl": {m: one(local, f"select count(*) from {rel[m]} where " + {"source": "source_key", "kpi_measure": "source_system", "unit_map": "source_system"}.get(m, "1") + (" = 'rpwdl'" if m in ("source", "kpi_measure", "unit_map") else " = 1")) for m in SEED_ROWS},
            "unmatched_X": one(local, f"select count(*) from {b} where geography_key = '_X'"),
            "areas": one(local, f"select count(distinct geography_key) from {b}"),
            "rows_by_period": local.sql(f"select period_label, date_key, count(*) from {b} group by all order by all").fetchall(),
            "frequencies": local.sql(f"select distinct frequency from {b}").fetchall(),
            "null_values": one(local, f"select count(*) from {b} where value is null"),
            "units": local.sql(f"select distinct unit_code from {b} order by 1").fetchall(),
            "map_rows_rpwdl": one(local, f"select count(*) from {rel['poc_map_geography']} where source_system = 'rpwdl'"),
            "kpi_rows (metrics input: is_total, kpi measures)": local.sql(f"select measure_key, count(*), min(value), max(value) from {b} where is_total and measure_key in ('rpwdl:register:entities:providers', 'rpwdl:register:practices:practices') group by 1 order by 1").fetchall(),
        }
        gold["voiv_sum_vs_PL"] = local.sql(f"select measure_key, period_label, sum(value) filter (geography_key <> 'PL') voiv, max(value) filter (geography_key = 'PL') pl from {b} where is_total group by all order by all").fetchall()
        gold["metrics_rows_per_kpi (is_total, not _X; ceiling 52)"] = local.sql(f"select measure_key, count(*) from {b} where is_total and geography_key <> '_X' and measure_key in ('rpwdl:register:entities:providers', 'rpwdl:register:practices:practices') group by 1 order by 1").fetchall()
        kpi = gold["kpi_rows (metrics input: is_total, kpi measures)"]
        bounds = {"rpwdl:register:entities:providers": 2e5, "rpwdl:register:practices:practices": 1e6}
        out_of_bounds = [r for r in kpi if r[2] < 0 or r[3] > bounds[r[0]]]
        if [r[0] for r in gold["rows_by_period"]] != ["2026", "2027"]:
            failures.append(f"gold periods {gold['rows_by_period']}")
        if gold["dup_series_key_x_date_key"] or any(gold["silver_identifier_columns"].values()) or gold["unmatched_X"] or gold["null_values"] or out_of_bounds:
            failures.append(f"gold checks {gold} out_of_bounds={out_of_bounds}")
        spot = local.sql(f"select measure_key, period_label, geography_key, value from {b} where geography_key in ('PL', 'TERC:14') and is_total order by all").fetchall()
        local.close()
        note("dbt build x2 (Silver, poc_map_geography, poc_build_rpwdl)", {
            "runs": runs, "second_build_same_tables": fingerprints[0] == fingerprints[1], "fingerprints": fingerprints,
            "views_onto_production": views,
            "legacy rows added to the Bronze copies": legacy, "silver rows vs yeared bronze rows": silver_vs_bronze,
            "silver by year: n/active/active_no_voiv/voivodeships/snapshot min,max": silver})
        note("gold", {**gold, "kpi bounds (metrics_value_range)": bounds, "out_of_bounds": out_of_bounds,
                      "spot PL, TERC:14 mazowieckie": [list(r) for r in spot]})
        note("result", {"failures": failures})
    except Exception as exc:  # noqa: BLE001
        failures.append(f"{type(exc).__name__}: {exc}")
        traceback.print_exc()
        annotate("error", "rpwdl years: failed", {"error": f"{type(exc).__name__}: {str(exc)[:2000]}"})
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
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
        annotate("warning", "rpwdl years: cleanup", {"objects_removed": removed, "tables_dropped": dropped, "failures": len(failures)})
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
