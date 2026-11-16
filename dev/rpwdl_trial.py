"""Dev trial of rpwdl (PLS-020, #2958) on the branch's final code: Landing (inspect, extract twice, verify) into
zz_dev_rpwdl/01_landing, Bronze (load twice) into namespace zz_dev_rpwdl, then a real dbt build (twice) of the five
Silver models, poc_map_geography (with the rpwdl registration) and poc_build_rpwdl with every dbt test attached to
them. dbt writes only to a DuckDB file on the runner that plays the database "r2" (target prod, so names and
namespaces are those of production); the production catalog is attached read-only as "lake" and the models' other
parents (silver.teryt_terc, the parents of poc_map_geography) are views onto it; the Bronze tables are copies of the
scratch tables. Summaries are annotations at warning level (the Bronze module's notices fill the notice slots).
Everything written to R2 is removed at the end (finally). No record content is reported, only counts and code lists.
Deleted before merge."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import traceback
import zipfile

from dbt.adapters.duckdb.credentials import Attachment
from dbt.adapters.duckdb.secrets import Secret
import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
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
# Ids read from the Landing ZIPs in memory only, to show no provider ledger or practice entry is in both ledger
# files (the Silver grain of these two tables is the file row, so the build cannot check it).
ID_COLUMNS = {"podmioty.csv": "ID Księgi", "praktyki.csv": "Id rejestru"}


def note(title: str, data, level: str = "warning") -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"[{title}] {text}", flush=True)
    annotate(level, f"rpwdl trial: {title}", data if len(text) < 3800 else {"truncated": text[:3700]})


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
    for table, arrow in arrow_tables.items():
        con.register(f"{table}_arrow", arrow)
        con.execute(f"create table bronze.{table} as select * from {table}_arrow")
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
    return aliases, views + [f"read_only_attach={attach.get('read_only', False)}"]


def cross_file_ids(client) -> dict:
    """Ids of provider ledgers and practice entries found in both ledger files of a register (counts only)."""
    bucket = req("STORAGE_LANDING_BUCKET_NAME")
    ids: dict[str, dict[str, list]] = {}
    con = duckdb.connect()
    for spec in bulk_file.declaration("rpwdl"):
        entity = spec["entity"]
        receipts = bulk_file.landed(client, bucket, bulk_file.prefix("rpwdl", entity, LAND))
        if not receipts:
            continue
        archive = zipfile.ZipFile(io.BytesIO(bulk_bronze.read_raw(client, bucket, "rpwdl", receipts[-1])))
        member = "podmioty.csv" if entity.startswith("healthcare_provider") else "praktyki.csv"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "m.csv"
            path.write_bytes(archive.read(member))
            values = [r[0] for r in con.sql(
                f"""select "{ID_COLUMNS[member]}" from read_csv(?, header = true, all_varchar = true, delim = ';',
                    quote = '"', escape = '"')""", params=[str(path)]).fetchall()]
        register, state = entity.rsplit("_", 1)
        ids.setdefault(register, {})[state] = values
    out = {}
    for register, states in ids.items():
        active, inactive = states.get("active", []), states.get("inactive", [])
        out[register] = {"in_both": len(set(active) & set(inactive)),
                         "repeated_in_a_file": len(active) - len(set(active)) + len(inactive) - len(set(inactive)),
                         "null_ids": sum(1 for v in active + inactive if v in (None, "", "NULL"))}
    con.close()
    return out


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    work = Path(tempfile.mkdtemp(prefix="rpwdl-dbt-"))
    con = duckdb.connect()
    one = lambda c, q: c.sql(q).fetchall()[0][0]  # noqa: E731
    failures: list[str] = []
    try:
        # Landing: inspect, extract twice, verify
        status = {"inspect": bulk_file.main("rpwdl", "inspect", landing_root=LAND)}
        for attempt in (1, 2):
            status[f"extract_{attempt}"] = landing._run("load", landing_root=LAND)
        status["verify"] = bulk_file.main("rpwdl", "verify", landing_root=LAND)
        receipts = []
        for spec in bulk_file.declaration("rpwdl"):
            receipts += bulk_file.landed(client, req("STORAGE_LANDING_BUCKET_NAME"), bulk_file.prefix("rpwdl", spec["entity"], LAND))
        landing_summary = {"exit": status, "receipts": len(receipts), "entities": len({r["entity"] for r in receipts}),
                           "bytes": sum(r["size_bytes"] for r in receipts),
                           "second_extract_new_copies": len(receipts) - len({r["entity"] for r in receipts})}
        if any(status.values()):
            failures.append(f"landing exit {status}")

        # Bronze: load twice
        counts = []
        for attempt in (1, 2):
            rc = bronze.load(0, landing_root=LAND, namespace=SCRATCH)
            snap = {}
            for table in bronze.TABLES:
                t = cat.load_table((SCRATCH, table))
                snap[table] = (len(bf.loaded_hashes(t)), t.scan().to_arrow().num_rows)
            counts.append({"exit": rc, **snap})
        try:
            rejected = cat.load_table((SCRATCH, bronze.REJECTED)).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001
            rejected = []
        arrow_tables = {}
        for table in bronze.TABLES:
            arrow_tables[table] = cat.load_table((SCRATCH, table)).scan().to_arrow()
            con.register(f"{table}_arrow", arrow_tables[table])
            con.sql(f"create table {table} as select * from {table}_arrow")
        bronze_rows = {t: one(con, f"select count(*) from {t}") for t in bronze.TABLES}
        columns = {t: [c for c, _ in bronze.columns(t)] for t in bronze.TABLES}
        forbidden = {t: sorted(FORBIDDEN & set(c)) for t, c in columns.items() if FORBIDDEN & set(c)}
        long_teryt = {t: one(con, f"select count(*) from {t} where regexp_matches(voivodeship_teryt, '^[0-9]{{3,}}')")
                      for t in bronze.TABLES if "voivodeship_teryt" in columns[t]}
        if forbidden or any(long_teryt.values()) or rejected or counts[0][next(iter(bronze.TABLES))] != counts[1][next(iter(bronze.TABLES))]:
            failures.append(f"bronze forbidden={forbidden} long_teryt={long_teryt} rejected={len(rejected)}")
        note("landing and bronze", {"landing": landing_summary, "bronze_files_rows_after_load_1_2": counts,
                                    "rows": bronze_rows, "rejected": [(r.get("entity"), (r.get("error") or "")[:200]) for r in rejected],
                                    "identifier_columns_in_bronze": forbidden, "teryt_longer_than_2_in_cut_columns": long_teryt})

        # Look before modelling: markers, non-conforming strings, code lists, TERYT shapes, cross-file ids
        markers = {}
        for table, (_, _, kept) in bronze.TABLES.items():
            for col in kept.values():
                n_null_text = one(con, f"select count(*) from {table} where {col} = 'NULL'")
                n_brak = one(con, f"select count(*) from {table} where lower(trim({col})) = 'brak wpisu'")
                n_empty = one(con, f"select count(*) from {table} where {col} is null or trim({col}) = ''")
                if n_null_text or n_brak or n_empty:
                    markers[f"{table}.{col}"] = [n_null_text, n_brak, n_empty]
        odd = {}
        for table, (_, _, kept) in bronze.TABLES.items():
            for col in kept.values():
                if col.endswith(("_date", "_start", "_end")):
                    rx = r"^\d{4}-\d{2}-\d{2}$"
                elif col in ("beds_total", "beds_intensive_care", "dialysis_stations", "day_places"):
                    rx = r"^\d+$"
                else:
                    continue
                bad = con.sql(f"select {col}, count(*) from {table} where {col} <> 'NULL' and lower(trim({col})) <> 'brak wpisu' and not regexp_matches({col}, '{rx}') group by 1 order by 2 desc limit 5").fetchall()
                if bad:
                    odd[f"{table}.{col}"] = bad
        teryt_col = {t: ("voivodeship_teryt" if "voivodeship_teryt" in columns[t] else "teryt") for t in bronze.TABLES}
        shapes = con.sql(" union all ".join(
            f"select '{t}' t, case when regexp_matches({c}, '^[0-9]+$') then length({c}) || ' digits' else {c} end shape, count(*) n from {t} group by all"
            for t, c in teryt_col.items()) + " order by all").fetchall()
        ids = cross_file_ids(client)
        if any(v["in_both"] or v["repeated_in_a_file"] for v in ids.values()):
            failures.append(f"cross-file ids {ids}")
        note("bronze profile", {"markers NULL / brak wpisu / empty": markers, "non-conforming dates and counts": odd,
                                "teryt shapes": shapes,
                                "practice kinds": con.sql("select entity, practice_kind, count(*) from rpwdl_practices group by all order by all").fetchall(),
                                "provider ledger and practice ids (Landing, in memory)": ids})

        # dbt: build twice on the local r2 file, every attached test
        aliases, views = setup_dbt(work, arrow_tables)
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
        silver = {m: local.sql(f"select count(*) n, count(*) filter (is_active) active, count(*) filter (voivodeship_code is null) no_voiv, count(*) filter (is_active and voivodeship_code is null) active_no_voiv, count(distinct voivodeship_code) voivs, min(start_date), max(start_date), min(snapshot_date), max(snapshot_date) from {rel[m]}").fetchall()[0] for m in SILVER}
        b = rel["poc_build_rpwdl"]
        gold = {
            "build_rows": one(local, f"select count(*) from {b}"),
            "dup_series_key_x_date_key": one(local, f"select count(*) from (select 1 from {b} group by measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json, date_key having count(*) > 1)"),
            "silver_identifier_columns": {m: sorted(FORBIDDEN & {r[0] for r in local.sql(f"select column_name from (describe {rel[m]})").fetchall()}) for m in SILVER},
            "code_list_rows_rpwdl": {m: one(local, f"select count(*) from {rel[m]} where " + {"source": "source_key", "kpi_measure": "source_system", "unit_map": "source_system"}.get(m, "1") + (" = 'rpwdl'" if m in ("source", "kpi_measure", "unit_map") else " = 1")) for m in SEED_ROWS},
            "unmatched_X": one(local, f"select count(*) from {b} where geography_key = '_X'"),
            "areas": one(local, f"select count(distinct geography_key) from {b}"),
            "periods": local.sql(f"select distinct period_label from {b}").fetchall(),
            "frequencies": local.sql(f"select distinct frequency from {b}").fetchall(),
            "null_values": one(local, f"select count(*) from {b} where value is null"),
            "units": local.sql(f"select distinct unit_code from {b} order by 1").fetchall(),
            "map_rows_rpwdl": one(local, f"select count(*) from {rel['poc_map_geography']} where source_system = 'rpwdl'"),
            "kpi_rows (metrics input: is_total, kpi measures)": local.sql(f"select measure_key, count(*), min(value), max(value) from {b} where is_total and measure_key in ('rpwdl:register:entities:providers', 'rpwdl:register:practices:practices') group by 1 order by 1").fetchall(),
            "voiv_sum_vs_PL": local.sql(f"select measure_key, sum(value) filter (geography_key <> 'PL') voiv, max(value) filter (geography_key = 'PL') pl from {b} where is_total group by all order by all").fetchall(),
        }
        gold["metrics_rows_per_kpi (is_total, not _X; ceiling 26)"] = local.sql(f"select measure_key, count(*) from {b} where is_total and geography_key <> '_X' and measure_key in ('rpwdl:register:entities:providers', 'rpwdl:register:practices:practices') group by 1 order by 1").fetchall()
        kpi = gold["kpi_rows (metrics input: is_total, kpi measures)"]
        bounds = {"rpwdl:register:entities:providers": 2e5, "rpwdl:register:practices:practices": 1e6}
        out_of_bounds = [r for r in kpi if r[2] < 0 or r[3] > bounds[r[0]]]
        if gold["dup_series_key_x_date_key"] or any(gold["silver_identifier_columns"].values()) or gold["unmatched_X"] or gold["null_values"] or out_of_bounds:
            failures.append(f"gold checks {gold} out_of_bounds={out_of_bounds}")
        spot = local.sql(f"select measure_key, breakdown_json, geography_key, value from {b} where geography_key in ('PL', 'TERC:14', 'TERC:24') and is_total order by all").fetchall()
        local.close()
        note("dbt build x2 (Silver, poc_map_geography, poc_build_rpwdl)", {
            "runs": runs, "second_build_same_tables": fingerprints[0] == fingerprints[1], "fingerprints": fingerprints,
            "views_onto_production": views,
            "silver n/active/no_voiv/active_no_voiv/voivodeships/start min,max/snapshot min,max": silver})
        note("gold", {**gold, "kpi bounds (metrics_value_range)": bounds, "out_of_bounds": out_of_bounds,
                      "spot PL, TERC:14 mazowieckie, TERC:24 slaskie": [list(r) for r in spot]})
        note("result", {"failures": failures})
    except Exception as exc:  # noqa: BLE001
        failures.append(f"{type(exc).__name__}: {exc}")
        traceback.print_exc()
        annotate("error", "rpwdl trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:2000]}"})
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
        annotate("warning", "rpwdl trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "failures": len(failures)})
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
