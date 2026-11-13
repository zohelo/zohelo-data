"""Dev trial of rpwdl (PLS-020, #2958) on the branch's final code: Landing (inspect, extract twice, verify) into
zz_dev_rpwdl/01_landing, Bronze (load twice) into namespace zz_dev_rpwdl, then the Silver, test and Gold SQL of the
branch applied in memory (Jinja-rendered with warehouse/macros/sources/rpwdl.sql), checks, profiles and Poland spot
values annotated. Reads silver.teryt_terc (read-only) for the voivodeship keys. Everything written is removed at the
end (finally). No record content is reported, only counts and code lists. Deleted before merge."""

from __future__ import annotations

import json
from pathlib import Path
import re
import sys
import traceback

import duckdb
import jinja2
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing import rpwdl as landing
from ingestion.to_landing.patterns import bulk_file
from ingestion.to_bronze.rpwdl import load as bronze

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_rpwdl"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/sources/rpwdl.sql").read_text()
SILVER = ["rpwdl_providers", "rpwdl_establishments", "rpwdl_units", "rpwdl_cells", "rpwdl_practices"]
BRONZE_KEYS = {"rpwdl_providers": "book_id", "rpwdl_establishments": "establishment_id", "rpwdl_units": "unit_id",
               "rpwdl_cells": "cell_id", "rpwdl_practices": "regexp_replace(entity, '_(active|inactive)$', ''), practice_id"}
KEYS = {"rpwdl_providers": "book_id", "rpwdl_establishments": "establishment_id", "rpwdl_units": "unit_id",
        "rpwdl_cells": "cell_id", "rpwdl_practices": "practice_register || '|' || practice_id"}


def render(path: Path, tables: dict[str, str]) -> str:
    env = jinja2.Environment()
    env.globals.update(source=lambda s, t: tables[t], ref=lambda m: tables[m])
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return env.from_string(text).render()


def note(title: str, data) -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"[{title}] {text}", flush=True)
    annotate("notice", f"rpwdl trial: {title}", data if len(text) < 3800 else {"truncated": text[:3700]})


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    one = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
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
        note("landing", {"exit": status, "receipts": len(receipts), "entities": len({r["entity"] for r in receipts}),
                         "bytes": sum(r["size_bytes"] for r in receipts),
                         "second_extract_new_copies": len(receipts) - len({r["entity"] for r in receipts})})

        # Bronze: load twice
        counts = []
        for attempt in (1, 2):
            rc = bronze.load(0, landing_root=LAND, namespace=SCRATCH)
            snap = {}
            for table in bronze.TABLES:
                t = cat.load_table((SCRATCH, table))
                snap[table] = (len(bf.loaded_hashes(t)), t.scan().to_arrow().num_rows)
            counts.append({"exit": rc, **{k: v for k, v in snap.items()}})
        try:
            rejected = cat.load_table((SCRATCH, bronze.REJECTED)).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001
            rejected = []
        for table in bronze.TABLES:
            con.register(f"{table}_arrow", cat.load_table((SCRATCH, table)).scan().to_arrow())
            con.sql(f"create table {table} as select * from {table}_arrow")
        bronze_rows = {t: one(f"select count(*) from {t}") for t in bronze.TABLES}
        per_entity = con.sql(" union all ".join(f"select '{t}' t, entity, count(*) n from {t} group by all" for t in bronze.TABLES)).fetchall()
        note("bronze", {"files_and_rows_after_load_1_2": counts, "rows": bronze_rows,
                        "rejected": [(r.get("entity"), (r.get("error") or "")[:200]) for r in rejected],
                        "rows_per_entity": {f"{t}:{e}": n for t, e, n in per_entity}})

        # Look before modelling: markers and non-conforming strings in Bronze
        prof = {}
        for table, (_, _, kept) in bronze.TABLES.items():
            for col in kept.values():
                n_null_text = one(f"select count(*) from {table} where {col} = 'NULL'")
                n_brak = one(f"select count(*) from {table} where lower(trim({col})) = 'brak wpisu'")
                if n_null_text or n_brak:
                    prof[f"{table}.{col}"] = [n_null_text, n_brak]
        note("bronze markers NULL / brak wpisu", prof)
        odd = {}
        for table, col in [(t, c) for t, (_, _, k) in bronze.TABLES.items() for c in k.values()]:
            if col.endswith(("_date", "_start", "_end")):
                rx = r"^\d{4}-\d{2}-\d{2}$"
            elif col in ("beds_total", "beds_intensive_care", "dialysis_stations", "day_places"):
                rx = r"^\d+$"
            else:
                continue
            bad = con.sql(f"select {col}, count(*) from {table} where {col} not in ('NULL') and lower(trim({col})) <> 'brak wpisu' and not regexp_matches({col}, '{rx}') group by 1 order by 2 desc limit 5").fetchall()
            if bad:
                odd[f"{table}.{col}"] = bad
        note("bronze non-conforming dates and counts", odd)
        codes = {
            "provider legal forms": con.sql("select legal_form_code, count(*) from rpwdl_providers group by 1 order by 2 desc").fetchall(),
            "provider founder codes": con.sql("select founder_code, count(*) from rpwdl_providers group by 1 order by 2 desc").fetchall(),
            "practice kinds": con.sql("select entity, practice_kind, count(*) from rpwdl_practices group by all order by all").fetchall(),
            "teryt lengths": con.sql(" union all ".join(f"select '{t}', length(teryt), count(*) from {t} group by all" for t in bronze.TABLES)).fetchall(),
            "keys in both ledger files": {t: one(f"select count(*) from (select {k} from {t} group by all having count(distinct entity) > 1)") for t, k in BRONZE_KEYS.items()},
        }
        note("bronze code lists", codes)

        # Silver
        silver_rows = {}
        for model in SILVER:
            sql = render(ROOT / f"warehouse/models/silver/rpwdl/{model}.sql", {t: t for t in bronze.TABLES})
            try:
                con.sql(f"create table s_{model} as {sql}")
                silver_rows[model] = one(f"select count(*) from s_{model}")
            except duckdb.Error as exc:
                failures.append(f"silver {model}: {str(exc)[:300]}")
        spec = yaml.safe_load((ROOT / "warehouse/models/silver/rpwdl/_rpwdl.yml").read_text())
        tests = {}
        for m in spec["models"]:
            name = m["name"]
            if name not in silver_rows:
                continue
            tests[f"{name} grain dup"] = one(f"select count(*) from (select {KEYS[name]} k from s_{name} group by 1 having count(*) > 1)")
            for c in m["columns"]:
                for t in c.get("tests", []):
                    if t == "not_null":
                        tests[f"{name}.{c['name']} null"] = one(f"select count(*) from s_{name} where {c['name']} is null")
                    elif t == "unique":
                        tests[f"{name}.{c['name']} dup"] = one(f"select count(*) from (select {c['name']} from s_{name} group by 1 having count(*) > 1)")
                    elif isinstance(t, dict) and "accepted_values" in t:
                        vals = t["accepted_values"]["arguments"]["values"]
                        lst = ", ".join("'" + str(v) + "'" for v in vals)
                        tests[f"{name}.{c['name']} not accepted"] = one(f"select count(*) from s_{name} where {c['name']} is not null and {c['name']} not in ({lst})")
            comp = render(ROOT / f"warehouse/tests/{name}_complete.sql", {name: f"s_{name}"})
            tests[f"{name}_complete rows"] = one(f"select count(*) from ({comp})")
        failing = {k: v for k, v in tests.items() if v}
        failures += [f"test {k}={v}" for k, v in failing.items()]
        profile = {m: con.sql(f"select count(*) n, count(*) filter (is_active) active, count(*) filter (voivodeship_code is null) no_voiv, count(*) filter (is_active and voivodeship_code is null) active_no_voiv, min(start_date), max(start_date), min(snapshot_date), max(snapshot_date) from s_{m}").fetchall()[0] for m in silver_rows}
        note("silver", {"rows": silver_rows, "tests_run": len(tests), "tests_failing": failing,
                        "profile n/active/no_voiv/active_no_voiv/start min,max/snapshot min,max": profile})

        # Gold
        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        con.sql("""create table geo_map as
            select 'rpwdl' as source_system, left(terc_code, 2) as source_code, 'TERC:' || terc_code as geography_key
            from terc where unit_level = 'wojewodztwo'
            union all select 'rpwdl', 'PL', 'PL'""")
        tables = {m: f"s_{m}" for m in SILVER} | {"poc_map_geography": "geo_map"}
        con.sql("create table g_build as " + render(ROOT / "warehouse/models/gold/poc_build_rpwdl.sql", tables))
        for m in ("healthcare_provider_rpwdl", "healthcare_establishment_rpwdl"):
            con.sql(f"create table g_{m} as " + render(ROOT / f"warehouse/models/gold/{m}.sql", tables))
        gold = {
            "build_rows": one("select count(*) from g_build"),
            "dup_series_date": one("select count(*) from (select 1 from g_build group by measure_key, geography_key, breakdown_json, date_key having count(*) > 1)"),
            "unmatched_X": one("select count(*) from g_build where geography_key = '_X'"),
            "areas": one("select count(distinct geography_key) from g_build"),
            "periods": con.sql("select distinct period_label from g_build").fetchall(),
            "null_values": one("select count(*) from g_build where value is null"),
            "provider_rows": one("select count(*) from g_healthcare_provider_rpwdl"),
            "provider_dup": one("select count(*) from (select book_id from g_healthcare_provider_rpwdl group by 1 having count(*) > 1)"),
            "establishment_rows": one("select count(*) from g_healthcare_establishment_rpwdl"),
            "establishment_dup": one("select count(*) from (select establishment_id from g_healthcare_establishment_rpwdl group by 1 having count(*) > 1)"),
            "metrics_rows": one("select count(*) from g_build where is_total and measure_key in ('rpwdl:register:entities:providers', 'rpwdl:register:practices:practices')"),
            "voiv_sum_vs_PL": con.sql("select measure_key, sum(value) filter (geography_key <> 'PL') voiv, max(value) filter (geography_key = 'PL') pl from g_build where is_total group by all order by all").fetchall(),
        }
        if gold["dup_series_date"] or gold["unmatched_X"] or gold["provider_dup"] or gold["establishment_dup"]:
            failures.append(f"gold checks {gold}")
        note("gold", gold)
        spot = con.sql("select measure_key, breakdown_json, geography_key, period_label, value from g_build where geography_key in ('PL', 'TERC:1400000', 'TERC:2400000') order by all").fetchall()
        note("gold spot values PL, mazowieckie, śląskie", [list(r) for r in spot][:60])
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
        annotate("notice", "rpwdl trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
