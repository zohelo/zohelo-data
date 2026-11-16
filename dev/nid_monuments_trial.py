"""Dev trial of nid_monuments (PLS-026, #2471-#2473) on the branch's final code: Landing (inspect, extract twice,
verify) into zz_dev_nid_monuments/01_landing, Bronze (load twice) into namespace zz_dev_nid_monuments, then the
Silver, test and Gold SQL of the branch applied in memory (Jinja-rendered with warehouse/macros/sources/
nid_monuments.sql), checks, profiles and Poland spot values annotated. Reads silver.teryt_terc (read-only) for the
geography map. Everything written is removed at the end (finally). Only counts, code lists and register numbers are
reported, no address. The framework's own annotations are printed to the log only (at most 10 annotations).
Deleted before merge."""

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
from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.nid_monuments import load as bronze
from ingestion.to_landing import nid_monuments as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "nid_monuments"
SCRATCH = "zz_dev_nid_monuments"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/sources/nid_monuments.sql").read_text()
LOGGED: list[str] = []


def quiet(level, title, data) -> None:  # framework annotations: log only
    text = json.dumps(data, default=str, ensure_ascii=False)
    LOGGED.append(f"{title}: {text[:300]}")
    print(f"[framework {level}] {title}: {text}", flush=True)


bulk_file.annotate = quiet
bulk_bronze.annotate = quiet


def render(path: Path | str, tables: dict[str, str]) -> str:
    env = jinja2.Environment()
    env.globals.update(source=lambda s, t: tables[t], ref=lambda m: tables[m])
    body = path.read_text() if isinstance(path, Path) else path
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", body, flags=re.S)
    return env.from_string(text).render()


def note(title: str, data) -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"[{title}] {text}", flush=True)
    annotate("notice", f"nid_monuments trial: {title}", data if len(text) < 3800 else {"truncated": text[:3700]})


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    one = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    failures: list[str] = []
    try:
        # Landing: inspect, extract twice, verify (the page is read once per call)
        specs = landing.specs()
        status = {"inspect": bulk_file.main(SOURCE, "inspect", landing_root=LAND, specs=specs)}
        for attempt in (1, 2):
            status[f"extract_{attempt}"] = bulk_file.main(SOURCE, "load", landing_root=LAND, specs=specs,
                                                          pause_seconds=landing.PAUSE_SECONDS)
        status["verify"] = bulk_file.main(SOURCE, "verify", landing_root=LAND,
                                          specs=[{"entity": e} for e, _ in landing.LISTS.values()])
        receipts = []
        for spec in specs:
            receipts += bulk_file.landed(client, req("STORAGE_LANDING_BUCKET_NAME"),
                                         bulk_file.prefix(SOURCE, spec["entity"], LAND))
        note("landing", {"exit": status, "receipts": len(receipts), "lists": len({r["entity"] for r in receipts}),
                         "bytes": sum(r["size_bytes"] for r in receipts),
                         "versions": sorted({r["version_token"] for r in receipts}),
                         "second_extract_new_copies": len(receipts) - len({r["entity"] for r in receipts}),
                         "urls": sorted({r["source_url"].rsplit("/", 1)[-1] for r in receipts})})
        if status["extract_1"] or status["extract_2"] or status["verify"]:
            failures.append(f"landing exit {status}")

        # Bronze: load twice
        counts = []
        table_id = (SCRATCH, "nid_monuments_items")
        for _ in (1, 2):
            rc = bronze.load(0, landing_root=LAND, namespace=SCRATCH)
            t = cat.load_table(table_id)
            counts.append({"exit": rc, "files": len(bf.loaded_hashes(t)), "rows": t.scan().to_arrow().num_rows})
        try:
            rejected = cat.load_table((SCRATCH, "nid_monuments_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001
            rejected = []
        con.register("b_arrow", cat.load_table(table_id).scan().to_arrow())
        con.sql("create table b as select * from b_arrow")
        per_list = con.sql("select entity, count(*), max(page_number), count(distinct heading_text), "
                           "max(line_count) from b group by 1 order by 1").fetchall()
        note("bronze", {"load_1_2": counts, "rejected": [(r.get("entity"), (r.get("error") or "")[:300])
                                                         for r in rejected],
                        "per_list items/pages/headings/max_lines": {e: [n, p, h, m] for e, n, p, h, m in per_list},
                        "state_texts": con.sql("select state_text, count(*) from b group by 1 order by 2 desc")
                        .fetchall()})
        if rejected or counts[0]["rows"] != counts[1]["rows"] or counts[0]["files"] != counts[1]["files"]:
            failures.append(f"bronze {counts} rejected {len(rejected)}")

        # Silver
        tables = {"nid_monuments_items": "s_items", "nid_monuments_decisions": "s_decisions"}
        con.sql("create table s_items as " + render(ROOT / "warehouse/models/silver/nid_monuments/nid_monuments_items.sql",
                                                    {"nid_monuments_items": "b"}))
        con.sql("create table s_decisions as " + render(
            ROOT / "warehouse/models/silver/nid_monuments/nid_monuments_decisions.sql", tables))
        spec = yaml.safe_load((ROOT / "warehouse/models/silver/nid_monuments/_nid_monuments.yml").read_text())
        tests = {}
        for m in spec["models"]:
            t = tables[m["name"]]
            for test in m.get("tests", []):
                col = test["unique"]["column_name"]
                tests[f"{m['name']} grain dup"] = one(f"select count(*) from (select {col} k from {t} group by 1 "
                                                      f"having count(*) > 1)")
            for c in m["columns"]:
                for test in c.get("tests", []):
                    if test == "not_null":
                        tests[f"{m['name']}.{c['name']} null"] = one(f"select count(*) from {t} where {c['name']} is null")
                    elif isinstance(test, dict) and "accepted_values" in test:
                        lst = ", ".join(f"'{v}'" for v in test["accepted_values"]["arguments"]["values"])
                        tests[f"{m['name']}.{c['name']} not accepted"] = one(
                            f"select count(*) from {t} where {c['name']} is not null and {c['name']} not in ({lst})")
            comp = render(ROOT / f"warehouse/tests/{m['name']}_complete.sql", tables)
            tests[f"{m['name']}_complete rows"] = con.sql(f"select * from ({comp})").fetchall()
        failing = {k: v for k, v in tests.items() if v}
        failures += [f"test {k}={v}" for k, v in failing.items()]
        roles = con.sql("select list_code, register_role, count(*) from s_items group by all order by all").fetchall()
        by_list = {}
        for lst, role, n in roles:
            by_list.setdefault(lst, {})[role] = n
        note("silver", {
            "rows": {"items": one("select count(*) from s_items"), "decisions": one("select count(*) from s_decisions"),
                     "register_keys": one("select count(distinct register_key) from s_decisions")},
            "tests_run": len(tests), "tests_failing": failing,
            "roles_per_list": by_list,
            "list_state_dates": con.sql("select list_code, min(page_state_date), max(page_state_date) from s_items "
                                        "group by 1 having min(page_state_date) <> max(page_state_date)").fetchall(),
        })
        prof = {
            "decisions undated (numbered, own)": one("select count(*) from s_decisions where register_role = 'numbered' "
                                                     "and decision_date is null"),
            "undated date texts (top)": con.sql("select coalesce(decision_date_text, '<none>'), count(*) from s_decisions "
                                                "where register_role = 'numbered' and decision_date is null "
                                                "group by 1 order by 2 desc limit 12").fetchall(),
            "number-only shapes (top)": con.sql(
                "select regexp_replace(regexp_replace(register_number, '[0-9]', '9', 'g'), '[A-Za-z]', 'a', 'g') s, "
                "count(*) from s_decisions where decision_date_text is null group by 1 order by 2 desc limit 12"
            ).fetchall(),
            "number shapes (top)": con.sql(
                "select regexp_replace(regexp_replace(register_number, '[0-9]+', '9', 'g'), '[A-Za-z]+', 'a', 'g') s, "
                "count(*) from s_decisions group by 1 order by 2 desc limit 15").fetchall(),
            "decision years min/max": con.sql("select min(decision_date), max(decision_date) from s_decisions").fetchall(),
            "future decisions": one("select count(*) from s_decisions where decision_date > current_date"),
            "items per register_key max": one("select max(n) from (select register_key, count(distinct item_number) n "
                                              "from s_decisions group by 1)"),
            "unnumbered samples (object, register text)": con.sql(
                "select list_code, object_name, is_group from s_items where register_role = 'unnumbered' "
                "using sample reservoir(12 rows) repeatable (7)").fetchall(),
            "object_name null": one("select count(*) from s_items where object_name is null"),
        }
        note("silver profile", prof)

        # Gold: geography map from silver.teryt_terc (current TERYT), as poc_dim_geography builds it
        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        con.sql("""create table geo as
            select 'TERC:' || terc_code as geography_key, terc_code, name_pl, unit_level as level_name
            from terc where unit_level in ('wojewodztwo', 'powiat')""")
        cte = jinja2.Environment().from_string(MACROS + "{{ poc_geography__nid_monuments() }}").render()
        con.sql(f"create table geo_map as with {cte} select * from nid_monuments")
        map_dups = one("select count(*) from (select source_code from geo_map group by 1 having count(*) > 1)")
        gtables = tables | {"poc_map_geography": "geo_map"}
        con.sql("create table g_build as " + render(ROOT / "warehouse/models/gold/poc_build_nid_monuments.sql", gtables))
        con.sql("create table g_register as " + render(ROOT / "warehouse/models/gold/nid_monument_register.sql", gtables))
        gold = {
            "build_rows": one("select count(*) from g_build"),
            "dup_series_date": one("select count(*) from (select 1 from g_build group by measure_key, geography_key, "
                                   "breakdown_json, date_key having count(*) > 1)"),
            "map_duplicate_codes": map_dups,
            "unmatched_X": con.sql("select geography_source_code, measure_key, value from g_build "
                                   "where geography_key = '_X' order by 1").fetchall(),
            "areas": one("select count(distinct geography_key) from g_build"),
            "dates": con.sql("select distinct date_key from g_build").fetchall(),
            "null_values": one("select count(*) from g_build where value is null"),
            "register_rows": one("select count(*) from g_register"),
            "register_dup_keys": one("select count(*) from (select register_key from g_register group by 1 "
                                     "having count(*) > 1)"),
            "register_null_geo": one("select count(*) from g_register where geography_key is null"),
            "register_X_or_multi": con.sql("select count(*) filter (geography_key = '_X'), "
                                           "count(*) filter (powiat_count > 1) from g_register").fetchall(),
            "powiat_sum_vs_voiv_vs_PL": con.sql(
                "select measure_key, sum(value) filter (geography_key like 'TERC:____' ), "
                "sum(value) filter (geography_key like 'TERC:__'), max(value) filter (geography_key = 'PL') "
                "from g_build group by 1 order by 1").fetchall(),
        }
        if gold["dup_series_date"] or gold["unmatched_X"] or gold["register_dup_keys"] or map_dups:
            failures.append("gold checks failed")
        note("gold", gold)
        spot = con.sql("select measure_key, geography_key, geography_source_code, period_label, value from g_build "
                       "where geography_key in ('PL', 'TERC:14', 'TERC:1465', 'TERC:12', 'TERC:1261', 'TERC:16', "
                       "'TERC:1661', 'TERC:02', 'TERC:32') order by all").fetchall()
        note("gold spot values Poland", {"rows": [list(r) for r in spot],
                                         "register sample": con.sql(
                                             "select register_key, geography_key, objects, decision_date "
                                             "from g_register where list_code = 'opo' order by register_key limit 5"
                                         ).fetchall()})
        note("ceilings measured", {"nid_monuments_items": one("select count(*) from s_items"),
                                   "nid_monuments_decisions": one("select count(*) from s_decisions"),
                                   "nid_monument_register": gold["register_rows"],
                                   "poc_build_nid_monuments": gold["build_rows"],
                                   "failures": failures})
    except Exception as exc:  # noqa: BLE001
        failures.append(f"{type(exc).__name__}: {exc}")
        traceback.print_exc()
        annotate("error", "nid_monuments trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:2000]}"})
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
        annotate("notice", "nid_monuments trial cleanup", {"objects_removed": removed, "tables_dropped": dropped,
                                                          "failures": failures[:10]})
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
