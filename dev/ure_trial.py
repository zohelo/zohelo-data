"""Dev trial of URE price indicators (register PL-ENV-020, #2870): Landing → Bronze → Silver → Gold into scratch.

Args: ``profile`` (robots.txt, Landing of the candidate pages twice, table profile of every page, Bronze load twice) or
``full`` (default: the same for the declared pages, then the Silver model, its schema tests and the Gold build stage
rendered with Jinja from warehouse/ and run in local DuckDB over the scratch Bronze table, each built twice).
Landing prefix and lake prefix zz_dev_ure/, Iceberg namespace zz_dev_ure; everything written is removed in `finally`.
robots.txt of www.ure.gov.pl is read once and respected; a refusal stops the trial. Deleted before merge.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import sys
import time
import urllib.robotparser

import duckdb
from jinja2 import Environment
import requests
import yaml

from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.ure import load as ure_load
from ingestion.to_landing.patterns import bulk_file

CAPTURED: list = []
ROBOTS: dict = {}


def _capture(level: str, title: str, data) -> None:
    CAPTURED.append((title, data))
    print(f"[{level}] {title}: {json.dumps(data, default=str)[:3000]}", flush=True)


bulk_file.annotate = _capture
bulk_bronze.annotate = _capture

SCRATCH = "zz_dev_ure"
SOURCE = "ure"
BASE = "https://www.ure.gov.pl"
ELECTRICITY = BASE + "/pl/energia-elektryczna/ceny-wskazniki"
GAS = BASE + "/pl/paliwa-gazowe/ceny-wskazniki"
CANDIDATES = [("page_7853", ELECTRICITY), ("page_8425", ELECTRICITY), ("page_10600", ELECTRICITY),
              ("page_9165", ELECTRICITY), ("page_7859", GAS)]


def note(title: str, data) -> None:
    text = json.dumps(data, default=str, ensure_ascii=False)
    print(f"### {title}: {text}", flush=True)
    annotate("notice", title, json.loads(text[:3800]) if len(text) <= 3800 else {"truncated": text[:3700]})


def robots(urls: list[str]) -> bool:
    r = requests.get(BASE + "/robots.txt", headers={"User-Agent": bulk_file.USER_AGENT}, timeout=30)
    parser = urllib.robotparser.RobotFileParser()
    parser.parse(r.text.splitlines() if r.status_code == 200 else [])
    allowed = {u[len(BASE):][:70]: parser.can_fetch(bulk_file.USER_AGENT, u) for u in urls}
    ok = r.status_code in (200, 404) and all(allowed.values())
    ROBOTS.update({"status": r.status_code, "content_type": r.headers.get("Content-Type"),
                            "server": r.headers.get("Server"), "body": re.sub(r"\s+", " ", r.text)[:900],
                            "allowed": allowed, "verdict": "proceed" if ok else "stop"})
    if not ok:
        note("ure robots.txt: stop", ROBOTS)
    return ok


def specs(mode: str) -> list[dict]:
    declared = bulk_file.declaration(SOURCE)
    if mode != "profile":
        return declared
    have = {s["entity"] for s in declared}
    extra = [{"entity": e, "url_from": {"page": page, "pattern": f"/ceny-wskazniki/{e.split('_')[1]},"},
              "format": "raw", "version": "last_modified"} for e, page in CANDIDATES if e not in have]
    return declared + extra


def landing(spec_list: list[dict]) -> dict:
    out = {}
    CAPTURED.clear()
    out["exit"] = [bulk_file.main(SOURCE, "load", landing_root=SCRATCH, specs=spec_list, pause_seconds=3) for _ in (1, 2)]
    out["outcomes"] = [[(f.get("entity"), f.get("outcome")) for f in d.get("files", [])] for t, d in CAPTURED if "landing" in t]
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = {}
    for s in spec_list:
        copies = bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, s["entity"], SCRATCH))
        held[s["entity"]] = copies
    out["copies_bytes_version"] = {e: [len(c), c[-1]["size_bytes"] if c else 0, c[-1]["version_token"] if c else None,
                                       c[-1].get("final_url", "")[len(BASE):][:90] if c else None] for e, c in held.items()}
    out["files"] = sum(1 for c in held.values() if c)
    out["bytes"] = sum(c[-1]["size_bytes"] for c in held.values() if c)
    return {"summary": out, "held": held}


def profile_pages(held: dict) -> None:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    for entity, copies in held.items():
        if not copies:
            continue
        body = bulk_bronze.read_raw(client, bucket, SOURCE, copies[-1])
        page = ure_load.parse(body)
        tables = []
        for n, t in enumerate(page["tables"], 1):
            widths = sorted({len(r) for r in t})
            tables.append({"n": n, "rows": len(t), "widths": widths,
                           "r1": [f"{c['tag']}{'*' + str(c['colspan']) if c['colspan'] > 1 else ''}{'^' + str(c['rowspan']) if c['rowspan'] > 1 else ''}:{c['text'][:60]}" for c in t[0]],
                           "r2": [c["text"][:40] for c in t[1]] if len(t) > 1 else None,
                           "r3": [c["text"][:40] for c in t[2]] if len(t) > 2 else None,
                           "last": [c["text"][:40] for c in t[-1]]})
        big = max(page["tables"], key=len, default=[])
        lines = [" | ".join(c["text"][:28] for c in r) for r in big]
        print(f"### rows {entity}: " + json.dumps(lines, ensure_ascii=False), flush=True)
        note(f"ure profile {entity}", {"title": page["title"], "published": page["published"],
                                       "modified": page["modified"], "bytes": len(body), "tables": tables[:4],
                                       "largest_table_rows": lines[:40]})


def bronze() -> tuple[dict, object]:
    CAPTURED.clear()
    out = {"exit": [ure_load.load(0, landing_root=SCRATCH, namespace=SCRATCH) for _ in (1, 2)]}
    out["loads"] = [{k: d.get(k) for k in ("candidates", "appended_files", "rows_added", "rejected")} for t, d in CAPTURED]
    cat = catalog()
    table = cat.load_table((SCRATCH, "ure_table_cells"))
    arrow = table.scan().to_arrow()
    out["files"] = len(list(table.scan().plan_files()))
    out["rows"] = arrow.num_rows
    try:
        rejected = cat.load_table((SCRATCH, "ure_rejected_files")).scan().to_arrow().to_pylist()
    except Exception:  # noqa: BLE001
        rejected = []
    out["rejected"] = [{"entity": r.get("entity"), "error": (r.get("error") or "")[:700]} for r in rejected]
    return out, arrow


def render(path: str, refs: dict[str, str], sources: dict[str, str]) -> str:
    macros = Path("warehouse/macros/poc_blocks.sql").read_text()
    return Environment(extensions=["jinja2.ext.do"]).from_string(macros + Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda name: refs.get(name, name), source=lambda _s, t: sources[t])


def one(con, q):
    return con.execute(q).fetchone()[0]


def rows(con, q, limit=60):
    return [[str(v) for v in r] for r in con.execute(q).fetchall()][:limit]


def schema_tests(con, model: str) -> dict:
    spec = yaml.safe_load(Path(f"warehouse/models/silver/ure/_{SOURCE}.yml").read_text())
    m = next(x for x in spec["models"] if x["name"] == model)
    results = {}
    for t in m.get("tests", []):
        if "unique" in t:
            expr = t["unique"]["column_name"]
            results["unique"] = one(con, f"select count(*) from (select {expr} k from {model} group by 1 having count(*) > 1)")
    for col in m.get("columns", []):
        for t in col.get("tests", []):
            if t == "not_null":
                results[f"not_null {col['name']}"] = one(con, f"select count(*) from {model} where {col['name']} is null")
            elif isinstance(t, dict) and "accepted_values" in t:
                vals = t["accepted_values"]["arguments"]["values"]
                lst = ", ".join("'" + str(v).replace("'", "''") + "'" for v in vals)
                results[f"accepted_values {col['name']}"] = one(
                    con, f"select count(*) from {model} where {col['name']} is not null and cast({col['name']} as varchar) not in ({lst})")
    return results


def bronze_profile(con) -> dict:
    return {
        "per_entity": rows(con, "select entity, any_value(page_modified), count(*), count(distinct row_number), max(column_number), "
                                "count(*) filter (where value_text is null or value_text = ''), count(distinct raw_sha256) from bronze_cells group by 1 order by 1"),
        "non_numeric_values": rows(con, "select value_text, count(*) from bronze_cells where cell_tag = 'td' and column_number > 1 "
                                        "and not regexp_matches(value_text, '^-?[0-9 ]+(,[0-9]+)?$') group by 1 order by 2 desc limit 40"),
    }


def silver_gold(con, result: dict) -> None:
    model = "ure_price_indicators"
    sql = render(f"warehouse/models/silver/ure/{model}.sql", {}, {"ure_table_cells": "bronze_cells"})
    counts = []
    for _ in (1, 2):
        con.execute(f"create or replace table {model} as {sql}")
        counts.append(one(con, f"select count(*) from {model}"))
    s = {"rows_two_builds": counts, "tests": schema_tests(con, model)}
    s["per_measure"] = rows(con, f"select indicator_code, frequency, unit_code, count(*), min(period_label), max(period_label), "
                                 f"min(value), max(value), count(*) filter (where value is null) from {model} group by all order by all")
    s["status"] = rows(con, f"select observation_status, count(*) from {model} group by 1")
    s["newest"] = rows(con, f"select indicator_code, frequency, period_label, value_text, value from {model} "
                            f"qualify row_number() over (partition by indicator_code, frequency order by period_start desc) <= 2 order by all")
    note("ure silver", s)
    result["silver"] = s
    gmap = render("warehouse/models/gold/poc_map_geography.sql", {}, {})
    cte = re.search(r"\nure as \((.*?)\n\),?\n", gmap, re.S).group(1)
    con.execute(f"create or replace table gmap_all as {cte}")
    build_sql = render("warehouse/models/gold/poc_build_ure.sql", {model: model, "poc_map_geography": "gmap_all"}, {})
    activated = Environment(extensions=["jinja2.ext.do"]).from_string(
        Path("warehouse/macros/poc_blocks.sql").read_text() + "{{ poc_activated_blocks() }}").render(ref=lambda n: n, config=lambda **_k: "")
    con.execute(f"create or replace table activated as select * from ({activated}) where source_system = '{SOURCE}'")
    gold = []
    for _ in (1, 2):
        con.execute(f"create or replace table build as {build_sql}")
        gold.append(one(con, "select count(*) from build"))
    g = {"rows_two_builds": gold}
    g["dup_series_date"] = one(con, "select count(*) from (select measure_key || '|' || geography_key || case when geography_key = '_X' "
                                    "then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json, date_key "
                                    "from build group by all having count(*) > 1)")
    g["freq_per_measure"] = one(con, "select count(*) from (select measure_key from build group by 1 having count(distinct frequency) > 1)")
    g["unmatched"] = one(con, "select count(*) from build where geography_key = '_X'")
    g["null_value"] = one(con, "select count(*) from build where value is null")
    g["build_check"] = rows(con, "select b.block_id, count(o.block_id), count(distinct o.measure_key), count(*) filter (where o.geography_key = '_X'), "
                                 "min(o.date_key), max(o.date_key) from activated b left join build o on o.block_id = b.block_id group by all order by 1")
    g["measures"] = rows(con, "select measure_key, frequency, unit_code, count(*), min(period_label), max(period_label) from build group by all order by 1")
    g["PL_newest"] = rows(con, "select measure_key, period_label, value, observation_status_key from build where geography_key = 'PL' "
                               "qualify row_number() over (partition by measure_key order by date_key desc) = 1 order by 1")
    g["columns"] = [d[0] for d in con.execute("select * from build limit 0").description]
    note("ure gold", g)


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
    note("ure scratch removed", removed)


def main(argv: list[str]) -> int:
    mode = argv[0] if argv else "full"
    status = 0
    try:
        spec_list = specs(mode)
        urls = [s["url"] if "url" in s else s["url_from"]["page"] for s in spec_list]
        if not robots(sorted(set(urls))):
            return 1
        time.sleep(3)
        land = landing(spec_list)
        note("ure robots.txt and landing", {"robots": ROBOTS, **land["summary"]})
        profile_pages(land["held"])
        b, arrow = bronze()
        con = duckdb.connect()
        con.register("bronze_cells", arrow)
        if arrow.num_rows:
            b["profile"] = bronze_profile(con)
        note("ure bronze", b)
        if mode != "profile":
            silver_gold(con, {})
    except Exception as exc:  # noqa: BLE001
        status = 1
        annotate("error", "ure trial failed", {"error": f"{type(exc).__name__}: {str(exc)[:1500]}"})
    finally:
        cleanup()
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
