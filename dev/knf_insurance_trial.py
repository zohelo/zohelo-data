"""Dev trial (scratch only, removed before merge): knf_insurance Landing → Bronze → Silver → Gold build stage.

python -m dev.knf_insurance_trial
Landing under zz_dev_knf_insurance/01_landing (landing bucket), Bronze namespace zz_dev_knf_insurance (files under
zz_dev_knf_insurance/ in the lakehouse bucket); both removed at the end (finally). Silver and the Gold build stage run
in DuckDB in memory from the scratch Bronze table. Profiles and cells go to the log and DEV_OUTPUT_DIR.
"""

from __future__ import annotations

import contextlib
import csv
import os
from pathlib import Path
import re
import traceback

import duckdb
import jinja2
import requests
import yaml

from ingestion.common import bulk_bronze, report
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze.knf_insurance import load as bronze
from ingestion.to_landing import knf_insurance as landing
from ingestion.to_landing.patterns import bulk_file

NS = "zz_dev_knf_insurance"
LANDING = f"{NS}/01_landing"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "/tmp/dev-output"))
SILVER = Path("warehouse/models/silver/knf_insurance")
GOLD = Path("warehouse/models/gold/poc_build_knf_insurance.sql")
MACROS = Path("warehouse/macros/xlsx_cells.sql").read_text()
REAL = report.annotate


def quiet(level, title, data):  # the steps' own annotations are printed; the trial writes at most 10
    print(f"[{level}] {title}: {str(data)[:1500]}", flush=True)


def q(con, sql):
    return con.execute(sql).fetchall()


def render(path: Path, sources: dict, refs: dict) -> str:
    text = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", path.read_text())
    return (
        jinja2.Environment()
        .from_string(MACROS + text)
        .render(source=lambda a, b: sources[(a, b)], ref=lambda n: refs[n], var=lambda *a: None)
    )


def robots() -> dict:
    r = requests.get("https://www.knf.gov.pl/robots.txt", headers={"User-Agent": bulk_file.USER_AGENT}, timeout=30)
    print("=== robots.txt", r.status_code, dict(r.headers), "\n", r.text[:3000], flush=True)
    disallow = [ln.split(":", 1)[1].strip() for ln in r.text.splitlines() if ln.lower().startswith("disallow")]
    paths = ["/publikacje_i_opracowania/dane_statystyczne", "/knf/pl/komponenty/img/"]
    return {"status": r.status_code, "disallow": disallow[:20], "blocked": [p for p in paths if any(d and p.startswith(d) for d in disallow)]}


def listing_dump() -> dict:
    links = landing._anchors(landing.LISTING)
    print("=== listing anchors")
    for href, text in links:
        if "knf.gov.pl" in href and ("articleId" in href or "dane_statystyczne" in href):
            print("  ", text[:140], "|", href[:200])
    named = [(t[:90], landing.release(t)) for h, t in links if landing.ARTICLE_LINK.search(h)]
    pages = [h for h, _ in links if landing.PAGE_LINK.search(h)]
    other_nav = sorted({h for h, _ in links if "dane_statystyczne" in h and "articleId" not in h})[:15]
    return {"articles": len(named), "named": [n for n in named if n[1]][:12], "unnamed": [n[0] for n in named if not n[1]][:12], "page_links": pages[:6], "other_nav": other_nav}


def tests(con, name: str) -> list:
    spec = next(m for f in SILVER.glob("_*.yml") for m in yaml.safe_load(f.read_text())["models"] if m["name"] == name)
    out = []
    for t in spec.get("tests", []):
        if "unique" in t:
            expr = t["unique"]["column_name"]
            out.append(("unique", q(con, f"select count(*) - count(distinct {expr}) from {name}")[0][0]))
    for col in spec.get("columns", []):
        for t in col.get("tests", []):
            if t == "not_null":
                out.append((f"not_null {col['name']}", q(con, f"select count(*) from {name} where {col['name']} is null")[0][0]))
            elif isinstance(t, dict) and "accepted_values" in t:
                vals = t["accepted_values"]["arguments"]["values"]
                lit = ", ".join(repr(v) if isinstance(v, str) else str(v) for v in vals)
                out.append((f"accepted {col['name']}", q(con, f"select count(*) from {name} where {col['name']} is not null and {col['name']} not in ({lit})")[0][0]))
    return out


def profile(con) -> dict:
    """Per entity: sheets, cell counts, and the cells of every sheet to the log (first 60 rows) and a CSV."""
    OUT.mkdir(parents=True, exist_ok=True)
    rows = q(con, "select entity, sheet_index, sheet_name, row_number, column_number, cell_type, value_text, number_format from cells order by all")
    with (OUT / "cells.csv").open("w", newline="") as f:
        csv.writer(f).writerows([("entity", "sheet_index", "sheet_name", "row", "col", "type", "value", "format"), *rows])
    newest = {}
    for (entity,) in q(con, "select distinct entity from cells order by 1"):
        newest[entity.split("_")[0] + ("_" + entity.rsplit("_", 1)[-1] if entity.startswith("bulletin") else "")] = entity
    for entity in newest.values():
        print(f"\n######## profile {entity}")
        for sheet, n, maxr, maxc in q(con, f"select sheet_name, count(*), max(row_number), max(column_number) from cells where entity = '{entity}' group by sheet_index, sheet_name order by sheet_index"):
            print(f"--- sheet {sheet!r}: {n} cells, rows {maxr}, cols {maxc}")
            for r, c, t, v, fmt in q(con, f"select row_number, column_number, cell_type, value_text, number_format from cells where entity = '{entity}' and sheet_name = $$${sheet}$$$ and row_number <= 60 order by 1, 2".replace("$$$", "$$")):
                print(f"   {r}:{c} {t} {v[:80]!r} {fmt}")
    return {
        "entities": q(con, "select split_part(entity, '_', 1), count(distinct entity), min(entity), max(entity), count(*) from cells group by 1 order by 1"),
        "sheets": q(con, "select split_part(entity, '_', 1), list(distinct sheet_name order by sheet_name)[:20] from cells group by 1 order by 1"),
        "non_numeric_short": q(con, "select value_text, count(*) from cells where cell_type in ('s','inlineStr','str') and length(trim(value_text)) <= 5 group by 1 order by 2 desc limit 25"),
    }


def main() -> None:
    report.annotate = bulk_bronze.annotate = bulk_file.annotate = quiet
    cat, client = catalog(), s3()
    head = os.environ.get("GITHUB_SHA", "")[:7]
    out: dict = {"head": head}
    try:
        out["robots"] = robots()
        if out["robots"]["status"] != 200 or out["robots"]["blocked"]:
            REAL("warning", "knf_insurance trial robots", out["robots"])
        out["listing"] = listing_dump()
    except Exception:  # noqa: BLE001
        out["probe_crash"] = traceback.format_exc()[-1200:]
    REAL("notice", "knf_insurance trial: robots and listing", out)
    if out.get("robots", {}).get("blocked"):
        return
    summary: dict = {"head": head}
    con = duckdb.connect()
    try:
        summary["landing_exit"] = [landing.extract(60, landing_root=LANDING) for _ in (1, 2)]
        held = sorted(landing.held_entities(LANDING))
        receipts = [r for e in held for r in bulk_file.landed(client, req("STORAGE_LANDING_BUCKET_NAME"), bulk_file.prefix(landing.SOURCE, e, LANDING))]
        summary["landing_files"] = len(receipts)
        summary["landing_bytes"] = sum(r["size_bytes"] for r in receipts)
        summary["landing_versions_per_entity_max"] = max((sum(1 for r in receipts if r["entity"] == e) for e in held), default=0)
        summary["entities"] = held
        summary["last_modified_sent"] = sum(1 for r in receipts if r["version_basis"] == "Last-Modified")
        summary["verify_exit"] = bulk_file.main(landing.SOURCE, "verify", landing_root=LANDING, specs=[{"entity": e} for e in held])
        rows = []
        for _ in (1, 2):
            bronze.load(0, landing_root=LANDING, namespace=NS)
            t = cat.load_table((NS, bronze.TABLE)).scan().to_arrow()
            rows.append((t.num_rows, len(cat.load_table((NS, bronze.TABLE)).inspect.files())))
        summary["bronze_rows_files_load1_load2"] = rows
        rejected = []
        with contextlib.suppress(Exception):
            rejected = cat.load_table((NS, bronze.REJECTED)).scan().to_arrow().to_pylist()
        summary["rejected"] = [(r["entity"], r["error"][:160]) for r in rejected]
        con.register("cells", t)
        REAL("notice", "knf_insurance trial: landing and bronze", summary)
    except Exception:  # noqa: BLE001
        summary["crash"] = traceback.format_exc()[-2500:]
        REAL("error", "knf_insurance trial: landing and bronze", summary)
        return
    try:
        REAL("notice", "knf_insurance trial: bronze profile", profile(con))
    except Exception:  # noqa: BLE001
        REAL("error", "knf_insurance trial: bronze profile", traceback.format_exc()[-1500:])
    if not SILVER.exists():
        return
    sources, refs, silver = {("knf_insurance", "knf_insurance_cells"): "cells"}, {}, {}
    try:
        for path in sorted(SILVER.glob("*.sql")):
            name = path.stem
            counts = []
            for _ in (1, 2):
                con.execute(f"create or replace table {name} as {render(path, sources, refs)}")
                counts.append(q(con, f"select count(*), sum(hash(*)) from (select * exclude (ingested_at) from {name})")[0])
            refs[name] = name
            results = tests(con, name)
            silver[name] = {"rows": counts[0][0], "rebuild_identical": counts[0] == counts[1], "tests_run": len(results), "test_failures": {k: v for k, v in results if v}}
        extra = Path("dev/knf_insurance_checks.sql")
        if extra.exists():
            for sql in [s for s in extra.read_text().split(";\n") if s.strip()]:
                title = sql.strip().splitlines()[0]
                try:
                    res = q(con, sql)
                    print(f"\n=== {title}")
                    for r in res[:200]:
                        print("   ", r)
                    if title.startswith("-- annotate"):
                        silver[title[12:60]] = res[:40]
                except Exception as exc:  # noqa: BLE001
                    print(f"\n=== {title} failed: {exc}")
        REAL("notice" if all(not s["test_failures"] for s in silver.values() if isinstance(s, dict)) else "error", "knf_insurance trial: silver", silver)
    except Exception:  # noqa: BLE001
        REAL("error", "knf_insurance trial: silver", {"done": silver, "crash": traceback.format_exc()[-2000:]})
        return
    if not GOLD.exists():
        return
    try:
        con.execute("create table geo as select * from (values ('knf_insurance', 'PL', 'PL')) t (source_system, source_code, geography_key)")
        refs["poc_map_geography"] = "geo"
        con.execute(f"create table g as {render(GOLD, sources, refs)}")
        gold = {
            "rows": q(con, "select count(*) from g")[0][0],
            "per_block": q(con, "select block_id || ':' || frequency, count(*), count(distinct measure_key), min(period_label), max(period_label) from g group by all order by 1"),
            "dup_series_key_x_date_key": q(con, "select count(*) from (select measure_key || '|' || geography_key || '|' || breakdown_json, date_key from g group by all having count(*) > 1)")[0][0],
            "frequencies_per_measure_max": q(con, "select max(n) from (select count(distinct frequency) n from g group by measure_key)")[0][0],
            "rows_geography_unmatched": q(con, "select count(*) from g where geography_key = '_X'")[0][0],
            "null_value_or_date": q(con, "select count(*) from g where value is null or date_key is null")[0][0],
            "units": q(con, "select unit_code, count(*) from g group by 1 order by 1"),
            "spot": q(con, "select measure_key, period_label, value, unit_code from g where is_total qualify row_number() over (partition by measure_key order by date_key desc) = 1 order by 1")[:30],
        }
        REAL("notice" if gold["dup_series_key_x_date_key"] == 0 else "error", "knf_insurance trial: gold build", gold)
    except Exception:  # noqa: BLE001
        REAL("error", "knf_insurance trial: gold build", traceback.format_exc()[-2000:])


def cleanup() -> dict:
    cat, client, gone = catalog(), s3(), {"tables": [], "objects": 0}
    for t in (bronze.TABLE, bronze.REJECTED):
        with contextlib.suppress(Exception):
            cat.drop_table((NS, t))
            gone["tables"].append(t)
    with contextlib.suppress(Exception):
        cat.drop_namespace(NS)
    for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{NS}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
                gone["objects"] += len(keys)
    gone["left"] = sum(
        len(p.get("Contents", []))
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{NS}/")
    )
    return gone


if __name__ == "__main__":
    try:
        main()
    finally:
        REAL("notice", "knf_insurance trial cleanup", cleanup())
