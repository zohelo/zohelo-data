"""Dev trial (scratch only): MF debt, MF state budget, MF Treasury debt, KRUS Landing → Bronze → Silver → Gold.

python -m dev.trial_mf [keep]
Writes Landing under zz_dev_mf_landing/ and Bronze namespace zz_dev_mf (files under zz_dev_mf/); both removed at the
end unless 'keep'. Silver and the Gold build stage run in DuckDB in memory from the scratch Bronze tables.
"""

from __future__ import annotations

import contextlib
import importlib
from pathlib import Path
import re
import sys
import traceback

import duckdb
import jinja2
import yaml

from ingestion.common.r2 import catalog, req, s3
from ingestion.to_landing.patterns import bulk_file

NS = "zz_dev_mf"
LANDING = "zz_dev_mf_landing"
SOURCES = ["krus", "mf_public_debt", "mf_treasury_debt", "mf_state_budget"]
SILVER = {
    "krus": ("krus_benefits", "warehouse/models/silver/krus"),
    "mf_public_debt": ("public_debt", "warehouse/models/silver/mf_public_debt"),
    "mf_treasury_debt": ("treasury_debt", "warehouse/models/silver/mf_treasury_debt"),
    "mf_state_budget": ("budget_execution", "warehouse/models/silver/mf_state_budget"),
}
MACROS = Path("warehouse/macros/xlsx_cells.sql").read_text()


def render(path: str, sources: dict, refs: dict) -> str:
    text = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", Path(path).read_text())
    return jinja2.Environment().from_string(MACROS + text).render(
        source=lambda a, b: sources[(a, b)], ref=lambda n: refs[n]
    )


def cleanup(cat, client) -> dict:
    gone = {"tables": 0, "objects": 0}
    for s in SOURCES:
        for t in (f"{s}_cells", f"{s}_rejected_files"):
            with contextlib.suppress(Exception):
                cat.drop_table((NS, t))
                gone["tables"] += 1
    with contextlib.suppress(Exception):
        cat.drop_namespace(NS)
    for bucket, prefix in ((req("STORAGE_LANDING_BUCKET_NAME"), LANDING + "/"), (req("STORAGE_LAKEHOUSE_BUCKET_NAME"), NS + "/")):
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
                gone["objects"] += len(keys)
    left = sum(
        len(p.get("Contents", []))
        for bucket, prefix in ((req("STORAGE_LANDING_BUCKET_NAME"), LANDING + "/"), (req("STORAGE_LAKEHOUSE_BUCKET_NAME"), NS + "/"))
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    )
    gone["left"] = left
    return gone


def tests(con, name: str, folder: str) -> list:
    """The model's yml tests, run in DuckDB: unique expression, not_null and accepted_values."""
    spec = next(m for f in Path(folder).glob("_*.yml") for m in yaml.safe_load(f.read_text())["models"] if m["name"] == name)
    out = []
    for t in spec.get("tests", []):
        expr = t["unique"]["column_name"]
        n = con.execute(f"select count(*) - count(distinct {expr}) from {name}").fetchone()[0]
        out.append(("unique", n))
    for col in spec.get("columns", []):
        for t in col.get("tests", []):
            if t == "not_null":
                out.append((f"not_null {col['name']}", con.execute(f"select count(*) from {name} where {col['name']} is null").fetchone()[0]))
            elif isinstance(t, dict) and "accepted_values" in t:
                vals = t["accepted_values"]["arguments"]["values"]
                lit = ", ".join(repr(v) if isinstance(v, str) else str(v) for v in vals)
                out.append((f"accepted {col['name']}", con.execute(f"select count(*) from {name} where {col['name']} is not null and {col['name']} not in ({lit})").fetchone()[0]))
    return out


def q(con, sql: str):
    return con.execute(sql).fetchall()


def main() -> None:
    cat, client = catalog(), s3()
    con = duckdb.connect()
    sources, refs = {}, {}
    for s in SOURCES:
        print(f"\n######## {s}", flush=True)
        try:
            for mode in ("inspect", "load", "load", "verify"):
                print(f"--- landing {mode} exit", bulk_file.main(s, mode, landing_root=LANDING), flush=True)
            load = importlib.import_module(f"ingestion.to_bronze.{s}.load")
            for n in (1, 2):
                load.load(0, landing_root=LANDING, namespace=NS)
                t = cat.load_table((NS, f"{s}_cells")).scan().to_arrow()
                print(f"--- bronze load {n}: rows {t.num_rows}", flush=True)
            with contextlib.suppress(Exception):
                rej = cat.load_table((NS, f"{s}_rejected_files")).scan().to_arrow().to_pylist()
                print("--- rejected", [(r["entity"], r["error"][:150]) for r in rej])
            con.register(f"{s}_cells", t)
            per = q(con, f"select entity, count(distinct raw_sha256), count(*), count(distinct sheet_name) from {s}_cells group by 1 order by 1")
            print("--- bronze per entity (files, cells, sheets)", per)
            sources[(s, f"{s}_cells")] = f"{s}_cells"
            name, folder = SILVER[s]
            con.execute(f"create table {name} as {render(f'{folder}/{name}.sql', sources, refs)}")
            refs[name] = name
            print(f"--- silver {name} rows", q(con, f"select count(*) from {name}")[0][0])
            print("--- tests (failing rows)", tests(con, name, folder))
        except Exception:  # noqa: BLE001 - trial: report and go on
            print("CRASH", traceback.format_exc()[-3000:], flush=True)
    show = {
        "public_debt": [
            "select sheet_name, block_title[:60], count(*), count(distinct item_label), min(period_end)::varchar, max(period_end)::varchar from public_debt group by 1, 2 order by 1, 2",
            "select item_label, item_occurrence, unit, period_end::varchar, round(value, 4) from public_debt where sheet_name = 'Dług w relacji do PKB' qualify row_number() over (partition by item_label, item_occurrence order by period_end desc) <= 2 order by 1, 2, 4",
            "select item_label, item_code, period_end::varchar, round(value, 1) from public_debt where sheet_name = 'Dług EDP' and period_end = (select max(period_end) from public_debt) order by source_row",
        ],
        "treasury_debt": [
            "select sheet_name, block_title[:60], unit, count(*), count(distinct item_label), min(period_end)::varchar, max(period_end)::varchar from treasury_debt group by 1, 2, 3 order by 1, 2",
            "select item_label[:50], item_occurrence, round(value, 1) from treasury_debt where sheet_name = 'zadłużenie wg instrumentów' and period_end = (select max(period_end) from treasury_debt where sheet_name = 'zadłużenie wg instrumentów') order by source_row limit 90",
        ],
        "budget_execution": [
            "select budget_year, measure, count(*), min(month_end), max(month_end), count(distinct item_label), count(item_key) from budget_execution group by 1, 2 order by 1, 2",
            "select budget_year, month_end, item_key, value_thousand_pln from budget_execution where item_key in ('revenue', 'expenditure', 'balance') and (month_end = 12 or measure = 'plan' or (budget_year = 2026 and month_end >= 8)) order by 1, 2 nulls first, 3",
            "select budget_year, list(distinct item_key order by item_key) from budget_execution group by 1 order by 1",
        ],
        "krus_benefits": [
            "select period_type, count(*), min(period_label), max(period_label), count(distinct benefit_key), list(distinct benefit_key order by benefit_key) from krus_benefits group by 1",
            "select period_label, benefit_key, measure, value, footnote from krus_benefits where benefit_key in ('all', 'old_age', 'survivors') order by period_start desc, period_type, benefit_key, measure limit 12",
        ],
    }
    for name, sqls in show.items():
        for sql in sqls:
            try:
                print(f"\n=== {name}: {sql[:90]}")
                for row in q(con, sql):
                    print("   ", row)
            except Exception as exc:  # noqa: BLE001
                print("   failed:", str(exc)[:500])
    try:
        con.execute(f"create table g as {render('warehouse/models/gold/poc_build_public_finance.sql', sources, refs)}")
        print("\n=== gold build_public_finance")
        for row in q(con, "select source_system, block_id, frequency, count(*), count(distinct measure_key), min(period_label), max(period_label) from g group by all order by 1, 2, 3"):
            print("   ", row)
        print("   duplicate measure × frequency × date:", q(con, "select count(*) from (select measure_key, frequency, date_key from g group by all having count(*) > 1)"))
        print("   nulls:", q(con, "select count(*) filter (where value is null), count(*) filter (where date_key is null) from g"))
        for row in q(con, "select measure_key, frequency, period_label, round(value, 2) from g where measure_key in ('mf_public_debt:debt_gdp:PC_GDP:GG', 'mf_treasury_debt:debt_instrument:MIO_PLN:TOTAL', 'mf_state_budget:execution_ytd:THS_PLN:balance', 'krus:benefits:PLN:old_age:avg_benefit_pln') qualify row_number() over (partition by measure_key, frequency order by date_key desc) <= 3 order by 1, 2, 3"):
            print("   ", row)
    except Exception:  # noqa: BLE001
        print("GOLD CRASH", traceback.format_exc()[-3000:])


if __name__ == "__main__":
    try:
        main()
    finally:
        if "keep" not in sys.argv:
            print("\ncleanup", cleanup(catalog(), s3()))
