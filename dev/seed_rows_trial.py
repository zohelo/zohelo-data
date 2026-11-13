"""Dev trial (#3000, deleted before merge): the eight code lists built both ways into scratch namespaces of the R2
Data Catalog and compared. New: the dbt models of warehouse/models/gold/seed_rows/ (namespace zz_dev_seed_rows),
with their tests. Old: the same CSV files as dbt seeds, in a temporary copy of the project that puts them back
under seeds/ with the column types main declares (namespace zz_dev_seed_rows_old). Compared per list: Iceberg
schema, row counts and rows only on one side; then each new table against production gold.<list> (which holds
what the last production build loaded). Last, the changed-only plan of this branch against production table
properties: what the first build after the merge would rebuild. Read-only against production; both scratch
namespaces are dropped at the end."""

from __future__ import annotations

from collections import Counter
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

import duckdb
import yaml

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import changed, seed_rows

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "warehouse"
NEW, OLD = "zz_dev_seed_rows", "zz_dev_seed_rows_old"
LISTS = seed_rows.names()
OLD_TYPES = """version: 2
seeds:
  - name: unit
    config: {column_types: {factor: "decimal(38,9)"}}
  - name: unit_map
    config: {column_types: {source_system: varchar, unit_code: varchar, measure_key_pattern: varchar, unit_key: varchar}}
"""
SUMMARY: dict = {}
DETAIL: dict = {}


def dbt(verb: str, project: Path, namespace: str, select: list[str], extra: list[str] | None = None) -> dict:
    topics = yaml.safe_load((PROJECT / "warehouse.yaml").read_text(encoding="utf-8"))["topics"]
    variables = json.dumps({"gold_schema": namespace, "topics": topics})
    env = {**os.environ, "DUCKDB_TEMP_DIRECTORY": "/tmp/duckdb_tmp"}
    base = ["--target", "prod", "--project-dir", str(project), "--profiles-dir", str(project), "--vars", variables]
    command = ["dbt", verb, *base, "--threads", "2", "--select", *select, *(extra or [])]
    run = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True)
    for wait in (60, 180, 300):  # HTTP 429 from the catalog
        if run.returncode == 0 or "429" not in run.stdout:
            break
        time.sleep(wait)
        run = subprocess.run(["dbt", "retry", *base], cwd=ROOT, env=env, capture_output=True, text=True)
    print(run.stdout[-12000:], flush=True)
    try:
        results = json.loads((project / "target" / "run_results.json").read_text())["results"]
    except Exception as exc:  # dbt stopped before writing results
        return {"exit": run.returncode, "error": f"{type(exc).__name__}", "tail": run.stdout[-700:]}
    bad = [[r["unique_id"].split(".")[2][:70], r["status"], (r.get("message") or "")[:200]] for r in results
           if r["status"] not in ("success", "pass")]
    return {"exit": run.returncode, "nodes": len(results), "status": dict(Counter(r["status"] for r in results)),
            "not_passed": bad[:8]}


def old_project() -> Path:
    """A copy of the project with the eight lists as dbt seeds again, as on main."""
    copy = Path(tempfile.mkdtemp(prefix="old_project_")) / "warehouse"
    shutil.copytree(PROJECT, copy, ignore=shutil.ignore_patterns("target", "logs", ".duckdb_tmp"))
    for name in LISTS:
        files = sorted((copy / "seed_rows" / name).glob("*.csv"))
        if [f.name for f in files] != ["_shared.csv"]:
            raise RuntimeError(f"{name}: expected _shared.csv only, found {[f.name for f in files]}")
        shutil.move(files[0], copy / "seeds" / f"{name}.csv")
    shutil.rmtree(copy / "seed_rows")
    shutil.rmtree(copy / "models" / "gold" / "seed_rows")
    (copy / "seeds" / "_old_types.yml").write_text(OLD_TYPES, encoding="utf-8")
    return copy


def load(cat, namespace: str, name: str):
    table = bf.retry(lambda: cat.load_table((namespace, name)), f"{namespace}.{name}")
    schema = [f"{field.name}:{field.field_type}:{'required' if field.required else 'optional'}"
              for field in table.schema().fields]
    return schema, bf.retry(lambda: table.scan().to_arrow(), f"scan {namespace}.{name}")


def compare(cat) -> None:
    con = duckdb.connect()
    same = True
    for name in LISTS:
        entry: dict = {}
        try:
            new_schema, new = load(cat, NEW, name)
            old_schema, old = load(cat, OLD, name)
            con.register("new_t", new)
            con.register("old_t", old)
            only_old = con.sql("select count(*) from (select * from old_t except all select * from new_t)").fetchone()[0]
            only_new = con.sql("select count(*) from (select * from new_t except all select * from old_t)").fetchone()[0]
            entry = {"rows_new": new.num_rows, "rows_old": old.num_rows, "only_old": only_old, "only_new": only_new,
                     "schema_same": new_schema == old_schema, "arrow_same": str(new.schema) == str(old.schema),
                     "csv_rows": len(seed_rows.rows(name))}
            if new_schema != old_schema:
                entry["schema_new"], entry["schema_old"] = new_schema, old_schema
            DETAIL.setdefault("schema", {})[name] = new_schema
            ok = entry["schema_same"] and entry["arrow_same"] and not only_old and not only_new
            ok = ok and new.num_rows == old.num_rows == entry["csv_rows"]
            same = same and ok
            try:  # production: what the last production build loaded from main's seed at that time
                prod_schema, prod = load(cat, "gold", name)
                con.register("prod_t", prod)
                key = next(iter(new.schema.names))
                entry["prod"] = {
                    "rows": prod.num_rows,
                    "schema_same": prod_schema == new_schema,
                    "only_prod": con.sql("select count(*) from (select * from prod_t except all select * from new_t)").fetchone()[0],
                    "only_new": con.sql("select count(*) from (select * from new_t except all select * from prod_t)").fetchone()[0],
                    "only_new_keys": [r[0] for r in con.sql(
                        f"select distinct {key} from (select * from new_t except all select * from prod_t) order by 1 limit 12").fetchall()],
                    "only_prod_keys": [r[0] for r in con.sql(
                        f"select distinct {key} from (select * from prod_t except all select * from new_t) order by 1 limit 12").fetchall()],
                }
                if prod_schema != new_schema:
                    entry["prod"]["schema_prod"] = prod_schema
            except Exception as exc:  # reported
                entry["prod"] = {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}
        except Exception as exc:  # reported, the trial goes on
            entry["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
            same = False
        SUMMARY.setdefault("lists", {})[name] = {k: v for k, v in entry.items() if k != "prod"}
        DETAIL.setdefault("prod", {})[name] = entry.get("prod")
    SUMMARY["old_and_new_identical"] = same


def first_build(cat) -> None:
    """The changed-only plan of this branch against production's table properties."""
    meta = changed.manifest("prod")
    plan = changed.plan(cat, meta)
    graph = changed.models(meta)
    namespace = {m["name"]: m["namespace"] for m in graph.values()}
    reasons = plan["reasons"]
    kinds = Counter(reason.split(":")[0] for reason in reasons.values())
    SUMMARY["first_build"] = {
        "planned_models": len(graph), "to_build": len(plan["select"]), "unchanged": plan["unchanged"],
        "by_reason": dict(kinds), "by_namespace": dict(Counter(namespace[n] for n in plan["select"])),
        "lists": {n: reasons.get(n) for n in LISTS}, "waiting": len(plan["waiting"]), "stalled": plan["stalled"],
        "full_refresh": plan["full_refresh"],
    }
    DETAIL["first_build"] = {
        "code_changed": sorted(n for n, r in reasons.items() if r == "code changed"),
        "no_properties_or_missing": sorted(n for n, r in reasons.items() if r in ("no build properties", "table missing")),
        "input_changed": sorted(n for n, r in reasons.items() if r.startswith("input changed"))[:60],
        "silver_to_build": sorted(n for n in plan["select"] if namespace[n] == "silver"),
        "waiting": plan["waiting"][:40],
    }


def cleanup(cat) -> None:
    out = {}
    for namespace in (NEW, OLD):
        dropped, failed = 0, []
        for _ in range(4):
            try:
                left = list(cat.list_tables(namespace))
            except Exception:  # the namespace was never created
                left = []
            if not left:
                break
            for ident in left:
                for wait in (0, 10, 30, 60):  # HTTP 429 from the catalog
                    time.sleep(wait)
                    try:
                        cat.drop_table(ident)
                        dropped += 1
                        break
                    except Exception as exc:  # retried, then reported
                        error = f"{ident[-1]}: {type(exc).__name__}"
                else:
                    failed.append(error)
        try:
            remaining = [i[-1] for i in cat.list_tables(namespace)]
            if not remaining:
                cat.drop_namespace(namespace)
        except Exception as exc:  # reported
            remaining = [f"{type(exc).__name__}"]
        out[namespace] = {"tables_dropped": dropped, "left": remaining, "failed": failed[:5]}
    SUMMARY["scratch"] = out


def main() -> int:
    cat = bf.retry(catalog, "catalog")
    try:
        select = ["path:models/gold/seed_rows"]
        SUMMARY["dbt_new_run"] = dbt("run", PROJECT, NEW, select)
        SUMMARY["dbt_new_test"] = dbt("test", PROJECT, NEW, select, ["--indirect-selection", "cautious"])
        SUMMARY["dbt_old_seed"] = dbt("seed", old_project(), OLD, LISTS)
        compare(cat)
        first_build(cat)
    except Exception as exc:
        SUMMARY["fatal"] = f"{type(exc).__name__}: {str(exc)[:700]}"
    finally:
        try:
            cleanup(cat)
        except Exception as exc:
            SUMMARY["scratch"] = {"error": f"{type(exc).__name__}: {str(exc)[:300]}"}
        annotate("notice", "p0 summary", {k: v for k, v in SUMMARY.items() if k != "lists"})
        annotate("notice", "p1 lists old vs new", SUMMARY.get("lists"))
        annotate("notice", "p2 new vs production", DETAIL.get("prod"))
        annotate("notice", "p3 first build", DETAIL.get("first_build"))
        annotate("notice", "p4 schema", DETAIL.get("schema"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
