"""Dev proof for #3204 PR B2: explicit column types in the per-block incremental models, and the file-growth trigger.

Usage: python -m dev.gold_b2_proof

Part 1 (production, read only, metadata only): the Iceberg schema of every Gold block table (tagged gold_block) and of
the six production per-block tables; columns whose type differs between blocks; the six tables' types against the
casts of this PR; whether their snapshot summaries count data and delete files (changed.live_files).

Part 2 (scratch namespace zz_dev_gold3204b2 only; dropped at the end, also on failure): three small production blocks
are copied read-only; the "odd" one is written with `value` as DECIMAL(38,6) (a block type other than the union's
DOUBLE). Every other registered block is passed as waiting. Steps, each a `dbt run` with gold_schema = scratch:

  full      no changed_blocks: the six created in full with the casts
  control   poc_fact_annual WITHOUT the casts (the PR B select list, rewritten on the runner), changed_blocks = [odd]:
            expected to fail on on_schema_change='fail' (the review's scenario)
  incr1     the six with the casts, changed_blocks = [odd]: must succeed; tables exact against their blocks
  incr2     the same again: files added per incremental run (file growth)

Output: $DEV_OUTPUT_DIR/gold_b2_proof.json and at most 6 annotations.
"""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

import duckdb

from ingestion.common import r2

ROOT = Path(__file__).resolve().parent.parent
NAMESPACE = "zz_dev_gold3204b2"
COPY = ["build_icp", "build_wgi", "build_zus_psz"]  # the first is the odd one (value as DECIMAL)
MODELS = {
    "poc_fact_annual": ("fact_annual", "A"),
    "poc_fact_semiannual": ("fact_semiannual", "S"),
    "poc_fact_quarterly": ("fact_quarterly", "Q"),
    "poc_fact_monthly": ("fact_monthly", "M"),
    "poc_fact_daily": ("fact_daily", "D"),
    "poc_dim_series": ("dim_series", None),
}
ICEBERG = {"varchar": "string", "double": "double", "date": "date", "boolean": "boolean", "bigint": "long"}
CAST = re.compile(r"cast\((.+) as ([a-z]+)\) as (\w+)")
ROW = "gold_block, measure_key, geography_key, date_key, frequency, cast(value as double)"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "."))


def annotate(level: str, title: str, data: dict) -> None:
    from ingestion.common.report import annotate as report

    report(level, title, data)


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    endpoint = r2.req("STORAGE_ENDPOINT_URL").replace("https://", "").strip("/")
    con.execute("CREATE SECRET r2_catalog (TYPE iceberg, TOKEN ?)", [r2.req("STORAGE_LAKEHOUSE_CATALOG_WRITE_TOKEN")])
    con.execute(
        "CREATE SECRET r2_files (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, REGION 'auto', URL_STYLE 'path')",
        [r2.req("STORAGE_WRITE_ACCESS_KEY"), r2.req("STORAGE_WRITE_SECRET_KEY"), endpoint],
    )
    con.execute(
        "ATTACH ? AS r2 (TYPE iceberg, SECRET r2_catalog, ENDPOINT ?)",
        [r2.req("STORAGE_LAKEHOUSE_CATALOG_NAME"), r2.req("STORAGE_LAKEHOUSE_CATALOG_URL")],
    )
    return con


def drop_namespace(cat) -> list[str]:
    dropped = []
    for _ in range(2):  # a table missing from the first listing (PR B proof) is caught by the second
        with contextlib.suppress(Exception):
            for ident in cat.list_tables(NAMESPACE):
                cat.drop_table(ident)
                dropped.append(".".join(ident))
    with contextlib.suppress(Exception):
        cat.drop_namespace(NAMESPACE)
    return dropped


def model_casts(model: str) -> dict[str, str]:
    """column → Iceberg type of the casts in a model's select list."""
    text = (ROOT / f"warehouse/models/gold/{model}.sql").read_text()
    text = text.replace("\n        ", " ").replace("\n    )", ")")
    return {m.group(3): ICEBERG[m.group(2)] for m in CAST.finditer(text)}


def schema(table) -> dict[str, str]:
    return {f.name: str(f.field_type) for f in table.schema().fields}


def production_types(cat, blocks: dict[str, str]) -> dict:
    from warehouse import changed

    per_block, missing = {}, []
    for model, table in sorted(blocks.items()):
        try:
            per_block[model] = schema(cat.load_table(("gold", table)))
        except Exception:
            missing.append(model)
    columns: dict[str, dict[str, list[str]]] = {}
    for model, cols in per_block.items():
        for name, kind in cols.items():
            columns.setdefault(name, {}).setdefault(kind, []).append(model)
    differing = {c: {k: (v if len(v) <= 6 else [*v[:6], f"+{len(v) - 6}"]) for k, v in t.items()}
                 for c, t in columns.items() if len(t) > 1}  # fmt: skip
    column_sets = {}
    for model, cols in per_block.items():
        column_sets.setdefault(",".join(cols), []).append(model)
    facts = {}
    for model, (table, _) in MODELS.items():
        try:
            loaded = cat.load_table(("gold", table))
        except Exception as exc:
            facts[table] = {"error": type(exc).__name__}
            continue
        have, want = schema(loaded), model_casts(model)
        snap = loaded.current_snapshot()
        summary = snap.summary if snap else None
        facts[table] = {
            "type_differs": {c: [have.get(c), t] for c, t in want.items() if have.get(c) != t},
            "files": changed.live_files(summary),
            "summary_counts_files": summary is not None and summary["total-data-files"] is not None,
        }
    return {
        "blocks_read": len(per_block),
        "blocks_missing": missing,
        "columns_differing_type": differing,
        "column_name_sets": len(column_sets),
        "facts": facts,
    }


def dbt(models: list[str], variables: dict, step: str) -> dict:
    from warehouse.build import TOPICS

    command = [
        "dbt", "run", "--target", "prod", "--project-dir", "warehouse", "--profiles-dir", "warehouse",
        "--select", *models, "--vars", json.dumps({"topics": TOPICS, "gold_schema": NAMESPACE, **variables}),
    ]  # fmt: skip
    started = time.time()
    done = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    results = json.loads((ROOT / "warehouse/target/run_results.json").read_text()).get("results", [])
    out = {"exit": done.returncode, "seconds": round(time.time() - started, 1)}
    if done.returncode:
        out["messages"] = {
            r["unique_id"].split(".")[-1]: str(r.get("message"))[:300] for r in results if r["status"] != "success"
        }
    (OUT / f"dbt_{step}.log").write_text(done.stdout + done.stderr)
    return out


def state(cat) -> dict:
    from warehouse import changed

    out = {}
    for table, _ in MODELS.values():
        loaded = cat.load_table((NAMESPACE, table))
        snap = loaded.current_snapshot()
        summary = snap.summary if snap else None
        out[table] = {
            "files": changed.live_files(summary),
            "data_files": int(summary["total-data-files"] or 0) if summary else 0,
            "delete_files": int(summary["total-delete-files"] or 0) if summary else 0,
            "types": schema(loaded),
        }
    return out


def exact(con, blocks: dict[str, str]) -> dict:
    out = {}
    for table, frequency in MODELS.values():
        if frequency is None:
            continue
        expected = " union all ".join(
            f"select '{m}' as gold_block, measure_key, geography_key, date_key, frequency, cast(value as double) "
            f"from r2.{NAMESPACE}.{t} where frequency = '{frequency}'"
            for m, t in blocks.items()
        )
        target = f"r2.{NAMESPACE}.{table}"
        out[table] = list(
            con.execute(
                f"select (select count(*) from {target}), "
                f"(select count(*) from (select {ROW} from {target} except all ({expected}))), "
                f"(select count(*) from (({expected}) except all select {ROW} from {target}))"
            ).fetchone()
        )
    return out


def main() -> int:
    from ingestion.common.r2 import catalog
    from warehouse import changed

    OUT.mkdir(parents=True, exist_ok=True)
    cat = catalog()
    con = connect()
    report: dict = {"namespace": NAMESPACE, "steps": {}}
    problems: list[str] = []
    fact_annual = ROOT / "warehouse/models/gold/poc_fact_annual.sql"
    fact_text = fact_annual.read_text()
    try:
        meta = changed.manifest()
        blocks_all = {
            n["name"]: changed.relation(n)[1]
            for n in meta["nodes"].values()
            if n.get("resource_type") == "model" and changed.BLOCK_TAG in n.get("tags", [])
        }
        report["production"] = production_types(cat, blocks_all)
        for table, info in report["production"]["facts"].items():
            if not info.get("summary_counts_files", True):
                problems.append(f"production {table}: snapshot summary does not count files")

        report["leftover_dropped"] = drop_namespace(cat)
        chosen = {m: t for m, t in blocks_all.items() if t in COPY}
        odd = next(m for m, t in chosen.items() if t == COPY[0])
        con.execute(f"create schema r2.{NAMESPACE}")
        for model, table in chosen.items():
            select = f"select * from r2.gold.{table}"
            if model == odd:
                select = f"select * replace (cast(value as decimal(38, 6)) as value) from r2.gold.{table}"
            con.execute(f"create table r2.{NAMESPACE}.{table} as {select}")
        report["odd_block"] = {"model": odd, "value_type": schema(cat.load_table((NAMESPACE, chosen[odd])))["value"]}
        waiting = sorted(set(blocks_all) - set(chosen))
        base = {"waiting_blocks": waiting}

        def step(name, models, variables, expect_ok=True):
            run = dbt(models, {**base, **variables}, name)
            data = {"dbt": run}
            if (run["exit"] == 0) != expect_ok:
                problems.append(f"{name}: dbt exit {run['exit']}, expected {'success' if expect_ok else 'failure'}")
            if run["exit"] == 0:
                data["exact"] = exact(con, chosen)
                for t, (rows, only_table, only_expected) in data["exact"].items():
                    if only_table or only_expected:
                        problems.append(f"{name}: {t} differs from its blocks ({rows}, {only_table}, {only_expected})")
            data["state"] = state(cat)
            report["steps"][name] = data
            return data

        full = step("full", list(MODELS), {})
        want = {model: model_casts(model) for model in MODELS}
        for model, (table, _) in MODELS.items():
            got = {c: t for c, t in full["state"][table]["types"].items()}
            if got != want[model]:
                problems.append(f"full: {table} types {got} differ from the casts {want[model]}")

        fact_annual.write_text(CAST.sub(r"\1 as \3", fact_text))  # the PR B select list, without the casts
        control = step(
            "control", ["poc_fact_annual"], {"changed_blocks": [odd], "full_refresh_models": []}, expect_ok=False
        )
        report["control_failed_on_schema"] = "schema" in json.dumps(control["dbt"].get("messages", {})).lower()
        fact_annual.write_text(fact_text)

        incr1 = step("incr1", list(MODELS), {"changed_blocks": [odd], "full_refresh_models": []})
        incr2 = step("incr2", list(MODELS), {"changed_blocks": [odd], "full_refresh_models": []})
        report["files_per_step"] = {
            t: [full["state"][t]["files"], incr1["state"][t]["files"], incr2["state"][t]["files"]]
            for t, _ in MODELS.values()
        }
        for t, counts in report["files_per_step"].items():
            if None in counts:
                problems.append(f"{t}: snapshot summary without file counts {counts}")
        report["file_growth_trigger_after_incr2"] = {
            t: changed.file_growth({"files": c[2], "base_files": c[0]}) for t, c in report["files_per_step"].items()
        }
    except Exception as exc:
        import traceback

        problems.append(f"{type(exc).__name__}: {str(exc)[:500]}")
        report["traceback"] = traceback.format_exc()[-2500:]
    finally:
        fact_annual.write_text(fact_text)
        report["dropped"] = len(drop_namespace(cat))
        with contextlib.suppress(Exception):
            cat.load_namespace_properties(NAMESPACE)
            problems.append("namespace still exists after the cleanup")
    report["problems"] = problems
    (OUT / "gold_b2_proof.json").write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    head = {k: v for k, v in report.items() if k not in ("steps", "production", "traceback")}
    if "traceback" in report:
        head["traceback"] = report["traceback"][-1200:]
    annotate("notice" if not problems else "error", "gold B2 proof: summary", head)
    annotate("notice", "gold B2 proof: production types (read only)", report.get("production", {}))
    for name, data in report["steps"].items():
        compact = {
            "dbt": data["dbt"],
            "exact": data.get("exact"),
            "files": {t: [s["data_files"], s["delete_files"]] for t, s in data["state"].items()},
            "legend": "exact: rows, only in table, only in blocks; files: data, delete",
        }
        annotate("notice", f"gold B2 proof: {name}", compact)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
