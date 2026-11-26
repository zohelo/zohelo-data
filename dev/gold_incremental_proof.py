"""Dev proof for #3204 PR B on the final code: the per-block incremental facts and poc_dim_series, built by dbt.

Usage: python -m dev.gold_incremental_proof

Only the scratch namespace zz_dev_gold3204b is written; production is read only (a few small gold.build_* blocks are
copied into the scratch namespace). Every other registered block is passed as waiting (var waiting_blocks), so the
six models read just the copied blocks. Steps, each a `dbt run` of the six models with gold_schema = the scratch
namespace:

  full         no var changed_blocks: the models are created in full (the explicit-filter path)
  incremental  block 1 changed (fewer rows, some values doubled), block 2 emptied (zero rows), block 3 removed from the
               registration (macros/poc_blocks.sql edited on the runner); changed_blocks = [block 1, block 2]
  noop         changed_blocks = [] (the "blocks changed" path with nothing to replace)
  refresh      full_refresh_models = the six models with changed_blocks = [] (the plan's full refresh, swap path)

After each step every table is compared exactly with what its blocks hold now (gold_block, measure_key,
geography_key, date_key, frequency, value per row for the facts; series per block and points for dim_series), a
PyIceberg scan is compared with DuckDB, and the commits (metadata log entries) and snapshots each step added are
counted, with the position deletes and total-records of the snapshot summary. The namespace is dropped at the end,
also on failure. Output: $DEV_OUTPUT_DIR/gold_incremental_proof.json.
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
NAMESPACE = "zz_dev_gold3204b"
CANDIDATES = ["build_wgi", "build_icp", "build_findex", "build_zus_psz", "build_nbp", "build_unhcr", "build_eea"]
MAX_ROWS = 400_000
MODELS = {  # model → (table, frequency or None for dim_series)
    "poc_fact_annual": ("fact_annual", "A"),
    "poc_fact_semiannual": ("fact_semiannual", "S"),
    "poc_fact_quarterly": ("fact_quarterly", "Q"),
    "poc_fact_monthly": ("fact_monthly", "M"),
    "poc_fact_daily": ("fact_daily", "D"),
    "poc_dim_series": ("dim_series", None),
}
ROW = "gold_block, measure_key, geography_key, date_key, frequency, value"
OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "."))


def annotate(level: str, title: str, data: dict) -> None:
    from ingestion.common.report import annotate as report

    report(level, title, data)


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    con.execute("SET preserve_insertion_order = false")
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
    with contextlib.suppress(Exception):
        for ident in cat.list_tables(NAMESPACE):
            cat.drop_table(ident)
            dropped.append(".".join(ident))
    with contextlib.suppress(Exception):
        cat.drop_namespace(NAMESPACE)
    return dropped


def dbt(variables: dict, step: str) -> dict:
    from warehouse.build import TOPICS

    command = [
        "dbt", "run", "--target", "prod", "--project-dir", "warehouse", "--profiles-dir", "warehouse",
        "--select", *MODELS, "--vars", json.dumps({"topics": TOPICS, "gold_schema": NAMESPACE, **variables}),
    ]  # fmt: skip
    started = time.time()
    done = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    results = json.loads((ROOT / "warehouse/target/run_results.json").read_text()).get("results", [])
    status = {r["unique_id"].split(".")[-1]: r["status"] for r in results}
    out = {"exit": done.returncode, "seconds": round(time.time() - started, 1), "status": status}
    if done.returncode:
        out["tail"] = (done.stdout[-2500:] + done.stderr[-500:]).strip()
        out["messages"] = {r["unique_id"].split(".")[-1]: str(r.get("message"))[:400] for r in results}
    (OUT / f"dbt_{step}.log").write_text(done.stdout + done.stderr)
    return out


def table_meta(cat, table: str) -> dict:
    loaded = cat.load_table((NAMESPACE, table))
    snap = loaded.current_snapshot()
    summary = snap.summary if snap and snap.summary else None
    return {
        "metadata_log": len(loaded.metadata.metadata_log),
        "snapshots": len(loaded.metadata.snapshots),
        "records": int(summary["total-records"] or 0) if summary else 0,
        "position_deletes": int(summary["total-position-deletes"] or 0) if summary else 0,
        "uuid": str(loaded.metadata.table_uuid),
    }


def expected_rows(blocks: dict[str, str], frequency: str | None) -> str:
    """The rows a fact must hold now: every registered copied block (model → table), filtered by frequency."""
    parts = [
        f"select '{model}' as gold_block, measure_key, geography_key, date_key, frequency, value "
        f"from r2.{NAMESPACE}.{table} where frequency = '{frequency}'"
        for model, table in blocks.items()
    ]
    return " union all ".join(parts)


def check(con, cat, blocks: dict[str, str]) -> dict:
    """Each table against its blocks, exactly; DuckDB against a PyIceberg scan."""
    out: dict = {}
    for model, (table, frequency) in MODELS.items():
        target = f"r2.{NAMESPACE}.{table}"
        if frequency is not None:
            expected = expected_rows(blocks, frequency)
            only_table, only_expected = con.execute(
                f"select (select count(*) from (select {ROW} from {target} except all ({expected}))), "
                f"(select count(*) from (({expected}) except all select {ROW} from {target}))"
            ).fetchone()
        else:
            expected = " union all ".join(
                f"select '{m}' as gold_block, count(*) as points from r2.{NAMESPACE}.{t}" for m, t in blocks.items()
            )
            # points per block equal the block's rows; series per block equal the distinct series of its rows
            only_table, only_expected = con.execute(
                f"select (select count(*) from (select gold_block, sum(points) from {target} group by 1 "
                f"except all select gold_block, points from ({expected}) where points > 0)), "
                f"(select count(*) from (select gold_block, points from ({expected}) where points > 0 "
                f"except all select gold_block, sum(points) from {target} group by 1))"
            ).fetchone()
        duck = con.execute(f"select count(*) from {target}").fetchone()[0]
        by_block = dict(con.execute(f"select gold_block, count(*) from {target} group by 1 order by 1").fetchall())
        scan = cat.load_table((NAMESPACE, table)).scan(selected_fields=("gold_block",)).to_arrow().num_rows
        out[table] = {
            "rows": duck,
            "pyiceberg_rows": scan,
            "only_table": only_table,
            "only_expected": only_expected,
            "by_block": by_block,
            "ok": only_table == 0 and only_expected == 0 and scan == duck,
        }
    return out


def block_signature(con, table: str, block: str) -> list:
    return list(
        con.execute(
            f"select count(*), sum(hash({ROW})) from r2.{NAMESPACE}.{table} where gold_block = ?", [block]
        ).fetchone()
    )


def main() -> int:
    from ingestion.common.r2 import catalog
    from warehouse import changed

    OUT.mkdir(parents=True, exist_ok=True)
    cat = catalog()
    con = connect()
    report: dict = {"namespace": NAMESPACE, "steps": {}}
    problems: list[str] = []
    registry = ROOT / "warehouse/macros/poc_blocks.sql"
    registry_text = registry.read_text()
    try:
        report["leftover_dropped"] = drop_namespace(cat)
        meta = changed.manifest()
        blocks_all = {
            n["name"]: changed.relation(n)[1]
            for n in meta["nodes"].values()
            if n.get("resource_type") == "model" and changed.BLOCK_TAG in n.get("tags", [])
        }
        legacy = set(re.findall(r"^\s*(\w+):\w+\s*$", registry_text, re.M))
        chosen: dict[str, str] = {}
        freq: dict[str, dict] = {}
        for table in CANDIDATES:
            model = next((m for m, t in blocks_all.items() if t == table), None)
            if model is None:
                continue
            try:
                n = con.execute(f"select count(*) from r2.gold.{table}").fetchone()[0]
            except Exception:
                continue
            if 0 < n <= MAX_ROWS:
                chosen[model] = table
                freq[model] = dict(
                    con.execute(f"select frequency, count(*) from r2.gold.{table} group by 1").fetchall()
                )
            if len(chosen) == 5:
                break
        order = sorted(chosen, key=lambda m: (m not in legacy, m))  # a legacy block first: it is removed in step 2
        removed, emptied, changed_block = order[0], order[1], order[2]
        report["blocks"] = {m: {"table": chosen[m], "rows_by_frequency": freq[m]} for m in order}
        report["roles"] = {"removed": removed, "emptied": emptied, "changed": changed_block, "untouched": order[3:]}
        con.execute(f"create schema r2.{NAMESPACE}")
        for model, table in chosen.items():
            con.execute(f"create table r2.{NAMESPACE}.{table} as select * from r2.gold.{table}")
        waiting = sorted(set(blocks_all) - set(chosen))

        def step(name: str, variables: dict, registered: dict[str, str], expect_commits: dict | None) -> dict:
            before = {t: table_meta(cat, t) for t, _ in MODELS.values()} if name != "full" else {}
            run = dbt({"waiting_blocks": waiting, **variables}, name)
            data: dict = {"dbt": run}
            if run["exit"]:
                problems.append(f"{name}: dbt failed")
                return data
            after = {t: table_meta(cat, t) for t, _ in MODELS.values()}
            data["tables"] = check(con, cat, registered)
            data["catalog"] = {
                t: {
                    "commits": a["metadata_log"] - before[t]["metadata_log"] if t in before else None,
                    "snapshots_added": a["snapshots"] - before[t]["snapshots"] if t in before else a["snapshots"],
                    "records": a["records"],
                    "position_deletes": a["position_deletes"],
                    "same_table": before[t]["uuid"] == a["uuid"] if t in before else None,
                }
                for t, a in after.items()
            }
            for t, d in data["tables"].items():
                if not d["ok"]:
                    problems.append(f"{name}: {t} differs from its blocks {d}")
            for t, c in data["catalog"].items():
                if expect_commits is not None and t in expect_commits and c["commits"] not in expect_commits[t]:
                    problems.append(f"{name}: {t} {c['commits']} commits, expected {expect_commits[t]}")
            return data

        registered = dict(chosen)
        report["steps"]["full"] = step("full", {}, registered, None)
        signatures = {
            t: {b: block_signature(con, t, b) for b in order[3:]} for t, f in MODELS.values() if f is not None
        }

        # Step 2: change one block, empty one, unregister one; replace incrementally.
        con.execute(f"drop table r2.{NAMESPACE}.{chosen[changed_block]}")
        con.execute(
            f"create table r2.{NAMESPACE}.{chosen[changed_block]} as "
            f"select * replace (case when hash(measure_key, geography_key, date_key) % 3 = 0 then value * 2 "
            f"else value end as value) from r2.gold.{chosen[changed_block]} "
            f"where hash(measure_key, geography_key, date_key) % 4 <> 0"
        )
        con.execute(f"drop table r2.{NAMESPACE}.{chosen[emptied]}")
        con.execute(
            f"create table r2.{NAMESPACE}.{chosen[emptied]} as select * from r2.gold.{chosen[emptied]} where false"
        )
        edited = re.sub(rf"^\s*{removed}:\w+\s*\n", "", registry_text, flags=re.M)
        if edited == registry_text:
            problems.append(f"could not unregister {removed}")
        registry.write_text(edited)
        registered = {m: t for m, t in chosen.items() if m != removed}
        # a table that held rows of the changed, emptied or removed block takes one commit; any other takes 0 or 1
        touched = {
            t
            for t, d in report["steps"]["full"].get("tables", {}).items()
            if set(d["by_block"]) & {changed_block, emptied, removed}
        }
        expect = {t: ({1} if t in touched else {0, 1}) for t, _ in MODELS.values()}
        report["steps"]["incremental"] = step(
            "incremental", {"changed_blocks": [changed_block, emptied], "full_refresh_models": []}, registered, expect
        )
        for t, sig in signatures.items():
            now = {b: block_signature(con, t, b) for b in sig}
            if now != sig:
                problems.append(f"incremental: untouched blocks changed in {t}: {sig} -> {now}")
        report["untouched_identical"] = not any("untouched" in p for p in problems)

        report["steps"]["noop"] = step(
            "noop",
            {"changed_blocks": [], "full_refresh_models": []},
            registered,
            {t: {0, 1} for t, _ in MODELS.values()},
        )
        report["steps"]["refresh"] = step(
            "refresh", {"changed_blocks": [], "full_refresh_models": list(MODELS)}, registered, None
        )
        for t, c in report["steps"]["refresh"].get("catalog", {}).items():
            if c["position_deletes"]:
                problems.append(f"refresh: {t} still has {c['position_deletes']} position deletes")
        # the plan's trigger on the incremental step's state
        report["position_delete_trigger"] = {
            t: c["position_deletes"] > changed.POSITION_DELETE_SHARE * c["records"]
            for t, c in report["steps"]["incremental"].get("catalog", {}).items()
        }
    except Exception as exc:
        import traceback

        problems.append(f"{type(exc).__name__}: {str(exc)[:500]}")
        report["traceback"] = traceback.format_exc()[-2500:]
    finally:
        registry.write_text(registry_text)
        report["dropped"] = len(drop_namespace(cat))
        with contextlib.suppress(Exception):
            cat.load_namespace_properties(NAMESPACE)
            problems.append("namespace still exists after the cleanup")
    report["problems"] = problems
    (OUT / "gold_incremental_proof.json").write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    head = {k: v for k, v in report.items() if k not in ("steps", "traceback")}
    if "traceback" in report:
        head["traceback"] = report["traceback"][-1500:]
    annotate("notice" if not problems else "error", "gold incremental proof: summary", head)
    for name, data in report["steps"].items():
        compact = {"dbt": {k: v for k, v in data.get("dbt", {}).items() if k != "status"}}
        compact["tables"] = {
            t: [d["rows"], d["pyiceberg_rows"], d["only_table"], d["only_expected"]]
            for t, d in data.get("tables", {}).items()
        }
        compact["catalog"] = {
            t: [c["commits"], c["snapshots_added"], c["records"], c["position_deletes"], c["same_table"]]
            for t, c in data.get("catalog", {}).items()
        }
        compact["legend"] = "tables: rows, pyiceberg rows, only in table, only expected; catalog: commits, " \
            "snapshots added, total-records, position deletes, same table uuid"  # fmt: skip
        annotate("notice", f"gold incremental proof: {name}", compact)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
