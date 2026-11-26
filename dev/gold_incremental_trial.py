"""Dev trial for #3204 PR B: per-block replacement in a Gold fact on the R2 Data Catalog, in zz_dev_gold3204 only.

Usage: python -m dev.gold_incremental_trial

Reads a few small production Gold blocks (gold.build_*; read-only) into a scratch fact with a trailing column
`gold_block`, then replaces blocks the way the incremental strategy of PR B would:

  duckdb_tx    BEGIN; DELETE WHERE gold_block IN (changed) OR gold_block NOT IN (registered); INSERT; COMMIT
               (DuckDB 2.0 on the Iceberg REST catalog; positional deletes)
  duckdb_auto  the same two statements without an explicit transaction (dbt-duckdb runs with disable_transactions)
  pyiceberg    the fallback: one table.overwrite(rows, In(changed) | NotIn(registered)) per change (PyIceberg 0.12)

Each step changes one block (fewer rows), removes another and registers a zero-row block. GO checks per step:
the statements succeed; untouched blocks keep their count and sum(value); the removed block is gone; the changed
block has exactly its new rows; a PyIceberg scan equals the DuckDB result. Recorded: snapshots (commits) added,
position deletes, catalog calls (PyIceberg counted; DuckDB's from its HTTP log when available).
The namespace and its tables are dropped at the end, also on failure. Output: $DEV_OUTPUT_DIR/gold_incremental.json.
"""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import sys
import time

import duckdb

from ingestion.common import r2

NAMESPACE = "zz_dev_gold3204"
CANDIDATES = [  # small production blocks; the first four that exist and are below MAX_ROWS are used
    "build_wgi",
    "build_findex",
    "build_icp",
    "build_zus_psz",
    "build_unhcr",
    "build_mrpips_unemployment",
    "build_nbp",
    "build_eea",
]
MAX_ROWS = 3_000_000
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
    with contextlib.suppress(Exception):  # DuckDB's own HTTP requests, to count its catalog calls
        con.execute("CALL enable_logging('HTTP')")
    return con


def http_calls(con) -> dict | None:
    """DuckDB's logged HTTP requests by method and kind (catalog vs data files); None when not logged."""
    try:
        rows = con.execute("SELECT message FROM duckdb_logs WHERE type = 'HTTP'").fetchall()
    except Exception:
        return None
    out: dict[str, int] = {}
    for (message,) in rows:
        text = str(message)
        method = next((m for m in ("GET", "HEAD", "POST", "PUT", "DELETE") if m in text[:200]), "?")
        kind = "catalog" if "/v1/" in text or "catalog" in text else "files"
        key = f"{kind} {method}"
        out[key] = out.get(key, 0) + 1
    return out


def reset_http(con) -> None:
    with contextlib.suppress(Exception):
        con.execute("CALL truncate_duckdb_logs()")


def per_block(con, table: str) -> dict[str, list]:
    rows = con.execute(
        f"SELECT gold_block, count(*), round(sum(value), 6) FROM {table} GROUP BY 1 ORDER BY 1"
    ).fetchall()
    return {b: [n, s] for b, n, s in rows}


def pyiceberg_blocks(cat, name: str) -> dict[str, list]:
    import pyarrow.compute as pc

    table = cat.load_table((NAMESPACE, name)).scan(selected_fields=("gold_block", "value")).to_arrow()
    out = {}
    for block in sorted(set(table.column("gold_block").to_pylist())):
        part = table.filter(pc.equal(table.column("gold_block"), block))
        total = pc.sum(part.column("value")).as_py()
        out[block] = [part.num_rows, round(total, 6) if total is not None else None]
    return out


KEY = ("gold_block", "measure_key", "geography_key", "breakdown_json", "date_key", "value")


def exact_rows(con, cat, table: str, name: str) -> dict:
    """Every row (KEY columns) read by DuckDB and by a PyIceberg scan, compared exactly as sorted lists."""
    cols = ", ".join(KEY)
    duck = sorted(tuple(r) for r in con.execute(f"SELECT {cols} FROM {table}").fetchall())
    arrow = cat.load_table((NAMESPACE, name)).scan(selected_fields=KEY).to_arrow()
    py = sorted(zip(*(arrow.column(c).to_pylist() for c in KEY), strict=True))
    return {
        "rows": len(duck),
        "equal": duck == py,
        "only_duckdb": len(set(duck) - set(py)),
        "only_py": len(set(py) - set(duck)),
    }


def close(a: dict, b: dict) -> bool:
    """Same blocks, same counts, sums equal to 1e-9 relative (float sums depend on the order of addition)."""
    return a.keys() == b.keys() and all(
        a[k][0] == b[k][0] and abs((a[k][1] or 0) - (b[k][1] or 0)) <= 1e-9 * max(1.0, abs(a[k][1] or 0)) for k in a
    )


def snapshot_info(cat, name: str) -> dict:
    table = cat.load_table((NAMESPACE, name))
    snap = table.current_snapshot()
    summary = dict(snap.summary.additional_properties) if snap and snap.summary else {}
    if snap and snap.summary:
        summary["operation"] = str(snap.summary.operation)
    return {
        "snapshots": len(table.metadata.snapshots),
        "total_records": summary.get("total-records"),
        "total_position_deletes": summary.get("total-position-deletes"),
        "total_delete_files": summary.get("total-delete-files"),
        "operation": summary.get("operation"),
    }


def calls_since(before: dict) -> dict:
    return {k: v - before.get(k, 0) for k, v in r2.CATALOG_CALLS.items() if v - before.get(k, 0)}


def expect(step: dict, before: dict, after: dict, changed: str, removed: str, zero: str, new_rows: list) -> None:
    problems = []
    for block, value in before.items():
        if block in (changed, removed):
            continue
        if block not in after or not close({block: value}, {block: after[block]}):
            problems.append(f"untouched {block}: {value} -> {after.get(block)}")
    if removed in after:
        problems.append(f"removed {removed} still has {after[removed]}")
    if changed not in after or not close({changed: new_rows}, {changed: after[changed]}):
        problems.append(f"changed {changed}: expected {new_rows}, got {after.get(changed)}")
    if zero in after:
        problems.append(f"zero-row block {zero} has rows")
    step["problems"] = step.get("problems", []) + problems


def step_duckdb(con, cat, table: str, name: str, blocks: list[str], explicit: bool) -> dict:
    """Replace blocks[0] with its rows of even date_key order (fewer rows, new sum), remove blocks[1], keep the
    rest, register a zero-row block."""
    changed, removed, keep = blocks[0], blocks[1], blocks[2:]
    zero = "poc_build_zz_zero_rows"
    registered = [changed, *keep, zero]
    before = per_block(con, table)
    # the changed block's new rows, staged locally first (dbt-duckdb builds the new rows in a temp table)
    con.execute(f"CREATE OR REPLACE TEMP TABLE staged AS SELECT * FROM {table} WHERE gold_block = '{changed}'")
    con.execute("DELETE FROM staged WHERE hash(measure_key, geography_key, breakdown_json, date_key) % 2 = 0")
    con.execute("UPDATE staged SET value = value + 1")
    new_rows = list(con.execute("SELECT count(*), round(sum(value), 6) FROM staged").fetchall()[0])
    snaps = snapshot_info(cat, name)["snapshots"]
    calls = dict(r2.CATALOG_CALLS)
    reset_http(con)
    inlist = ", ".join(f"'{b}'" for b in [changed])
    reglist = ", ".join(f"'{b}'" for b in registered)
    step: dict = {"mode": "duckdb_tx" if explicit else "duckdb_auto", "changed": changed, "removed": removed}
    started = time.time()
    try:
        if explicit:
            con.execute("BEGIN TRANSACTION")
        con.execute(f"DELETE FROM {table} WHERE gold_block IN ({inlist}) OR gold_block NOT IN ({reglist})")
        con.execute(f"INSERT INTO {table} SELECT * FROM staged")
        if explicit:
            con.execute("COMMIT")
        step["ok"] = True
    except Exception as exc:
        step["ok"] = False
        step["error"] = f"{type(exc).__name__}: {str(exc)[:700]}"
        with contextlib.suppress(Exception):
            con.execute("ROLLBACK")
        return step
    step["seconds"] = round(time.time() - started, 1)
    step["duckdb_http"] = http_calls(con)
    after = per_block(con, table)
    expect(step, before, after, changed, removed, zero, new_rows)
    scan = pyiceberg_blocks(cat, name)
    if not close(scan, after):
        step["problems"].append(f"pyiceberg scan differs: {scan} vs duckdb {after}")
    step["exact"] = exact_rows(con, cat, table, name)
    if not step["exact"]["equal"]:
        step["problems"].append(f"row sets differ between DuckDB and PyIceberg: {step['exact']}")
    info = snapshot_info(cat, name)
    step.update(
        snapshots_added=info["snapshots"] - snaps,
        snapshot=info,
        pyiceberg_calls=calls_since(calls),
        before=before,
        after=after,
    )
    return step


def to_arrow(relation):
    """A DuckDB relation as a pyarrow Table (the method's name changed in DuckDB 2.0)."""
    for method in ("to_arrow_table", "arrow", "fetch_arrow_table"):
        if hasattr(relation, method):
            result = getattr(relation, method)()
            return result.read_all() if hasattr(result, "read_all") else result
    raise AttributeError("no Arrow export on this DuckDB relation")


def step_pyiceberg(con, cat, table: str, name: str, blocks: list[str]) -> dict:
    """The fallback: one PyIceberg overwrite with a filter on gold_block (one commit)."""
    from pyiceberg.expressions import In, NotIn, Or

    changed, removed, keep = blocks[0], blocks[1], blocks[2:]
    zero = "poc_build_zz_zero_rows"
    registered = [changed, *keep, zero]
    before = per_block(con, table)
    con.execute(f"CREATE OR REPLACE TEMP TABLE staged AS SELECT * FROM {table} WHERE gold_block = '{changed}'")
    con.execute("DELETE FROM staged WHERE hash(measure_key, geography_key, breakdown_json, date_key) % 3 = 0")
    con.execute("UPDATE staged SET value = value * 2")
    new_rows = list(con.execute("SELECT count(*), round(sum(value), 6) FROM staged").fetchall()[0])
    iceberg = cat.load_table((NAMESPACE, name))
    arrow = to_arrow(con.sql("SELECT * FROM staged")).cast(iceberg.schema().as_arrow())
    snaps = len(iceberg.metadata.snapshots)
    calls = dict(r2.CATALOG_CALLS)
    step: dict = {"mode": "pyiceberg", "changed": changed, "removed": removed}
    started = time.time()
    try:
        iceberg.overwrite(arrow, overwrite_filter=Or(In("gold_block", [changed]), NotIn("gold_block", registered)))
        step["ok"] = True
    except Exception as exc:
        step["ok"] = False
        step["error"] = f"{type(exc).__name__}: {str(exc)[:700]}"
        return step
    step["seconds"] = round(time.time() - started, 1)
    con.execute("SELECT 1")  # DuckDB re-reads the table metadata on the next query
    after = per_block(con, table)
    expect(step, before, after, changed, removed, zero, new_rows)
    scan = pyiceberg_blocks(cat, name)
    if not close(scan, after):
        step["problems"].append(f"pyiceberg scan differs: {scan} vs duckdb {after}")
    step["exact"] = exact_rows(con, cat, table, name)
    if not step["exact"]["equal"]:
        step["problems"].append(f"row sets differ between DuckDB and PyIceberg: {step['exact']}")
    info = snapshot_info(cat, name)
    step.update(
        snapshots_added=info["snapshots"] - snaps, snapshot=info, pyiceberg_calls=calls_since(calls), after=after
    )
    return step


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    cat = r2.catalog()
    report: dict = {"namespace": NAMESPACE, "duckdb": duckdb.__version__}
    blocks: list[str] = []
    for name in CANDIDATES:
        try:
            snap = cat.load_table(("gold", name)).current_snapshot()
        except Exception:
            continue
        rows = int(snap.summary.get("total-records") or 0) if snap and snap.summary else 0
        if 0 < rows <= MAX_ROWS:
            blocks.append(name)
            report.setdefault("source_rows", {})[name] = rows
        if len(blocks) == 4:
            break
    if len(blocks) < 4:
        annotate("error", "gold incremental trial: not enough small blocks", report)
        return 1
    models = ["poc_" + b for b in blocks]
    con = connect()
    status = 1
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS r2.{NAMESPACE}")
        union = " UNION ALL ".join(
            f"SELECT *, '{m}' AS gold_block FROM r2.gold.{b}" for b, m in zip(blocks, models, strict=True)
        )
        results = {}
        for name, mode in (("fact_tx", "tx"), ("fact_auto", "auto"), ("fact_py", "py")):
            table = f"r2.{NAMESPACE}.{name}"
            report["results"] = results
            con.execute(f"CREATE TABLE {table} AS {union}")
            start = per_block(con, table)
            steps: list[dict] = []
            results[name] = {"start": start, "steps": steps}
            try:
                if mode == "py":
                    steps.append(step_pyiceberg(con, cat, table, name, models))
                else:
                    # two steps: the second one changes another block on top of the first one's deletes
                    steps.append(step_duckdb(con, cat, table, name, models, explicit=mode == "tx"))
                    if steps[0].get("ok"):
                        second = [models[2], models[0], models[3]]
                        steps.append(step_duckdb(con, cat, table, name, second, mode == "tx"))
            except Exception as exc:
                steps.append({"mode": mode, "ok": False, "error": f"{type(exc).__name__}: {str(exc)[:700]}"})
        problems = [
            f"{name} {s['mode']}: {p}"
            for name, r in results.items()
            for s in r["steps"]
            for p in ([s["error"]] if not s.get("ok") else s.get("problems", []))
        ]
        report["problems"] = problems
        report["go_duckdb_delete"] = all(
            s.get("ok") and not s.get("problems") for n in ("fact_tx", "fact_auto") for s in results[n]["steps"]
        )
        report["go_pyiceberg_fallback"] = all(
            s.get("ok") and not s.get("problems") for s in results["fact_py"]["steps"]
        )
        status = 0 if report["go_duckdb_delete"] or report["go_pyiceberg_fallback"] else 1
    except Exception as exc:
        report["crash"] = f"{type(exc).__name__}: {str(exc)[:800]}"
    finally:
        dropped = []
        for name in ("fact_tx", "fact_auto", "fact_py"):
            try:
                cat.drop_table((NAMESPACE, name), purge_requested=True)
                dropped.append(name)
            except Exception:
                pass
        try:
            cat.drop_namespace(NAMESPACE)
            report["namespace_dropped"] = True
        except Exception as exc:
            report["namespace_dropped"] = f"{type(exc).__name__}: {str(exc)[:200]}"
        report["tables_dropped"] = dropped
        report["catalog_calls_total"] = dict(r2.CATALOG_CALLS)
    (OUT / "gold_incremental.json").write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    head = {k: v for k, v in report.items() if k != "results"}
    annotate("notice" if status == 0 else "error", "gold incremental trial: summary", head)
    for name, r in (report.get("results") or {}).items():  # 3 more annotations
        compact = [{k: v for k, v in s.items() if k not in ("before", "after")} for s in r["steps"]]
        annotate("notice", f"gold incremental trial: {name}", {"start": r["start"], "steps": compact})
    return status


if __name__ == "__main__":
    sys.exit(main())
