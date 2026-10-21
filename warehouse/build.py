"""silver_gold.build: dbt build of the warehouse SQL project against the R2 Data Catalog.

The run filter becomes dbt's --select (e.g. "silver.nbp" or "nbp_fx_rates+"). An empty
filter (the scheduled run) builds only the models whose inputs or code changed since their
last build (warehouse/changed.py, owner policy #822), incremental models incrementally unless
their code changed; "fqn:*" builds everything (incremental models incrementally). Eurostat and
GUS BDL observations are not dbt models: their loaders write Silver per entity (design #875,
warehouse/entity/). A build whose only failures are the catalog's rate
limit (HTTP 429) is resumed with `dbt retry` after declared waits, with fewer threads.
Target prod writes to the silver/gold namespaces; results (models, tests, failures) are
published per build as an annotation (GitHub keeps the first 10 per step) and in the step
summary (all builds).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import time

import yaml

from ingestion.common.report import ANNOTATION_LIMIT, annotate, step_summary

PROJECT = Path(__file__).resolve().parent
_RETRY = yaml.safe_load((PROJECT / "warehouse.yaml").read_text())
RATE_LIMIT_WAITS = [float(s) for s in _RETRY["rate_limit_retry_waits"]]
RATE_LIMIT_THREADS = int(_RETRY["rate_limit_retry_threads"])
# Owner policy #822: only small tables are rebuilt in full; each full-rebuild model has a row ceiling.
FULL_REBUILD = _RETRY["full_rebuild"]


def main(select: str = "", target: str = "prod", variables: dict | None = None) -> int:
    try:
        return _main(select, target, variables)
    except Exception:
        import traceback

        annotate("error", "warehouse build crashed", {"traceback": traceback.format_exc()[-3000:]})
        raise


def _main(select: str, target: str, variables: dict | None) -> int:
    selectors = [s.strip() for s in select.replace(",", " ").split() if s.strip()]
    if not selectors and target == "prod":
        return changed_only(target, variables)
    return build_once(selectors, target, variables)[0]


def changed_only(target: str, variables: dict | None) -> int:
    """The scheduled build: only models whose inputs or code changed (warehouse/changed.py)."""
    from ingestion.common.r2 import catalog
    from warehouse import changed

    cat = catalog()
    meta = changed.manifest(target)
    plan = changed.plan(cat, meta)
    step_summary(f"## warehouse · changed models\n\n```json\n{json.dumps(plan, indent=2)}\n```\n")
    if not plan["select"]:
        from warehouse import catalog as data_catalog

        # Descriptions, tests and metrics can change without a rebuild; the parse's manifest is current.
        published = data_catalog.publish()
        summary = {"target": target, "changed_only": {"rebuilt": 0, **_why(plan)}, "catalog": published}
        annotate("notice", "warehouse dbt build", summary)
        return 0
    variables = dict(variables or {})
    if plan["full_refresh"]:
        variables["full_refresh_models"] = plan["full_refresh"]
    return build_once(plan["select"], target, variables, changed_plan={"cat": cat, "meta": meta, **plan})[0]


def _why(plan: dict) -> dict:
    """Counts of the rebuild reasons by kind, and of unchanged models (fits any annotation)."""
    kinds: dict[str, int] = {}
    for reason in plan["reasons"].values():
        kind = reason.split(":")[0]
        kinds[kind] = kinds.get(kind, 0) + 1
    return {"unchanged": plan["unchanged"], "why": kinds}


MAX_FAILURES_SHOWN = 8
ANNOTATION_FIT = ANNOTATION_LIMIT - 100  # room for the title


def _short(message: str, head: int = 200, tail: int = 600) -> str:
    """A failure message cut to fit an annotation, keeping its end: errors such as Iceberg
    commit failures put a long catalog URL first and the HTTP status and reason last."""
    return message if len(message) <= head + tail else f"{message[:head]} ... {message[-tail:]}"


def _for_annotation(summary: dict, shown: int = MAX_FAILURES_SHOWN) -> dict:
    """The summary fitted to one annotation: failures go last, at most `shown` of them (fewer
    if escaping makes them too long) with a count of the rest, their messages sharing 2,400
    characters, so the batch and catalog results are never cut off. The step summary keeps
    every failure in full."""
    failures = summary["failures"]
    out = {k: v for k, v in summary.items() if k != "failures"}
    budget = 2400 // max(1, min(len(failures), shown))
    listed = [
        {**f, "message": _short(f["message"], head=min(200, budget // 4), tail=max(1, min(600, budget * 3 // 4)))}
        for f in failures[:shown]
    ]
    # JSON escaping (quotes, newlines, non-ASCII) can still push it over: drop failures from
    # the end until the escaped text fits, counting them in failures_more.
    while True:
        if len(failures) > len(listed):
            out["failures_more"] = len(failures) - len(listed)
        out["failures"] = listed
        if not listed or len(json.dumps(out, default=str, separators=(",", ":"))) <= ANNOTATION_FIT:
            return out
        listed = listed[:-1]


RESULTS = PROJECT / "target" / "run_results.json"


def _dbt(command: list[str]) -> tuple[int, list[dict], str]:
    """Run one dbt command; returns its exit code, the results it wrote and its output tail.
    `dbt retry` reads the previous run_results.json, so only results newer than the start count."""
    started = time.time()
    completed = subprocess.run(command, cwd=PROJECT, env={**os.environ}, text=True, capture_output=True)
    print(completed.stdout[-20000:], completed.stderr[-5000:], sep="\n", flush=True)
    results = []
    if RESULTS.exists() and RESULTS.stat().st_mtime >= started:
        results = json.loads(RESULTS.read_text()).get("results", [])
    return completed.returncode, results, completed.stdout[-1500:]


FAILED = ("error", "fail", "runtime error")


def _rate_limited_only(results: list[dict]) -> bool:
    """True when the build failed and every failure is the catalog's HTTP 429 (rate limit)."""
    failures = [r for r in results if r["status"] in FAILED]
    return bool(failures) and all("HTTP 429" in str(r.get("message")) for r in failures)


def build_once(
    selectors: list[str], target: str, variables: dict | None, changed_plan: dict | None = None
) -> tuple[int, int]:
    """One dbt build; returns (exit code, 0). With a changed-only plan, a successful build
    records its build properties on the models it wrote."""
    started_ms = int(time.time() * 1000)
    command = ["dbt", "build", "--target", target, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
    if selectors:
        command += ["--select", *selectors]
    variables = dict(variables or {})
    if variables:
        command += ["--vars", json.dumps(variables)]
    returncode, results, tail = _dbt(command)
    # `dbt retry` re-runs the failed and skipped nodes of the previous invocation with its
    # arguments and writes results for those nodes only; they replace the earlier ones.
    latest = {r["unique_id"]: r for r in results}
    retries = []
    for wait in RATE_LIMIT_WAITS:
        if returncode == 0 or not _rate_limited_only(list(latest.values())):
            break
        time.sleep(wait)
        retry = ["dbt", "retry", "--target", target, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
        returncode, retried, tail = _dbt([*retry, "--threads", str(RATE_LIMIT_THREADS)])
        retries.append({"after_seconds": wait, "nodes": len(retried), "exit_code": returncode})
        if not retried:
            retries[-1]["output_tail"] = tail[-600:]  # why it wrote no results (job logs are unreadable)
            break
        latest.update({r["unique_id"]: r for r in retried})
    results = list(latest.values())
    summary = {
        "target": target,
        "select": selectors,
        "exit_code": returncode,
        "by_status": {},
        "failures": [],
    }
    if retries:
        summary["rate_limit_retries"] = retries
    for r in results:
        summary["by_status"][r["status"]] = summary["by_status"].get(r["status"], 0) + 1
        if r["status"] in FAILED:
            summary["failures"].append({"node": r["unique_id"], "message": str(r.get("message"))})
    if not results:
        summary["output_tail"] = tail
    if changed_plan is not None:
        from warehouse import changed

        summary["changed_only"] = {
            "rebuilt": len(selectors),
            "full_refresh": changed_plan["full_refresh"],
            **_why(changed_plan),
        }
        summary["changed_only"]["properties"] = changed.record(
            changed_plan["cat"], changed_plan["meta"], results, started_ms
        )
    if target == "prod" and any(r["unique_id"].startswith("model.") and r["status"] == "success" for r in results):
        try:
            summary["row_ceilings"] = row_ceilings(results)
        except Exception as exc:  # the check never hides the build's own result
            summary["row_ceilings"] = {"above_twice": [], "errors": [f"{type(exc).__name__}: {str(exc)[:200]}"]}
        if summary["row_ceilings"]["above_twice"] and returncode == 0:
            returncode = 1
    if returncode == 0 and target == "prod":
        from warehouse import catalog

        summary["catalog"] = catalog.publish()
    level = "notice" if returncode == 0 else "error"
    ceilings = summary.get("row_ceilings") or {}
    if level == "notice" and (ceilings.get("over") or ceilings.get("unlisted") or ceilings.get("errors")):
        level = "warning"
    annotate(level, "warehouse dbt build", _for_annotation(summary))
    step_summary(f"## warehouse · dbt build\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return returncode, 0


def table_rows(table) -> int:
    """Rows of a table's current snapshot: its summary's total-records, or the data files'
    record counts when a writer left the totals out (no data scan; deletes not subtracted)."""
    from pyiceberg.manifest import DataFileContent

    snapshot = table.current_snapshot()
    if snapshot is None:
        return 0
    total = snapshot.summary.get("total-records") if snapshot.summary is not None else None
    if total is not None:
        return int(total)
    return sum(
        entry.data_file.record_count
        for manifest in snapshot.manifests(table.io)
        for entry in manifest.fetch_manifest_entry(table.io)
        if entry.data_file.content == DataFileContent.DATA
    )


def row_ceilings(results: list[dict]) -> dict:
    """Owner policy #822 (#868): the rows of each full-rebuild model this build wrote, against
    its ceiling in warehouse.yaml `full_rebuild`. Above the ceiling: a warning; above twice the
    ceiling: the build fails, so a full rebuild that grew is noticed and made incremental (or
    given an exception). A full-rebuild model missing from the list is reported as unlisted
    (repo_check fails on it too). dbt's manifest of this build gives materializations and tables."""
    from ingestion.common.r2 import catalog
    from warehouse import changed

    meta = json.loads((PROJECT / "target" / "manifest.json").read_text())
    cat = catalog()
    out: dict = {"checked": 0, "over": [], "above_twice": [], "unlisted": [], "errors": []}
    for r in results:
        uid = r["unique_id"]
        node = meta["nodes"].get(uid)
        if not uid.startswith("model.") or r["status"] != "success" or node is None:
            continue
        if node.get("config", {}).get("materialized") == "incremental":
            continue
        name = node["name"]
        if name not in FULL_REBUILD:
            out["unlisted"].append(name)
            continue
        try:
            rows = table_rows(cat.load_table(changed.relation(node)))
        except Exception as exc:  # reported, never hides the build's own result
            out["errors"].append(f"{name}: {type(exc).__name__}: {str(exc)[:150]}")
            continue
        out["checked"] += 1
        ceiling = int(FULL_REBUILD[name]["rows"])
        if rows > ceiling:
            out["over"].append({"model": name, "rows": rows, "ceiling": ceiling})
        if rows > 2 * ceiling:
            out["above_twice"].append(name)
    out["errors"] = out["errors"][:5]
    return out
